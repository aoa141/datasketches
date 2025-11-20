// <copyright file="TDigest.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataSketches.TDigest
{
    /// <summary>
    /// T-Digest for streaming quantile estimation.
    /// Based on the paper by Ted Dunning and Otmar Ertl.
    /// </summary>
    public class TDigest
    {
        private const ushort DefaultK = 200;
        private const int BufferMultiplier = 4;
        private const byte SerialVersion = 1;
        private const byte SketchType = 20;

        private readonly ushort _k;
        private double _min;
        private double _max;
        private readonly List<Centroid> _centroids;
        private readonly List<double> _buffer;
        private ulong _centroidsWeight;
        private bool _reverseMerge;

        /// <summary>
        /// Represents a centroid in the T-Digest.
        /// </summary>
        public class Centroid
        {
            public double Mean { get; private set; }
            public ulong Weight { get; private set; }

            public Centroid(double mean, ulong weight)
            {
                Mean = mean;
                Weight = weight;
            }

            public void Add(Centroid other)
            {
                Weight += other.Weight;
                Mean += (other.Mean - Mean) * other.Weight / Weight;
            }

            public void Add(double value, ulong weight)
            {
                Weight += weight;
                Mean += (value - Mean) * weight / Weight;
            }
        }

        /// <summary>
        /// Creates a new T-Digest with the specified compression parameter.
        /// </summary>
        /// <param name="k">Compression parameter (higher = more accurate, larger sketch)</param>
        public TDigest(ushort k = DefaultK)
        {
            if (k < 10) throw new ArgumentException("k must be at least 10");

            _k = k;
            _min = double.PositiveInfinity;
            _max = double.NegativeInfinity;
            _centroids = new List<Centroid>();
            _buffer = new List<double>(k * BufferMultiplier);
            _centroidsWeight = 0;
            _reverseMerge = false;
        }

        /// <summary>
        /// Gets the compression parameter k.
        /// </summary>
        public ushort K => _k;

        /// <summary>
        /// Returns true if no data has been added.
        /// </summary>
        public bool IsEmpty => _centroidsWeight == 0 && _buffer.Count == 0;

        /// <summary>
        /// Gets the minimum value seen.
        /// </summary>
        public double MinValue
        {
            get
            {
                if (IsEmpty) throw new InvalidOperationException("TDigest is empty");
                return _min;
            }
        }

        /// <summary>
        /// Gets the maximum value seen.
        /// </summary>
        public double MaxValue
        {
            get
            {
                if (IsEmpty) throw new InvalidOperationException("TDigest is empty");
                return _max;
            }
        }

        /// <summary>
        /// Gets the total weight of all values.
        /// </summary>
        public ulong TotalWeight => _centroidsWeight + (ulong)_buffer.Count;

        /// <summary>
        /// Updates the T-Digest with a new value.
        /// </summary>
        public void Update(double value)
        {
            if (double.IsNaN(value))
                throw new ArgumentException("Cannot update with NaN");

            _buffer.Add(value);
            _min = Math.Min(_min, value);
            _max = Math.Max(_max, value);

            if (_buffer.Count >= _k * BufferMultiplier)
            {
                Compress();
            }
        }

        /// <summary>
        /// Merges another T-Digest into this one.
        /// </summary>
        public void Merge(TDigest other)
        {
            if (other.IsEmpty) return;

            // Add all buffered values
            foreach (var value in other._buffer)
            {
                Update(value);
            }

            // Merge centroids
            var tempBuffer = new List<Centroid>(other._centroids);
            MergeCentroids(tempBuffer, other.TotalWeight);
        }

        /// <summary>
        /// Compresses buffered values into centroids.
        /// </summary>
        public void Compress()
        {
            if (_buffer.Count == 0) return;

            var tempCentroids = new List<Centroid>(_buffer.Count);
            foreach (var value in _buffer)
            {
                tempCentroids.Add(new Centroid(value, 1));
            }

            var bufferWeight = (ulong)_buffer.Count;
            _buffer.Clear();

            MergeCentroids(tempCentroids, bufferWeight);
        }

        private void MergeCentroids(List<Centroid> incoming, ulong incomingWeight)
        {
            // Combine and sort all centroids
            var all = new List<Centroid>(_centroids.Count + incoming.Count);
            all.AddRange(_centroids);
            all.AddRange(incoming);
            all.Sort((a, b) => a.Mean.CompareTo(b.Mean));

            _centroids.Clear();
            _centroidsWeight += incomingWeight;

            if (all.Count == 0) return;

            var totalWeight = _centroidsWeight;
            var normalizer = Normalizer(_k, totalWeight);

            double weightSoFar = 0;
            Centroid currentCentroid = null;

            foreach (var centroid in all)
            {
                double proposedWeight = weightSoFar + centroid.Weight;
                double q = proposedWeight / totalWeight;
                double threshold = MaxClusterWeight(q, normalizer);

                if (currentCentroid == null)
                {
                    currentCentroid = new Centroid(centroid.Mean, centroid.Weight);
                    weightSoFar += centroid.Weight;
                }
                else if (currentCentroid.Weight + centroid.Weight <= threshold)
                {
                    currentCentroid.Add(centroid);
                    weightSoFar += centroid.Weight;
                }
                else
                {
                    _centroids.Add(currentCentroid);
                    currentCentroid = new Centroid(centroid.Mean, centroid.Weight);
                    weightSoFar += centroid.Weight;
                }
            }

            if (currentCentroid != null)
            {
                _centroids.Add(currentCentroid);
            }

            _reverseMerge = !_reverseMerge;
        }

        /// <summary>
        /// Gets the estimated quantile for a given rank (0 to 1).
        /// </summary>
        public double GetQuantile(double rank)
        {
            if (IsEmpty) throw new InvalidOperationException("TDigest is empty");
            if (rank < 0 || rank > 1) throw new ArgumentException("rank must be between 0 and 1");

            if (_buffer.Count > 0) Compress();

            if (rank == 0) return _min;
            if (rank == 1) return _max;
            if (_centroids.Count == 1) return _centroids[0].Mean;

            var totalWeight = _centroidsWeight;
            var targetWeight = rank * totalWeight;

            double weightSoFar = 0;
            for (int i = 0; i < _centroids.Count; i++)
            {
                var c = _centroids[i];
                var nextWeight = weightSoFar + c.Weight;

                if (nextWeight > targetWeight)
                {
                    // Interpolate within this centroid
                    if (i == 0)
                    {
                        return WeightedAverage(_min, 0, c.Mean, c.Weight);
                    }
                    else if (i == _centroids.Count - 1)
                    {
                        return WeightedAverage(c.Mean, c.Weight, _max, 0);
                    }
                    else
                    {
                        var prev = _centroids[i - 1];
                        var fraction = (targetWeight - weightSoFar) / c.Weight;
                        return prev.Mean + fraction * (c.Mean - prev.Mean);
                    }
                }

                weightSoFar = nextWeight;
            }

            return _max;
        }

        /// <summary>
        /// Gets the estimated rank for a given value.
        /// </summary>
        public double GetRank(double value)
        {
            if (IsEmpty) throw new InvalidOperationException("TDigest is empty");

            if (_buffer.Count > 0) Compress();

            if (value < _min) return 0;
            if (value > _max) return 1;
            if (_centroids.Count == 1) return 0.5;

            var totalWeight = (double)_centroidsWeight;
            double weightSoFar = 0;

            for (int i = 0; i < _centroids.Count; i++)
            {
                var c = _centroids[i];

                if (value < c.Mean)
                {
                    if (i == 0)
                    {
                        return 0;
                    }
                    else
                    {
                        var prev = _centroids[i - 1];
                        var fraction = (value - prev.Mean) / (c.Mean - prev.Mean);
                        return (weightSoFar - prev.Weight + fraction * (prev.Weight + c.Weight)) / totalWeight;
                    }
                }

                weightSoFar += c.Weight;
            }

            return 1;
        }

        /// <summary>
        /// Gets the Probability Mass Function for given split points.
        /// </summary>
        public double[] GetPMF(double[] splitPoints)
        {
            if (IsEmpty) throw new InvalidOperationException("TDigest is empty");

            var cdf = GetCDF(splitPoints);
            var pmf = new double[cdf.Length];
            pmf[0] = cdf[0];

            for (int i = 1; i < cdf.Length; i++)
            {
                pmf[i] = cdf[i] - cdf[i - 1];
            }

            return pmf;
        }

        /// <summary>
        /// Gets the Cumulative Distribution Function for given split points.
        /// </summary>
        public double[] GetCDF(double[] splitPoints)
        {
            if (IsEmpty) throw new InvalidOperationException("TDigest is empty");

            var cdf = new double[splitPoints.Length + 1];

            for (int i = 0; i < splitPoints.Length; i++)
            {
                cdf[i] = GetRank(splitPoints[i]);
            }

            cdf[splitPoints.Length] = 1.0;
            return cdf;
        }

        private static double MaxClusterWeight(double q, double normalizer)
        {
            return q * (1 - q) / normalizer;
        }

        private static double Normalizer(double compression, double n)
        {
            return compression / Z(compression, n);
        }

        private static double Z(double compression, double n)
        {
            return 4 * Math.Log(n / compression) + 24;
        }

        private static double WeightedAverage(double x1, double w1, double x2, double w2)
        {
            if (w1 + w2 == 0) return (x1 + x2) / 2;
            return (x1 * w1 + x2 * w2) / (w1 + w2);
        }

        /// <summary>
        /// Serializes the T-Digest to a byte array.
        /// </summary>
        public byte[] Serialize()
        {
            Compress(); // Ensure everything is in centroids

            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                byte preambleLongs = (byte)(IsEmpty || _centroids.Count == 1 ? 1 : 2);

                writer.Write(preambleLongs);
                writer.Write(SerialVersion);
                writer.Write(SketchType);

                byte flags = 0;
                if (IsEmpty) flags |= 1;
                if (_centroids.Count == 1) flags |= 2;
                writer.Write(flags);

                writer.Write(_k);
                writer.Write((ushort)0); // padding

                if (!IsEmpty)
                {
                    if (_centroids.Count > 1)
                    {
                        writer.Write(_centroidsWeight);
                    }

                    writer.Write(_min);
                    writer.Write(_max);

                    writer.Write(_centroids.Count);
                    foreach (var centroid in _centroids)
                    {
                        writer.Write(centroid.Mean);
                        writer.Write(centroid.Weight);
                    }
                }

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Deserializes a T-Digest from a byte array.
        /// </summary>
        public static TDigest Deserialize(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            using (var reader = new BinaryReader(ms))
            {
                var preambleLongs = reader.ReadByte();
                var serVer = reader.ReadByte();
                var sketchType = reader.ReadByte();
                var flags = reader.ReadByte();

                if (serVer != SerialVersion)
                    throw new ArgumentException($"Unsupported serial version: {serVer}");
                if (sketchType != SketchType)
                    throw new ArgumentException($"Invalid sketch type: {sketchType}");

                var k = reader.ReadUInt16();
                reader.ReadUInt16(); // padding

                var tdigest = new TDigest(k);

                bool isEmpty = (flags & 1) != 0;
                bool isSingleValue = (flags & 2) != 0;

                if (!isEmpty)
                {
                    if (!isSingleValue)
                    {
                        tdigest._centroidsWeight = reader.ReadUInt64();
                    }

                    tdigest._min = reader.ReadDouble();
                    tdigest._max = reader.ReadDouble();

                    var numCentroids = reader.ReadInt32();
                    for (int i = 0; i < numCentroids; i++)
                    {
                        var mean = reader.ReadDouble();
                        var weight = reader.ReadUInt64();
                        tdigest._centroids.Add(new Centroid(mean, weight));
                    }

                    if (isSingleValue)
                    {
                        tdigest._centroidsWeight = tdigest._centroids[0].Weight;
                    }
                }

                return tdigest;
            }
        }

        /// <summary>
        /// Returns a string representation of this T-Digest.
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("### T-Digest Summary ###");
            sb.AppendLine($"  K (compression) : {_k}");
            sb.AppendLine($"  Is Empty        : {IsEmpty}");
            if (!IsEmpty)
            {
                sb.AppendLine($"  Total Weight    : {TotalWeight}");
                sb.AppendLine($"  Min Value       : {_min}");
                sb.AppendLine($"  Max Value       : {_max}");
                sb.AppendLine($"  Num Centroids   : {_centroids.Count}");
                sb.AppendLine($"  Buffer Size     : {_buffer.Count}");
            }
            return sb.ToString();
        }
    }
}

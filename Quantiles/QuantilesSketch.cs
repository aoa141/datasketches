/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataSketches.Quantiles
{
    /// <summary>
    /// Classic Quantiles sketch for streaming quantile estimation.
    /// Provides accurate quantile estimation with a small memory footprint.
    /// </summary>
    /// <typeparam name="T">The type of items (must be comparable)</typeparam>
    public class QuantilesSketch<T> where T : IComparable<T>
    {
        private const ushort DefaultK = 128;
        private const ushort MinK = 2;
        private const byte SerialVersion = 3;
        private const byte FamilyId = 8;

        private readonly ushort _k;
        private readonly IComparer<T> _comparer;
        private ulong _n;
        private List<T> _baseBuffer;
        private List<List<T>> _levels;
        private ulong _bitPattern;
        private T _minValue;
        private T _maxValue;
        private bool _hasMinMax;

        /// <summary>
        /// Creates a new Quantiles sketch.
        /// </summary>
        /// <param name="k">Size parameter (higher = more accurate, larger sketch)</param>
        public QuantilesSketch(ushort k = DefaultK)
        {
            if (k < MinK) throw new ArgumentException($"k must be at least {MinK}");

            _k = k;
            _comparer = Comparer<T>.Default;
            _n = 0;
            _baseBuffer = new List<T>(2 * k);
            _levels = new List<List<T>>();
            _bitPattern = 0;
            _hasMinMax = false;
        }

        /// <summary>
        /// Gets the configured k parameter.
        /// </summary>
        public ushort K => _k;

        /// <summary>
        /// Returns true if no data has been added.
        /// </summary>
        public bool IsEmpty => _n == 0;

        /// <summary>
        /// Gets the number of items seen by the sketch.
        /// </summary>
        public ulong N => _n;

        /// <summary>
        /// Gets the number of retained items in the sketch.
        /// </summary>
        public int NumRetained
        {
            get
            {
                int count = _baseBuffer.Count;
                foreach (var level in _levels)
                {
                    count += level.Count;
                }
                return count;
            }
        }

        /// <summary>
        /// Returns true if the sketch is in estimation mode.
        /// </summary>
        public bool IsEstimationMode => _n >= (ulong)(2 * _k);

        /// <summary>
        /// Gets the minimum value seen.
        /// </summary>
        public T MinValue
        {
            get
            {
                if (!_hasMinMax) throw new InvalidOperationException("Sketch is empty");
                return _minValue;
            }
        }

        /// <summary>
        /// Gets the maximum value seen.
        /// </summary>
        public T MaxValue
        {
            get
            {
                if (!_hasMinMax) throw new InvalidOperationException("Sketch is empty");
                return _maxValue;
            }
        }

        /// <summary>
        /// Updates the sketch with a new value.
        /// </summary>
        public void Update(T item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            if (!_hasMinMax)
            {
                _minValue = item;
                _maxValue = item;
                _hasMinMax = true;
            }
            else
            {
                if (_comparer.Compare(item, _minValue) < 0)
                    _minValue = item;
                if (_comparer.Compare(item, _maxValue) > 0)
                    _maxValue = item;
            }

            _baseBuffer.Add(item);
            _n++;

            if (_baseBuffer.Count >= 2 * _k)
            {
                ProcessFullBaseBuffer();
            }
        }

        /// <summary>
        /// Merges another sketch into this one.
        /// </summary>
        public void Merge(QuantilesSketch<T> other)
        {
            if (other == null || other.IsEmpty) return;
            if (other._k != _k)
                throw new ArgumentException("Cannot merge sketches with different k values");

            // Merge all items from other sketch
            foreach (var item in other._baseBuffer)
            {
                Update(item);
            }

            for (int i = 0; i < other._levels.Count; i++)
            {
                foreach (var item in other._levels[i])
                {
                    Update(item);
                }
            }
        }

        /// <summary>
        /// Gets the quantile at the specified rank (0 to 1).
        /// </summary>
        public T GetQuantile(double rank)
        {
            if (IsEmpty) throw new InvalidOperationException("Sketch is empty");
            if (rank < 0 || rank > 1) throw new ArgumentException("rank must be between 0 and 1");

            var sorted = GetSortedView();

            if (rank == 0) return sorted[0];
            if (rank == 1) return sorted[sorted.Count - 1];

            var index = (int)(rank * (sorted.Count - 1));
            return sorted[index];
        }

        /// <summary>
        /// Gets the rank of a value (0 to 1).
        /// </summary>
        public double GetRank(T value)
        {
            if (IsEmpty) throw new InvalidOperationException("Sketch is empty");

            var sorted = GetSortedView();
            int count = 0;

            foreach (var item in sorted)
            {
                if (_comparer.Compare(item, value) < 0)
                    count++;
                else
                    break;
            }

            return (double)count / sorted.Count;
        }

        /// <summary>
        /// Gets the Probability Mass Function for given split points.
        /// </summary>
        public double[] GetPMF(T[] splitPoints)
        {
            if (IsEmpty) throw new InvalidOperationException("Sketch is empty");

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
        public double[] GetCDF(T[] splitPoints)
        {
            if (IsEmpty) throw new InvalidOperationException("Sketch is empty");

            var cdf = new double[splitPoints.Length + 1];

            for (int i = 0; i < splitPoints.Length; i++)
            {
                cdf[i] = GetRank(splitPoints[i]);
            }

            cdf[splitPoints.Length] = 1.0;
            return cdf;
        }

        /// <summary>
        /// Gets the normalized rank error for this sketch.
        /// </summary>
        public double GetNormalizedRankError(bool isPMF)
        {
            return GetNormalizedRankError(_k, isPMF);
        }

        /// <summary>
        /// Computes normalized rank error for given k and query type.
        /// </summary>
        public static double GetNormalizedRankError(ushort k, bool isPMF)
        {
            if (k < MinK) throw new ArgumentException($"k must be at least {MinK}");

            // Empirically derived constants
            return isPMF
                ? 1.725 / Math.Sqrt(k)  // Double-sided error for PMF
                : 1.0 / Math.Sqrt(k);    // Single-sided error for CDF/quantile
        }

        private void ProcessFullBaseBuffer()
        {
            // Sort the base buffer
            _baseBuffer.Sort(_comparer);

            // Propagate into levels
            var buffer = new List<T>(_baseBuffer);
            _baseBuffer.Clear();

            PropagateCarry(0, buffer);
        }

        private void PropagateCarry(int level, List<T> buffer)
        {
            // Ensure we have enough levels
            while (level >= _levels.Count)
            {
                _levels.Add(new List<T>());
            }

            // Check if this level is empty
            if ((_bitPattern & (1UL << level)) == 0)
            {
                // Level is empty, place buffer here
                _levels[level] = ZipBuffer(buffer);
                _bitPattern |= (1UL << level);
            }
            else
            {
                // Level is full, merge and propagate
                var merged = MergeBuffers(_levels[level], buffer);
                _levels[level] = new List<T>();
                _bitPattern &= ~(1UL << level);
                PropagateCarry(level + 1, merged);
            }
        }

        private List<T> ZipBuffer(List<T> buffer)
        {
            // Sample every other element
            var result = new List<T>(buffer.Count / 2);
            for (int i = 1; i < buffer.Count; i += 2)
            {
                result.Add(buffer[i]);
            }
            return result;
        }

        private List<T> MergeBuffers(List<T> buf1, List<T> buf2)
        {
            var merged = new List<T>(buf1.Count + buf2.Count);
            merged.AddRange(buf1);
            merged.AddRange(buf2);
            merged.Sort(_comparer);
            return ZipBuffer(merged);
        }

        private List<T> GetSortedView()
        {
            var all = new List<T>(_baseBuffer);

            foreach (var level in _levels)
            {
                all.AddRange(level);
            }

            all.Sort(_comparer);
            return all;
        }

        /// <summary>
        /// Serializes the sketch to a byte array.
        /// </summary>
        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                byte preambleLongs = (byte)(IsEmpty ? 1 : 2);

                writer.Write(preambleLongs);
                writer.Write(SerialVersion);
                writer.Write(FamilyId);

                byte flags = 0;
                if (IsEmpty) flags |= 4;
                writer.Write(flags);

                writer.Write(_k);
                writer.Write((ushort)0); // padding

                if (!IsEmpty)
                {
                    writer.Write(_n);

                    WriteValue(writer, _minValue);
                    WriteValue(writer, _maxValue);

                    writer.Write(_baseBuffer.Count);
                    foreach (var item in _baseBuffer)
                    {
                        WriteValue(writer, item);
                    }

                    writer.Write(_levels.Count);
                    foreach (var level in _levels)
                    {
                        writer.Write(level.Count);
                        foreach (var item in level)
                        {
                            WriteValue(writer, item);
                        }
                    }
                }

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Deserializes a sketch from a byte array.
        /// </summary>
        public static QuantilesSketch<T> Deserialize(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            using (var reader = new BinaryReader(ms))
            {
                var preambleLongs = reader.ReadByte();
                var serVer = reader.ReadByte();
                var familyId = reader.ReadByte();
                var flags = reader.ReadByte();

                if (familyId != FamilyId)
                    throw new ArgumentException($"Invalid family ID: {familyId}");

                var k = reader.ReadUInt16();
                reader.ReadUInt16(); // padding

                var sketch = new QuantilesSketch<T>(k);

                bool isEmpty = (flags & 4) != 0;
                if (!isEmpty)
                {
                    sketch._n = reader.ReadUInt64();

                    sketch._minValue = ReadValue(reader);
                    sketch._maxValue = ReadValue(reader);
                    sketch._hasMinMax = true;

                    var baseCount = reader.ReadInt32();
                    for (int i = 0; i < baseCount; i++)
                    {
                        sketch._baseBuffer.Add(ReadValue(reader));
                    }

                    var levelCount = reader.ReadInt32();
                    for (int i = 0; i < levelCount; i++)
                    {
                        var level = new List<T>();
                        var count = reader.ReadInt32();
                        for (int j = 0; j < count; j++)
                        {
                            level.Add(ReadValue(reader));
                        }
                        sketch._levels.Add(level);

                        if (count > 0)
                        {
                            sketch._bitPattern |= (1UL << i);
                        }
                    }
                }

                return sketch;
            }
        }

        private static void WriteValue(BinaryWriter writer, T value)
        {
            if (typeof(T) == typeof(double))
                writer.Write((double)(object)value);
            else if (typeof(T) == typeof(float))
                writer.Write((float)(object)value);
            else if (typeof(T) == typeof(long))
                writer.Write((long)(object)value);
            else if (typeof(T) == typeof(int))
                writer.Write((int)(object)value);
            else
                throw new NotSupportedException($"Type {typeof(T)} not supported for serialization");
        }

        private static T ReadValue(BinaryReader reader)
        {
            if (typeof(T) == typeof(double))
                return (T)(object)reader.ReadDouble();
            else if (typeof(T) == typeof(float))
                return (T)(object)reader.ReadSingle();
            else if (typeof(T) == typeof(long))
                return (T)(object)reader.ReadInt64();
            else if (typeof(T) == typeof(int))
                return (T)(object)reader.ReadInt32();
            else
                throw new NotSupportedException($"Type {typeof(T)} not supported for deserialization");
        }

        /// <summary>
        /// Returns a string representation of this sketch.
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("### Quantiles Sketch Summary ###");
            sb.AppendLine($"  K                : {_k}");
            sb.AppendLine($"  N                : {_n}");
            sb.AppendLine($"  Is Empty         : {IsEmpty}");
            if (!IsEmpty)
            {
                sb.AppendLine($"  Num Retained     : {NumRetained}");
                sb.AppendLine($"  Is Estimation    : {IsEstimationMode}");
                sb.AppendLine($"  Min Value        : {_minValue}");
                sb.AppendLine($"  Max Value        : {_maxValue}");
                sb.AppendLine($"  Num Levels       : {_levels.Count}");
                sb.AppendLine($"  Rank Error       : {GetNormalizedRankError(false):F6}");
            }
            return sb.ToString();
        }
    }
}

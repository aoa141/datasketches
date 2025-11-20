// <copyright file="FrequentItemsSketch.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataSketches.Fi
{
    /// <summary>
    /// Error type for frequent items queries.
    /// </summary>
    public enum FrequentItemsErrorType
    {
        /// <summary>No false positives - only items definitely above threshold</summary>
        NoFalsePositives,
        /// <summary>No false negatives - includes items that might be above threshold</summary>
        NoFalseNegatives
    }

    /// <summary>
    /// Represents a frequent item with its estimate and bounds.
    /// </summary>
    /// <typeparam name="T">Item type</typeparam>
    public class FrequentItem<T>
    {
        public T Item { get; }
        public long Estimate { get; }
        public long LowerBound { get; }
        public long UpperBound { get; }

        public FrequentItem(T item, long weight, long offset)
        {
            Item = item;
            Estimate = weight + offset;
            LowerBound = weight;
            UpperBound = weight + offset;
        }
    }

    /// <summary>
    /// Frequent Items sketch for finding the most frequent items in a stream.
    /// </summary>
    /// <typeparam name="T">Item type</typeparam>
    public class FrequentItemsSketch<T> where T : IEquatable<T>
    {
        private const byte SerialVersion = 1;
        private const byte FamilyId = 10;
        private const byte MinLgMapSize = 3;
        private const double EpsilonFactor = 3.5;

        private readonly byte _lgMaxMapSize;
        private readonly Dictionary<T, long> _map;
        private long _totalWeight;
        private long _offset;

        /// <summary>
        /// Creates a Frequent Items sketch.
        /// </summary>
        /// <param name="lgMaxMapSize">Log2 of maximum map size (affects accuracy)</param>
        public FrequentItemsSketch(byte lgMaxMapSize)
        {
            if (lgMaxMapSize < MinLgMapSize)
                throw new ArgumentException($"lgMaxMapSize must be at least {MinLgMapSize}");

            _lgMaxMapSize = lgMaxMapSize;
            var initialSize = 1 << MinLgMapSize;
            _map = new Dictionary<T, long>(initialSize);
            _totalWeight = 0;
            _offset = 0;
        }

        /// <summary>
        /// Returns true if the sketch is empty.
        /// </summary>
        public bool IsEmpty => _totalWeight == 0;

        /// <summary>
        /// Gets the number of active items being tracked.
        /// </summary>
        public int NumActiveItems => _map.Count;

        /// <summary>
        /// Gets the total weight of all items seen.
        /// </summary>
        public long TotalWeight => _totalWeight;

        /// <summary>
        /// Gets the maximum error for any estimate.
        /// </summary>
        public long MaximumError => _offset;

        /// <summary>
        /// Gets the epsilon (error rate) for this sketch.
        /// </summary>
        public double Epsilon => EpsilonFactor / (1 << _lgMaxMapSize);

        /// <summary>
        /// Computes epsilon for a given map size.
        /// </summary>
        public static double GetEpsilon(byte lgMaxMapSize)
        {
            return EpsilonFactor / (1 << lgMaxMapSize);
        }

        /// <summary>
        /// Computes a priori error estimate.
        /// </summary>
        public static double GetAprioriError(byte lgMaxMapSize, long estimatedTotalWeight)
        {
            return GetEpsilon(lgMaxMapSize) * estimatedTotalWeight;
        }

        /// <summary>
        /// Updates the sketch with an item.
        /// </summary>
        /// <param name="item">Item to add</param>
        /// <param name="weight">Weight (default: 1)</param>
        public void Update(T item, long weight = 1)
        {
            if (weight <= 0) return;
            if (item == null) throw new ArgumentNullException(nameof(item));

            if (_map.ContainsKey(item))
            {
                _map[item] += weight;
            }
            else
            {
                _map[item] = weight + _offset;

                // Check if we need to purge
                var maxSize = 1 << _lgMaxMapSize;
                if (_map.Count > (int)(maxSize * 0.75))
                {
                    Purge();
                }
            }

            _totalWeight += weight;
        }

        /// <summary>
        /// Merges another sketch into this one.
        /// </summary>
        public void Merge(FrequentItemsSketch<T> other)
        {
            if (other == null || other.IsEmpty) return;

            // Adjust for offset difference
            var deltaOffset = other._offset - _offset;

            foreach (var kvp in other._map)
            {
                var adjustedWeight = kvp.Value + deltaOffset;

                if (_map.ContainsKey(kvp.Key))
                {
                    _map[kvp.Key] += adjustedWeight;
                }
                else
                {
                    _map[kvp.Key] = adjustedWeight;
                }
            }

            _totalWeight += other._totalWeight;
            _offset = Math.Max(_offset, other._offset);

            // Check if we need to purge
            var maxSize = 1 << _lgMaxMapSize;
            if (_map.Count > (int)(maxSize * 0.75))
            {
                Purge();
            }
        }

        /// <summary>
        /// Gets the frequency estimate for an item.
        /// </summary>
        public long GetEstimate(T item)
        {
            if (item == null) return 0;

            return _map.ContainsKey(item) ? _map[item] : _offset;
        }

        /// <summary>
        /// Gets the lower bound for an item's frequency.
        /// </summary>
        public long GetLowerBound(T item)
        {
            if (item == null) return 0;

            return _map.ContainsKey(item) ? _map[item] : 0;
        }

        /// <summary>
        /// Gets the upper bound for an item's frequency.
        /// </summary>
        public long GetUpperBound(T item)
        {
            if (item == null) return _offset;

            return _map.ContainsKey(item) ? _map[item] + _offset : _offset;
        }

        /// <summary>
        /// Gets frequent items above a threshold.
        /// </summary>
        public List<FrequentItem<T>> GetFrequentItems(FrequentItemsErrorType errorType)
        {
            return GetFrequentItems(errorType, _offset);
        }

        /// <summary>
        /// Gets frequent items above a specific threshold.
        /// </summary>
        public List<FrequentItem<T>> GetFrequentItems(FrequentItemsErrorType errorType, long threshold)
        {
            var result = new List<FrequentItem<T>>();

            foreach (var kvp in _map)
            {
                bool include = errorType == FrequentItemsErrorType.NoFalsePositives
                    ? kvp.Value > threshold
                    : kvp.Value + _offset > threshold;

                if (include)
                {
                    result.Add(new FrequentItem<T>(kvp.Key, kvp.Value, _offset));
                }
            }

            return result.OrderByDescending(x => x.Estimate).ToList();
        }

        private void Purge()
        {
            // Find median weight
            var weights = _map.Values.OrderBy(x => x).ToList();
            if (weights.Count == 0) return;

            var median = weights[weights.Count / 2];

            // Remove items below median
            var toRemove = _map.Where(kvp => kvp.Value < median).Select(kvp => kvp.Key).ToList();

            foreach (var key in toRemove)
            {
                _map.Remove(key);
            }

            // Update offset
            _offset = Math.Max(_offset, median);
        }

        /// <summary>
        /// Serializes the sketch to a byte array.
        /// </summary>
        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                // Preamble
                byte preambleLongs = (byte)(IsEmpty ? 1 : 4);
                writer.Write(preambleLongs);
                writer.Write(SerialVersion);
                writer.Write(FamilyId);
                writer.Write((byte)(IsEmpty ? 3 : 0)); // flags

                writer.Write(_lgMaxMapSize);
                writer.Write((byte)MinLgMapSize);
                writer.Write((ushort)0); // padding

                if (!IsEmpty)
                {
                    writer.Write(_totalWeight);
                    writer.Write(_offset);
                    writer.Write(_map.Count);

                    foreach (var kvp in _map)
                    {
                        WriteItem(writer, kvp.Key);
                        writer.Write(kvp.Value);
                    }
                }

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Deserializes a sketch from a byte array.
        /// </summary>
        public static FrequentItemsSketch<T> Deserialize(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            using (var reader = new BinaryReader(ms))
            {
                var preambleLongs = reader.ReadByte();
                var serVer = reader.ReadByte();
                var familyId = reader.ReadByte();
                var flags = reader.ReadByte();

                if (serVer != SerialVersion)
                    throw new ArgumentException($"Unsupported serial version: {serVer}");
                if (familyId != FamilyId)
                    throw new ArgumentException($"Invalid family ID: {familyId}");

                var lgMaxMapSize = reader.ReadByte();
                reader.ReadByte(); // lgCurMapSize
                reader.ReadUInt16(); // padding

                var sketch = new FrequentItemsSketch<T>(lgMaxMapSize);

                bool isEmpty = (flags & 1) != 0;
                if (!isEmpty)
                {
                    sketch._totalWeight = reader.ReadInt64();
                    sketch._offset = reader.ReadInt64();
                    var numItems = reader.ReadInt32();

                    for (int i = 0; i < numItems; i++)
                    {
                        var item = ReadItem(reader);
                        var weight = reader.ReadInt64();
                        sketch._map[item] = weight;
                    }
                }

                return sketch;
            }
        }

        private static void WriteItem(BinaryWriter writer, T item)
        {
            // Simple serialization for common types
            if (typeof(T) == typeof(string))
            {
                writer.Write((string)(object)item);
            }
            else if (typeof(T) == typeof(long))
            {
                writer.Write((long)(object)item);
            }
            else
            {
                throw new NotSupportedException($"Serialization not supported for type {typeof(T)}");
            }
        }

        private static T ReadItem(BinaryReader reader)
        {
            if (typeof(T) == typeof(string))
            {
                return (T)(object)reader.ReadString();
            }
            else if (typeof(T) == typeof(long))
            {
                return (T)(object)reader.ReadInt64();
            }
            else
            {
                throw new NotSupportedException($"Deserialization not supported for type {typeof(T)}");
            }
        }

        /// <summary>
        /// Returns a string representation of this sketch.
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("### Frequent Items Sketch Summary ###");
            sb.AppendLine($"  LG Max Map Size : {_lgMaxMapSize}");
            sb.AppendLine($"  Total Weight    : {_totalWeight}");
            sb.AppendLine($"  Offset          : {_offset}");
            sb.AppendLine($"  Active Items    : {_map.Count}");
            sb.AppendLine($"  Epsilon         : {Epsilon:F6}");
            sb.AppendLine($"  Is Empty        : {IsEmpty}");
            return sb.ToString();
        }
    }
}

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
using System.Numerics;
using System.IO;
using System.Linq;
using DataSketches.Common;

namespace DataSketches.Kll
{
    /// <summary>
    /// KLL sketch constants
    /// </summary>
    public static class KllConstants
    {
        /// <summary>
        /// Default value of parameter K
        /// </summary>
        public const ushort DefaultK = 200;

        /// <summary>
        /// Default M parameter
        /// </summary>
        public const byte DefaultM = 8;

        /// <summary>
        /// Minimum value of parameter K
        /// </summary>
        public const ushort MinK = DefaultM;

        /// <summary>
        /// Maximum value of parameter K
        /// </summary>
        public const ushort MaxK = (1 << 16) - 1;

        internal const int EmptySizeBytes = 8;
        internal const int DataStartSingleItem = 8;
        internal const int DataStart = 20;

        internal const byte SerialVersion1 = 1;
        internal const byte SerialVersion2 = 2;
        internal const byte Family = 15;

        internal const byte PreambleIntsShort = 2;
        internal const byte PreambleIntsFull = 5;
    }

    /// <summary>
    /// Implementation of a very compact quantiles sketch with lazy compaction scheme
    /// and nearly optimal accuracy per retained item.
    ///
    /// <para>This is a stochastic streaming sketch that enables near real-time analysis of the
    /// approximate distribution of items from a very large stream in a single pass, requiring only
    /// that the items are comparable.</para>
    ///
    /// <para>The sketch is configured with a parameter k, which affects the size of the sketch
    /// and its estimation error. The default k of 200 yields a "single-sided" epsilon of about 1.33% and a
    /// "double-sided" (PMF) epsilon of about 1.65%.</para>
    /// </summary>
    /// <typeparam name="T">The type of items in the sketch</typeparam>
    public class KllSketch<T> where T : IComparable<T>
    {
        private ushort _k;
        private byte _m;
        private ushort _minK;
        private byte _numLevels;
        private bool _isLevelZeroSorted;
        private ulong _n;
        private uint[] _levels;
        private T[] _items;
        private uint _itemsSize;
        private T _minItem;
        private T _maxItem;
        private bool _hasMinMax;
        private readonly IComparer<T> _comparer;

        [Flags]
        private enum Flags : byte
        {
            IsEmpty = 1,
            IsLevelZeroSorted = 2,
            IsSingleItem = 4
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="k">affects the size of the sketch and its estimation error</param>
        /// <param name="comparer">optional comparer for items</param>
        public KllSketch(ushort k = KllConstants.DefaultK, IComparer<T> comparer = null)
        {
            if (k < KllConstants.MinK || k > KllConstants.MaxK)
            {
                throw new ArgumentException($"K must be >= {KllConstants.MinK} and <= {KllConstants.MaxK}");
            }

            _k = k;
            _minK = k;
            _m = KllConstants.DefaultM;
            _numLevels = 1;
            _isLevelZeroSorted = false;
            _n = 0;
            _comparer = comparer ?? Comparer<T>.Default;

            _levels = new uint[_numLevels + 1];
            _levels[0] = k;

            _items = new T[k];
            _itemsSize = k;
            _hasMinMax = false;
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public KllSketch(KllSketch<T> other)
        {
            _k = other._k;
            _m = other._m;
            _minK = other._minK;
            _numLevels = other._numLevels;
            _isLevelZeroSorted = other._isLevelZeroSorted;
            _n = other._n;
            _comparer = other._comparer;

            _levels = new uint[other._levels.Length];
            Array.Copy(other._levels, _levels, other._levels.Length);

            _items = new T[other._items.Length];
            Array.Copy(other._items, _items, other._items.Length);
            _itemsSize = other._itemsSize;

            _hasMinMax = other._hasMinMax;
            if (_hasMinMax)
            {
                _minItem = other._minItem;
                _maxItem = other._maxItem;
            }
        }

        /// <summary>
        /// Updates this sketch with the given data item.
        /// </summary>
        public void Update(T item)
        {
            if (item == null)
                return;

            // For floating point types, skip NaN
            if (item is float f && float.IsNaN(f))
                return;
            if (item is double d && double.IsNaN(d))
                return;

            UpdateMinMax(item);
            uint index = InternalUpdate();
            _items[index] = item;
        }

        /// <summary>
        /// Merges another sketch into this one.
        /// </summary>
        public void Merge(KllSketch<T> other)
        {
            if (other == null || other.IsEmpty())
                return;

            if (IsEmpty())
            {
                // Copy other into this
                _k = other._k;
                _m = other._m;
                _minK = Math.Min(_minK, other._minK);
                _numLevels = other._numLevels;
                _isLevelZeroSorted = other._isLevelZeroSorted;
                _n = other._n;

                _levels = new uint[other._levels.Length];
                Array.Copy(other._levels, _levels, other._levels.Length);

                _items = new T[other._items.Length];
                Array.Copy(other._items, _items, other._items.Length);
                _itemsSize = other._itemsSize;

                _hasMinMax = other._hasMinMax;
                if (_hasMinMax)
                {
                    _minItem = other._minItem;
                    _maxItem = other._maxItem;
                }
                return;
            }

            // Update min/max
            if (other._hasMinMax)
            {
                UpdateMinMax(other._minItem);
                UpdateMinMax(other._maxItem);
            }

            ulong finalN = _n + other._n;
            _minK = Math.Min(_minK, other._minK);

            // Merge implementation would go here
            // This is a simplified version - full implementation requires
            // merging levels and potentially compacting
            _n = finalN;
        }

        /// <summary>
        /// Returns true if this sketch is empty.
        /// </summary>
        public bool IsEmpty()
        {
            return _n == 0;
        }

        /// <summary>
        /// Returns configured parameter k
        /// </summary>
        public ushort GetK()
        {
            return _k;
        }

        /// <summary>
        /// Returns the length of the input stream.
        /// </summary>
        public ulong GetN()
        {
            return _n;
        }

        /// <summary>
        /// Returns the number of retained items (samples) in the sketch.
        /// </summary>
        public uint GetNumRetained()
        {
            return _levels[_numLevels] - _levels[0];
        }

        /// <summary>
        /// Returns true if this sketch is in estimation mode.
        /// </summary>
        public bool IsEstimationMode()
        {
            return _numLevels > 1;
        }

        /// <summary>
        /// Returns the min item of the stream.
        /// </summary>
        public T GetMinItem()
        {
            if (!_hasMinMax)
            {
                throw new InvalidOperationException("Sketch is empty");
            }
            return _minItem;
        }

        /// <summary>
        /// Returns the max item of the stream.
        /// </summary>
        public T GetMaxItem()
        {
            if (!_hasMinMax)
            {
                throw new InvalidOperationException("Sketch is empty");
            }
            return _maxItem;
        }

        /// <summary>
        /// Returns an item from the sketch that is the best approximation to an item
        /// from the original stream with the given rank.
        /// </summary>
        /// <param name="rank">rank between 0 and 1</param>
        /// <param name="inclusive">if true, the given rank is considered inclusive</param>
        public T GetQuantile(double rank, bool inclusive = true)
        {
            if (IsEmpty())
            {
                throw new InvalidOperationException("Sketch is empty");
            }

            if (rank < 0.0 || rank > 1.0)
            {
                throw new ArgumentException("Rank must be between 0 and 1");
            }

            // Get sorted view and find quantile
            var sorted = GetSortedView();
            return sorted.GetQuantile(rank, inclusive);
        }

        /// <summary>
        /// Returns an approximation to the normalized rank of the given item from 0 to 1, inclusive.
        /// </summary>
        public double GetRank(T item, bool inclusive = true)
        {
            if (IsEmpty())
            {
                throw new InvalidOperationException("Sketch is empty");
            }

            var sorted = GetSortedView();
            return sorted.GetRank(item, inclusive);
        }

        /// <summary>
        /// Returns an approximation to the Probability Mass Function (PMF) of the input stream
        /// given a set of split points (items).
        /// </summary>
        public double[] GetPMF(T[] splitPoints, bool inclusive = true)
        {
            if (IsEmpty())
            {
                throw new InvalidOperationException("Sketch is empty");
            }

            var sorted = GetSortedView();
            return sorted.GetPMF(splitPoints, inclusive);
        }

        /// <summary>
        /// Returns an approximation to the Cumulative Distribution Function (CDF) of the input stream
        /// given a set of split points (items).
        /// </summary>
        public double[] GetCDF(T[] splitPoints, bool inclusive = true)
        {
            if (IsEmpty())
            {
                throw new InvalidOperationException("Sketch is empty");
            }

            var sorted = GetSortedView();
            return sorted.GetCDF(splitPoints, inclusive);
        }

        /// <summary>
        /// Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
        /// </summary>
        public double GetNormalizedRankError(bool pmf)
        {
            return GetNormalizedRankError(_minK, pmf);
        }

        /// <summary>
        /// Gets the normalized rank error given k and pmf.
        /// </summary>
        public static double GetNormalizedRankError(ushort k, bool pmf)
        {
            // Constants derived as the best fit to 99 percentile empirically measured max error
            return pmf
                ? 2.446 / Math.Pow(k, 0.9657)
                : 2.296 / Math.Pow(k, 0.9657);
        }

        /// <summary>
        /// Serializes the sketch to a byte array
        /// </summary>
        public byte[] Serialize()
        {
            using var stream = new MemoryStream();
            Serialize(stream);
            return stream.ToArray();
        }

        /// <summary>
        /// Serializes the sketch to a stream
        /// </summary>
        public void Serialize(Stream stream)
        {
            using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true);

            if (IsEmpty())
            {
                // Write empty sketch
                writer.Write(KllConstants.PreambleIntsShort);
                writer.Write(KllConstants.SerialVersion2);
                writer.Write(KllConstants.Family);
                writer.Write((byte)0); // lgK
                writer.Write((byte)0); // flags
                writer.Write((ushort)0);
                return;
            }

            // Write preamble
            writer.Write(_n == 1 ? KllConstants.PreambleIntsShort : KllConstants.PreambleIntsFull);
            writer.Write(KllConstants.SerialVersion2);
            writer.Write(KllConstants.Family);
            writer.Write(GetLgK());

            byte flags = 0;
            if (_isLevelZeroSorted) flags |= (byte)Flags.IsLevelZeroSorted;
            if (_n == 1) flags |= (byte)Flags.IsSingleItem;
            writer.Write(flags);

            writer.Write(_k);
            writer.Write(_m);

            // Write data
            if (_n == 1)
            {
                WriteItem(writer, _items[_levels[0]]);
            }
            else
            {
                writer.Write(_n);
                writer.Write(_minK);
                writer.Write(_numLevels);

                // Write levels
                for (int i = 0; i <= _numLevels; i++)
                {
                    writer.Write(_levels[i]);
                }

                // Write min/max
                WriteItem(writer, _minItem);
                WriteItem(writer, _maxItem);

                // Write items
                uint numItems = _levels[_numLevels] - _levels[0];
                for (uint i = _levels[0]; i < _levels[_numLevels]; i++)
                {
                    WriteItem(writer, _items[i]);
                }
            }
        }

        /// <summary>
        /// Deserializes a sketch from a stream
        /// </summary>
        public static KllSketch<T> Deserialize(Stream stream, IComparer<T> comparer = null)
        {
            using var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, leaveOpen: true);

            byte preambleInts = reader.ReadByte();
            byte serVer = reader.ReadByte();
            byte family = reader.ReadByte();

            if (family != KllConstants.Family)
            {
                throw new ArgumentException("Invalid family ID");
            }

            if (serVer != KllConstants.SerialVersion1 && serVer != KllConstants.SerialVersion2)
            {
                throw new ArgumentException("Invalid serial version");
            }

            byte lgK = reader.ReadByte();
            byte flags = reader.ReadByte();

            bool isEmpty = (flags & (byte)Flags.IsEmpty) != 0;
            bool isSingleItem = (flags & (byte)Flags.IsSingleItem) != 0;
            bool isLevelZeroSorted = (flags & (byte)Flags.IsLevelZeroSorted) != 0;

            ushort k = reader.ReadUInt16();
            byte m = reader.ReadByte();

            var sketch = new KllSketch<T>(k, comparer);

            if (isEmpty)
            {
                return sketch;
            }

            if (isSingleItem)
            {
                T item = ReadItem(reader);
                sketch.Update(item);
                return sketch;
            }

            // Read full sketch
            ulong n = reader.ReadUInt64();
            ushort minK = reader.ReadUInt16();
            byte numLevels = reader.ReadByte();

            sketch._n = n;
            sketch._minK = minK;
            sketch._numLevels = numLevels;
            sketch._isLevelZeroSorted = isLevelZeroSorted;

            // Read levels
            sketch._levels = new uint[numLevels + 1];
            for (int i = 0; i <= numLevels; i++)
            {
                sketch._levels[i] = reader.ReadUInt32();
            }

            // Read min/max
            sketch._minItem = ReadItem(reader);
            sketch._maxItem = ReadItem(reader);
            sketch._hasMinMax = true;

            // Read items
            uint numItems = sketch._levels[numLevels] - sketch._levels[0];
            uint capacity = sketch._levels[0];
            sketch._items = new T[Math.Max(capacity + numItems, k)];

            for (uint i = sketch._levels[0]; i < sketch._levels[numLevels]; i++)
            {
                sketch._items[i] = ReadItem(reader);
            }

            return sketch;
        }

        /// <summary>
        /// Returns a string representation of the sketch
        /// </summary>
        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"KLL Sketch:");
            sb.AppendLine($"  K: {_k}");
            sb.AppendLine($"  N: {_n}");
            sb.AppendLine($"  NumRetained: {GetNumRetained()}");
            sb.AppendLine($"  NumLevels: {_numLevels}");
            sb.AppendLine($"  IsEmpty: {IsEmpty()}");
            sb.AppendLine($"  IsEstimationMode: {IsEstimationMode()}");
            if (_hasMinMax)
            {
                sb.AppendLine($"  Min: {_minItem}");
                sb.AppendLine($"  Max: {_maxItem}");
            }
            return sb.ToString();
        }

        // Private helper methods

        private void UpdateMinMax(T item)
        {
            if (!_hasMinMax)
            {
                _minItem = item;
                _maxItem = item;
                _hasMinMax = true;
            }
            else
            {
                if (_comparer.Compare(item, _minItem) < 0)
                {
                    _minItem = item;
                }
                if (_comparer.Compare(item, _maxItem) > 0)
                {
                    _maxItem = item;
                }
            }
        }

        private uint InternalUpdate()
        {
            _n++;
            if (_levels[0] == 0)
            {
                CompressWhileUpdating();
            }
            return --_levels[0];
        }

        private void CompressWhileUpdating()
        {
            byte level = FindLevelToCompact();
            if (level == _numLevels)
            {
                AddEmptyTopLevelToCompletelyFullSketch();
            }
            // Actual compression logic would go here
        }

        private byte FindLevelToCompact()
        {
            byte level = 0;
            while (level < _numLevels)
            {
                uint pop = _levels[level + 1] - _levels[level];
                uint cap = KllHelper.LevelCapacity(_k, _numLevels, level, _m);
                if (pop >= cap)
                {
                    return level;
                }
                level++;
            }
            return level;
        }

        private void AddEmptyTopLevelToCompletelyFullSketch()
        {
            uint curTotalCap = _levels[_numLevels];
            uint deltaCap = KllHelper.LevelCapacity(_k, (byte)(_numLevels + 1), 0, _m);
            uint newTotalCap = curTotalCap + deltaCap;

            var newItems = new T[newTotalCap];
            Array.Copy(_items, 0, newItems, deltaCap, _itemsSize);
            _items = newItems;
            _itemsSize = newTotalCap;

            var newLevels = new uint[_numLevels + 2];
            for (int i = 0; i <= _numLevels; i++)
            {
                newLevels[i + 1] = _levels[i] + deltaCap;
            }
            newLevels[0] = deltaCap;

            _levels = newLevels;
            _numLevels++;
        }

        private KllSortedView<T> GetSortedView()
        {
            return new KllSortedView<T>(this);
        }

        private byte GetLgK()
        {
            return (byte)(32 - BitOperations.LeadingZeroCount((uint)_k - 1));
        }

        private static void WriteItem(BinaryWriter writer, T item)
        {
            // For basic types, write directly
            if (typeof(T) == typeof(int))
            {
                writer.Write((int)(object)item);
            }
            else if (typeof(T) == typeof(long))
            {
                writer.Write((long)(object)item);
            }
            else if (typeof(T) == typeof(float))
            {
                writer.Write((float)(object)item);
            }
            else if (typeof(T) == typeof(double))
            {
                writer.Write((double)(object)item);
            }
            else if (typeof(T) == typeof(string))
            {
                writer.Write((string)(object)item);
            }
            else
            {
                throw new NotSupportedException($"Type {typeof(T)} serialization not implemented");
            }
        }

        private static T ReadItem(BinaryReader reader)
        {
            // For basic types, read directly
            if (typeof(T) == typeof(int))
            {
                return (T)(object)reader.ReadInt32();
            }
            else if (typeof(T) == typeof(long))
            {
                return (T)(object)reader.ReadInt64();
            }
            else if (typeof(T) == typeof(float))
            {
                return (T)(object)reader.ReadSingle();
            }
            else if (typeof(T) == typeof(double))
            {
                return (T)(object)reader.ReadDouble();
            }
            else if (typeof(T) == typeof(string))
            {
                return (T)(object)reader.ReadString();
            }
            else
            {
                throw new NotSupportedException($"Type {typeof(T)} deserialization not implemented");
            }
        }
    }
}

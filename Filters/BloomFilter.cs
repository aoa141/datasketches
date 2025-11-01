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
 * "AS IS" * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.IO;
using System.Text;

using DataSketches.Common;
namespace DataSketches.Filters
{
    /// <summary>
    /// A Bloom filter for probabilistic set membership testing.
    /// When querying, there are no false negatives but there may be false positives.
    /// </summary>
    public class BloomFilter
    {
        private const byte FamilyId = 21;
        private const byte SerialVersion = 1;
        private const byte PreambleEmpty = 3;
        private const byte PreambleStandard = 4;
        private const byte EmptyFlagMask = 4;

        private readonly ulong _seed;
        private readonly ushort _numHashes;
        private readonly ulong _capacityBits;
        private ulong _numBitsSet;
        private readonly ulong[] _bitArray;
        private bool _isDirty;

        private BloomFilter(ulong numBits, ushort numHashes, ulong seed)
        {
            if (numBits == 0) throw new ArgumentException("numBits must be positive");
            if (numHashes == 0) throw new ArgumentException("numHashes must be positive");

            _capacityBits = numBits;
            _numHashes = numHashes;
            _seed = seed;
            _bitArray = new ulong[(numBits + 63) / 64];
            _numBitsSet = 0;
            _isDirty = true;
        }

        /// <summary>
        /// Gets the number of bits in the filter.
        /// </summary>
        public ulong Capacity => _capacityBits;

        /// <summary>
        /// Gets the number of hash functions.
        /// </summary>
        public ushort NumHashes => _numHashes;

        /// <summary>
        /// Gets the hash seed.
        /// </summary>
        public ulong Seed => _seed;

        /// <summary>
        /// Returns true if the filter is empty.
        /// </summary>
        public bool IsEmpty => _numBitsSet == 0;

        /// <summary>
        /// Gets the number of bits currently set.
        /// </summary>
        public ulong BitsUsed
        {
            get
            {
                if (_isDirty)
                    UpdateNumBitsSet();
                return _numBitsSet;
            }
        }

        /// <summary>
        /// Resets the filter to its initial empty state.
        /// </summary>
        public void Reset()
        {
            Array.Clear(_bitArray, 0, _bitArray.Length);
            _numBitsSet = 0;
            _isDirty = false;
        }

        /// <summary>
        /// Updates the filter with a string value.
        /// </summary>
        public void Update(string item)
        {
            if (string.IsNullOrEmpty(item)) return;
            var bytes = Encoding.UTF8.GetBytes(item);
            Update(bytes);
        }

        /// <summary>
        /// Updates the filter with a numeric value.
        /// </summary>
        public void Update(ulong item)
        {
            var bytes = BitConverter.GetBytes(item);
            Update(bytes);
        }

        /// <summary>
        /// Updates the filter with a numeric value.
        /// </summary>
        public void Update(long item)
        {
            var bytes = BitConverter.GetBytes(item);
            Update(bytes);
        }

        /// <summary>
        /// Updates the filter with a double value.
        /// </summary>
        public void Update(double item)
        {
            var bytes = BitConverter.GetBytes(item);
            Update(bytes);
        }

        /// <summary>
        /// Updates the filter with raw bytes.
        /// </summary>
        public void Update(byte[] data)
        {
            var (h0, h1) = ComputeHashes(data);
            InternalUpdate(h0, h1);
        }

        /// <summary>
        /// Queries the filter for membership.
        /// </summary>
        public bool Query(string item)
        {
            if (string.IsNullOrEmpty(item)) return false;
            var bytes = Encoding.UTF8.GetBytes(item);
            return Query(bytes);
        }

        /// <summary>
        /// Queries the filter for a numeric value.
        /// </summary>
        public bool Query(ulong item)
        {
            var bytes = BitConverter.GetBytes(item);
            return Query(bytes);
        }

        /// <summary>
        /// Queries the filter for a numeric value.
        /// </summary>
        public bool Query(long item)
        {
            var bytes = BitConverter.GetBytes(item);
            return Query(bytes);
        }

        /// <summary>
        /// Queries the filter for a double value.
        /// </summary>
        public bool Query(double item)
        {
            var bytes = BitConverter.GetBytes(item);
            return Query(bytes);
        }

        /// <summary>
        /// Queries the filter with raw bytes.
        /// </summary>
        public bool Query(byte[] data)
        {
            var (h0, h1) = ComputeHashes(data);
            return InternalQuery(h0, h1);
        }

        /// <summary>
        /// Updates the filter and returns whether it was present before.
        /// </summary>
        public bool QueryAndUpdate(byte[] data)
        {
            var (h0, h1) = ComputeHashes(data);
            return InternalQueryAndUpdate(h0, h1);
        }

        /// <summary>
        /// Unions another Bloom filter into this one (OR operation).
        /// </summary>
        public void UnionWith(BloomFilter other)
        {
            if (!IsCompatible(other))
                throw new ArgumentException("Filters are not compatible");

            for (int i = 0; i < _bitArray.Length; i++)
            {
                _bitArray[i] |= other._bitArray[i];
            }
            _isDirty = true;
        }

        /// <summary>
        /// Intersects this filter with another (AND operation).
        /// </summary>
        public void Intersect(BloomFilter other)
        {
            if (!IsCompatible(other))
                throw new ArgumentException("Filters are not compatible");

            for (int i = 0; i < _bitArray.Length; i++)
            {
                _bitArray[i] &= other._bitArray[i];
            }
            _isDirty = true;
        }

        /// <summary>
        /// Inverts all bits in the filter.
        /// </summary>
        public void Invert()
        {
            for (int i = 0; i < _bitArray.Length; i++)
            {
                _bitArray[i] = ~_bitArray[i];
            }
            _isDirty = true;
        }

        /// <summary>
        /// Checks if another filter is compatible for union/intersection.
        /// </summary>
        public bool IsCompatible(BloomFilter other)
        {
            return _capacityBits == other._capacityBits &&
                   _numHashes == other._numHashes &&
                   _seed == other._seed;
        }

        private void InternalUpdate(ulong h0, ulong h1)
        {
            for (int i = 0; i < _numHashes; i++)
            {
                var bitIndex = (h0 + (ulong)i * h1) % _capacityBits;
                SetBit(bitIndex);
            }
            _isDirty = true;
        }

        private bool InternalQuery(ulong h0, ulong h1)
        {
            for (int i = 0; i < _numHashes; i++)
            {
                var bitIndex = (h0 + (ulong)i * h1) % _capacityBits;
                if (!GetBit(bitIndex))
                    return false;
            }
            return true;
        }

        private bool InternalQueryAndUpdate(ulong h0, ulong h1)
        {
            bool wasPresent = true;

            for (int i = 0; i < _numHashes; i++)
            {
                var bitIndex = (h0 + (ulong)i * h1) % _capacityBits;
                if (!GetBit(bitIndex))
                {
                    wasPresent = false;
                    SetBit(bitIndex);
                }
            }

            if (!wasPresent)
                _isDirty = true;

            return wasPresent;
        }

        private void SetBit(ulong bitIndex)
        {
            var arrayIndex = bitIndex / 64;
            var bitPosition = (int)(bitIndex % 64);
            _bitArray[arrayIndex] |= (1UL << bitPosition);
        }

        private bool GetBit(ulong bitIndex)
        {
            var arrayIndex = bitIndex / 64;
            var bitPosition = (int)(bitIndex % 64);
            return (_bitArray[arrayIndex] & (1UL << bitPosition)) != 0;
        }

        private (ulong, ulong) ComputeHashes(byte[] data)
        {
            HashState hashState;
            MurmurHash3.Hash128(data, _seed, out hashState);
            return (hashState.H1, hashState.H2);
        }

        private void UpdateNumBitsSet()
        {
            ulong count = 0;
            foreach (var word in _bitArray)
            {
                count += (ulong)CountBits(word);
            }
            _numBitsSet = count;
            _isDirty = false;
        }

        private static int CountBits(ulong value)
        {
            int count = 0;
            while (value != 0)
            {
                count++;
                value &= value - 1; // Clear the lowest set bit
            }
            return count;
        }

        /// <summary>
        /// Serializes the filter to a byte array.
        /// </summary>
        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                // Preamble
                writer.Write(IsEmpty ? PreambleEmpty : PreambleStandard);
                writer.Write(SerialVersion);
                writer.Write(FamilyId);
                writer.Write((byte)(IsEmpty ? EmptyFlagMask : 0));

                writer.Write(_numHashes);
                writer.Write((ushort)0); // padding
                writer.Write((uint)0); // unused

                writer.Write(_capacityBits);

                if (!IsEmpty)
                {
                    writer.Write(BitsUsed);

                    // Write bit array
                    foreach (var word in _bitArray)
                    {
                        writer.Write(word);
                    }
                }

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Deserializes a filter from a byte array.
        /// </summary>
        public static BloomFilter Deserialize(byte[] bytes)
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

                var numHashes = reader.ReadUInt16();
                reader.ReadUInt16(); // padding
                reader.ReadUInt32(); // unused

                var capacityBits = reader.ReadUInt64();

                var filter = new BloomFilter(capacityBits, numHashes, 9001);

                bool isEmpty = (flags & EmptyFlagMask) != 0;
                if (!isEmpty)
                {
                    filter._numBitsSet = reader.ReadUInt64();

                    for (int i = 0; i < filter._bitArray.Length; i++)
                    {
                        filter._bitArray[i] = reader.ReadUInt64();
                    }
                    filter._isDirty = false;
                }

                return filter;
            }
        }

        /// <summary>
        /// Returns a string representation of this filter.
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("### Bloom Filter Summary ###");
            sb.AppendLine($"  Capacity (bits) : {_capacityBits}");
            sb.AppendLine($"  Num Hashes      : {_numHashes}");
            sb.AppendLine($"  Bits Used       : {BitsUsed}");
            sb.AppendLine($"  Is Empty        : {IsEmpty}");
            if (!IsEmpty)
            {
                sb.AppendLine($"  Fill Ratio      : {(double)BitsUsed / _capacityBits:P2}");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Builder for creating Bloom filters.
        /// </summary>
        public static class Builder
        {
            /// <summary>
            /// Suggests the optimal number of hash functions.
            /// </summary>
            public static ushort SuggestNumHashes(ulong maxDistinctItems, ulong numFilterBits)
            {
                if (maxDistinctItems == 0 || numFilterBits == 0)
                    throw new ArgumentException("Parameters must be positive");

                var ratio = (double)numFilterBits / maxDistinctItems;
                var numHashes = (int)Math.Round(ratio * Math.Log(2));
                return (ushort)Math.Max(1, Math.Min(numHashes, ushort.MaxValue));
            }

            /// <summary>
            /// Suggests the optimal number of hash functions for a target FPP.
            /// </summary>
            public static ushort SuggestNumHashes(double targetFalsePositiveProb)
            {
                if (targetFalsePositiveProb <= 0 || targetFalsePositiveProb >= 1)
                    throw new ArgumentException("targetFalsePositiveProb must be between 0 and 1");

                var numHashes = (int)Math.Ceiling(-Math.Log(targetFalsePositiveProb) / Math.Log(2));
                return (ushort)Math.Max(1, Math.Min(numHashes, ushort.MaxValue));
            }

            /// <summary>
            /// Suggests the optimal number of bits for a filter.
            /// </summary>
            public static ulong SuggestNumFilterBits(ulong maxDistinctItems, double targetFalsePositiveProb)
            {
                if (maxDistinctItems == 0)
                    throw new ArgumentException("maxDistinctItems must be positive");
                if (targetFalsePositiveProb <= 0 || targetFalsePositiveProb >= 1)
                    throw new ArgumentException("targetFalsePositiveProb must be between 0 and 1");

                var numBits = -(double)maxDistinctItems * Math.Log(targetFalsePositiveProb) / (Math.Log(2) * Math.Log(2));
                return (ulong)Math.Ceiling(numBits);
            }

            /// <summary>
            /// Creates a Bloom filter based on accuracy requirements.
            /// </summary>
            public static BloomFilter CreateByAccuracy(ulong maxDistinctItems, double targetFalsePositiveProb, ulong seed = 9001)
            {
                var numBits = SuggestNumFilterBits(maxDistinctItems, targetFalsePositiveProb);
                var numHashes = SuggestNumHashes(maxDistinctItems, numBits);
                return new BloomFilter(numBits, numHashes, seed);
            }

            /// <summary>
            /// Creates a Bloom filter with specified size parameters.
            /// </summary>
            public static BloomFilter CreateBySize(ulong numBits, ushort numHashes, ulong seed = 9001)
            {
                return new BloomFilter(numBits, numHashes, seed);
            }
        }
    }
}

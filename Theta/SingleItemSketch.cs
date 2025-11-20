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
using System.Text;
using Apache.DataSketches.Common;
using Apache.DataSketches.Hash;
using Apache.DataSketches.ThetaCommon;
using static Apache.DataSketches.Theta.PreambleUtil;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// A CompactSketch that holds only one item hash.
    /// </summary>
    internal sealed class SingleItemSketch : CompactSketch
    {
        private static readonly long DEFAULT_SEED_HASH =
            ThetaUtil.ComputeSeedHash(ThetaUtil.DEFAULT_UPDATE_SEED) & 0xFFFFL;

        // For backward compatibility, a candidate pre0_ long must have:
        // Flags (byte 5): Ordered, Compact, NOT Empty, Read Only, LittleEndian = 11010 = 0x1A.
        // Flags mask will be 0x1F.
        // SingleItem flag may not be set due to a historical bug, so we can't depend on it for now.
        // However, if the above flags are correct, preLongs == 1, SerVer >= 3, FamilyID == 3,
        // and the hash seed matches, it is virtually guaranteed that we have a SingleItem Sketch.

        private static readonly long PRE0_LO6_SI = 0x00_00_3A_00_00_03_03_01L; //with SI flag
        private long pre0_ = 0;
        private long hash_ = 0;

        // Internal Constructor. All checking & hashing has been done, assumes default seed
        private SingleItemSketch(long hash)
        {
            pre0_ = (DEFAULT_SEED_HASH << 48) | PRE0_LO6_SI;
            hash_ = hash;
        }

        // All checking & hashing has been done, given the relevant seed
        internal SingleItemSketch(long hash, long seed)
        {
            long seedHash = ThetaUtil.ComputeSeedHash(seed) & 0xFFFFL;
            pre0_ = (seedHash << 48) | PRE0_LO6_SI;
            hash_ = hash;
        }

        // All checking & hashing has been done, given the relevant seedHash
        internal SingleItemSketch(long hash, short seedHash)
        {
            long seedH = seedHash & 0xFFFFL;
            pre0_ = (seedH << 48) | PRE0_LO6_SI;
            hash_ = hash;
        }

        /// <summary>
        /// Creates a SingleItemSketch on the heap given a SingleItemSketch Memory image and a seedHash.
        /// Checks the seed hash of the given Memory against the given seedHash.
        /// </summary>
        internal static SingleItemSketch Heapify(ReadOnlySpan<byte> srcMem, short expectedSeedHash)
        {
            ThetaUtil.CheckSeedHashes((short)ExtractSeedHash(srcMem), expectedSeedHash);
            bool singleItem = OtherCheckForSingleItem(srcMem);
            if (singleItem)
            {
                long hash = BitConverter.ToInt64(srcMem.Slice(8, 8));
                return new SingleItemSketch(hash, expectedSeedHash);
            }
            throw new SketchesArgumentException("Input Memory is not a SingleItemSketch.");
        }

        public override CompactSketch Compact(bool dstOrdered, Span<byte> dstMem)
        {
            if (dstMem == null || dstMem.Length == 0)
            {
                return this;
            }
            else
            {
                BitConverter.TryWriteBytes(dstMem.Slice(0, 8), pre0_);
                BitConverter.TryWriteBytes(dstMem.Slice(8, 8), hash_);
                // Note: DirectCompactSketch not yet fully implemented
                throw new NotImplementedException("DirectCompactSketch not yet implemented");
                //return new DirectCompactSketch(dstMem);
            }
        }

        // Create methods using the default seed

        /// <summary>
        /// Create this sketch with a long.
        /// </summary>
        internal static SingleItemSketch Create(long datum)
        {
            long[] data = { datum };
            return new SingleItemSketch(MurmurHash3.Hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        /// <summary>
        /// Create this sketch with the given double (or float) datum.
        /// </summary>
        internal static SingleItemSketch Create(double datum)
        {
            double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
            long[] data = { BitConverter.DoubleToInt64Bits(d) }; // canonicalize all NaN forms
            return new SingleItemSketch(MurmurHash3.Hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        /// <summary>
        /// Create this sketch with the given String.
        /// </summary>
        internal static SingleItemSketch Create(string datum)
        {
            if (string.IsNullOrEmpty(datum)) { return null; }
            byte[] data = Encoding.UTF8.GetBytes(datum);
            return new SingleItemSketch(MurmurHash3.Hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        /// <summary>
        /// Create this sketch with the given byte array.
        /// </summary>
        internal static SingleItemSketch Create(byte[] data)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        /// <summary>
        /// Create this sketch with the given char array.
        /// </summary>
        internal static SingleItemSketch Create(char[] data)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        /// <summary>
        /// Create this sketch with the given integer array.
        /// </summary>
        internal static SingleItemSketch Create(int[] data)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        /// <summary>
        /// Create this sketch with the given long array.
        /// </summary>
        internal static SingleItemSketch Create(long[] data)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1);
        }

        // Updates with a user specified seed

        /// <summary>
        /// Create this sketch with a long and a seed.
        /// </summary>
        internal static SingleItemSketch Create(long datum, long seed)
        {
            long[] data = { datum };
            return new SingleItemSketch(MurmurHash3.Hash(data, seed)[0] >>> 1, seed);
        }

        /// <summary>
        /// Create this sketch with the given double (or float) datum and a seed.
        /// </summary>
        internal static SingleItemSketch Create(double datum, long seed)
        {
            double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
            long[] data = { BitConverter.DoubleToInt64Bits(d) }; // canonicalize all NaN forms
            return new SingleItemSketch(MurmurHash3.Hash(data, seed)[0] >>> 1, seed);
        }

        /// <summary>
        /// Create this sketch with the given String and a seed.
        /// </summary>
        internal static SingleItemSketch Create(string datum, long seed)
        {
            if (string.IsNullOrEmpty(datum)) { return null; }
            byte[] data = Encoding.UTF8.GetBytes(datum);
            return new SingleItemSketch(MurmurHash3.Hash(data, seed)[0] >>> 1, seed);
        }

        /// <summary>
        /// Create this sketch with the given byte array and a seed.
        /// </summary>
        internal static SingleItemSketch Create(byte[] data, long seed)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, seed)[0] >>> 1, seed);
        }

        /// <summary>
        /// Create this sketch with the given char array and a seed.
        /// </summary>
        internal static SingleItemSketch Create(char[] data, long seed)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, seed)[0] >>> 1, seed);
        }

        /// <summary>
        /// Create this sketch with the given integer array and a seed.
        /// </summary>
        internal static SingleItemSketch Create(int[] data, long seed)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, seed)[0] >>> 1, seed);
        }

        /// <summary>
        /// Create this sketch with the given long array (as an item) and a seed.
        /// </summary>
        internal static SingleItemSketch Create(long[] data, long seed)
        {
            if (data == null || data.Length == 0) { return null; }
            return new SingleItemSketch(MurmurHash3.Hash(data, seed)[0] >>> 1, seed);
        }

        // Sketch overrides

        public override int GetCountLessThanThetaLong(long thetaLong)
        {
            return (hash_ < thetaLong) ? 1 : 0;
        }

        public override int GetCurrentBytes()
        {
            return 16;
        }

        public override double GetEstimate()
        {
            return 1.0;
        }

        public override HashIterator Iterator()
        {
            return new HeapCompactHashIterator(new long[] { hash_ });
        }

        public override double GetLowerBound(int numStdDev)
        {
            return 1.0;
        }

        public override int GetRetainedEntries(bool valid)
        {
            return 1;
        }

        public override long GetThetaLong()
        {
            return long.MaxValue;
        }

        public override double GetUpperBound(int numStdDev)
        {
            return 1.0;
        }

        public override bool IsEmpty()
        {
            return false;
        }

        public override bool IsOrdered()
        {
            return true;
        }

        public override byte[] ToByteArray()
        {
            byte[] output = new byte[16];
            BitConverter.TryWriteBytes(output.AsSpan(0, 8), pre0_);
            BitConverter.TryWriteBytes(output.AsSpan(8, 8), hash_);
            return output;
        }

        // Restricted methods

        internal override long[] GetCache()
        {
            return new long[] { hash_ };
        }

        internal override int GetCompactPreambleLongs()
        {
            return 1;
        }

        internal override int GetCurrentDataLongs()
        {
            return 1;
        }

        internal override int GetCurrentPreambleLongs()
        {
            return 1;
        }

        internal override ReadOnlyMemory<byte>? GetMemory()
        {
            return null;
        }

        internal override short GetSeedHash()
        {
            return (short)(pre0_ >>> 48);
        }

        internal static bool OtherCheckForSingleItem(ReadOnlySpan<byte> mem)
        {
            return OtherCheckForSingleItem(
                ExtractPreLongs(mem),
                ExtractSerVer(mem),
                ExtractFamilyID(mem),
                ExtractFlags(mem));
        }

        internal static bool OtherCheckForSingleItem(int preLongs, int serVer, int famId, int flags)
        {
            // Flags byte: SI=X, Ordered=T, Compact=T, Empty=F, ReadOnly=T, BigEndian=F = X11010 = 0x1A.
            // Flags mask will be 0x1F.
            // SingleItem flag may not be set due to a historical bug, so we can't depend on it for now.
            // However, if the above flags are correct, preLongs == 1, SerVer >= 3, FamilyID == 3,
            // and the hash seed matches (not done here), it is virtually guaranteed that we have a
            // SingleItem Sketch.
            bool numPreLongs = preLongs == 1;
            bool numSerVer = serVer >= 3;
            bool numFamId = famId == (int)Family.COMPACT;
            bool numFlags = (flags & 0x1F) == 0x1A; //no SI, yet
            bool singleFlag = (flags & SINGLEITEM_FLAG_MASK) > 0;
            return (numPreLongs && numSerVer && numFamId && numFlags) || singleFlag;
        }
    }
}

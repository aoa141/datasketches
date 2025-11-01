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
using System.Numerics;
using DataSketches.Common;

namespace DataSketches.Hll
{
    /// <summary>
    /// HLL sketch mode enumeration
    /// </summary>
    public enum HllMode
    {
        List = 0,
        Set = 1,
        Hll = 2
    }

    /// <summary>
    /// HLL sketch constants
    /// </summary>
    public static class HllConstants
    {
        // Preamble stuff
        public const byte SerVer = 1;
        public const byte FamilyId = 7;

        public const byte EmptyFlagMask = 4;
        public const byte CompactFlagMask = 8;
        public const byte OutOfOrderFlagMask = 16;
        public const byte FullSizeFlagMask = 32;

        public const int PreambleIntsByte = 0;
        public const int SerVerByte = 1;
        public const int FamilyByte = 2;
        public const int LgKByte = 3;
        public const int LgArrByte = 4;
        public const int FlagsByte = 5;
        public const int ListCountByte = 6;
        public const int HllCurMinByte = 6;
        public const int ModeByte = 7; // lo2bits = curMode, next 2 bits = tgtHllMode

        // Coupon List
        public const int ListIntArrStart = 8;
        public const byte ListPreInts = 2;

        // Coupon Hash Set
        public const int HashSetCountInt = 8;
        public const int HashSetIntArrStart = 12;
        public const byte HashSetPreInts = 3;

        // HLL
        public const byte HllPreInts = 10;
        public const int HllByteArrStart = 40;
        public const int HipAccumDouble = 8;
        public const int KxQ0Double = 16;
        public const int KxQ1Double = 24;
        public const int CurMinCountInt = 32;
        public const int AuxCountInt = 36;

        public const int EmptySketchSizeBytes = 8;

        // Other HllUtil stuff
        public const byte KeyBits26 = 26;
        public const byte ValBits6 = 6;
        public const uint KeyMask26 = (1u << KeyBits26) - 1;
        public const uint ValMask6 = (1u << ValBits6) - 1;
        public const uint Empty = 0;
        public const byte MinLogK = 4;
        public const byte MaxLogK = 21;

        public const double HllHipRseFactor = 0.8325546; // sqrt(ln(2))
        public const double HllNonHipRseFactor = 1.03896; // sqrt((3 * ln(2)) - 1)
        public const double CouponRseFactor = 0.409; // at transition point not the asymptote
        public const double CouponRse = CouponRseFactor / (1 << 13);

        public const byte LgInitListSize = 3;
        public const byte LgInitSetSize = 5;
        public const uint ResizeNumer = 3;
        public const uint ResizeDenom = 4;

        public const byte LoNibbleMask = 0x0f;
        public const byte HiNibbleMask = 0xf0;
        public const byte AuxToken = 0xf;

        /// <summary>
        /// Log2 table sizes for exceptions based on lgK from 0 to 26.
        /// However, only lgK from 4 to 21 are used.
        /// </summary>
        public static readonly byte[] LgAuxArrInts = new byte[]
        {
            0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   // 0 - 9
            4, 4, 5, 5, 6, 7, 8, 9, 10, 11, // 10-19
            12, 13, 14, 15, 16, 17, 18      // 20-26
        };
    }

    /// <summary>
    /// HLL utility functions
    /// </summary>
    public static class HllUtil
    {
        /// <summary>
        /// Creates a coupon from hash values
        /// </summary>
        public static uint Coupon(ulong hash0, ulong hash1)
        {
            uint addr26 = (uint)(hash0 & HllConstants.KeyMask26);
            byte lz = (byte)BitOperations.LeadingZeroCount(hash1);
            byte value = (byte)((lz > 62 ? 62 : lz) + 1);
            return (uint)((value << HllConstants.KeyBits26) | addr26);
        }

        /// <summary>
        /// Creates a coupon from a HashState
        /// </summary>
        public static uint Coupon(HashState hashState)
        {
            return Coupon(hashState.H1, hashState.H2);
        }

        /// <summary>
        /// Hash a value
        /// </summary>
        public static HashState Hash(byte[] key, ulong seed = 0)
        {
            HashState state; MurmurHash3.Hash128(key, seed, out state); return state;
        }

        /// <summary>
        /// Check and validate lgK parameter
        /// </summary>
        public static byte CheckLgK(byte lgK)
        {
            if (lgK >= HllConstants.MinLogK && lgK <= HllConstants.MaxLogK)
            {
                return lgK;
            }
            throw new ArgumentException($"Invalid value of k: {lgK}");
        }

        /// <summary>
        /// Get relative error estimate
        /// </summary>
        public static double GetRelErr(bool upperBound, bool unioned, byte lgConfigK, byte numStdDev)
        {
            CheckLgK(lgConfigK);
            if (lgConfigK > 12)
            {
                double rseFactor = unioned
                    ? HllConstants.HllNonHipRseFactor
                    : HllConstants.HllHipRseFactor;
                uint configK = 1u << lgConfigK;
                return (upperBound ? -1 : 1) * (numStdDev * rseFactor) / Math.Sqrt(configK);
            }
            else
            {
                return RelativeErrorTables.GetRelErr(upperBound, unioned, lgConfigK, numStdDev);
            }
        }

        /// <summary>
        /// Check memory size requirements
        /// </summary>
        public static void CheckMemSize(ulong minBytes, ulong capBytes)
        {
            if (capBytes < minBytes)
            {
                throw new ArgumentException($"Given destination array is not large enough: {capBytes}");
            }
        }

        /// <summary>
        /// Check number of standard deviations
        /// </summary>
        public static void CheckNumStdDev(byte numStdDev)
        {
            if (numStdDev < 1 || numStdDev > 3)
            {
                throw new ArgumentException("NumStdDev may not be less than 1 or greater than 3.");
            }
        }

        /// <summary>
        /// Create a pair from slot number and value
        /// </summary>
        public static uint Pair(uint slotNo, byte value)
        {
            return (uint)((value << HllConstants.KeyBits26) | (slotNo & HllConstants.KeyMask26));
        }

        /// <summary>
        /// Get low 26 bits from coupon
        /// </summary>
        public static uint GetLow26(uint coupon)
        {
            return coupon & HllConstants.KeyMask26;
        }

        /// <summary>
        /// Get value from coupon
        /// </summary>
        public static byte GetValue(uint coupon)
        {
            return (byte)(coupon >> HllConstants.KeyBits26);
        }

        /// <summary>
        /// Simple integer log2 (n must be power of 2)
        /// </summary>
        public static byte SimpleIntLog2(uint n)
        {
            if (n == 0)
            {
                throw new InvalidOperationException("cannot take log of 0");
            }
            return (byte)BitOperations.TrailingZeroCount(n);
        }

        /// <summary>
        /// Compute log of array size in ints
        /// </summary>
        public static byte ComputeLgArrInts(HllMode mode, uint count, byte lgConfigK)
        {
            // assume value missing and recompute
            if (mode == HllMode.List)
            {
                return HllConstants.LgInitListSize;
            }

            uint ceilPwr2 = MathUtils.CeilingPowerOf2(count);
            if ((HllConstants.ResizeDenom * count) > (HllConstants.ResizeNumer * ceilPwr2))
            {
                ceilPwr2 <<= 1;
            }

            if (mode == HllMode.Set)
            {
                return Math.Max(HllConstants.LgInitSetSize, SimpleIntLog2(ceilPwr2));
            }

            // only used for HLL4
            return Math.Max(HllConstants.LgAuxArrInts[lgConfigK], SimpleIntLog2(ceilPwr2));
        }
    }
}

// <copyright file="MathUtils.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;

namespace DataSketches.Common
{
    /// <summary>
    /// Math utilities for DataSketches
    /// </summary>
    public static class MathUtils
    {
        /// <summary>
        /// Compute the next highest power of 2 of 32-bit n
        /// Taken from https://graphics.stanford.edu/~seander/bithacks.html
        /// </summary>
        public static uint CeilingPowerOf2(uint n)
        {
            --n;
            n |= n >> 1;
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;
            return ++n;
        }

        /// <summary>
        /// Count leading zeros in a 64-bit value
        /// </summary>
        public static int CountLeadingZeros(ulong value)
        {
            if (value == 0) return 64;
            int count = 0;
            if ((value & 0xFFFFFFFF00000000UL) == 0) { count += 32; value <<= 32; }
            if ((value & 0xFFFF000000000000UL) == 0) { count += 16; value <<= 16; }
            if ((value & 0xFF00000000000000UL) == 0) { count += 8; value <<= 8; }
            if ((value & 0xF000000000000000UL) == 0) { count += 4; value <<= 4; }
            if ((value & 0xC000000000000000UL) == 0) { count += 2; value <<= 2; }
            if ((value & 0x8000000000000000UL) == 0) { count += 1; }
            return count;
        }

        /// <summary>
        /// Count trailing zeros in a 64-bit value
        /// </summary>
        public static int CountTrailingZeros(ulong value)
        {
            if (value == 0) return 64;
            int count = 0;
            if ((value & 0x00000000FFFFFFFFUL) == 0) { count += 32; value >>= 32; }
            if ((value & 0x000000000000FFFFUL) == 0) { count += 16; value >>= 16; }
            if ((value & 0x00000000000000FFUL) == 0) { count += 8; value >>= 8; }
            if ((value & 0x000000000000000FUL) == 0) { count += 4; value >>= 4; }
            if ((value & 0x0000000000000003UL) == 0) { count += 2; value >>= 2; }
            if ((value & 0x0000000000000001UL) == 0) { count += 1; }
            return count;
        }
    }
}

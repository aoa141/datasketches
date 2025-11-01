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

namespace DataSketches.Common
{
    /// <summary>
    /// Provides utilities for counting leading and trailing zeros in integers.
    /// </summary>
    public static class CountZeros
    {
        private static readonly byte[] ByteLeadingZerosTable = new byte[256]
        {
            8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4,
            3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };

        private static readonly byte[] ByteTrailingZerosTable = new byte[256]
        {
            8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
            4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
        };

        private const ulong FCLZ_MASK_56 = 0x00ffffffffffffffUL;
        private const ulong FCLZ_MASK_48 = 0x0000ffffffffffffUL;
        private const ulong FCLZ_MASK_40 = 0x000000ffffffffffUL;
        private const ulong FCLZ_MASK_32 = 0x00000000ffffffffUL;
        private const ulong FCLZ_MASK_24 = 0x0000000000ffffffUL;
        private const ulong FCLZ_MASK_16 = 0x000000000000ffffUL;
        private const ulong FCLZ_MASK_08 = 0x00000000000000ffUL;

        /// <summary>
        /// Counts the number of leading zeros in a 64-bit unsigned integer.
        /// </summary>
        /// <param name="input">The value to check</param>
        /// <returns>Number of leading zeros</returns>
        public static byte CountLeadingZerosInU64(ulong input)
        {
            if (input > FCLZ_MASK_56)
                return ByteLeadingZerosTable[(input >> 56) & FCLZ_MASK_08];
            if (input > FCLZ_MASK_48)
                return (byte)(8 + ByteLeadingZerosTable[(input >> 48) & FCLZ_MASK_08]);
            if (input > FCLZ_MASK_40)
                return (byte)(16 + ByteLeadingZerosTable[(input >> 40) & FCLZ_MASK_08]);
            if (input > FCLZ_MASK_32)
                return (byte)(24 + ByteLeadingZerosTable[(input >> 32) & FCLZ_MASK_08]);
            if (input > FCLZ_MASK_24)
                return (byte)(32 + ByteLeadingZerosTable[(input >> 24) & FCLZ_MASK_08]);
            if (input > FCLZ_MASK_16)
                return (byte)(40 + ByteLeadingZerosTable[(input >> 16) & FCLZ_MASK_08]);
            if (input > FCLZ_MASK_08)
                return (byte)(48 + ByteLeadingZerosTable[(input >> 8) & FCLZ_MASK_08]);
            return (byte)(56 + ByteLeadingZerosTable[input & FCLZ_MASK_08]);
        }

        /// <summary>
        /// Counts the number of leading zeros in a 32-bit unsigned integer.
        /// </summary>
        /// <param name="input">The value to check</param>
        /// <returns>Number of leading zeros</returns>
        public static byte CountLeadingZerosInU32(uint input)
        {
            if (input > FCLZ_MASK_24)
                return ByteLeadingZerosTable[(input >> 24) & FCLZ_MASK_08];
            if (input > FCLZ_MASK_16)
                return (byte)(8 + ByteLeadingZerosTable[(input >> 16) & FCLZ_MASK_08]);
            if (input > FCLZ_MASK_08)
                return (byte)(16 + ByteLeadingZerosTable[(input >> 8) & FCLZ_MASK_08]);
            return (byte)(24 + ByteLeadingZerosTable[input & FCLZ_MASK_08]);
        }

        /// <summary>
        /// Counts the number of trailing zeros in a 32-bit unsigned integer.
        /// </summary>
        /// <param name="input">The value to check</param>
        /// <returns>Number of trailing zeros</returns>
        public static byte CountTrailingZerosInU32(uint input)
        {
            for (int i = 0; i < 4; i++)
            {
                int byteVal = (int)(input & 0xff);
                if (byteVal != 0)
                    return (byte)((i << 3) + ByteTrailingZerosTable[byteVal]);
                input >>= 8;
            }
            return 32;
        }

        /// <summary>
        /// Counts the number of trailing zeros in a 64-bit unsigned integer.
        /// </summary>
        /// <param name="input">The value to check</param>
        /// <returns>Number of trailing zeros</returns>
        public static byte CountTrailingZerosInU64(ulong input)
        {
            for (int i = 0; i < 8; i++)
            {
                int byteVal = (int)(input & 0xff);
                if (byteVal != 0)
                    return (byte)((i << 3) + ByteTrailingZerosTable[byteVal]);
                input >>= 8;
            }
            return 64;
        }
    }
}

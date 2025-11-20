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

namespace Apache.DataSketches.Hash
{
    /// <summary>
    /// The XxHash is a fast, non-cryptographic, 64-bit hash function that has
    /// excellent avalanche and 2-way bit independence properties.
    /// </summary>
    /// <remarks>
    /// Author: Lee Rhodes
    /// </remarks>
    public static class XxHash
    {
        private const ulong PRIME64_1 = 11400714785074694791UL;
        private const ulong PRIME64_2 = 14029467366897019727UL;
        private const ulong PRIME64_3 = 1609587929392839161UL;
        private const ulong PRIME64_4 = 9650029242287828579UL;
        private const ulong PRIME64_5 = 2870177450012600261UL;

        /// <summary>
        /// Compute the hash of the given memory span.
        /// </summary>
        /// <param name="mem">The given memory span</param>
        /// <param name="offsetBytes">Starting at this offset in bytes</param>
        /// <param name="lengthBytes">Continuing for this number of bytes</param>
        /// <param name="seed">Use this seed for the hash function</param>
        /// <returns>The resulting 64-bit hash value.</returns>
        public static long Hash(ReadOnlySpan<byte> mem, long offsetBytes, long lengthBytes, long seed)
        {
            return (long)Hash64(mem.Slice((int)offsetBytes, (int)lengthBytes), (ulong)seed);
        }

        /// <summary>
        /// Returns a 64-bit hash.
        /// </summary>
        /// <param name="input">A long</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>The hash</returns>
        public static long HashLong(long input, long seed)
        {
            ulong hash = (ulong)seed + PRIME64_5 + 8UL;
            hash ^= BitOperations.RotateLeft((ulong)input * PRIME64_2, 31) * PRIME64_1;
            hash = BitOperations.RotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;

            hash ^= hash >> 33;
            hash *= PRIME64_2;
            hash ^= hash >> 29;
            hash *= PRIME64_3;
            hash ^= hash >> 32;

            return (long)hash;
        }

        private static ulong Hash64(ReadOnlySpan<byte> data, ulong seed)
        {
            int length = data.Length;
            ulong hash;
            int offset = 0;

            if (length >= 32)
            {
                ulong v1 = seed + PRIME64_1 + PRIME64_2;
                ulong v2 = seed + PRIME64_2;
                ulong v3 = seed;
                ulong v4 = seed - PRIME64_1;

                while (offset <= length - 32)
                {
                    v1 += BitConverter.ToUInt64(data.Slice(offset)) * PRIME64_2;
                    v1 = BitOperations.RotateLeft(v1, 31);
                    v1 *= PRIME64_1;
                    offset += 8;

                    v2 += BitConverter.ToUInt64(data.Slice(offset)) * PRIME64_2;
                    v2 = BitOperations.RotateLeft(v2, 31);
                    v2 *= PRIME64_1;
                    offset += 8;

                    v3 += BitConverter.ToUInt64(data.Slice(offset)) * PRIME64_2;
                    v3 = BitOperations.RotateLeft(v3, 31);
                    v3 *= PRIME64_1;
                    offset += 8;

                    v4 += BitConverter.ToUInt64(data.Slice(offset)) * PRIME64_2;
                    v4 = BitOperations.RotateLeft(v4, 31);
                    v4 *= PRIME64_1;
                    offset += 8;
                }

                hash = BitOperations.RotateLeft(v1, 1) +
                       BitOperations.RotateLeft(v2, 7) +
                       BitOperations.RotateLeft(v3, 12) +
                       BitOperations.RotateLeft(v4, 18);

                v1 *= PRIME64_2;
                v1 = BitOperations.RotateLeft(v1, 31);
                v1 *= PRIME64_1;
                hash ^= v1;
                hash = hash * PRIME64_1 + PRIME64_4;

                v2 *= PRIME64_2;
                v2 = BitOperations.RotateLeft(v2, 31);
                v2 *= PRIME64_1;
                hash ^= v2;
                hash = hash * PRIME64_1 + PRIME64_4;

                v3 *= PRIME64_2;
                v3 = BitOperations.RotateLeft(v3, 31);
                v3 *= PRIME64_1;
                hash ^= v3;
                hash = hash * PRIME64_1 + PRIME64_4;

                v4 *= PRIME64_2;
                v4 = BitOperations.RotateLeft(v4, 31);
                v4 *= PRIME64_1;
                hash ^= v4;
                hash = hash * PRIME64_1 + PRIME64_4;
            }
            else
            {
                hash = seed + PRIME64_5;
            }

            hash += (ulong)length;

            while (offset <= length - 8)
            {
                ulong k1 = BitConverter.ToUInt64(data.Slice(offset));
                k1 *= PRIME64_2;
                k1 = BitOperations.RotateLeft(k1, 31);
                k1 *= PRIME64_1;
                hash ^= k1;
                hash = BitOperations.RotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
                offset += 8;
            }

            if (offset <= length - 4)
            {
                hash ^= BitConverter.ToUInt32(data.Slice(offset)) * PRIME64_1;
                hash = BitOperations.RotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
                offset += 4;
            }

            while (offset < length)
            {
                hash ^= data[offset] * PRIME64_5;
                hash = BitOperations.RotateLeft(hash, 11) * PRIME64_1;
                offset++;
            }

            hash ^= hash >> 33;
            hash *= PRIME64_2;
            hash ^= hash >> 29;
            hash *= PRIME64_3;
            hash ^= hash >> 32;

            return hash;
        }
    }
}

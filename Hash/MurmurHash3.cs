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
using Apache.DataSketches.Common;

namespace Apache.DataSketches.Hash
{
    /// <summary>
    /// The MurmurHash3 is a fast, non-cryptographic, 128-bit hash function that has
    /// excellent avalanche and 2-way bit independence properties.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Austin Appleby's C++ MurmurHash3_x64_128(...), final revision 150,
    /// which is in the Public Domain, was the inspiration for this implementation in C#.
    /// </para>
    /// <para>
    /// This C# implementation pays close attention to the C++ algorithms in order to
    /// maintain bit-wise compatibility, but the design is quite different.
    /// </para>
    /// Author: Lee Rhodes
    /// </remarks>
    public static class MurmurHash3
    {
        // Hash of long

        /// <summary>
        /// Hash the given long.
        /// </summary>
        /// <param name="key">The input long.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2.</returns>
        public static long[] Hash(long key, long seed)
        {
            HashState hashState = new HashState(seed, seed);
            return hashState.FinalMix128(key, 0, sizeof(long));
        }

        // Hash of long[]

        /// <summary>
        /// Hash the given long[] array.
        /// </summary>
        /// <param name="key">The input long[] array. It must be non-null and non-empty.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2.</returns>
        public static long[] Hash(long[] key, long seed)
        {
            return Hash(key, 0, key.Length, seed);
        }

        /// <summary>
        /// Hash a portion of the given long[] array.
        /// </summary>
        /// <param name="key">The input long[] array. It must be non-null and non-empty.</param>
        /// <param name="offsetLongs">The starting offset in longs.</param>
        /// <param name="lengthLongs">The length in longs of the portion of the array to be hashed.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2</returns>
        public static long[] Hash(long[] key, int offsetLongs, int lengthLongs, long seed)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            int arrLen = key.Length;
            CheckPositive(arrLen);
            Util.CheckBounds(offsetLongs, lengthLongs, arrLen);
            HashState hashState = new HashState(seed, seed);

            // Number of full 128-bit blocks of 2 longs (the body).
            // Possible exclusion of a remainder of 1 long.
            int nblocks = lengthLongs >> 1; // longs / 2

            // Process the 128-bit blocks (the body) into the hash
            for (int i = 0; i < nblocks; i++)
            {
                long k1 = key[offsetLongs + (i << 1)];     // offsetLongs + 0, 2, 4, ...
                long k2 = key[offsetLongs + (i << 1) + 1]; // offsetLongs + 1, 3, 5, ...
                hashState.BlockMix128(k1, k2);
            }

            // Get the tail index wrt hashed portion, remainder length
            int tail = nblocks << 1; // 2 longs / block
            int rem = lengthLongs - tail; // remainder longs: 0,1

            // Get the tail
            long k1Tail = rem == 0 ? 0 : key[offsetLongs + tail]; // k2 -> 0
            // Mix the tail into the hash and return
            return hashState.FinalMix128(k1Tail, 0, lengthLongs << 3); // convert to bytes
        }

        // Hash of int[]

        /// <summary>
        /// Hash the given int[] array.
        /// </summary>
        /// <param name="key">The input int[] array. It must be non-null and non-empty.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2.</returns>
        public static long[] Hash(int[] key, long seed)
        {
            return Hash(key, 0, key.Length, seed);
        }

        /// <summary>
        /// Hash a portion of the given int[] array.
        /// </summary>
        /// <param name="key">The input int[] array. It must be non-null and non-empty.</param>
        /// <param name="offsetInts">The starting offset in ints.</param>
        /// <param name="lengthInts">The length in ints of the portion of the array to be hashed.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2.</returns>
        public static long[] Hash(int[] key, int offsetInts, int lengthInts, long seed)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            int arrLen = key.Length;
            CheckPositive(arrLen);
            Util.CheckBounds(offsetInts, lengthInts, arrLen);
            HashState hashState = new HashState(seed, seed);

            // Number of full 128-bit blocks of 4 ints.
            // Possible exclusion of a remainder of up to 3 ints.
            int nblocks = lengthInts >> 2; // ints / 4

            // Process the 128-bit blocks (the body) into the hash
            for (int i = 0; i < nblocks; i++) // 4 ints per block
            {
                long k1Block = GetLong(key, offsetInts + (i << 2), 2);     // offsetInts + 0, 4, 8, ...
                long k2Block = GetLong(key, offsetInts + (i << 2) + 2, 2); // offsetInts + 2, 6, 10, ...
                hashState.BlockMix128(k1Block, k2Block);
            }

            // Get the tail index wrt hashed portion, remainder length
            int tail = nblocks << 2; // 4 ints per block
            int rem = lengthInts - tail; // remainder ints: 0,1,2,3

            // Get the tail
            long k1;
            long k2;
            if (rem > 2) // k1 -> whole; k2 -> partial
            {
                k1 = GetLong(key, offsetInts + tail, 2);
                k2 = GetLong(key, offsetInts + tail + 2, rem - 2);
            }
            else // k1 -> whole(2), partial(1) or 0; k2 == 0
            {
                k1 = rem == 0 ? 0 : GetLong(key, offsetInts + tail, rem);
                k2 = 0;
            }
            // Mix the tail into the hash and return
            return hashState.FinalMix128(k1, k2, lengthInts << 2); // convert to bytes
        }

        // Hash of char[]

        /// <summary>
        /// Hash the given char[] array.
        /// </summary>
        /// <param name="key">The input char[] array. It must be non-null and non-empty.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2</returns>
        public static long[] Hash(char[] key, long seed)
        {
            return Hash(key, 0, key.Length, seed);
        }

        /// <summary>
        /// Hash a portion of the given char[] array.
        /// </summary>
        /// <param name="key">The input char[] array. It must be non-null and non-empty.</param>
        /// <param name="offsetChars">The starting offset in chars.</param>
        /// <param name="lengthChars">The length in chars of the portion of the array to be hashed.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2</returns>
        public static long[] Hash(char[] key, int offsetChars, int lengthChars, long seed)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            int arrLen = key.Length;
            CheckPositive(arrLen);
            Util.CheckBounds(offsetChars, lengthChars, arrLen);
            HashState hashState = new HashState(seed, seed);

            // Number of full 128-bit blocks of 8 chars.
            // Possible exclusion of a remainder of up to 7 chars.
            int nblocks = lengthChars >> 3; // chars / 8

            // Process the 128-bit blocks (the body) into the hash
            for (int i = 0; i < nblocks; i++) // 8 chars per block
            {
                long k1Block = GetLong(key, offsetChars + (i << 3), 4);     // offsetChars + 0, 8, 16, ...
                long k2Block = GetLong(key, offsetChars + (i << 3) + 4, 4); // offsetChars + 4, 12, 20, ...
                hashState.BlockMix128(k1Block, k2Block);
            }

            // Get the tail index wrt hashed portion, remainder length
            int tail = nblocks << 3; // 8 chars per block
            int rem = lengthChars - tail; // remainder chars: 0,1,2,3,4,5,6,7

            // Get the tail
            long k1;
            long k2;
            if (rem > 4) // k1 -> whole; k2 -> partial
            {
                k1 = GetLong(key, offsetChars + tail, 4);
                k2 = GetLong(key, offsetChars + tail + 4, rem - 4);
            }
            else // k1 -> whole, partial or 0; k2 == 0
            {
                k1 = rem == 0 ? 0 : GetLong(key, offsetChars + tail, rem);
                k2 = 0;
            }
            // Mix the tail into the hash and return
            return hashState.FinalMix128(k1, k2, lengthChars << 1); // convert to bytes
        }

        // Hash of byte[]

        /// <summary>
        /// Hash the given byte[] array.
        /// </summary>
        /// <param name="key">The input byte[] array. It must be non-null and non-empty.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2.</returns>
        public static long[] Hash(byte[] key, long seed)
        {
            return Hash(key, 0, key.Length, seed);
        }

        /// <summary>
        /// Hash a portion of the given byte[] array.
        /// </summary>
        /// <param name="key">The input byte[] array. It must be non-null and non-empty.</param>
        /// <param name="offsetBytes">The starting offset in bytes.</param>
        /// <param name="lengthBytes">The length in bytes of the portion of the array to be hashed.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2.</returns>
        public static long[] Hash(byte[] key, int offsetBytes, int lengthBytes, long seed)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            int arrLen = key.Length;
            CheckPositive(arrLen);
            Util.CheckBounds(offsetBytes, lengthBytes, arrLen);
            HashState hashState = new HashState(seed, seed);

            // Number of full 128-bit blocks of 16 bytes.
            // Possible exclusion of a remainder of up to 15 bytes.
            int nblocks = lengthBytes >> 4; // bytes / 16

            // Process the 128-bit blocks (the body) into the hash
            for (int i = 0; i < nblocks; i++) // 16 bytes per block
            {
                long k1Block = GetLong(key, offsetBytes + (i << 4), 8);     // offsetBytes + 0, 16, 32, ...
                long k2Block = GetLong(key, offsetBytes + (i << 4) + 8, 8); // offsetBytes + 8, 24, 40, ...
                hashState.BlockMix128(k1Block, k2Block);
            }

            // Get the tail index wrt hashed portion, remainder length
            int tail = nblocks << 4; // 16 bytes per block
            int rem = lengthBytes - tail; // remainder bytes: 0,1,...,15

            // Get the tail
            long k1;
            long k2;
            if (rem > 8) // k1 -> whole; k2 -> partial
            {
                k1 = GetLong(key, offsetBytes + tail, 8);
                k2 = GetLong(key, offsetBytes + tail + 8, rem - 8);
            }
            else // k1 -> whole, partial or 0; k2 == 0
            {
                k1 = rem == 0 ? 0 : GetLong(key, offsetBytes + tail, rem);
                k2 = 0;
            }
            // Mix the tail into the hash and return
            return hashState.FinalMix128(k1, k2, lengthBytes);
        }

        // Hash of ReadOnlySpan<byte>

        /// <summary>
        /// Hash the given ReadOnlySpan of bytes.
        /// </summary>
        /// <param name="key">The input ReadOnlySpan. It must be non-empty.</param>
        /// <param name="seed">A long valued seed.</param>
        /// <returns>A 128-bit hash of the input as a long array of size 2.</returns>
        public static long[] Hash(ReadOnlySpan<byte> key, long seed)
        {
            int lengthBytes = key.Length;
            CheckPositive(lengthBytes);
            HashState hashState = new HashState(seed, seed);

            // Number of full 128-bit blocks of 16 bytes.
            // Possible exclusion of a remainder of up to 15 bytes.
            int nblocks = lengthBytes >> 4; // bytes / 16

            // Process the 128-bit blocks (the body) into the hash
            for (int i = 0; i < nblocks; i++) // 16 bytes per block
            {
                long k1Block = GetLong(key, i << 4, 8);     // 0, 16, 32, ...
                long k2Block = GetLong(key, (i << 4) + 8, 8); // 8, 24, 40, ...
                hashState.BlockMix128(k1Block, k2Block);
            }

            // Get the tail index wrt hashed portion, remainder length
            int tail = nblocks << 4; // 16 bytes per block
            int rem = lengthBytes - tail; // remainder bytes: 0,1,...,15

            // Get the tail
            long k1;
            long k2;
            if (rem > 8) // k1 -> whole; k2 -> partial
            {
                k1 = GetLong(key, tail, 8);
                k2 = GetLong(key, tail + 8, rem - 8);
            }
            else // k1 -> whole, partial or 0; k2 == 0
            {
                k1 = rem == 0 ? 0 : GetLong(key, tail, rem);
                k2 = 0;
            }
            // Mix the tail into the hash and return
            return hashState.FinalMix128(k1, k2, lengthBytes);
        }

        // HashState class

        /// <summary>
        /// Common processing of the 128-bit hash state independent of input type.
        /// </summary>
        private sealed class HashState
        {
            private const long C1 = unchecked((long)0x87c37b91114253d5UL);
            private const long C2 = unchecked((long)0x4cf5ad432745937fUL);
            private long h1;
            private long h2;

            public HashState(long h1, long h2)
            {
                this.h1 = h1;
                this.h2 = h2;
            }

            /// <summary>
            /// Block mix (128-bit block) of input key to internal hash state.
            /// </summary>
            /// <param name="k1">Intermediate mix value</param>
            /// <param name="k2">Intermediate mix value</param>
            public void BlockMix128(long k1, long k2)
            {
                h1 ^= MixK1(k1);
                h1 = (long)BitOperations.RotateLeft((ulong)h1, 27);
                h1 += h2;
                h1 = h1 * 5 + 0x52dce729;

                h2 ^= MixK2(k2);
                h2 = (long)BitOperations.RotateLeft((ulong)h2, 31);
                h2 += h1;
                h2 = h2 * 5 + 0x38495ab5;
            }

            public long[] FinalMix128(long k1, long k2, long inputLengthBytes)
            {
                h1 ^= MixK1(k1);
                h2 ^= MixK2(k2);
                h1 ^= inputLengthBytes;
                h2 ^= inputLengthBytes;
                h1 += h2;
                h2 += h1;
                h1 = FinalMix64(h1);
                h2 = FinalMix64(h2);
                h1 += h2;
                h2 += h1;
                return new long[] { h1, h2 };
            }

            /// <summary>
            /// Final self mix of h.
            /// </summary>
            /// <param name="h">Input to final mix</param>
            /// <returns>Mix</returns>
            private static long FinalMix64(long h)
            {
                h ^= (long)((ulong)h >> 33);
                h *= unchecked((long)0xff51afd7ed558ccdUL);
                h ^= (long)((ulong)h >> 33);
                h *= unchecked((long)0xc4ceb9fe1a85ec53UL);
                h ^= (long)((ulong)h >> 33);
                return h;
            }

            /// <summary>
            /// Self mix of k1
            /// </summary>
            /// <param name="k1">Input argument</param>
            /// <returns>Mix</returns>
            private static long MixK1(long k1)
            {
                k1 *= C1;
                k1 = (long)BitOperations.RotateLeft((ulong)k1, 31);
                k1 *= C2;
                return k1;
            }

            /// <summary>
            /// Self mix of k2
            /// </summary>
            /// <param name="k2">Input argument</param>
            /// <returns>Mix</returns>
            private static long MixK2(long k2)
            {
                k2 *= C2;
                k2 = (long)BitOperations.RotateLeft((ulong)k2, 33);
                k2 *= C1;
                return k2;
            }
        }

        // Helper methods

        /// <summary>
        /// Gets a long from the given int array starting at the given int array index and continuing for
        /// remainder (rem) integers. The integers are extracted in little-endian order.
        /// </summary>
        /// <param name="intArr">The given input int array.</param>
        /// <param name="index">Zero-based index from the start of the int array.</param>
        /// <param name="rem">Remainder integers. An integer in the range [1,2].</param>
        /// <returns>long</returns>
        private static long GetLong(int[] intArr, int index, int rem)
        {
            long output = 0L;
            for (int i = rem; i-- > 0;) // i= 1,0
            {
                int v = intArr[index + i];
                output ^= ((long)v & 0xFFFFFFFFL) << (i * 32); // equivalent to |=
            }
            return output;
        }

        /// <summary>
        /// Gets a long from the given char array starting at the given char array index and continuing for
        /// remainder (rem) chars. The chars are extracted in little-endian order.
        /// </summary>
        /// <param name="charArr">The given input char array.</param>
        /// <param name="index">Zero-based index from the start of the char array.</param>
        /// <param name="rem">Remainder chars. An integer in the range [1,4].</param>
        /// <returns>A long</returns>
        private static long GetLong(char[] charArr, int index, int rem)
        {
            long output = 0L;
            for (int i = rem; i-- > 0;) // i= 3,2,1,0
            {
                char c = charArr[index + i];
                output ^= ((long)c & 0xFFFFL) << (i * 16); // equivalent to |=
            }
            return output;
        }

        /// <summary>
        /// Gets a long from the given byte array starting at the given byte array index and continuing for
        /// remainder (rem) bytes. The bytes are extracted in little-endian order.
        /// </summary>
        /// <param name="bArr">The given input byte array.</param>
        /// <param name="index">Zero-based index from the start of the byte array.</param>
        /// <param name="rem">Remainder bytes. An integer in the range [1,8].</param>
        /// <returns>A long</returns>
        private static long GetLong(byte[] bArr, int index, int rem)
        {
            long output = 0L;
            for (int i = rem; i-- > 0;) // i= 7,6,5,4,3,2,1,0
            {
                byte b = bArr[index + i];
                output ^= ((long)b & 0xFFL) << (i * 8); // equivalent to |=
            }
            return output;
        }

        /// <summary>
        /// Gets a long from the given ReadOnlySpan starting at the given index and continuing for
        /// remainder (rem) bytes. The bytes are extracted in little-endian order.
        /// </summary>
        /// <param name="span">The given input ReadOnlySpan.</param>
        /// <param name="index">Zero-based index from the start of the span.</param>
        /// <param name="rem">Remainder bytes. An integer in the range [1,8].</param>
        /// <returns>A long</returns>
        private static long GetLong(ReadOnlySpan<byte> span, int index, int rem)
        {
            if (rem == 8)
            {
                return BitConverter.ToInt64(span.Slice(index, 8));
            }
            long output = 0L;
            for (int i = rem; i-- > 0;) // i= 7,6,5,4,3,2,1,0
            {
                byte b = span[index + i];
                output ^= ((long)b & 0xFFL) << (i * 8); // equivalent to |=
            }
            return output;
        }

        private static void CheckPositive(long size)
        {
            if (size <= 0)
            {
                throw new SketchesArgumentException($"Array size must not be negative or zero: {size}");
            }
        }
    }
}

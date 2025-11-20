// <copyright file="MurmurHash3.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

// Minimally modified from Austin Appleby's code:
//  * Removed MurmurHash3_x86_32 and MurmurHash3_x86_128
//  * Changed input seed in MurmurHash3_x64_128 to uint64_t
//  * Define and use HashState reference to return result
//  * Made entire hash function defined inline
//  * Added compute_seed_hash
//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

// Note - The x86 and x64 versions do _not_ produce the same results, as the
// algorithms are optimized for their respective platforms. You can still
// compile and run any of them on any platform, but your performance with the
// non-native version will be less than optimal.

using System;
using System.Runtime.CompilerServices;

namespace DataSketches.Common
{
    /// <summary>
    /// Hash state for MurmurHash3 128-bit hash output.
    /// </summary>
    public struct HashState
    {
        public ulong H1;
        public ulong H2;
    }

    /// <summary>
    /// MurmurHash3 implementation for 128-bit hashing.
    /// </summary>
    public static class MurmurHash3
    {
        private const ulong C1 = 0x87c37b91114253d5UL;
        private const ulong C2 = 0x4cf5ad432745937fUL;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong RotateLeft64(ulong x, int r)
        {
            return (x << r) | (x >> (64 - r));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong GetBlock64(ReadOnlySpan<byte> data, int index)
        {
            int offset = index * 8;
            return BitConverter.ToUInt64(data.Slice(offset, 8));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong FMix64(ulong k)
        {
            k ^= k >> 33;
            k *= 0xff51afd7ed558ccdUL;
            k ^= k >> 33;
            k *= 0xc4ceb9fe1a85ec53UL;
            k ^= k >> 33;
            return k;
        }

        /// <summary>
        /// Computes MurmurHash3 128-bit hash.
        /// </summary>
        /// <param name="key">Data to hash</param>
        /// <param name="seed">Seed value</param>
        /// <param name="state">Output hash state</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Hash128(ReadOnlySpan<byte> key, ulong seed, out HashState state)
        {
            state.H1 = seed;
            state.H2 = seed;

            int lenBytes = key.Length;

            // Number of full 128-bit blocks of 16 bytes.
            // Possible exclusion of a remainder of up to 15 bytes.
            int nblocks = lenBytes >> 4; // bytes / 16

            // Process the 128-bit blocks (the body) into the hash
            for (int i = 0; i < nblocks; ++i)
            {
                ulong k1 = GetBlock64(key, i * 2 + 0);
                ulong k2 = GetBlock64(key, i * 2 + 1);

                k1 *= C1;
                k1 = RotateLeft64(k1, 31);
                k1 *= C2;
                state.H1 ^= k1;

                state.H1 = RotateLeft64(state.H1, 27);
                state.H1 += state.H2;
                state.H1 = state.H1 * 5 + 0x52dce729;

                k2 *= C2;
                k2 = RotateLeft64(k2, 33);
                k2 *= C1;
                state.H2 ^= k2;

                state.H2 = RotateLeft64(state.H2, 31);
                state.H2 += state.H1;
                state.H2 = state.H2 * 5 + 0x38495ab5;
            }

            // tail
            ReadOnlySpan<byte> tail = key.Slice(nblocks << 4);

            ulong k1Tail = 0;
            ulong k2Tail = 0;

            switch (lenBytes & 15)
            {
                case 15: k2Tail ^= (ulong)tail[14] << 48; goto case 14;
                case 14: k2Tail ^= (ulong)tail[13] << 40; goto case 13;
                case 13: k2Tail ^= (ulong)tail[12] << 32; goto case 12;
                case 12: k2Tail ^= (ulong)tail[11] << 24; goto case 11;
                case 11: k2Tail ^= (ulong)tail[10] << 16; goto case 10;
                case 10: k2Tail ^= (ulong)tail[9] << 8; goto case 9;
                case 9:
                    k2Tail ^= (ulong)tail[8] << 0;
                    k2Tail *= C2;
                    k2Tail = RotateLeft64(k2Tail, 33);
                    k2Tail *= C1;
                    state.H2 ^= k2Tail;
                    goto case 8;
                case 8: k1Tail ^= (ulong)tail[7] << 56; goto case 7;
                case 7: k1Tail ^= (ulong)tail[6] << 48; goto case 6;
                case 6: k1Tail ^= (ulong)tail[5] << 40; goto case 5;
                case 5: k1Tail ^= (ulong)tail[4] << 32; goto case 4;
                case 4: k1Tail ^= (ulong)tail[3] << 24; goto case 3;
                case 3: k1Tail ^= (ulong)tail[2] << 16; goto case 2;
                case 2: k1Tail ^= (ulong)tail[1] << 8; goto case 1;
                case 1:
                    k1Tail ^= (ulong)tail[0] << 0;
                    k1Tail *= C1;
                    k1Tail = RotateLeft64(k1Tail, 31);
                    k1Tail *= C2;
                    state.H1 ^= k1Tail;
                    break;
            }

            // finalization
            state.H1 ^= (ulong)lenBytes;
            state.H2 ^= (ulong)lenBytes;

            state.H1 += state.H2;
            state.H2 += state.H1;

            state.H1 = FMix64(state.H1);
            state.H2 = FMix64(state.H2);

            state.H1 += state.H2;
            state.H2 += state.H1;
        }

        /// <summary>
        /// Computes a 16-bit seed hash from a 64-bit seed.
        /// </summary>
        /// <param name="seed">Input seed</param>
        /// <returns>16-bit seed hash</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort ComputeSeedHash(ulong seed)
        {
            Span<byte> seedBytes = stackalloc byte[8];
            BitConverter.TryWriteBytes(seedBytes, seed);
            Hash128(seedBytes, 0, out HashState hashes);
            return (ushort)(hashes.H1 & 0xffff);
        }
    }
}

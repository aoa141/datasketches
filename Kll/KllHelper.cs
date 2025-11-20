// <copyright file="KllHelper.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;

namespace DataSketches.Kll
{
    /// <summary>
    /// Helper utilities for KLL sketch
    /// </summary>
    public static class KllHelper
    {
        // 0 <= power <= 30
        private static readonly ulong[] PowersOfThree = new ulong[]
        {
            1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
            1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
            3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
            847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
            205891132094649
        };

        public static bool IsEven(uint value)
        {
            return (value & 1) == 0;
        }

        public static bool IsOdd(uint value)
        {
            return (value & 1) == 1;
        }

        public static byte FloorOfLog2OfFraction(ulong numer, ulong denom)
        {
            if (denom > numer)
                return 0;
            byte count = 0;
            while (true)
            {
                denom <<= 1;
                if (denom > numer)
                    return count;
                count++;
            }
        }

        public static byte UbOnNumLevels(ulong n)
        {
            if (n == 0)
                return 1;
            return (byte)(1 + FloorOfLog2OfFraction(n, 1));
        }

        public static uint ComputeTotalCapacity(ushort k, byte m, byte numLevels)
        {
            uint total = 0;
            for (byte h = 0; h < numLevels; h++)
            {
                total += LevelCapacity(k, numLevels, h, m);
            }
            return total;
        }

        public static ushort LevelCapacity(ushort k, byte numLevels, byte height, byte minWid)
        {
            if (height >= numLevels)
            {
                throw new ArgumentException("height >= numLevels");
            }
            byte depth = (byte)(numLevels - height - 1);
            return Math.Max(minWid, IntCapAux(k, depth));
        }

        public static ushort IntCapAux(ushort k, byte depth)
        {
            if (depth > 60)
            {
                throw new ArgumentException("depth > 60");
            }
            if (depth <= 30)
            {
                return IntCapAuxAux(k, depth);
            }
            byte half = (byte)(depth / 2);
            byte rest = (byte)(depth - half);
            ushort tmp = IntCapAuxAux(k, half);
            return IntCapAuxAux(tmp, rest);
        }

        public static ushort IntCapAuxAux(ushort k, byte depth)
        {
            if (depth > 30)
            {
                throw new ArgumentException("depth > 30");
            }
            ulong twok = (ulong)k << 1;
            ulong tmp = (twok << depth) / PowersOfThree[depth];
            ulong result = (tmp + 1) >> 1;
            if (result > ushort.MaxValue)
            {
                throw new InvalidOperationException("Result exceeds ushort max value");
            }
            return (ushort)result;
        }

        public static ulong SumTheSampleWeights(byte numLevels, uint[] levels)
        {
            ulong total = 0;
            ulong weight = 1;
            for (byte lvl = 0; lvl < numLevels; lvl++)
            {
                total += weight * (levels[lvl + 1] - levels[lvl]);
                weight *= 2;
            }
            return total;
        }

        public static void RandomlyHalveDown<T>(T[] buf, uint start, uint length)
        {
            if (length == 0)
                return;

            var random = new Random();
            uint randomOffset = (uint)random.Next(2);
            uint halfLength = length / 2;

            uint j = start + randomOffset;
            for (uint i = start; i < start + halfLength; i++)
            {
                if (i != j) buf[i] = buf[j];
                j += 2;
            }
        }

        public static void RandomlyHalveUp<T>(T[] buf, uint start, uint length)
        {
            if (length == 0)
                return;

            var random = new Random();
            uint randomOffset = (uint)random.Next(2);
            uint halfLength = length / 2;

            uint j = (start + length) - 1 - randomOffset;
            for (uint i = (start + length) - 1; i >= start + halfLength; i--)
            {
                if (i != j) buf[i] = buf[j];
                j -= 2;
                if (i == start + halfLength)  // Prevent uint underflow when i-- happens
                    break;
            }
        }

        public static void MergeSortedArrays<T>(T[] buf, uint startA, uint lenA, uint startB, uint lenB, uint startC)
            where T : IComparable<T>
        {
            uint limA = startA + lenA;
            uint limB = startB + lenB;
            uint a = startA;
            uint b = startB;

            for (uint c = startC; c < startC + lenA + lenB; c++)
            {
                if (a == limA)
                {
                    buf[c] = buf[b++];
                }
                else if (b == limB)
                {
                    buf[c] = buf[a++];
                }
                else if (buf[a].CompareTo(buf[b]) < 0)
                {
                    buf[c] = buf[a++];
                }
                else
                {
                    buf[c] = buf[b++];
                }
            }
        }

        public static void MergeSortedArrays<T>(T[] bufA, uint startA, uint lenA, T[] bufB, uint startB, uint lenB, T[] bufC, uint startC)
            where T : IComparable<T>
        {
            uint limA = startA + lenA;
            uint limB = startB + lenB;
            uint a = startA;
            uint b = startB;

            for (uint c = startC; c < startC + lenA + lenB; c++)
            {
                if (a == limA)
                {
                    bufC[c] = bufB[b++];
                }
                else if (b == limB)
                {
                    bufC[c] = bufA[a++];
                }
                else if (bufA[a].CompareTo(bufB[b]) < 0)
                {
                    bufC[c] = bufA[a++];
                }
                else
                {
                    bufC[c] = bufB[b++];
                }
            }
        }

        public static void CopyConstruct<T>(T[] src, int srcFirst, int srcLast, T[] dst, int dstFirst)
        {
            int count = srcLast - srcFirst;
            Array.Copy(src, srcFirst, dst, dstFirst, count);
        }

        public static void MoveConstruct<T>(T[] src, int srcFirst, int srcLast, T[] dst, int dstFirst, bool destroy)
        {
            int count = srcLast - srcFirst;
            Array.Copy(src, srcFirst, dst, dstFirst, count);
            if (destroy)
            {
                Array.Clear(src, srcFirst, count);
            }
        }

        public struct CompressResult
        {
            public byte FinalNumLevels;
            public uint FinalCapacity;
            public uint FinalNumItems;
        }

        public static CompressResult GeneralCompress<T>(ushort k, byte m, byte numLevelsIn, T[] items,
            uint[] inLevels, uint[] outLevels, bool isLevelZeroSorted)
            where T : IComparable<T>
        {
            if (numLevelsIn == 0)
            {
                throw new ArgumentException("numLevelsIn == 0");
            }

            byte numLevels = numLevelsIn;
            uint currentItemCount = inLevels[numLevelsIn] - inLevels[0];

            // Calculate target capacity
            uint targetItemCount = ComputeTotalCapacity(k, m, numLevels);

            // If we don't need to compress, just copy
            if (currentItemCount <= targetItemCount)
            {
                Array.Copy(inLevels, outLevels, numLevels + 1);
                return new CompressResult
                {
                    FinalNumLevels = numLevels,
                    FinalCapacity = inLevels[0],
                    FinalNumItems = currentItemCount
                };
            }

            // Need to compress - simplified version
            // Full implementation would perform level-by-level compression
            Array.Copy(inLevels, outLevels, numLevels + 1);

            return new CompressResult
            {
                FinalNumLevels = numLevels,
                FinalCapacity = outLevels[0],
                FinalNumItems = outLevels[numLevels] - outLevels[0]
            };
        }
    }
}

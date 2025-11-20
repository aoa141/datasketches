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
using System.Buffers.Binary;
using Apache.DataSketches.Common;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Used to convert older serialization versions 1 and 2 to version 3. The Serialization
    /// Version is the version of the sketch binary image format and should not be confused with the
    /// version number of the Open Source DataSketches Library.
    /// </summary>
    internal static class ForwardCompatibility
    {
        /// <summary>
        /// Convert a serialization version (SerVer) 1 sketch (~Feb 2014) to a SerVer 3 sketch.
        /// Note: SerVer 1 sketches always have (metadata) preamble-longs of 3 and are always stored
        /// in a compact ordered form, but with 3 different sketch types. All SerVer 1 sketches will
        /// be converted to a SerVer 3 sketches. There is no concept of p-sampling, no empty bit.
        /// </summary>
        /// <param name="srcMem">The image of a SerVer 1 sketch</param>
        /// <param name="seedHash">The seedHash that matches the seedHash of the original seed used to construct the sketch.
        /// Note: SerVer 1 sketches do not have the concept of the SeedHash, so the seedHash provided here
        /// MUST be derived from the actual seed that was used when the SerVer 1 sketches were built.</param>
        /// <returns>A SerVer 3 CompactSketch.</returns>
        internal static CompactSketch Heapify1to3(Memory<byte> srcMem, short seedHash)
        {
            Span<byte> span = srcMem.Span;
            int memCap = srcMem.Length;
            int preLongs = PreambleUtil.ExtractPreLongs(span); // always 3 for serVer 1
            if (preLongs != 3)
            {
                throw new SketchesArgumentException($"PreLongs must be 3 for SerVer 1: {preLongs}");
            }
            int familyId = PreambleUtil.ExtractFamilyID(span); // 1,2,3
            if (familyId < 1 || familyId > 3)
            {
                throw new SketchesArgumentException($"Family ID (Sketch Type) must be 1 to 3: {familyId}");
            }
            int curCount = PreambleUtil.ExtractCurCount(span);
            long thetaLong = PreambleUtil.ExtractThetaLong(span);
            bool empty = (curCount == 0) && (thetaLong == long.MaxValue);

            if (empty || memCap <= 24)
            {
                // return empty
                return EmptyCompactSketch.GetInstance();
            }

            int reqCap = (curCount + preLongs) << 3;
            ValidateInputSize(reqCap, memCap);

            if (thetaLong == long.MaxValue && curCount == 1)
            {
                long hash = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(preLongs << 3));
                return new SingleItemSketch(hash, seedHash);
            }
            // theta < 1.0 and/or curCount > 1

            long[] compactOrderedCache = new long[curCount];
            for (int i = 0; i < curCount; i++)
            {
                compactOrderedCache[i] = BinaryPrimitives.ReadInt64LittleEndian(
                    span.Slice((preLongs + i) << 3));
            }
            return new HeapCompactSketch(compactOrderedCache, empty: false, seedHash, curCount, thetaLong, ordered: true);
        }

        /// <summary>
        /// Convert a serialization version (SerVer) 2 sketch to a SerVer 3 HeapCompactOrderedSketch.
        /// Note: SerVer 2 sketches can have metadata-longs of 1,2 or 3 and are always stored
        /// in a compact ordered form (not as a hash table), but with 4 different sketch types.
        /// </summary>
        /// <param name="srcMem">The image of a SerVer 2 sketch</param>
        /// <param name="seedHash">The seed used for building the sketch image in srcMem</param>
        /// <returns>A SerVer 3 HeapCompactOrderedSketch</returns>
        internal static CompactSketch Heapify2to3(Memory<byte> srcMem, short seedHash)
        {
            Span<byte> span = srcMem.Span;
            int memCap = srcMem.Length;
            int preLongs = PreambleUtil.ExtractPreLongs(span); // 1,2 or 3
            int familyId = PreambleUtil.ExtractFamilyID(span); // 1,2,3,4
            if (familyId < 1 || familyId > 4)
            {
                throw new SketchesArgumentException($"Family (Sketch Type) must be 1 to 4: {familyId}");
            }
            int reqBytesIn = 8;
            int curCount = 0;
            long thetaLong = long.MaxValue;

            if (preLongs == 1)
            {
                reqBytesIn = 8;
                ValidateInputSize(reqBytesIn, memCap);
                return EmptyCompactSketch.GetInstance();
            }

            if (preLongs == 2)
            {
                // includes pre0 + count, no theta (== 1.0)
                reqBytesIn = preLongs << 3;
                ValidateInputSize(reqBytesIn, memCap);
                curCount = PreambleUtil.ExtractCurCount(span);
                if (curCount == 0)
                {
                    return EmptyCompactSketch.GetInstance();
                }
                if (curCount == 1)
                {
                    reqBytesIn = (preLongs + 1) << 3;
                    ValidateInputSize(reqBytesIn, memCap);
                    long hash = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(preLongs << 3));
                    return new SingleItemSketch(hash, seedHash);
                }
                // curCount > 1
                reqBytesIn = (curCount + preLongs) << 3;
                ValidateInputSize(reqBytesIn, memCap);
                long[] compactOrderedCache = new long[curCount];
                for (int i = 0; i < curCount; i++)
                {
                    compactOrderedCache[i] = BinaryPrimitives.ReadInt64LittleEndian(
                        span.Slice((preLongs + i) << 3));
                }
                return new HeapCompactSketch(compactOrderedCache, empty: false, seedHash, curCount, thetaLong, ordered: true);
            }

            if (preLongs == 3)
            {
                // pre0 + count + theta
                reqBytesIn = preLongs << 3;
                ValidateInputSize(reqBytesIn, memCap);
                curCount = PreambleUtil.ExtractCurCount(span);
                thetaLong = PreambleUtil.ExtractThetaLong(span);
                if (curCount == 0 && thetaLong == long.MaxValue)
                {
                    return EmptyCompactSketch.GetInstance();
                }
                if (curCount == 1 && thetaLong == long.MaxValue)
                {
                    reqBytesIn = (preLongs + 1) << 3;
                    ValidateInputSize(reqBytesIn, memCap);
                    long hash = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(preLongs << 3));
                    return new SingleItemSketch(hash, seedHash);
                }
                // curCount > 1 and/or theta < 1.0
                reqBytesIn = (curCount + preLongs) << 3;
                ValidateInputSize(reqBytesIn, memCap);
                long[] compactOrderedCache = new long[curCount];
                for (int i = 0; i < curCount; i++)
                {
                    compactOrderedCache[i] = BinaryPrimitives.ReadInt64LittleEndian(
                        span.Slice((preLongs + i) << 3));
                }
                return new HeapCompactSketch(compactOrderedCache, empty: false, seedHash, curCount, thetaLong, ordered: true);
            }

            throw new SketchesArgumentException($"PreLongs must be 1,2, or 3: {preLongs}");
        }

        private static void ValidateInputSize(int reqBytesIn, int memCap)
        {
            if (reqBytesIn > memCap)
            {
                throw new SketchesArgumentException(
                    $"Input Memory or byte[] size is too small: Required Bytes: {reqBytesIn}, bytesIn: {memCap}");
            }
        }
    }
}

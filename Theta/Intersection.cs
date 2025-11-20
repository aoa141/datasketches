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
using System.Linq;
using Apache.DataSketches.Common;
using Apache.DataSketches.ThetaCommon;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// The API for intersection operations
    /// </summary>
    public abstract class Intersection : SetOperation
    {
        public override Family GetFamily()
        {
            return Family.INTERSECTION;
        }

        /// <summary>
        /// Gets the result of this operation as an ordered CompactSketch on the heap.
        /// This does not disturb the underlying data structure of this intersection.
        /// The Intersect(Sketch) method must have been called at least once, otherwise an
        /// exception will be thrown. This is because a virgin Intersection object represents the
        /// Universal Set, which has an infinite number of values.
        /// </summary>
        /// <returns>The result of this operation as an ordered CompactSketch on the heap</returns>
        public CompactSketch GetResult()
        {
            return GetResult(true, null);
        }

        /// <summary>
        /// Gets the result of this operation as a CompactSketch in the given dstMem.
        /// This does not disturb the underlying data structure of this intersection.
        /// The Intersect(Sketch) method must have been called at least once, otherwise an
        /// exception will be thrown. This is because a virgin Intersection object represents the
        /// Universal Set, which has an infinite number of values.
        /// Note that presenting an intersection with an empty sketch sets the internal
        /// state of the intersection to empty = true, and current count = 0. This is consistent with
        /// the mathematical definition of the intersection of any set with the empty set is always empty.
        /// Presenting an intersection with a null argument will throw an exception.
        /// </summary>
        /// <param name="dstOrdered">If true, the result will be ordered</param>
        /// <param name="dstMem">If not null, the result will be stored in this Memory</param>
        /// <returns>The result of this operation as a CompactSketch stored in the given dstMem,
        /// which can be either on or off-heap.</returns>
        public abstract CompactSketch GetResult(bool dstOrdered, Memory<byte>? dstMem);

        /// <summary>
        /// Returns true if there is a valid intersection result available
        /// </summary>
        /// <returns>True if there is a valid intersection result available</returns>
        public abstract bool HasResult();

        /// <summary>
        /// Resets this Intersection for stateful operations only.
        /// The seed remains intact, otherwise reverts to
        /// the Universal Set: theta = 1.0, no retained data and empty = false.
        /// </summary>
        public abstract void Reset();

        /// <summary>
        /// Serialize this intersection to a byte array form.
        /// </summary>
        /// <returns>Byte array of this intersection</returns>
        public abstract byte[] ToByteArray();

        /// <summary>
        /// Intersect the given sketch with the internal state.
        /// This method can be repeatedly called.
        /// If the given sketch is null the internal state becomes the empty sketch.
        /// Theta will become the minimum of thetas seen so far.
        /// </summary>
        /// <param name="sketchIn">The given sketch</param>
        public abstract void Intersect(Sketch sketchIn);

        /// <summary>
        /// Perform intersect set operation on the two given sketch arguments and return the result as an
        /// ordered CompactSketch on the heap.
        /// </summary>
        /// <param name="a">The first sketch argument</param>
        /// <param name="b">The second sketch argument</param>
        /// <returns>An ordered CompactSketch on the heap</returns>
        public CompactSketch Intersect(Sketch a, Sketch b)
        {
            return Intersect(a, b, true, null);
        }

        /// <summary>
        /// Perform intersect set operation on the two given sketches and return the result as a CompactSketch.
        /// </summary>
        /// <param name="a">The first sketch argument</param>
        /// <param name="b">The second sketch argument</param>
        /// <param name="dstOrdered">If true, the result will be ordered</param>
        /// <param name="dstMem">If not null, the result will be stored in this Memory</param>
        /// <returns>The result as a CompactSketch.</returns>
        public abstract CompactSketch Intersect(Sketch a, Sketch b, bool dstOrdered,
            Memory<byte>? dstMem);

        // Restricted

        /// <summary>
        /// Returns the maximum lgArrLongs given the capacity of the Memory.
        /// </summary>
        /// <param name="dstMem">The given Memory</param>
        /// <returns>The maximum lgArrLongs given the capacity of the Memory</returns>
        protected static int GetMaxLgArrLongs(Memory<byte> dstMem)
        {
            int preBytes = CONST_PREAMBLE_LONGS << 3;
            long cap = dstMem.Length;
            return Util.FloorPowerOf2((int)(cap - preBytes)) >> 3;
        }

        protected static void CheckMinSizeMemory(Memory<byte> mem)
        {
            int minBytes = (CONST_PREAMBLE_LONGS << 3) + (8 << ThetaUtil.MIN_LG_ARR_LONGS); // 280
            long cap = mem.Length;
            if (cap < minBytes)
            {
                throw new SketchesArgumentException(
                    $"Memory must be at least {minBytes} bytes. Actual capacity: {cap}");
            }
        }

        /// <summary>
        /// Compact first 2^lgArrLongs of given array
        /// </summary>
        /// <param name="srcCache">Source cache array</param>
        /// <param name="lgArrLongs">The log-base-2 of array longs</param>
        /// <param name="curCount">Must be correct</param>
        /// <param name="thetaLong">The theta long value</param>
        /// <param name="dstOrdered">True if output array must be sorted</param>
        /// <returns>The compacted array</returns>
        internal static long[] CompactCachePart(long[] srcCache, int lgArrLongs,
            int curCount, long thetaLong, bool dstOrdered)
        {
            if (curCount == 0)
            {
                return new long[0];
            }
            long[] cacheOut = new long[curCount];
            int len = 1 << lgArrLongs;
            int j = 0;
            for (int i = 0; i < len; i++)
            {
                long v = srcCache[i];
                if (v <= 0L || v >= thetaLong) { continue; }
                cacheOut[j++] = v;
            }
            System.Diagnostics.Debug.Assert(curCount == j);
            if (dstOrdered)
            {
                Array.Sort(cacheOut);
            }
            return cacheOut;
        }

        protected static void MemChecks(Memory<byte> srcMem)
        {
            // Get Preamble
            // Note: Intersection does not use lgNomLongs (or k), per se.
            // seedHash loaded and checked in private constructor
            Span<byte> span = srcMem.Span;
            int preLongs = PreambleUtil.ExtractPreLongs(span);
            int serVer = PreambleUtil.ExtractSerVer(span);
            int famID = PreambleUtil.ExtractFamilyID(span);
            bool empty = (PreambleUtil.ExtractFlags(span) & PreambleUtil.EMPTY_FLAG_MASK) > 0;
            int curCount = PreambleUtil.ExtractCurCount(span);

            // Checks
            if (preLongs != CONST_PREAMBLE_LONGS)
            {
                throw new SketchesArgumentException(
                    $"Memory PreambleLongs must equal {CONST_PREAMBLE_LONGS}: {preLongs}");
            }
            if (serVer != PreambleUtil.SER_VER)
            {
                throw new SketchesArgumentException($"Serialization Version must equal {PreambleUtil.SER_VER}");
            }
            Family.INTERSECTION.CheckFamilyID(famID);
            if (empty)
            {
                if (curCount != 0)
                {
                    throw new SketchesArgumentException(
                        $"srcMem empty state inconsistent with curCount: {empty},{curCount}");
                }
                // empty = true AND curCount_ = 0: OK
            } // else empty = false, curCount could be anything
        }
    }
}

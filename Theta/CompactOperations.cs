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
using Apache.DataSketches.Common;
using static Apache.DataSketches.Theta.PreambleUtil;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Compact operations utility class
    /// </summary>
    internal static class CompactOperations
    {
        internal static CompactSketch ComponentsToCompact( // No error checking
            long thetaLong,
            int curCount,
            short seedHash,
            bool srcEmpty,
            bool srcCompact,
            bool srcOrdered,
            bool dstOrdered,
            Span<byte> dstMem,
            long[] hashArr) // may not be compacted, ordered or unordered, may be null
        {
            bool direct = dstMem != null && dstMem.Length > 0;
            bool empty = srcEmpty || ((curCount == 0) && (thetaLong == long.MaxValue));
            bool single = (curCount == 1) && (thetaLong == long.MaxValue);
            long[] hashArrOut;

            if (!srcCompact)
            {
                hashArrOut = CompactCache(hashArr, curCount, thetaLong, dstOrdered);
            }
            else
            {
                hashArrOut = hashArr;
            }

            if (!srcOrdered && dstOrdered && !empty && !single)
            {
                Array.Sort(hashArrOut);
            }

            // Note: for empty or single we always output the ordered form.
            bool dstOrderedOut = (empty || single) ? true : dstOrdered;

            if (direct)
            {
                int preLongs = ComputeCompactPreLongs(empty, curCount, thetaLong);
                int flags = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK; // always LE
                flags |= empty ? EMPTY_FLAG_MASK : 0;
                flags |= dstOrderedOut ? ORDERED_FLAG_MASK : 0;
                flags |= single ? SINGLEITEM_FLAG_MASK : 0;

                LoadCompactMemory(hashArrOut, seedHash, curCount, thetaLong, dstMem, (byte)flags, preLongs);
                // Note: DirectCompactSketch not yet fully implemented
                throw new NotImplementedException("DirectCompactSketch not yet implemented");
                //return new DirectCompactSketch(dstMem);
            }
            else
            { // Heap
                if (empty)
                {
                    return EmptyCompactSketch.GetInstance();
                }
                if (single)
                {
                    return new SingleItemSketch(hashArrOut[0], seedHash);
                }
                return new HeapCompactSketch(hashArrOut, empty, seedHash, curCount, thetaLong, dstOrderedOut);
            }
        }

        /// <summary>
        /// Heapify or convert a source Theta Sketch Memory image into a heap or target Memory CompactSketch.
        /// This assumes hashSeed is OK; serVer = 3.
        /// </summary>
        internal static CompactSketch MemoryToCompact(
            ReadOnlySpan<byte> srcMem,
            bool dstOrdered,
            Span<byte> dstMem)
        {
            // Extract Pre0 fields and Flags from srcMem
            int srcPreLongs = ExtractPreLongs(srcMem);
            int srcSerVer = ExtractSerVer(srcMem); // not used
            int srcFamId = ExtractFamilyID(srcMem);
            int srcLgArrLongs = ExtractLgArrLongs(srcMem);
            int srcFlags = ExtractFlags(srcMem);
            short srcSeedHash = (short)ExtractSeedHash(srcMem);

            // srcFlags
            bool srcReadOnlyFlag = (srcFlags & READ_ONLY_FLAG_MASK) > 0;
            bool srcEmptyFlag = (srcFlags & EMPTY_FLAG_MASK) > 0;
            bool srcCompactFlag = (srcFlags & COMPACT_FLAG_MASK) > 0;
            bool srcOrderedFlag = (srcFlags & ORDERED_FLAG_MASK) > 0;
            bool srcSingleFlag = (srcFlags & SINGLEITEM_FLAG_MASK) > 0;

            bool single = srcSingleFlag
                || SingleItemSketch.OtherCheckForSingleItem(srcPreLongs, srcSerVer, srcFamId, srcFlags);

            // Extract pre1 and pre2 fields
            int curCount = single ? 1 : (srcPreLongs > 1) ? ExtractCurCount(srcMem) : 0;
            long thetaLong = (srcPreLongs > 2) ? ExtractThetaLong(srcMem) : long.MaxValue;

            // Do some basic checks...
            if (srcEmptyFlag)
            {
                System.Diagnostics.Debug.Assert((curCount == 0) && (thetaLong == long.MaxValue));
            }
            if (single)
            {
                System.Diagnostics.Debug.Assert((curCount == 1) && (thetaLong == long.MaxValue));
            }
            CheckFamilyAndFlags(srcFamId, srcCompactFlag, srcReadOnlyFlag);

            // Dispatch empty and single cases
            // Note: for empty and single we always output the ordered form.
            bool dstOrderedOut = (srcEmptyFlag || single) ? true : dstOrdered;

            if (srcEmptyFlag)
            {
                if (dstMem != null && dstMem.Length > 0)
                {
                    EmptyCompactSketch.EMPTY_COMPACT_SKETCH_ARR.CopyTo(dstMem);
                    // Note: DirectCompactSketch not yet fully implemented
                    throw new NotImplementedException("DirectCompactSketch not yet implemented");
                    //return new DirectCompactSketch(dstMem);
                }
                else
                {
                    return EmptyCompactSketch.GetInstance();
                }
            }

            if (single)
            {
                long hash = BitConverter.ToInt64(srcMem.Slice(srcPreLongs << 3, 8));
                SingleItemSketch sis = new SingleItemSketch(hash, srcSeedHash);
                if (dstMem != null && dstMem.Length > 0)
                {
                    byte[] sisBytes = sis.ToByteArray();
                    sisBytes.CopyTo(dstMem);
                    // Note: DirectCompactSketch not yet fully implemented
                    throw new NotImplementedException("DirectCompactSketch not yet implemented");
                    //return new DirectCompactSketch(dstMem);
                }
                else
                { // heap
                    return sis;
                }
            }

            // Extract hashArr > 1
            long[] hashArr;
            if (srcCompactFlag)
            {
                hashArr = new long[curCount];
                int offset = srcPreLongs << 3;
                for (int i = 0; i < curCount; i++)
                {
                    hashArr[i] = BitConverter.ToInt64(srcMem.Slice(offset + (i << 3), 8));
                }
            }
            else
            { // update sketch, thus hashTable form
                int srcCacheLen = 1 << srcLgArrLongs;
                long[] tempHashArr = new long[srcCacheLen];
                int offset = srcPreLongs << 3;
                for (int i = 0; i < srcCacheLen; i++)
                {
                    tempHashArr[i] = BitConverter.ToInt64(srcMem.Slice(offset + (i << 3), 8));
                }
                hashArr = CompactCache(tempHashArr, curCount, thetaLong, dstOrderedOut);
            }

            int flagsOut = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK
                | ((dstOrderedOut) ? ORDERED_FLAG_MASK : 0);

            // Load the destination.
            if (dstMem != null && dstMem.Length > 0)
            {
                LoadCompactMemory(hashArr, srcSeedHash, curCount, thetaLong, dstMem,
                    (byte)flagsOut, srcPreLongs);
                // Note: DirectCompactSketch not yet fully implemented
                throw new NotImplementedException("DirectCompactSketch not yet implemented");
                //return new DirectCompactSketch(dstMem);
            }
            else
            { // heap
                return new HeapCompactSketch(hashArr, srcEmptyFlag, srcSeedHash, curCount, thetaLong,
                    dstOrderedOut);
            }
        }

        private static void CheckFamilyAndFlags(
            int srcFamId,
            bool srcCompactFlag,
            bool srcReadOnlyFlag)
        {
            Family srcFamily = FamilyExtensions.IdToFamily(srcFamId);
            if (srcCompactFlag)
            {
                if ((srcFamily == Family.COMPACT) && srcReadOnlyFlag) { return; }
            }
            else
            {
                if (srcFamily == Family.ALPHA) { return; }
                if (srcFamily == Family.QUICKSELECT) { return; }
            }
            throw new SketchesArgumentException(
                "Possible Corruption: Family does not match flags: Family: "
                    + srcFamily.ToString()
                    + ", Compact Flag: " + srcCompactFlag
                    + ", ReadOnly Flag: " + srcReadOnlyFlag);
        }

        // All arguments must be valid and correct including flags.
        // Used as helper to create byte arrays as well as loading Memory for direct compact sketches
        internal static void LoadCompactMemory(
            long[] compactHashArr,
            short seedHash,
            int curCount,
            long thetaLong,
            Span<byte> dstMem,
            byte flags,
            int preLongs)
        {
            System.Diagnostics.Debug.Assert((dstMem != null && dstMem.Length > 0) && (compactHashArr != null));

            int outLongs = preLongs + curCount;
            int outBytes = outLongs << 3;
            int dstBytes = dstMem.Length;

            if (outBytes > dstBytes)
            {
                throw new SketchesArgumentException($"Insufficient Memory: {dstBytes}, Need: {outBytes}");
            }

            byte famID = (byte)Family.COMPACT;

            // The first 8 bytes (pre0)
            InsertPreLongs(dstMem, preLongs);              // RF not used = 0
            InsertSerVer(dstMem, SER_VER);
            InsertFamilyID(dstMem, famID);
            // The following initializes the lgNomLongs and lgArrLongs to 0.
            // They are not used in CompactSketches.
            BitConverter.TryWriteBytes(dstMem.Slice(LG_NOM_LONGS_BYTE, 2), (short)0);
            InsertFlags(dstMem, flags);
            InsertSeedHash(dstMem, seedHash);

            if ((preLongs == 1) && (curCount == 1))
            { // singleItem, theta = 1.0
                BitConverter.TryWriteBytes(dstMem.Slice(8, 8), compactHashArr[0]);
                return;
            }

            if (preLongs > 1)
            {
                InsertCurCount(dstMem, curCount);
                InsertP(dstMem, 1.0f);
            }

            if (preLongs > 2)
            {
                InsertThetaLong(dstMem, thetaLong);
            }

            if (curCount > 0)
            { // theta could be < 1.0.
                int offset = preLongs << 3;
                for (int i = 0; i < curCount; i++)
                {
                    BitConverter.TryWriteBytes(dstMem.Slice(offset + (i << 3), 8), compactHashArr[i]);
                }
            }
            // if prelongs == 3 & curCount == 0, theta could be < 1.0.
        }

        /// <summary>
        /// Copies then compacts, cleans, and may sort the resulting array.
        /// The source cache can be a hash table with interstitial zeros or
        /// "dirty" values, which are hash values greater than theta.
        /// </summary>
        internal static long[] CompactCache(long[] srcCache, int curCount,
            long thetaLong, bool dstOrdered)
        {
            if (curCount == 0)
            {
                return new long[0];
            }

            long[] cacheOut = new long[curCount];
            int len = srcCache.Length;
            int j = 0;

            for (int i = 0; i < len; i++)
            { // scan the full srcCache
                long v = srcCache[i];
                if ((v <= 0L) || (v >= thetaLong)) { continue; } // ignoring zeros or dirty values
                cacheOut[j++] = v;
            }

            if (j < curCount)
            {
                throw new SketchesStateException(
                    "Possible Corruption: curCount parameter is incorrect.");
            }

            if (dstOrdered && (curCount > 1))
            {
                Array.Sort(cacheOut);
            }

            return cacheOut;
        }

        /// <summary>
        /// This corrects a temporary anomalous condition where compact() is called on an UpdateSketch
        /// that was initialized with p &lt; 1.0 and update() was never called.
        /// </summary>
        internal static long CorrectThetaOnCompact(bool empty, int curCount, long thetaLong)
        {
            return (empty && (curCount == 0)) ? long.MaxValue : thetaLong;
        }

        /// <summary>
        /// This checks for the illegal condition where curCount > 0 and the state of
        /// empty = true.
        /// </summary>
        internal static void CheckIllegalCurCountAndEmpty(bool empty, int curCount)
        {
            if (empty && (curCount != 0))
            {
                throw new SketchesStateException("Illegal State: Empty=true and Current Count != 0.");
            }
        }

        /// <summary>
        /// This compute number of preamble longs for a compact sketch.
        /// </summary>
        internal static int ComputeCompactPreLongs(bool empty, int curCount, long thetaLong)
        {
            return (thetaLong < long.MaxValue) ? 3 : empty ? 1 : (curCount > 1) ? 2 : 1;
        }

        /// <summary>
        /// This checks for the singleItem Compact Sketch.
        /// </summary>
        internal static bool IsSingleItem(bool empty, int curCount, long thetaLong)
        {
            return !empty && (curCount == 1) && (thetaLong == long.MaxValue);
        }
    }
}

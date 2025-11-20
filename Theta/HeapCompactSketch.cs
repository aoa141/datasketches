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
using static Apache.DataSketches.Theta.CompactOperations;
using static Apache.DataSketches.Theta.PreambleUtil;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Parent class of the Heap Compact Sketches.
    /// </summary>
    internal class HeapCompactSketch : CompactSketch
    {
        private readonly long thetaLong_;
        private readonly int curCount_;
        private readonly int preLongs_;
        private readonly short seedHash_;
        private readonly bool empty_;
        private readonly bool ordered_;
        private readonly bool singleItem_;
        private readonly long[] cache_;

        /// <summary>
        /// Constructs this sketch from correct, valid components.
        /// </summary>
        /// <param name="cache">in compact form</param>
        /// <param name="empty">The correct Empty state.</param>
        /// <param name="seedHash">The correct Seed Hash.</param>
        /// <param name="curCount">correct value</param>
        /// <param name="thetaLong">The correct thetaLong.</param>
        /// <param name="ordered">true if ordered</param>
        internal HeapCompactSketch(long[] cache, bool empty, short seedHash,
            int curCount, long thetaLong, bool ordered)
        {
            seedHash_ = seedHash;
            curCount_ = curCount;
            empty_ = empty;
            ordered_ = ordered;
            cache_ = cache;
            // Computed
            thetaLong_ = CorrectThetaOnCompact(empty, curCount, thetaLong);
            preLongs_ = ComputeCompactPreLongs(empty, curCount, thetaLong); // considers singleItem
            singleItem_ = IsSingleItem(empty, curCount, thetaLong);
            CheckIllegalCurCountAndEmpty(empty, curCount);
        }

        // Sketch overrides

        public override CompactSketch Compact(bool dstOrdered, Span<byte> dstMem)
        {
            if ((dstMem == null || dstMem.Length == 0) && (dstOrdered == false || ordered_ == dstOrdered))
            {
                return this;
            }
            return ComponentsToCompact(
                GetThetaLong(),
                GetRetainedEntries(true),
                GetSeedHash(),
                IsEmpty(),
                true,
                ordered_,
                dstOrdered,
                dstMem,
                (long[])GetCache().Clone());
        }

        public override int GetCurrentBytes()
        {
            return (preLongs_ + curCount_) << 3;
        }

        public override double GetEstimate()
        {
            return Sketch.Estimate(thetaLong_, curCount_);
        }

        public override int GetRetainedEntries(bool valid)
        {
            return curCount_;
        }

        public override long GetThetaLong()
        {
            return thetaLong_;
        }

        public override bool IsEmpty()
        {
            return empty_;
        }

        public override bool IsOrdered()
        {
            return ordered_;
        }

        public override HashIterator Iterator()
        {
            return new HeapCompactHashIterator(cache_);
        }

        // Restricted methods

        internal override long[] GetCache()
        {
            return cache_;
        }

        internal override int GetCompactPreambleLongs()
        {
            return preLongs_;
        }

        internal override int GetCurrentDataLongs()
        {
            return curCount_;
        }

        internal override int GetCurrentPreambleLongs()
        {
            return preLongs_;
        }

        internal override ReadOnlyMemory<byte>? GetMemory()
        {
            return null;
        }

        internal override short GetSeedHash()
        {
            return seedHash_;
        }

        // Use of Memory is convenient. The byteArray and Memory are loaded simultaneously.
        public override byte[] ToByteArray()
        {
            int bytes = GetCurrentBytes();
            byte[] byteArray = new byte[bytes];
            Span<byte> dstMem = byteArray.AsSpan();

            int emptyBit = IsEmpty() ? EMPTY_FLAG_MASK : 0;
            int orderedBit = ordered_ ? ORDERED_FLAG_MASK : 0;
            int singleItemBit = singleItem_ ? SINGLEITEM_FLAG_MASK : 0;
            byte flags = (byte)(emptyBit | READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK
                | orderedBit | singleItemBit);
            int preLongs = GetCompactPreambleLongs();

            LoadCompactMemory(
                GetCache(),
                GetSeedHash(),
                GetRetainedEntries(true),
                GetThetaLong(),
                dstMem,
                flags,
                preLongs);

            return byteArray;
        }
    }
}

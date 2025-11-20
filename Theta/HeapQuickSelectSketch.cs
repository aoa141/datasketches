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
using System.Diagnostics;
using Apache.DataSketches.Common;
using Apache.DataSketches.ThetaCommon;
using static Apache.DataSketches.Common.Util;
using static Apache.DataSketches.Theta.CompactOperations;
using static Apache.DataSketches.Theta.PreambleUtil;
using static Apache.DataSketches.Theta.UpdateReturnState;
using static Apache.DataSketches.ThetaCommon.QuickSelect;

namespace Apache.DataSketches.Theta
{
    internal class HeapQuickSelectSketch : HeapUpdateSketch
    {
        private readonly Family MY_FAMILY;
        private readonly int preambleLongs_;
        private int lgArrLongs_;
        private int hashTableThreshold_;  // never serialized
        internal int curCount_;
        internal long thetaLong_;
        internal bool empty_;
        private long[] cache_;

        private HeapQuickSelectSketch(int lgNomLongs, long seed, float p,
            ResizeFactor rf, int preambleLongs, Family family)
            : base(lgNomLongs, seed, p, rf)
        {
            preambleLongs_ = preambleLongs;
            MY_FAMILY = family;
        }

        /// <summary>
        /// Construct a new sketch instance on the java heap.
        /// </summary>
        internal HeapQuickSelectSketch(int lgNomLongs, long seed, float p,
            ResizeFactor rf, bool unionGadget)
            : base(lgNomLongs, seed, p, rf)
        {
            // Choose family, preambleLongs
            if (unionGadget)
            {
                preambleLongs_ = Family.UNION.GetMinPreLongs();
                MY_FAMILY = Family.UNION;
            }
            else
            {
                preambleLongs_ = Family.QUICKSELECT.GetMinPreLongs();
                MY_FAMILY = Family.QUICKSELECT;
            }

            lgArrLongs_ = ThetaUtil.StartingSubMultiple(lgNomLongs + 1, rf.GetLg(), ThetaUtil.MIN_LG_ARR_LONGS);
            hashTableThreshold_ = GetHashTableThreshold(lgNomLongs, lgArrLongs_);
            curCount_ = 0;
            thetaLong_ = (long)(p * LONG_MAX_VALUE_AS_DOUBLE);
            empty_ = true; // other flags: bigEndian = readOnly = compact = ordered = false;
            cache_ = new long[1 << lgArrLongs_];
        }

        /// <summary>
        /// Heapify a sketch from a Memory UpdateSketch or Union object containing sketch data.
        /// </summary>
        internal static HeapQuickSelectSketch HeapifyInstance(ReadOnlySpan<byte> srcMem, long seed)
        {
            int preambleLongs = ExtractPreLongs(srcMem);            // byte 0
            int lgNomLongs = ExtractLgNomLongs(srcMem);             // byte 3
            int lgArrLongs = ExtractLgArrLongs(srcMem);             // byte 4

            UpdateSketch.CheckUnionQuickSelectFamily(srcMem, preambleLongs, lgNomLongs);
            UpdateSketch.CheckMemIntegrity(srcMem, seed, preambleLongs, lgNomLongs, lgArrLongs);

            float p = ExtractP(srcMem);                             // bytes 12-15
            int memlgRF = ExtractLgResizeFactor(srcMem);            // byte 0
            ResizeFactor memRF = ResizeFactorExtensions.GetRF(memlgRF);
            int familyID = ExtractFamilyID(srcMem);
            Family family = FamilyExtensions.IdToFamily(familyID);

            if (UpdateSketch.IsResizeFactorIncorrect(srcMem, lgNomLongs, lgArrLongs))
            {
                memRF = ResizeFactor.X2; // X2 always works.
            }

            HeapQuickSelectSketch hqss = new HeapQuickSelectSketch(lgNomLongs, seed, p, memRF,
                preambleLongs, family);
            hqss.lgArrLongs_ = lgArrLongs;
            hqss.hashTableThreshold_ = GetHashTableThreshold(lgNomLongs, lgArrLongs);
            hqss.curCount_ = ExtractCurCount(srcMem);
            hqss.thetaLong_ = ExtractThetaLong(srcMem);
            hqss.empty_ = IsEmptyFlag(srcMem);
            hqss.cache_ = new long[1 << lgArrLongs];

            // Read in as hash table
            int offset = preambleLongs << 3;
            for (int i = 0; i < (1 << lgArrLongs); i++)
            {
                hqss.cache_[i] = BitConverter.ToInt64(srcMem.Slice(offset + (i << 3), 8));
            }

            return hqss;
        }

        // Sketch overrides

        public override double GetEstimate()
        {
            return Sketch.Estimate(thetaLong_, curCount_);
        }

        public override Family GetFamily()
        {
            return MY_FAMILY;
        }

        public override int GetRetainedEntries(bool valid)
        {
            return curCount_;
        }

        public override long GetThetaLong()
        {
            return empty_ ? long.MaxValue : thetaLong_;
        }

        public override bool IsEmpty()
        {
            return empty_;
        }

        public override HashIterator Iterator()
        {
            return new HeapHashIterator(cache_, thetaLong_);
        }

        public override byte[] ToByteArray()
        {
            return ToByteArray(preambleLongs_, (byte)MY_FAMILY.GetID());
        }

        public override bool IsReadOnly()
        {
            return false;
        }

        public override bool HasMemory()
        {
            return false;
        }

        // UpdateSketch overrides

        public override UpdateSketch Rebuild()
        {
            if (GetRetainedEntries(true) > (1 << GetLgNomLongs()))
            {
                QuickSelectAndRebuild();
            }
            return this;
        }

        public override void Reset()
        {
            ResizeFactor rf = GetResizeFactor();
            int lgArrLongsSM = ThetaUtil.StartingSubMultiple(lgNomLongs_ + 1, rf.GetLg(), ThetaUtil.MIN_LG_ARR_LONGS);

            if (lgArrLongsSM == lgArrLongs_)
            {
                int arrLongs = cache_.Length;
                Debug.Assert((1 << lgArrLongs_) == arrLongs);
                Array.Fill(cache_, 0L);
            }
            else
            {
                cache_ = new long[1 << lgArrLongsSM];
                lgArrLongs_ = lgArrLongsSM;
            }

            hashTableThreshold_ = GetHashTableThreshold(lgNomLongs_, lgArrLongs_);
            empty_ = true;
            curCount_ = 0;
            thetaLong_ = (long)(GetP() * LONG_MAX_VALUE_AS_DOUBLE);
        }

        // Restricted methods

        internal override long[] GetCache()
        {
            return cache_;
        }

        internal override int GetCompactPreambleLongs()
        {
            return ComputeCompactPreLongs(empty_, curCount_, thetaLong_);
        }

        internal override int GetCurrentDataLongs()
        {
            return 1 << lgArrLongs_;
        }

        internal override int GetCurrentPreambleLongs()
        {
            return preambleLongs_;
        }

        // Only used by ConcurrentHeapThetaBuffer & Test
        internal int GetHashTableThreshold()
        {
            return hashTableThreshold_;
        }

        internal override int GetLgArrLongs()
        {
            return lgArrLongs_;
        }

        internal override ReadOnlyMemory<byte>? GetMemory()
        {
            return null;
        }

        internal override UpdateReturnState HashUpdate(long hash)
        {
            HashOperations.CheckHashCorruption(hash);
            empty_ = false;

            // The over-theta test
            if (HashOperations.ContinueCondition(thetaLong_, hash))
            {
                return RejectedOverTheta; // signal that hash was rejected due to theta.
            }

            // The duplicate test
            if (HashOperations.HashSearchOrInsert(cache_, lgArrLongs_, hash) >= 0)
            {
                return RejectedDuplicate; // Duplicate, not inserted
            }

            // Insertion occurred, must increment curCount
            curCount_++;

            if (IsOutOfSpace(curCount_))
            { // we need to do something, we are out of space
                // must rebuild or resize
                if (lgArrLongs_ <= lgNomLongs_)
                { // resize
                    ResizeCache();
                    return InsertedCountIncrementedResized;
                }
                // Already at tgt size, must rebuild
                Debug.Assert(lgArrLongs_ == (lgNomLongs_ + 1), $"lgArr: {lgArrLongs_}, lgNom: {lgNomLongs_}");
                QuickSelectAndRebuild(); // Changes thetaLong_, curCount_, reassigns cache
                return InsertedCountIncrementedRebuilt;
            }

            return InsertedCountIncremented;
        }

        internal override bool IsDirty()
        {
            return false;
        }

        internal override bool IsOutOfSpace(int numEntries)
        {
            return numEntries > hashTableThreshold_;
        }

        // Must resize. Changes lgArrLongs_, cache_, hashTableThreshold;
        // theta and count don't change.
        // Used by hashUpdate()
        private void ResizeCache()
        {
            ResizeFactor rf = GetResizeFactor();
            int lgMaxArrLongs = lgNomLongs_ + 1;
            int lgDeltaLongs = lgMaxArrLongs - lgArrLongs_;
            int lgResizeFactor = Math.Max(Math.Min(rf.GetLg(), lgDeltaLongs), 1); // rf_.lg() could be 0
            lgArrLongs_ += lgResizeFactor; // new arr size

            long[] tgtArr = new long[1 << lgArrLongs_];
            int newCount = HashOperations.HashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);

            Debug.Assert(newCount == curCount_);  // Assumes no dirty values.
            curCount_ = newCount;

            cache_ = tgtArr;
            hashTableThreshold_ = GetHashTableThreshold(lgNomLongs_, lgArrLongs_);
        }

        // Array stays the same size. Changes theta and thus count
        private void QuickSelectAndRebuild()
        {
            int arrLongs = 1 << lgArrLongs_; // generally 2 * k

            int pivot = (1 << lgNomLongs_) + 1; // pivot for QS = k + 1

            thetaLong_ = SelectExcludingZeros(cache_, curCount_, pivot); // messes up the cache_

            // Now we rebuild to clean up dirty data, update count, reconfigure as a hash table
            long[] tgtArr = new long[arrLongs];
            curCount_ = HashOperations.HashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
            cache_ = tgtArr;
            // hashTableThreshold stays the same
        }

        /// <summary>
        /// Returns the cardinality limit given the current size of the hash table array.
        /// </summary>
        private static int GetHashTableThreshold(int lgNomLongs, int lgArrLongs)
        {
            double fraction = (lgArrLongs <= lgNomLongs) ? ThetaUtil.RESIZE_THRESHOLD : ThetaUtil.REBUILD_THRESHOLD;
            return (int)(fraction * (1 << lgArrLongs));
        }
    }
}

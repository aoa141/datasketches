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
using Apache.DataSketches.ThetaCommon;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Shared code for the HeapUnion and DirectUnion implementations.
    /// </summary>
    internal class UnionImpl : Union
    {
        /// <summary>
        /// Although the gadget object is initially an UpdateSketch, in the context of a Union it is used
        /// as a specialized buffer that happens to leverage much of the machinery of an UpdateSketch.
        /// However, in this context some of the key invariants of the sketch algorithm are intentionally
        /// violated as an optimization. As a result this object cannot be considered as an UpdateSketch
        /// and should never be exported as an UpdateSketch. Its internal state is not necessarily
        /// finalized and may contain garbage. Also its internal concept of "nominal entries" or "k" can
        /// be meaningless. It is private for very good reasons.
        /// </summary>
        private readonly UpdateSketch _gadget;
        private readonly short _expectedSeedHash; // eliminates having to compute the seedHash on every union
        private long _unionThetaLong; // when on-heap, this is the only copy
        private bool _unionEmpty;  // when on-heap, this is the only copy

        private UnionImpl(UpdateSketch gadget, long seed)
        {
            _gadget = gadget;
            _expectedSeedHash = ThetaUtil.ComputeSeedHash(seed);
        }

        /// <summary>
        /// Construct a new Union SetOperation on the heap.
        /// Called by SetOperationBuilder.
        /// </summary>
        /// <param name="lgNomLongs">The log-base-2 of nominal entries</param>
        /// <param name="seed">The hash seed</param>
        /// <param name="p">The sampling probability</param>
        /// <param name="rf">The resize factor</param>
        /// <returns>Instance of this sketch</returns>
        internal static UnionImpl InitNewHeapInstance(
            int lgNomLongs,
            long seed,
            float p,
            ResizeFactor rf)
        {
            // Create with UNION family
            UpdateSketch gadget = new HeapQuickSelectSketch(lgNomLongs, seed, p, rf, unionGadget: true);
            UnionImpl unionImpl = new UnionImpl(gadget, seed);
            unionImpl._unionThetaLong = gadget.GetThetaLong();
            unionImpl._unionEmpty = gadget.IsEmpty();
            return unionImpl;
        }

        /// <summary>
        /// Construct a new Direct Union in the off-heap destination Memory.
        /// Called by SetOperationBuilder.
        /// </summary>
        /// <param name="lgNomLongs">The log-base-2 of nominal entries</param>
        /// <param name="seed">The hash seed</param>
        /// <param name="p">The sampling probability</param>
        /// <param name="rf">The resize factor</param>
        /// <param name="dstMem">The given Memory object destination. It will be cleared prior to use.</param>
        /// <returns>This class</returns>
        internal static UnionImpl InitNewDirectInstance(
            int lgNomLongs,
            long seed,
            float p,
            ResizeFactor rf,
            Memory<byte> dstMem)
        {
            // Note: DirectQuickSelectSketch not yet implemented in C# port
            // This would require porting DirectQuickSelectSketch and MemoryRequestServer
            throw new NotImplementedException(
                "Direct (off-heap) Union is not yet implemented in the C# port. Use heap-based Union.");
        }

        /// <summary>
        /// Heapify a Union from a Memory Union object containing data.
        /// Called by SetOperation.
        /// </summary>
        /// <param name="srcMem">The source Memory Union object.</param>
        /// <param name="expectedSeed">The seed used to validate the given Memory image.</param>
        /// <returns>This class</returns>
        internal static UnionImpl HeapifyInstance(Memory<byte> srcMem, long expectedSeed)
        {
            Family.UNION.CheckFamilyID(PreambleUtil.ExtractFamilyID(srcMem.Span));
            UpdateSketch gadget = HeapQuickSelectSketch.HeapifyInstance(srcMem.Span, expectedSeed);
            UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
            unionImpl._unionThetaLong = PreambleUtil.ExtractUnionThetaLong(srcMem.Span);
            unionImpl._unionEmpty = PreambleUtil.IsEmptyFlag(srcMem.Span);
            return unionImpl;
        }

        /// <summary>
        /// Wrap a Union object around a Union Memory object containing data.
        /// Called by SetOperation.
        /// </summary>
        /// <param name="srcMem">The source Memory object.</param>
        /// <param name="expectedSeed">The seed used to validate the given Memory image.</param>
        /// <param name="readOnly">If true, the memory is read-only</param>
        /// <returns>This class</returns>
        internal static UnionImpl WrapInstance(Memory<byte> srcMem, long expectedSeed, bool readOnly)
        {
            // Note: Direct implementations not yet ported to C#
            // For now, heapify the data
            if (readOnly)
            {
                // Read-only wrap - heapify for now
                return HeapifyInstance(srcMem, expectedSeed);
            }
            else
            {
                // Writable - heapify for now
                return HeapifyInstance(srcMem, expectedSeed);
            }
        }

        public override int GetCurrentBytes()
        {
            return _gadget.GetCurrentBytes();
        }

        public override int GetMaxUnionBytes()
        {
            int lgK = _gadget.GetLgNomLongs();
            return (16 << lgK) + (Family.UNION.GetMaxPreLongs() << 3);
        }

        public override CompactSketch GetResult()
        {
            return GetResult(true, null);
        }

        public override CompactSketch GetResult(bool dstOrdered, Memory<byte>? dstMem)
        {
            Span<byte> dstMemSpan = dstMem.HasValue ? dstMem.Value.Span : Span<byte>.Empty;
            int gadgetCurCount = _gadget.GetRetainedEntries(valid: true);
            int k = 1 << _gadget.GetLgNomLongs();
            long[] gadgetCacheCopy = _gadget.GetCache();

            // Pull back to k
            long curGadgetThetaLong = _gadget.GetThetaLong();
            long adjGadgetThetaLong = gadgetCurCount > k
                ? QuickSelect.SelectExcludingZeros(gadgetCacheCopy, gadgetCurCount, k + 1)
                : curGadgetThetaLong;

            // Finalize Theta and curCount
            long unionThetaLong = _unionThetaLong;

            long minThetaLong = Math.Min(Math.Min(curGadgetThetaLong, adjGadgetThetaLong), unionThetaLong);
            int curCountOut = minThetaLong < curGadgetThetaLong
                ? HashOperations.Count(gadgetCacheCopy, minThetaLong)
                : gadgetCurCount;

            // Compact the cache
            long[] compactCacheOut =
                CompactOperations.CompactCache(gadgetCacheCopy, curCountOut, minThetaLong, dstOrdered);
            bool empty = _gadget.IsEmpty() && _unionEmpty;
            short seedHash = _gadget.GetSeedHash();
            return CompactOperations.ComponentsToCompact(
                minThetaLong, curCountOut, seedHash, empty, srcCompact: true, srcOrdered: dstOrdered,
                dstOrdered, dstMemSpan, compactCacheOut);
        }

        public override bool HasMemory()
        {
            return false; // Heap implementation
        }

        public override bool IsDirect()
        {
            return false; // Heap implementation
        }

        public override bool IsSameResource(Memory<byte> that)
        {
            return false; // Heap implementation
        }

        public override void Reset()
        {
            _gadget.Reset();
            _unionThetaLong = _gadget.GetThetaLong();
            _unionEmpty = _gadget.IsEmpty();
        }

        public override byte[] ToByteArray()
        {
            byte[] gadgetByteArr = _gadget.ToByteArray();
            Span<byte> mem = gadgetByteArr;
            PreambleUtil.InsertUnionThetaLong(mem, _unionThetaLong);
            if (_gadget.IsEmpty() != _unionEmpty)
            {
                PreambleUtil.ClearEmpty(mem);
                _unionEmpty = false;
            }
            return gadgetByteArr;
        }

        public override CompactSketch Combine(Sketch sketchA, Sketch sketchB, bool dstOrdered,
            Memory<byte>? dstMem)
        {
            Reset();
            Update(sketchA);
            Update(sketchB);
            CompactSketch csk = GetResult(dstOrdered, dstMem);
            Reset();
            return csk;
        }

        public override void Update(Sketch sketchIn)
        {
            // UNION Empty Rule: AND the empty states.

            if (sketchIn == null || sketchIn.IsEmpty())
            {
                // null and empty is interpreted as (Theta = 1.0, count = 0, empty = T). Nothing changes
                return;
            }
            // sketchIn is valid and not empty
            ThetaUtil.CheckSeedHashes(_expectedSeedHash, sketchIn.GetSeedHash());
            if (sketchIn is SingleItemSketch)
            {
                _gadget.HashUpdate(sketchIn.GetCache()[0]);
                return;
            }
            Sketch.CheckSketchAndMemoryFlags(sketchIn);

            _unionThetaLong = Math.Min(Math.Min(_unionThetaLong, sketchIn.GetThetaLong()), _gadget.GetThetaLong()); // Theta rule
            _unionEmpty = false;
            int curCountIn = sketchIn.GetRetainedEntries(valid: true);
            if (curCountIn > 0)
            {
                if (sketchIn.IsOrdered() && sketchIn is CompactSketch)
                {
                    // Ordered, thus compact - Use early stop
                    long[] cacheIn = sketchIn.GetCache(); // not a copy!
                    for (int i = 0; i < curCountIn; i++)
                    {
                        long hashIn = cacheIn[i];
                        if (hashIn >= _unionThetaLong) { break; } // "early stop"
                        _gadget.HashUpdate(hashIn); // backdoor update, hash function is bypassed
                    }
                }
                else
                {
                    // either not-ordered compact or Hash Table form. A HT may have dirty values.
                    long[] cacheIn = sketchIn.GetCache(); // if off-heap this will be a copy
                    int arrLongs = cacheIn.Length;
                    for (int i = 0, c = 0; i < arrLongs && c < curCountIn; i++)
                    {
                        long hashIn = cacheIn[i];
                        if (hashIn <= 0L || hashIn >= _unionThetaLong) { continue; } // rejects dirty values
                        _gadget.HashUpdate(hashIn); // backdoor update, hash function is bypassed
                        c++; // ensures against invalid state inside the incoming sketch
                    }
                }
            }
            _unionThetaLong = Math.Min(_unionThetaLong, _gadget.GetThetaLong()); // Theta rule with gadget
        }

        public override void Update(Memory<byte> skMem)
        {
            if (skMem.IsEmpty) { return; }
            int cap = skMem.Length;
            if (cap < 16) { return; } // empty or garbage
            Span<byte> span = skMem.Span;
            int serVer = PreambleUtil.ExtractSerVer(span);
            int fam = PreambleUtil.ExtractFamilyID(span);

            if (serVer == 4)
            {
                // compressed ordered compact
                // performance can be improved by decompression while performing the union
                // potentially only partial decompression might be needed
                ThetaUtil.CheckSeedHashes(_expectedSeedHash, (short)PreambleUtil.ExtractSeedHash(span));
                CompactSketch csk = CompactSketch.Wrap(span);
                Update(csk);
                return;
            }
            if (serVer == 3)
            {
                // The OpenSource sketches (Aug 4, 2015) starts with serVer = 3
                if (fam < 1 || fam > 3)
                {
                    throw new SketchesArgumentException(
                        $"Family must be Alpha, QuickSelect, or Compact: {FamilyExtensions.IdToFamily(fam)}");
                }
                ProcessVer3(skMem);
                return;
            }
            if (serVer == 2)
            {
                // older Sketch, which is compact and ordered
                ThetaUtil.CheckSeedHashes(_expectedSeedHash, (short)PreambleUtil.ExtractSeedHash(span));
                CompactSketch csk = ForwardCompatibility.Heapify2to3(skMem, _expectedSeedHash);
                Update(csk);
                return;
            }
            if (serVer == 1)
            {
                // much older Sketch, which is compact and ordered, no seedHash
                CompactSketch csk = ForwardCompatibility.Heapify1to3(skMem, _expectedSeedHash);
                Update(csk);
                return;
            }

            throw new SketchesArgumentException($"SerVer is unknown: {serVer}");
        }

        // Has seedHash, p, could have 0 entries & theta < 1.0,
        // could be unordered, ordered, compact, or not compact,
        // could be Alpha, QuickSelect, or Compact.
        private void ProcessVer3(Memory<byte> skMem)
        {
            Span<byte> span = skMem.Span;
            int preLongs = PreambleUtil.ExtractPreLongs(span);

            if (preLongs == 1)
            {
                if (SingleItemSketch.OtherCheckForSingleItem(span))
                {
                    long hash = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(8));
                    _gadget.HashUpdate(hash);
                    return;
                }
                return; // empty
            }
            ThetaUtil.CheckSeedHashes(_expectedSeedHash, (short)PreambleUtil.ExtractSeedHash(span));
            int curCountIn;
            long thetaLongIn;

            if (preLongs == 2)
            {
                // exact mode
                curCountIn = PreambleUtil.ExtractCurCount(span);
                if (curCountIn == 0) { return; } // should be > 0, but if it is 0 return empty anyway.
                thetaLongIn = long.MaxValue;
            }
            else
            {
                // prelongs == 3
                // curCount may be 0 (e.g., from intersection); but sketch cannot be empty.
                curCountIn = PreambleUtil.ExtractCurCount(span);
                thetaLongIn = PreambleUtil.ExtractThetaLong(span);
            }

            _unionThetaLong = Math.Min(Math.Min(_unionThetaLong, thetaLongIn), _gadget.GetThetaLong()); // theta rule
            _unionEmpty = false;
            int flags = PreambleUtil.ExtractFlags(span);
            bool ordered = (flags & PreambleUtil.ORDERED_FLAG_MASK) != 0;
            if (ordered)
            {
                // must be compact
                for (int i = 0; i < curCountIn; i++)
                {
                    int offsetBytes = (preLongs + i) << 3;
                    long hashIn = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(offsetBytes));
                    if (hashIn >= _unionThetaLong) { break; } // "early stop"
                    _gadget.HashUpdate(hashIn); // backdoor update, hash function is bypassed
                }
            }
            else
            {
                // not-ordered, could be compact or hash-table form
                bool compact = (flags & PreambleUtil.COMPACT_FLAG_MASK) != 0;
                int size = compact ? curCountIn : 1 << PreambleUtil.ExtractLgArrLongs(span);

                for (int i = 0; i < size; i++)
                {
                    int offsetBytes = (preLongs + i) << 3;
                    long hashIn = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(offsetBytes));
                    if (hashIn <= 0L || hashIn >= _unionThetaLong) { continue; }
                    _gadget.HashUpdate(hashIn); // backdoor update, hash function is bypassed
                }
            }

            _unionThetaLong = Math.Min(_unionThetaLong, _gadget.GetThetaLong()); // sync thetaLongs
        }

        public override void Update(long datum)
        {
            _gadget.Update(datum);
        }

        public override void Update(double datum)
        {
            _gadget.Update(datum);
        }

        public override void Update(string datum)
        {
            _gadget.Update(datum);
        }

        public override void Update(byte[] data)
        {
            _gadget.Update(data);
        }

        public override void Update(ReadOnlySpan<byte> data)
        {
            _gadget.Update(data.ToArray());
        }

        public override void Update(char[] data)
        {
            _gadget.Update(data);
        }

        public override void Update(int[] data)
        {
            _gadget.Update(data);
        }

        public override void Update(long[] data)
        {
            _gadget.Update(data);
        }

        // Restricted

        internal override long[] GetCache()
        {
            return _gadget.GetCache();
        }

        internal override int GetRetainedEntries()
        {
            return _gadget.GetRetainedEntries(valid: true);
        }

        internal override short GetSeedHash()
        {
            return _gadget.GetSeedHash();
        }

        internal override long GetThetaLong()
        {
            return Math.Min(_unionThetaLong, _gadget.GetThetaLong());
        }

        internal override bool IsEmpty()
        {
            return _gadget.IsEmpty() && _unionEmpty;
        }

        public override Family GetFamily()
        {
            return Family.UNION;
        }
    }
}

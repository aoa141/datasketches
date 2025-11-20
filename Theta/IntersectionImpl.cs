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
    /// Intersection operation for Theta Sketches.
    /// This implementation uses data either on-heap or off-heap in a given Memory
    /// that is owned and managed by the caller.
    /// The off-heap Memory, which if managed properly, will greatly reduce the need for
    /// the JVM to perform garbage collection.
    /// </summary>
    internal class IntersectionImpl : Intersection
    {
        protected readonly short _seedHash;
        protected readonly bool _readOnly; // True if this sketch is to be treated as read only

        // Note: Intersection does not use lgNomLongs or k, per se.
        protected int _lgArrLongs; // current size of hash table
        protected int _curCount; // curCount of HT, if < 0 means Universal Set (US) is true
        protected long _thetaLong;
        protected bool _empty; // A virgin intersection represents the Universal Set, so empty is FALSE!
        protected long[] _hashTable; // retained entries of the intersection, on-heap only.

        /// <summary>
        /// Constructor: Sets the class finals and computes, sets and checks the seedHash.
        /// </summary>
        /// <param name="seed">Used to validate incoming sketch arguments.</param>
        /// <param name="readOnly">True if memory is to be treated as read only.</param>
        protected IntersectionImpl(long seed, bool readOnly)
        {
            _readOnly = readOnly;
            _seedHash = ThetaUtil.ComputeSeedHash(seed);
        }

        /// <summary>
        /// Factory: Construct a new Intersection target on the heap.
        /// Called by SetOperationBuilder, test.
        /// </summary>
        /// <param name="seed">The hash seed</param>
        /// <returns>A new IntersectionImpl on the heap</returns>
        internal static IntersectionImpl InitNewHeapInstance(long seed)
        {
            IntersectionImpl impl = new IntersectionImpl(seed, readOnly: false);
            impl.HardReset();
            return impl;
        }

        /// <summary>
        /// Factory: Construct a new Intersection target direct to the given destination Memory.
        /// Called by SetOperationBuilder, test.
        /// </summary>
        /// <param name="seed">The hash seed</param>
        /// <param name="dstMem">Destination Memory</param>
        /// <returns>A new IntersectionImpl that may be off-heap</returns>
        internal static IntersectionImpl InitNewDirectInstance(long seed, Memory<byte> dstMem)
        {
            // Note: Direct memory operations not yet implemented in C# port
            throw new NotImplementedException(
                "Direct (off-heap) Intersection is not yet implemented in the C# port. Use heap-based Intersection.");
        }

        /// <summary>
        /// Factory: Heapify an intersection target from a Memory image containing data.
        /// </summary>
        /// <param name="srcMem">The source Memory object.</param>
        /// <param name="seed">The hash seed</param>
        /// <returns>An IntersectionImpl instance on the heap</returns>
        internal static IntersectionImpl HeapifyInstance(Memory<byte> srcMem, long seed)
        {
            IntersectionImpl impl = new IntersectionImpl(seed, readOnly: false);
            MemChecks(srcMem);

            Span<byte> span = srcMem.Span;
            // Initialize
            impl._lgArrLongs = PreambleUtil.ExtractLgArrLongs(span);
            impl._curCount = PreambleUtil.ExtractCurCount(span);
            impl._thetaLong = PreambleUtil.ExtractThetaLong(span);
            impl._empty = (PreambleUtil.ExtractFlags(span) & PreambleUtil.EMPTY_FLAG_MASK) > 0;
            if (!impl._empty)
            {
                if (impl._curCount > 0)
                {
                    impl._hashTable = new long[1 << impl._lgArrLongs];
                    int preBytes = CONST_PREAMBLE_LONGS << 3;
                    for (int i = 0; i < (1 << impl._lgArrLongs); i++)
                    {
                        impl._hashTable[i] = BinaryPrimitives.ReadInt64LittleEndian(
                            span.Slice(preBytes + (i << 3)));
                    }
                }
            }
            return impl;
        }

        /// <summary>
        /// Factory: Wrap an Intersection target around the given source Memory containing intersection data.
        /// </summary>
        /// <param name="srcMem">The source Memory image.</param>
        /// <param name="seed">The hash seed</param>
        /// <param name="readOnly">True if memory is to be treated as read only</param>
        /// <returns>An IntersectionImpl that wraps a source Memory that contains an Intersection image</returns>
        internal static IntersectionImpl WrapInstance(
            Memory<byte> srcMem,
            long seed,
            bool readOnly)
        {
            // For now, heapify the data (Direct memory not yet implemented)
            return HeapifyInstance(srcMem, seed);
        }

        public override CompactSketch Intersect(Sketch a, Sketch b, bool dstOrdered,
            Memory<byte>? dstMem)
        {
            if (_readOnly) { throw new SketchesReadOnlyException(); }
            HardReset();
            Intersect(a);
            Intersect(b);
            CompactSketch csk = GetResult(dstOrdered, dstMem);
            HardReset();
            return csk;
        }

        public override void Intersect(Sketch sketchIn)
        {
            if (sketchIn == null)
            {
                throw new SketchesArgumentException("Intersection argument must not be null.");
            }
            if (_readOnly) { throw new SketchesReadOnlyException(); }
            if (_empty || sketchIn.IsEmpty())
            {
                // empty rule
                // Because of the def of null above and the Empty Rule (which is OR), empty_ must be true.
                // Whatever the current internal state, we make our local empty.
                ResetToEmpty();
                return;
            }
            ThetaUtil.CheckSeedHashes(_seedHash, sketchIn.GetSeedHash());
            // Set minTheta
            _thetaLong = Math.Min(_thetaLong, sketchIn.GetThetaLong()); // Theta rule
            _empty = false;

            // The truth table for the following state machine. MinTheta is set above.
            // Incoming sketch is not null and not empty, but could have 0 count and Theta < 1.0
            //   Case  curCount  sketchInEntries | Actions
            //     1      <0            0        | First intersect, set curCount = 0; HT = null; minTh; exit
            //     2       0            0        | set curCount = 0; HT = null; minTh; exit
            //     3      >0            0        | set curCount = 0; HT = null; minTh; exit
            //     4                             | Not used
            //     5      <0           >0        | First intersect, clone SketchIn; exit
            //     6       0           >0        | set curCount = 0; HT = null; minTh; exit
            //     7      >0           >0        | Perform full intersect
            int sketchInEntries = sketchIn.GetRetainedEntries(valid: true);

            // states 1,2,3,6
            if (_curCount == 0 || sketchInEntries == 0)
            {
                _curCount = 0;
                _hashTable = null; // No need for a HT. Don't bother clearing mem if valid
            }
            // state 5
            else if (_curCount < 0 && sketchInEntries > 0)
            {
                _curCount = sketchIn.GetRetainedEntries(valid: true);
                int requiredLgArrLongs = HashOperations.MinLgHashTableSize(_curCount, ThetaUtil.REBUILD_THRESHOLD);
                _lgArrLongs = requiredLgArrLongs;

                // On the heap, allocate a HT
                _hashTable = new long[1 << _lgArrLongs];
                MoveDataToTgt(sketchIn.GetCache(), _curCount);
            }
            // state 7
            else if (_curCount > 0 && sketchInEntries > 0)
            {
                // Sets resulting hashTable, curCount and adjusts lgArrLongs
                PerformIntersect(sketchIn);
            }
        }

        public override CompactSketch GetResult(bool dstOrdered, Memory<byte>? dstMem)
        {
            if (_curCount < 0)
            {
                throw new SketchesStateException(
                    "Calling getResult() with no intervening intersections would represent the infinite set, " +
                    "which is not a legal result.");
            }
            Span<byte> dstMemSpan = dstMem.HasValue ? dstMem.Value.Span : Span<byte>.Empty;
            long[] compactCache;
            bool srcOrdered, srcCompact;
            if (_curCount == 0)
            {
                compactCache = new long[0];
                srcCompact = true;
                srcOrdered = false; // hashTable, even tho empty
                return CompactOperations.ComponentsToCompact(
                    _thetaLong, _curCount, _seedHash, _empty, srcCompact, srcOrdered, dstOrdered,
                    dstMemSpan, compactCache);
            }
            // else curCount > 0
            long[] hashTable = _hashTable;
            compactCache = CompactCachePart(hashTable, _lgArrLongs, _curCount, _thetaLong, dstOrdered);
            srcCompact = true;
            srcOrdered = dstOrdered;
            return CompactOperations.ComponentsToCompact(
                _thetaLong, _curCount, _seedHash, _empty, srcCompact, srcOrdered, dstOrdered,
                dstMemSpan, compactCache);
        }

        public override bool HasMemory()
        {
            return false; // Heap implementation
        }

        public override bool HasResult()
        {
            return _curCount >= 0;
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
            HardReset();
        }

        public override byte[] ToByteArray()
        {
            int preBytes = CONST_PREAMBLE_LONGS << 3;
            int dataBytes = _curCount > 0 ? 8 << _lgArrLongs : 0;
            byte[] byteArrOut = new byte[preBytes + dataBytes];
            Span<byte> memOut = byteArrOut;

            // preamble
            memOut[PreambleUtil.PREAMBLE_LONGS_BYTE] = (byte)CONST_PREAMBLE_LONGS; // RF not used = 0
            memOut[PreambleUtil.SER_VER_BYTE] = (byte)PreambleUtil.SER_VER;
            memOut[PreambleUtil.FAMILY_BYTE] = (byte)Family.INTERSECTION.GetID();
            memOut[PreambleUtil.LG_NOM_LONGS_BYTE] = 0; // not used
            memOut[PreambleUtil.LG_ARR_LONGS_BYTE] = (byte)_lgArrLongs;
            if (_empty)
            {
                memOut[PreambleUtil.FLAGS_BYTE] |= (byte)PreambleUtil.EMPTY_FLAG_MASK;
            }
            BinaryPrimitives.WriteInt16LittleEndian(memOut.Slice(PreambleUtil.SEED_HASH_SHORT), _seedHash);
            BinaryPrimitives.WriteInt32LittleEndian(memOut.Slice(PreambleUtil.RETAINED_ENTRIES_INT), _curCount);
            BinaryPrimitives.WriteSingleLittleEndian(memOut.Slice(PreambleUtil.P_FLOAT), 1.0f);
            BinaryPrimitives.WriteInt64LittleEndian(memOut.Slice(PreambleUtil.THETA_LONG), _thetaLong);

            // data
            if (_curCount > 0)
            {
                for (int i = 0; i < (1 << _lgArrLongs); i++)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(memOut.Slice(preBytes + (i << 3)), _hashTable[i]);
                }
            }
            return byteArrOut;
        }

        // restricted

        /// <summary>
        /// Gets the number of retained entries from this operation. If negative, it is interpreted
        /// as the infinite Universal Set.
        /// </summary>
        internal override int GetRetainedEntries()
        {
            return _curCount;
        }

        internal override bool IsEmpty()
        {
            return _empty;
        }

        internal override long[] GetCache()
        {
            return _hashTable != null ? _hashTable : new long[0];
        }

        internal override short GetSeedHash()
        {
            return _seedHash;
        }

        internal override long GetThetaLong()
        {
            return _thetaLong;
        }

        public override Family GetFamily()
        {
            return Family.INTERSECTION;
        }

        private void PerformIntersect(Sketch sketchIn)
        {
            // curCount and input data are nonzero, match against HT
            System.Diagnostics.Debug.Assert(_curCount > 0 && !_empty);
            long[] cacheIn = sketchIn.GetCache();
            int arrLongsIn = cacheIn.Length;
            long[] hashTable = _hashTable;

            // allocate space for matching
            long[] matchSet = new long[Math.Min(_curCount, sketchIn.GetRetainedEntries(valid: true))];

            int matchSetCount = 0;
            if (sketchIn.IsOrdered())
            {
                // ordered compact, which enables early stop
                for (int i = 0; i < arrLongsIn; i++)
                {
                    long hashIn = cacheIn[i];
                    if (hashIn >= _thetaLong)
                    {
                        break; // early stop assumes that hashes in input sketch are ordered!
                    }
                    int foundIdx = HashOperations.HashSearch(hashTable, _lgArrLongs, hashIn);
                    if (foundIdx == -1) { continue; }
                    matchSet[matchSetCount++] = hashIn;
                }
            }
            else
            {
                // either unordered compact or hash table
                for (int i = 0; i < arrLongsIn; i++)
                {
                    long hashIn = cacheIn[i];
                    if (hashIn <= 0L || hashIn >= _thetaLong) { continue; }
                    int foundIdx = HashOperations.HashSearch(hashTable, _lgArrLongs, hashIn);
                    if (foundIdx == -1) { continue; }
                    matchSet[matchSetCount++] = hashIn;
                }
            }

            // reduce effective array size to minimum
            _curCount = matchSetCount;
            _lgArrLongs = HashOperations.MinLgHashTableSize(matchSetCount, ThetaUtil.REBUILD_THRESHOLD);
            Array.Fill(_hashTable, 0, 0, 1 << _lgArrLongs); // clear for rebuild

            if (_curCount > 0)
            {
                MoveDataToTgt(matchSet, matchSetCount); // move matchSet to target
            }
            else
            {
                if (_thetaLong == long.MaxValue)
                {
                    _empty = true;
                }
            }
        }

        private void MoveDataToTgt(long[] arr, int count)
        {
            int arrLongsIn = arr.Length;
            int tmpCnt = 0;
            // On Heap. Assumes HT exists and is large enough
            for (int i = 0; i < arrLongsIn; i++)
            {
                long hashIn = arr[i];
                if (HashOperations.ContinueCondition(_thetaLong, hashIn)) { continue; }
                HashOperations.HashInsertOnly(_hashTable, _lgArrLongs, hashIn);
                tmpCnt++;
            }
            System.Diagnostics.Debug.Assert(tmpCnt == count,
                $"Intersection Count Check: got: {tmpCnt}, expected: {count}");
        }

        private void HardReset()
        {
            ResetCommon();
            _curCount = -1; // Universal Set
            _empty = false;
        }

        private void ResetToEmpty()
        {
            ResetCommon();
            _curCount = 0;
            _empty = true;
        }

        private void ResetCommon()
        {
            if (_readOnly) { throw new SketchesReadOnlyException(); }
            _lgArrLongs = ThetaUtil.MIN_LG_ARR_LONGS;
            _thetaLong = long.MaxValue;
            _hashTable = null;
        }
    }
}

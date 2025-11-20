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
using Apache.DataSketches.ThetaCommon;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Implements the A-and-not-B operations.
    /// </summary>
    internal class AnotBImpl : AnotB
    {
        private readonly short _seedHash;
        private bool _empty;
        private long _thetaLong;
        private long[] _hashArr = new long[0]; // compact array w curCount_ entries
        private int _curCount;

        /// <summary>
        /// Construct a new AnotB SetOperation on the heap. Called by SetOperation.Builder.
        /// </summary>
        /// <param name="seed">The hash seed</param>
        internal AnotBImpl(long seed)
            : this(ThetaUtil.ComputeSeedHash(seed))
        {
        }

        /// <summary>
        /// Construct a new AnotB SetOperation on the heap.
        /// </summary>
        /// <param name="seedHash">16 bit hash of the chosen update seed.</param>
        private AnotBImpl(short seedHash)
        {
            _seedHash = seedHash;
            Reset();
        }

        public override void SetA(Sketch skA)
        {
            if (skA == null)
            {
                Reset();
                throw new SketchesArgumentException("The input argument <i>A</i> must not be null");
            }
            if (skA.IsEmpty())
            {
                Reset();
                return;
            }
            // skA is not empty
            ThetaUtil.CheckSeedHashes(_seedHash, skA.GetSeedHash());

            // process A
            _hashArr = GetHashArrA(skA);
            _empty = false;
            _thetaLong = skA.GetThetaLong();
            _curCount = _hashArr.Length;
        }

        public override void NotB(Sketch skB)
        {
            if (_empty || skB == null || skB.IsEmpty()) { return; }
            // local and skB is not empty
            ThetaUtil.CheckSeedHashes(_seedHash, skB.GetSeedHash());

            _thetaLong = Math.Min(_thetaLong, skB.GetThetaLong());

            // process B
            _hashArr = GetResultHashArr(_thetaLong, _curCount, _hashArr, skB);
            _curCount = _hashArr.Length;
            _empty = _curCount == 0 && _thetaLong == long.MaxValue;
        }

        public override CompactSketch GetResult(bool reset)
        {
            return GetResult(true, null, reset);
        }

        public override CompactSketch GetResult(bool dstOrdered, Memory<byte>? dstMem, bool reset)
        {
            long[] hashArrCopy = new long[_hashArr.Length];
            Array.Copy(_hashArr, hashArrCopy, _hashArr.Length);
            Span<byte> dstMemSpan = dstMem.HasValue ? dstMem.Value.Span : Span<byte>.Empty;
            CompactSketch result = CompactOperations.ComponentsToCompact(
                _thetaLong, _curCount, _seedHash, _empty, srcCompact: true, srcOrdered: false,
                dstOrdered, dstMemSpan, hashArrCopy);
            if (reset) { Reset(); }
            return result;
        }

        public override CompactSketch ANotB(Sketch skA, Sketch skB, bool dstOrdered,
            Memory<byte>? dstMem)
        {
            if (skA == null || skB == null)
            {
                throw new SketchesArgumentException("Neither argument may be null");
            }
            // Both skA & skB are not null

            long minThetaLong = Math.Min(skA.GetThetaLong(), skB.GetThetaLong());
            Span<byte> dstMemSpan = dstMem.HasValue ? dstMem.Value.Span : Span<byte>.Empty;

            if (skA.IsEmpty()) { return skA.Compact(dstOrdered, dstMemSpan); }
            // A is not Empty
            ThetaUtil.CheckSeedHashes(skA.GetSeedHash(), _seedHash);

            if (skB.IsEmpty())
            {
                return skA.Compact(dstOrdered, dstMemSpan);
            }
            ThetaUtil.CheckSeedHashes(skB.GetSeedHash(), _seedHash);
            // Both skA & skB are not empty

            // process A
            long[] hashArrA = GetHashArrA(skA);
            int countA = hashArrA.Length;

            // process B
            long[] hashArrOut = GetResultHashArr(minThetaLong, countA, hashArrA, skB); // out is clone
            int countOut = hashArrOut.Length;
            bool empty = countOut == 0 && minThetaLong == long.MaxValue;

            CompactSketch result = CompactOperations.ComponentsToCompact(
                minThetaLong, countOut, _seedHash, empty, srcCompact: true, srcOrdered: false,
                dstOrdered, dstMemSpan, hashArrOut);
            return result;
        }

        internal override int GetRetainedEntries()
        {
            return _curCount;
        }

        // restricted

        private static long[] GetHashArrA(Sketch skA)
        {
            // Get skA cache as array
            CompactSketch cskA = skA.Compact(dstOrdered: false, dstMem: Span<byte>.Empty); // sorting not required
            long[] hashArrA = new long[cskA.GetCache().Length];
            Array.Copy(cskA.GetCache(), hashArrA, cskA.GetCache().Length);
            return hashArrA;
        }

        private static long[] GetResultHashArr(
            long minThetaLong,
            int countA,
            long[] hashArrA,
            Sketch skB)
        {
            // Rebuild/get hashtable of skB
            long[] hashTableB; // read only
            long[] thetaCache = skB.GetCache();
            int countB = skB.GetRetainedEntries(valid: true);
            if (skB is CompactSketch)
            {
                hashTableB = HashOperations.ConvertToHashTable(thetaCache, countB, minThetaLong, ThetaUtil.REBUILD_THRESHOLD);
            }
            else
            {
                hashTableB = thetaCache;
            }

            // build temporary result arrays of skA
            long[] tmpHashArrA = new long[countA];

            // search for non matches and build temp arrays
            int lgHTBLen = BitOperations.TrailingZeroCount((uint)hashTableB.Length);
            int nonMatches = 0;
            for (int i = 0; i < countA; i++)
            {
                long hash = hashArrA[i];
                if (hash != 0 && hash < minThetaLong)
                {
                    // only allows hashes of A < minTheta
                    int index = HashOperations.HashSearch(hashTableB, lgHTBLen, hash);
                    if (index == -1)
                    {
                        tmpHashArrA[nonMatches] = hash;
                        nonMatches++;
                    }
                }
            }
            long[] result = new long[nonMatches];
            Array.Copy(tmpHashArrA, result, nonMatches);
            return result;
        }

        private void Reset()
        {
            _thetaLong = long.MaxValue;
            _empty = true;
            _hashArr = new long[0];
            _curCount = 0;
        }

        internal override long[] GetCache()
        {
            long[] result = new long[_hashArr.Length];
            Array.Copy(_hashArr, result, _hashArr.Length);
            return result;
        }

        internal override short GetSeedHash()
        {
            return _seedHash;
        }

        internal override long GetThetaLong()
        {
            return _thetaLong;
        }

        internal override bool IsEmpty()
        {
            return _empty;
        }

        public override bool HasMemory()
        {
            return false;
        }

        public override bool IsDirect()
        {
            return false;
        }

        public override bool IsSameResource(Memory<byte> that)
        {
            return false;
        }

        public override Family GetFamily()
        {
            return Family.A_NOT_B;
        }
    }
}

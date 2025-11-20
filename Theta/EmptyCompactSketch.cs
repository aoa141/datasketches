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

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Singleton empty CompactSketch.
    /// </summary>
    internal sealed class EmptyCompactSketch : CompactSketch
    {
        // For backward compatibility, a candidate long must have Flags= compact, read-only,
        // COMPACT-Family=3, SerVer=3, PreLongs=1, and be exactly 8 bytes long. The seedHash is ignored.
        // NOTE: The empty and ordered flags may or may not be set
        private const long EMPTY_SKETCH_MASK = 0x00_00_EB_00_00_FF_FF_FFL;
        private const long EMPTY_SKETCH_TEST = 0x00_00_0A_00_00_03_03_01L;

        // When returning a byte array the empty and ordered bits are also set
        internal static readonly byte[] EMPTY_COMPACT_SKETCH_ARR = { 1, 3, 3, 0, 0, 0x1E, 0, 0 };

        private static readonly EmptyCompactSketch EMPTY_COMPACT_SKETCH = new EmptyCompactSketch();

        private EmptyCompactSketch() { }

        internal static EmptyCompactSketch GetInstance()
        {
            return EMPTY_COMPACT_SKETCH;
        }

        // This should be a heapify
        internal static EmptyCompactSketch GetHeapInstance(ReadOnlySpan<byte> srcMem)
        {
            long pre0 = BitConverter.ToInt64(srcMem.Slice(0, 8));
            if (TestCandidatePre0(pre0))
            {
                return EMPTY_COMPACT_SKETCH;
            }
            long maskedPre0 = pre0 & EMPTY_SKETCH_MASK;
            throw new SketchesArgumentException(
                $"Input Memory does not match required Preamble. " +
                $"Memory Pre0: {maskedPre0:X16}, required Pre0: {EMPTY_SKETCH_TEST:X16}");
        }

        public override CompactSketch Compact(bool dstOrdered, Span<byte> dstMem)
        {
            if (dstMem == null || dstMem.Length == 0)
            {
                return GetInstance();
            }
            EMPTY_COMPACT_SKETCH_ARR.CopyTo(dstMem);
            // Note: DirectCompactSketch not yet fully implemented
            throw new NotImplementedException("DirectCompactSketch not yet implemented");
            //return new DirectCompactSketch(dstMem);
        }

        internal static bool TestCandidatePre0(long candidate)
        {
            return (candidate & EMPTY_SKETCH_MASK) == EMPTY_SKETCH_TEST;
        }

        public override int GetCurrentBytes()
        {
            return 8;
        }

        public override double GetEstimate()
        {
            return 0;
        }

        public override int GetRetainedEntries(bool valid)
        {
            return 0;
        }

        public override long GetThetaLong()
        {
            return long.MaxValue;
        }

        public override bool IsEmpty()
        {
            return true;
        }

        public override bool IsOrdered()
        {
            return true;
        }

        public override HashIterator Iterator()
        {
            return new HeapCompactHashIterator(new long[0]);
        }

        public override byte[] ToByteArray()
        {
            return (byte[])EMPTY_COMPACT_SKETCH_ARR.Clone();
        }

        internal override long[] GetCache()
        {
            return new long[0];
        }

        internal override int GetCompactPreambleLongs()
        {
            return 1;
        }

        internal override int GetCurrentDataLongs()
        {
            return 0;
        }

        internal override int GetCurrentPreambleLongs()
        {
            return 1;
        }

        internal override ReadOnlyMemory<byte>? GetMemory()
        {
            return null;
        }

        internal override short GetSeedHash()
        {
            return 0;
        }
    }
}

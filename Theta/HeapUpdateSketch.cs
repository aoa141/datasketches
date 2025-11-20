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
using Apache.DataSketches.ThetaCommon;
using static Apache.DataSketches.Theta.CompactOperations;
using static Apache.DataSketches.Theta.PreambleUtil;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// The parent class for Heap Updatable Theta Sketches.
    /// </summary>
    internal abstract class HeapUpdateSketch : UpdateSketch
    {
        internal readonly int lgNomLongs_;
        private readonly long seed_;
        private readonly float p_;
        private readonly ResizeFactor rf_;

        internal HeapUpdateSketch(int lgNomLongs, long seed, float p, ResizeFactor rf)
        {
            lgNomLongs_ = Math.Max(lgNomLongs, ThetaUtil.MIN_LG_NOM_LONGS);
            seed_ = seed;
            p_ = p;
            rf_ = rf;
        }

        // Sketch

        public override int GetCurrentBytes()
        {
            int preLongs = GetCurrentPreambleLongs();
            int dataLongs = GetCurrentDataLongs();
            return (preLongs + dataLongs) << 3;
        }

        // UpdateSketch

        public override int GetLgNomLongs()
        {
            return lgNomLongs_;
        }

        internal override float GetP()
        {
            return p_;
        }

        public override ResizeFactor GetResizeFactor()
        {
            return rf_;
        }

        internal override long GetSeed()
        {
            return seed_;
        }

        // Restricted methods

        internal override short GetSeedHash()
        {
            return ThetaUtil.ComputeSeedHash(GetSeed());
        }

        // Used by HeapAlphaSketch and HeapQuickSelectSketch / Theta UpdateSketch
        internal byte[] ToByteArray(int preLongs, byte familyID)
        {
            if (IsDirty()) { Rebuild(); }
            CheckIllegalCurCountAndEmpty(IsEmpty(), GetRetainedEntries(true));

            int preBytes = (preLongs << 3) & 0x3F; // 24 bytes
            int dataBytes = GetCurrentDataLongs() << 3;
            byte[] byteArrOut = new byte[preBytes + dataBytes];
            Span<byte> memOut = byteArrOut.AsSpan();

            // Preamble first 8 bytes. Note: only compact can be reduced to 8 bytes.
            int lgRf = GetResizeFactor().GetLg() & 0x3;
            InsertPreLongs(memOut, preLongs);                // byte 0 low 6 bits
            InsertLgResizeFactor(memOut, lgRf);              // byte 0 high 2 bits
            InsertSerVer(memOut, SER_VER);                   // byte 1
            InsertFamilyID(memOut, familyID);                // byte 2
            InsertLgNomLongs(memOut, GetLgNomLongs());       // byte 3
            InsertLgArrLongs(memOut, GetLgArrLongs());       // byte 4
            InsertSeedHash(memOut, GetSeedHash());           // bytes 6 & 7

            InsertCurCount(memOut, GetRetainedEntries(true));
            InsertP(memOut, GetP());

            long thetaLong = CorrectThetaOnCompact(IsEmpty(), GetRetainedEntries(true), GetThetaLong());
            InsertThetaLong(memOut, thetaLong);

            // Flags: BigEnd=0, ReadOnly=0, Empty=X, compact=0, ordered=0
            byte flags = IsEmpty() ? (byte)EMPTY_FLAG_MASK : (byte)0;
            InsertFlags(memOut, flags);

            // Data
            int arrLongs = 1 << GetLgArrLongs();
            long[] cache = GetCache();
            int offset = preBytes;
            for (int i = 0; i < arrLongs; i++)
            {
                BitConverter.TryWriteBytes(memOut.Slice(offset + (i << 3), 8), cache[i]);
            }

            return byteArrOut;
        }
    }
}

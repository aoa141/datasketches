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
using System.IO;

namespace DataSketches.Hll
{
    /// <summary>
    /// Abstract base class for HLL sketch implementations
    /// </summary>
    internal abstract class HllSketchImpl
    {
        protected byte _lgConfigK;
        protected TargetHllType _tgtHllType;
        protected HllMode _mode;
        protected bool _startFullSize;

        protected HllSketchImpl(byte lgConfigK, TargetHllType tgtHllType, HllMode mode, bool startFullSize)
        {
            _lgConfigK = lgConfigK;
            _tgtHllType = tgtHllType;
            _mode = mode;
            _startFullSize = startFullSize;
        }

        public byte LgConfigK => _lgConfigK;
        public TargetHllType TgtHllType => _tgtHllType;
        public HllMode Mode => _mode;

        public abstract void Serialize(Stream stream, bool compact);
        public abstract byte[] Serialize(bool compact, int headerSizeBytes);
        public abstract HllSketchImpl Copy();
        public abstract HllSketchImpl CopyAs(TargetHllType tgtType);
        public abstract HllSketchImpl Reset();
        public abstract HllSketchImpl CouponUpdate(uint coupon);

        public abstract double GetEstimate();
        public abstract double GetCompositeEstimate();
        public abstract double GetUpperBound(byte numStdDev);
        public abstract double GetLowerBound(byte numStdDev);

        public abstract uint GetUpdatableSerializationBytes();
        public abstract uint GetCompactSerializationBytes();

        public abstract bool IsCompact();
        public abstract bool IsEmpty();
        public abstract bool IsOutOfOrderFlag();

        public static TargetHllType ExtractTgtHllType(byte modeByte)
        {
            return (TargetHllType)((modeByte >> 2) & 0x3);
        }

        public static HllMode ExtractCurMode(byte modeByte)
        {
            return (HllMode)(modeByte & 0x3);
        }

        protected byte MakeFlagsByte(bool compact)
        {
            byte flags = 0;
            if (IsEmpty()) flags |= HllConstants.EmptyFlagMask;
            if (compact) flags |= HllConstants.CompactFlagMask;
            if (IsOutOfOrderFlag()) flags |= HllConstants.OutOfOrderFlagMask;
            if (_startFullSize) flags |= HllConstants.FullSizeFlagMask;
            return flags;
        }

        protected byte MakeModeByte()
        {
            return (byte)(((byte)_tgtHllType << 2) | (byte)_mode);
        }
    }

    /// <summary>
    /// Factory for creating HLL sketch implementations
    /// </summary>
    internal static class HllSketchImplFactory
    {
        public static HllSketchImpl NewInstance(byte lgConfigK, TargetHllType tgtType, bool startFullSize)
        {
            if (startFullSize)
            {
                return new HllArray(lgConfigK, tgtType);
            }
            return new CouponList(lgConfigK, tgtType);
        }

        public static HllSketchImpl Deserialize(Stream stream)
        {
            using var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, leaveOpen: true);

            byte preInts = reader.ReadByte();
            byte serVer = reader.ReadByte();
            byte familyId = reader.ReadByte();
            byte lgK = reader.ReadByte();
            byte lgArr = reader.ReadByte();
            byte flags = reader.ReadByte();

            if (familyId != HllConstants.FamilyId)
            {
                throw new ArgumentException("Invalid family ID");
            }

            bool isEmpty = (flags & HllConstants.EmptyFlagMask) != 0;
            if (isEmpty)
            {
                byte modeByte = reader.ReadByte();
                var tgtType = HllSketchImpl.ExtractTgtHllType(modeByte);
                return new CouponList(lgK, tgtType);
            }

            byte modeByte2 = reader.ReadByte();
            var mode = HllSketchImpl.ExtractCurMode(modeByte2);
            var tgtHllType = HllSketchImpl.ExtractTgtHllType(modeByte2);

            // Based on mode, create appropriate implementation
            switch (mode)
            {
                case HllMode.List:
                case HllMode.Set:
                    return CouponList.Deserialize(reader, lgK, tgtHllType, mode);
                case HllMode.Hll:
                    return HllArray.Deserialize(reader, lgK, tgtHllType);
                default:
                    throw new ArgumentException($"Unknown mode: {mode}");
            }
        }
    }

    /// <summary>
    /// Simplified CouponList implementation (stub)
    /// </summary>
    internal class CouponList : HllSketchImpl
    {
        private readonly List<uint> _coupons;
        private bool _oooFlag;

        public CouponList(byte lgConfigK, TargetHllType tgtType)
            : base(lgConfigK, tgtType, HllMode.List, false)
        {
            _coupons = new List<uint>();
            _oooFlag = false;
        }

        public override HllSketchImpl Copy()
        {
            var copy = new CouponList(_lgConfigK, _tgtHllType);
            copy._coupons.AddRange(_coupons);
            copy._oooFlag = _oooFlag;
            return copy;
        }

        public override HllSketchImpl CopyAs(TargetHllType tgtType)
        {
            var copy = new CouponList(_lgConfigK, tgtType);
            copy._coupons.AddRange(_coupons);
            copy._oooFlag = _oooFlag;
            return copy;
        }

        public override HllSketchImpl Reset()
        {
            return new CouponList(_lgConfigK, _tgtHllType);
        }

        public override HllSketchImpl CouponUpdate(uint coupon)
        {
            if (!_coupons.Contains(coupon))
            {
                _coupons.Add(coupon);
            }
            // Check if need to promote to HLL
            uint threshold = (uint)(1 << (_lgConfigK - 2));
            if (_coupons.Count > threshold)
            {
                return PromoteToHll();
            }
            return this;
        }

        private HllArray PromoteToHll()
        {
            var hll = new HllArray(_lgConfigK, _tgtHllType);
            foreach (var coupon in _coupons)
            {
                hll.CouponUpdate(coupon);
            }
            return hll;
        }

        public override double GetEstimate()
        {
            return _coupons.Count;
        }

        public override double GetCompositeEstimate()
        {
            return GetEstimate();
        }

        public override double GetUpperBound(byte numStdDev)
        {
            HllUtil.CheckNumStdDev(numStdDev);
            return GetEstimate() + numStdDev * Math.Sqrt(GetEstimate());
        }

        public override double GetLowerBound(byte numStdDev)
        {
            HllUtil.CheckNumStdDev(numStdDev);
            return Math.Max(0, GetEstimate() - numStdDev * Math.Sqrt(GetEstimate()));
        }

        public override uint GetUpdatableSerializationBytes()
        {
            return (uint)(HllConstants.ListIntArrStart + _coupons.Count * 4);
        }

        public override uint GetCompactSerializationBytes()
        {
            return GetUpdatableSerializationBytes();
        }

        public override bool IsCompact() => false;
        public override bool IsEmpty() => _coupons.Count == 0;
        public override bool IsOutOfOrderFlag() => _oooFlag;

        public override void Serialize(Stream stream, bool compact)
        {
            using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true);
            writer.Write(HllConstants.ListPreInts);
            writer.Write(HllConstants.SerVer);
            writer.Write(HllConstants.FamilyId);
            writer.Write(_lgConfigK);
            writer.Write((byte)0); // lgArr
            writer.Write(MakeFlagsByte(compact));
            writer.Write((byte)_coupons.Count);
            writer.Write(MakeModeByte());

            foreach (var coupon in _coupons)
            {
                writer.Write(coupon);
            }
        }

        public override byte[] Serialize(bool compact, int headerSizeBytes)
        {
            using var stream = new MemoryStream();
            if (headerSizeBytes > 0)
            {
                stream.Write(new byte[headerSizeBytes], 0, headerSizeBytes);
            }
            Serialize(stream, compact);
            return stream.ToArray();
        }

        internal static CouponList Deserialize(BinaryReader reader, byte lgK, TargetHllType tgtType, HllMode mode)
        {
            var list = new CouponList(lgK, tgtType);
            byte count = reader.ReadByte();

            for (int i = 0; i < count; i++)
            {
                uint coupon = reader.ReadUInt32();
                list._coupons.Add(coupon);
            }

            return list;
        }
    }

    /// <summary>
    /// Simplified HllArray implementation (stub)
    /// </summary>
    internal class HllArray : HllSketchImpl
    {
        private byte[] _hllByteArr;
        private double _hipAccum;
        private double _kxq0;
        private double _kxq1;
        private byte _curMin;
        private uint _numAtCurMin;

        public HllArray(byte lgConfigK, TargetHllType tgtType)
            : base(lgConfigK, tgtType, HllMode.Hll, true)
        {
            int arrSize = HllArrBytes(tgtType, lgConfigK);
            _hllByteArr = new byte[arrSize];
            _hipAccum = 0;
            _kxq0 = 1 << lgConfigK;
            _kxq1 = 0;
            _curMin = 0;
            _numAtCurMin = (uint)(1 << lgConfigK);
        }

        public static int HllArrBytes(TargetHllType tgtType, byte lgK)
        {
            int k = 1 << lgK;
            return tgtType switch
            {
                TargetHllType.Hll4 => k / 2,
                TargetHllType.Hll6 => (k * 3) / 4,
                TargetHllType.Hll8 => k,
                _ => throw new ArgumentException($"Unknown target type: {tgtType}")
            };
        }

        public override HllSketchImpl Copy()
        {
            var copy = new HllArray(_lgConfigK, _tgtHllType);
            Array.Copy(_hllByteArr, copy._hllByteArr, _hllByteArr.Length);
            copy._hipAccum = _hipAccum;
            copy._kxq0 = _kxq0;
            copy._kxq1 = _kxq1;
            copy._curMin = _curMin;
            copy._numAtCurMin = _numAtCurMin;
            return copy;
        }

        public override HllSketchImpl CopyAs(TargetHllType tgtType)
        {
            if (tgtType == _tgtHllType)
                return Copy();
            // Convert between HLL types - simplified
            return new HllArray(_lgConfigK, tgtType);
        }

        public override HllSketchImpl Reset()
        {
            return new HllArray(_lgConfigK, _tgtHllType);
        }

        public override HllSketchImpl CouponUpdate(uint coupon)
        {
            uint slotNo = HllUtil.GetLow26(coupon);
            byte newValue = HllUtil.GetValue(coupon);

            // Simplified update - actual implementation would update the byte array
            // based on the target type (4, 6, or 8 bits per bucket)

            return this;
        }

        public override double GetEstimate()
        {
            // Use HIP estimator if available
            if (_hipAccum != 0)
            {
                return _hipAccum;
            }
            return GetCompositeEstimate();
        }

        public override double GetCompositeEstimate()
        {
            // Simplified HLL estimation
            uint k = (uint)(1 << _lgConfigK);
            double rawEst = k * k / _kxq0;
            return rawEst;
        }

        public override double GetUpperBound(byte numStdDev)
        {
            return GetEstimate() * (1 + HllUtil.GetRelErr(true, false, _lgConfigK, numStdDev));
        }

        public override double GetLowerBound(byte numStdDev)
        {
            return GetEstimate() * (1 + HllUtil.GetRelErr(false, false, _lgConfigK, numStdDev));
        }

        public override uint GetUpdatableSerializationBytes()
        {
            return (uint)(HllConstants.HllByteArrStart + _hllByteArr.Length);
        }

        public override uint GetCompactSerializationBytes()
        {
            return GetUpdatableSerializationBytes();
        }

        public override bool IsCompact() => false;
        public override bool IsEmpty() => _numAtCurMin == (1u << _lgConfigK);
        public override bool IsOutOfOrderFlag() => false;

        public override void Serialize(Stream stream, bool compact)
        {
            using var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true);
            writer.Write(HllConstants.HllPreInts);
            writer.Write(HllConstants.SerVer);
            writer.Write(HllConstants.FamilyId);
            writer.Write(_lgConfigK);
            writer.Write((byte)0); // lgArr - not used for HLL
            writer.Write(MakeFlagsByte(compact));
            writer.Write(_curMin);
            writer.Write(MakeModeByte());

            writer.Write(_hipAccum);
            writer.Write(_kxq0);
            writer.Write(_kxq1);
            writer.Write(_numAtCurMin);
            writer.Write(0u); // aux count

            writer.Write(_hllByteArr);
        }

        public override byte[] Serialize(bool compact, int headerSizeBytes)
        {
            using var stream = new MemoryStream();
            if (headerSizeBytes > 0)
            {
                stream.Write(new byte[headerSizeBytes], 0, headerSizeBytes);
            }
            Serialize(stream, compact);
            return stream.ToArray();
        }

        internal static HllArray Deserialize(BinaryReader reader, byte lgK, TargetHllType tgtType)
        {
            var hll = new HllArray(lgK, tgtType);
            hll._hipAccum = reader.ReadDouble();
            hll._kxq0 = reader.ReadDouble();
            hll._kxq1 = reader.ReadDouble();
            hll._numAtCurMin = reader.ReadUInt32();
            uint auxCount = reader.ReadUInt32();

            int arrSize = HllArrBytes(tgtType, lgK);
            hll._hllByteArr = reader.ReadBytes(arrSize);

            return hll;
        }
    }
}

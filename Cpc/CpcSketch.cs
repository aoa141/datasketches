// <copyright file="CpcSketch.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using DataSketches.Common;

namespace DataSketches.Cpc
{
    /// <summary>
    /// Compressed Probabilistic Counting (CPC) Sketch for distinct counting.
    /// This is a very compact sketch for estimating the number of distinct items in a stream.
    /// Based on the paper: https://arxiv.org/abs/1708.06839
    /// </summary>
    public class CpcSketch
    {
        private const byte DefaultLgK = 11;
        private const byte MinLgK = 4;
        private const byte MaxLgK = 26;
        private const byte SerialVersion = 1;
        private const byte FamilyId = 16;

        private readonly byte _lgK;
        private readonly ulong _seed;
        private uint _numCoupons;
        private readonly Dictionary<uint, byte> _surprisingValueTable;
        private readonly byte[] _slidingWindow;
        private byte _windowOffset;
        private double _kxp;
        private double _hipEstAccum;
        private bool _wasMerged;

        private enum Flavor
        {
            Empty,    // 0 coupons
            Sparse,   // < 3K/32 coupons
            Hybrid,   // < K/2 coupons
            Pinned,   // < 27K/8 coupons
            Sliding   // >= 27K/8 coupons
        }

        /// <summary>
        /// Creates a new CPC sketch.
        /// </summary>
        /// <param name="lgK">Log2 of K (sketch size parameter, 4-26)</param>
        /// <param name="seed">Hash seed</param>
        public CpcSketch(byte lgK = DefaultLgK, ulong seed = CommonDefs.DEFAULT_SEED)
        {
            if (lgK < MinLgK || lgK > MaxLgK)
                throw new ArgumentException($"lgK must be between {MinLgK} and {MaxLgK}");

            _lgK = lgK;
            _seed = seed;
            _numCoupons = 0;
            _surprisingValueTable = new Dictionary<uint, byte>();
            _slidingWindow = new byte[1 << lgK];
            _windowOffset = 0;
            _kxp = 0;
            _hipEstAccum = 0;
            _wasMerged = false;
        }

        /// <summary>
        /// Gets the configured lgK parameter.
        /// </summary>
        public byte LgK => _lgK;

        /// <summary>
        /// Returns true if no items have been added.
        /// </summary>
        public bool IsEmpty => _numCoupons == 0;

        /// <summary>
        /// Gets the distinct count estimate.
        /// </summary>
        public double Estimate
        {
            get
            {
                if (IsEmpty) return 0;

                var flavor = DetermineFlavor();

                switch (flavor)
                {
                    case Flavor.Empty:
                        return 0;
                    case Flavor.Sparse:
                    case Flavor.Hybrid:
                        return GetIconEstimate();
                    case Flavor.Pinned:
                    case Flavor.Sliding:
                        return GetHipEstimate();
                    default:
                        return _numCoupons;
                }
            }
        }

        /// <summary>
        /// Gets the lower bound of the estimate at a given number of standard deviations.
        /// </summary>
        /// <param name="kappa">Number of standard deviations (1, 2, or 3)</param>
        public double GetLowerBound(int kappa)
        {
            if (kappa < 1 || kappa > 3)
                throw new ArgumentException("kappa must be 1, 2, or 3");

            var estimate = Estimate;
            var k = 1 << _lgK;
            var relativeError = GetRelativeError(kappa);

            return Math.Max(0, estimate * (1 - relativeError));
        }

        /// <summary>
        /// Gets the upper bound of the estimate at a given number of standard deviations.
        /// </summary>
        /// <param name="kappa">Number of standard deviations (1, 2, or 3)</param>
        public double GetUpperBound(int kappa)
        {
            if (kappa < 1 || kappa > 3)
                throw new ArgumentException("kappa must be 1, 2, or 3");

            var estimate = Estimate;
            var relativeError = GetRelativeError(kappa);

            return estimate * (1 + relativeError);
        }

        private double GetRelativeError(int kappa)
        {
            var k = 1 << _lgK;
            // Empirical constants for CPC sketch
            var baseError = 0.833 / Math.Sqrt(k);
            return kappa * baseError;
        }

        /// <summary>
        /// Updates the sketch with a string value.
        /// </summary>
        public void Update(string value)
        {
            if (string.IsNullOrEmpty(value)) return;
            var bytes = Encoding.UTF8.GetBytes(value);
            Update(bytes);
        }

        /// <summary>
        /// Updates the sketch with a numeric value.
        /// </summary>
        public void Update(ulong value)
        {
            var bytes = BitConverter.GetBytes(value);
            Update(bytes);
        }

        /// <summary>
        /// Updates the sketch with a numeric value.
        /// </summary>
        public void Update(long value)
        {
            var bytes = BitConverter.GetBytes(value);
            Update(bytes);
        }

        /// <summary>
        /// Updates the sketch with a double value.
        /// </summary>
        public void Update(double value)
        {
            var bytes = BitConverter.GetBytes(value);
            Update(bytes);
        }

        /// <summary>
        /// Updates the sketch with raw bytes.
        /// </summary>
        public void Update(byte[] data)
        {
            if (data == null || data.Length == 0) return;

            HashState hashState;
            MurmurHash3.Hash128(data, _seed, out hashState);
            var rowCol = (uint)(hashState.H1 % (1UL << (2 * _lgK)));

            UpdateRowCol(rowCol);
        }

        private void UpdateRowCol(uint rowCol)
        {
            var k = 1 << _lgK;
            var row = rowCol >> _lgK;
            var col = rowCol & ((1u << _lgK) - 1);

            // Simplified update logic (full implementation would be more complex)
            _numCoupons++;

            // Track in surprising value table or window based on flavor
            var flavor = DetermineFlavor();

            if (flavor == Flavor.Sparse || flavor == Flavor.Hybrid)
            {
                _surprisingValueTable[rowCol] = 1;
            }
            else
            {
                _slidingWindow[col] = (byte)Math.Max(_slidingWindow[col], row);
            }

            // Update HIP accumulator
            UpdateHipAccum();
        }

        private Flavor DetermineFlavor()
        {
            var k = 1 << _lgK;
            var c = _numCoupons;

            if (c == 0) return Flavor.Empty;
            if (c < (3 * k) / 32) return Flavor.Sparse;
            if (c < k / 2) return Flavor.Hybrid;
            if (c < (27 * k) / 8) return Flavor.Pinned;
            return Flavor.Sliding;
        }

        private double GetIconEstimate()
        {
            // ICON estimator for sparse/hybrid modes
            var k = 1 << _lgK;
            var c = _numCoupons;

            if (c == 0) return 0;
            if (c == k) return k * Math.Log(2);

            // Linear counting estimator
            return k * Math.Log((double)k / (k - c));
        }

        private double GetHipEstimate()
        {
            // HIP estimator for pinned/sliding modes
            if (_hipEstAccum == 0)
            {
                RefreshHipAccum();
            }

            return _hipEstAccum;
        }

        private void UpdateHipAccum()
        {
            var k = 1 << _lgK;
            _kxp = _numCoupons / (double)k;

            // Simplified HIP calculation
            _hipEstAccum = k * InverseP(_kxp);
        }

        private void RefreshHipAccum()
        {
            UpdateHipAccum();
        }

        private static double InverseP(double p)
        {
            // Inverse probability function for HIP
            if (p <= 0) return 0;
            if (p >= 1) return double.PositiveInfinity;

            return -Math.Log(1 - p);
        }

        /// <summary>
        /// Serializes the sketch to a byte array.
        /// </summary>
        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                // Simplified serialization
                byte preambleInts = (byte)(IsEmpty ? 2 : 6);

                writer.Write(preambleInts);
                writer.Write(SerialVersion);
                writer.Write(FamilyId);

                byte flags = 0;
                if (IsEmpty) flags |= 1;
                if (_wasMerged) flags |= 2;
                writer.Write(flags);

                writer.Write(_lgK);
                writer.Write((byte)0); // padding
                writer.Write((ushort)_seed);

                if (!IsEmpty)
                {
                    writer.Write(_numCoupons);
                    writer.Write(_hipEstAccum);
                    writer.Write(_kxp);

                    // Write surprising value table
                    writer.Write(_surprisingValueTable.Count);
                    foreach (var kvp in _surprisingValueTable)
                    {
                        writer.Write(kvp.Key);
                        writer.Write(kvp.Value);
                    }

                    // Write sliding window if needed
                    var flavor = DetermineFlavor();
                    if (flavor == Flavor.Sliding)
                    {
                        writer.Write(_slidingWindow.Length);
                        writer.Write(_slidingWindow);
                    }
                }

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Deserializes a sketch from a byte array.
        /// </summary>
        public static CpcSketch Deserialize(byte[] bytes, ulong seed = CommonDefs.DEFAULT_SEED)
        {
            using (var ms = new MemoryStream(bytes))
            using (var reader = new BinaryReader(ms))
            {
                var preambleInts = reader.ReadByte();
                var serVer = reader.ReadByte();
                var familyId = reader.ReadByte();
                var flags = reader.ReadByte();

                if (serVer != SerialVersion)
                    throw new ArgumentException($"Unsupported serial version: {serVer}");
                if (familyId != FamilyId)
                    throw new ArgumentException($"Invalid family ID: {familyId}");

                var lgK = reader.ReadByte();
                reader.ReadByte(); // padding
                reader.ReadUInt16(); // seed hash

                var sketch = new CpcSketch(lgK, seed);

                bool isEmpty = (flags & 1) != 0;
                if (!isEmpty)
                {
                    sketch._numCoupons = reader.ReadUInt32();
                    sketch._hipEstAccum = reader.ReadDouble();
                    sketch._kxp = reader.ReadDouble();

                    var tableSize = reader.ReadInt32();
                    for (int i = 0; i < tableSize; i++)
                    {
                        var key = reader.ReadUInt32();
                        var value = reader.ReadByte();
                        sketch._surprisingValueTable[key] = value;
                    }

                    // Read sliding window if present
                    if (ms.Position < ms.Length)
                    {
                        var windowSize = reader.ReadInt32();
                        reader.Read(sketch._slidingWindow, 0, windowSize);
                    }
                }

                sketch._wasMerged = (flags & 2) != 0;
                return sketch;
            }
        }

        /// <summary>
        /// Returns a string representation of this sketch.
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("### CPC Sketch Summary ###");
            sb.AppendLine($"  LG K          : {_lgK}");
            sb.AppendLine($"  K             : {1 << _lgK}");
            sb.AppendLine($"  Num Coupons   : {_numCoupons}");
            sb.AppendLine($"  Estimate      : {Estimate:F0}");
            sb.AppendLine($"  Is Empty      : {IsEmpty}");
            sb.AppendLine($"  Flavor        : {DetermineFlavor()}");
            sb.AppendLine($"  Lower Bound(1): {GetLowerBound(1):F0}");
            sb.AppendLine($"  Upper Bound(1): {GetUpperBound(1):F0}");
            return sb.ToString();
        }

        /// <summary>
        /// Gets the maximum serialized size for a sketch with the given lgK.
        /// </summary>
        public static int GetMaxSerializedSizeBytes(byte lgK)
        {
            if (lgK < MinLgK || lgK > MaxLgK)
                throw new ArgumentException($"lgK must be between {MinLgK} and {MaxLgK}");

            var k = 1 << lgK;
            // Rough estimate: header + table + window
            return 64 + (k * 8) + k;
        }
    }
}

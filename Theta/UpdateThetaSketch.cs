// <copyright file="UpdateThetaSketch.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.Collections.Generic;
using System.Text;
using DataSketches.Common;

namespace DataSketches.Theta
{
    /// <summary>
    /// Update Theta sketch for building from input data via Update methods.
    /// </summary>
    public class UpdateThetaSketch : ThetaSketch
    {
        private const double ResizeThreshold = 0.5;
        private const double RebuildThreshold = 15.0 / 16.0;
        private const byte StrideHashBits = 7;
        private const uint StrideMask = (1 << StrideHashBits) - 1;

        private bool isEmpty;
        private byte lgCurSize;
        private byte lgNomSize;
        private CommonDefs.ResizeFactor rf;
        private float p;
        private uint numEntries;
        private ulong theta;
        private ulong seed;
        private ulong[] entries;
        private ushort seedHash;

        /// <summary>
        /// Private constructor. Use Builder to create instances.
        /// </summary>
        private UpdateThetaSketch(byte lgCurSize, byte lgNomSize, CommonDefs.ResizeFactor rf, float p, ulong theta, ulong seed)
        {
            this.lgCurSize = lgCurSize;
            this.lgNomSize = lgNomSize;
            this.rf = rf;
            this.p = p;
            this.theta = theta;
            this.seed = seed;
            this.seedHash = MurmurHash3.ComputeSeedHash(seed);
            this.isEmpty = true;
            this.numEntries = 0;
            this.entries = new ulong[1 << lgCurSize];
        }

        public override bool IsEmpty => isEmpty;
        public override bool IsOrdered => false;
        public override ushort GetSeedHash() => seedHash;
        public override ulong GetTheta64() => theta;
        public override uint GetNumRetained() => numEntries;

        /// <summary>
        /// Gets the configured nominal number of entries (log2).
        /// </summary>
        public byte GetLgK() => lgNomSize;

        /// <summary>
        /// Gets the configured resize factor.
        /// </summary>
        public CommonDefs.ResizeFactor GetResizeFactor() => rf;

        /// <summary>
        /// Updates this sketch with a string value.
        /// </summary>
        public void Update(string value)
        {
            if (value == null) return;
            var bytes = System.Text.Encoding.UTF8.GetBytes(value);
            Update(bytes, bytes.Length);
        }

        /// <summary>
        /// Updates this sketch with a 64-bit unsigned integer.
        /// </summary>
        public void Update(ulong value)
        {
            Span<byte> bytes = stackalloc byte[8];
            BitConverter.TryWriteBytes(bytes, value);
            Update(bytes, bytes.Length);
        }

        /// <summary>
        /// Updates this sketch with a 64-bit signed integer.
        /// </summary>
        public void Update(long value)
        {
            Span<byte> bytes = stackalloc byte[8];
            BitConverter.TryWriteBytes(bytes, value);
            Update(bytes, bytes.Length);
        }

        /// <summary>
        /// Updates this sketch with a 32-bit unsigned integer.
        /// </summary>
        public void Update(uint value)
        {
            Update((long)value);
        }

        /// <summary>
        /// Updates this sketch with a 32-bit signed integer.
        /// </summary>
        public void Update(int value)
        {
            Update((long)value);
        }

        /// <summary>
        /// Updates this sketch with a 16-bit unsigned integer.
        /// </summary>
        public void Update(ushort value)
        {
            Update((long)value);
        }

        /// <summary>
        /// Updates this sketch with a 16-bit signed integer.
        /// </summary>
        public void Update(short value)
        {
            Update((long)value);
        }

        /// <summary>
        /// Updates this sketch with an 8-bit unsigned integer.
        /// </summary>
        public void Update(byte value)
        {
            Update((long)value);
        }

        /// <summary>
        /// Updates this sketch with an 8-bit signed integer.
        /// </summary>
        public void Update(sbyte value)
        {
            Update((long)value);
        }

        /// <summary>
        /// Updates this sketch with a double-precision floating point value.
        /// </summary>
        public void Update(double value)
        {
            long canonicalValue = CanonicalDouble(value);
            Span<byte> bytes = stackalloc byte[8];
            BitConverter.TryWriteBytes(bytes, canonicalValue);
            Update(bytes, bytes.Length);
        }

        /// <summary>
        /// Updates this sketch with a single-precision floating point value.
        /// </summary>
        public void Update(float value)
        {
            Update((double)value);
        }

        /// <summary>
        /// Updates this sketch with raw bytes.
        /// </summary>
        public void Update(ReadOnlySpan<byte> data, int length)
        {
            ulong hash = HashAndScreen(data, length);
            if (hash == 0) return;
            isEmpty = false;
            Insert(hash);
        }

        /// <summary>
        /// Removes retained entries in excess of the nominal size k (if any).
        /// </summary>
        public void Trim()
        {
            if (numEntries > (uint)(1 << lgNomSize))
            {
                Rebuild();
            }
        }

        /// <summary>
        /// Resets the sketch to the initial empty state.
        /// </summary>
        public void Reset()
        {
            isEmpty = true;
            numEntries = 0;
            theta = ThetaBuildHelper.StartingThetaFromP(p);
            Array.Clear(entries, 0, entries.Length);
        }

        /// <summary>
        /// Converts this sketch to a compact sketch.
        /// </summary>
        /// <param name="ordered">If true, produce an ordered sketch</param>
        public CompactThetaSketch Compact(bool ordered = true)
        {
            return new CompactThetaSketch(this, ordered);
        }

        public override IEnumerator<ulong> GetEnumerator()
        {
            for (int i = 0; i < entries.Length; i++)
            {
                if (entries[i] != 0)
                    yield return entries[i];
            }
        }

        protected override void PrintSpecifics(StringBuilder sb)
        {
            sb.AppendLine($"  Lg K: {lgNomSize}");
            sb.AppendLine($"  Lg Current Size: {lgCurSize}");
            sb.AppendLine($"  Resize Factor: {rf}");
            sb.AppendLine($"  Sampling Probability: {p}");
        }

        private ulong HashAndScreen(ReadOnlySpan<byte> data, int length)
        {
            MurmurHash3.Hash128(data.Slice(0, length), seed, out HashState hashes);
            ulong hash = hashes.H1 >> 1; // Unsigned shift to make positive
            return (hash < theta) ? hash : 0;
        }

        internal void Insert(ulong hash)
        {
            // Check if we need to resize/rebuild BEFORE trying to insert
            uint capacity = GetCapacity(lgCurSize, lgNomSize);
            if (numEntries >= capacity)
            {
                if (lgCurSize <= lgNomSize)
                {
                    Resize();
                }
                else
                {
                    Rebuild();
                }
            }

            // Now find a slot for this hash
            var (index, found) = FindSafe(hash);
            if (found) return;

            // If index is -1, table is full - resize and try again
            if (index < 0)
            {
                if (lgCurSize <= lgNomSize)
                {
                    Resize();
                }
                else
                {
                    Rebuild();
                }
                (index, found) = FindSafe(hash);
                if (found) return;

                if (index < 0)
                {
                    throw new InvalidOperationException("Hash table is full even after resize/rebuild!");
                }
            }

            isEmpty = false;
            entries[index] = hash;
            numEntries++;
        }

        private (int index, bool found) FindSafe(ulong key)
        {
            uint size = (uint)(1 << lgCurSize);
            uint index = (uint)(key & (size - 1));
            uint stride = GetStride(key, lgCurSize);
            uint startIndex = index;

            do
            {
                ulong entry = entries[index];
                if (entry == 0) return ((int)index, false);
                if (entry == key) return ((int)index, true);
                index = (index + stride) & (size - 1);
            } while (index != startIndex);

            // Table is full and key not found
            return (-1, false);
        }

        private (int index, bool found) Find(ulong key)
        {
            uint size = (uint)(1 << lgCurSize);
            uint index = (uint)(key & (size - 1));
            uint stride = GetStride(key, lgCurSize);
            uint startIndex = index;

            do
            {
                ulong entry = entries[index];
                if (entry == 0) return ((int)index, false);
                if (entry == key) return ((int)index, true);
                index = (index + stride) & (size - 1);
            } while (index != startIndex);

            // If we've looped all the way around, the table is full and key not found
            throw new InvalidOperationException("Hash table is full and key not found!");
        }

        private void Resize()
        {
            byte lgResizeFactor = (byte)rf;
            byte newLgCurSize = (byte)Math.Min(lgCurSize + lgResizeFactor, lgNomSize + 1);

            ulong[] oldEntries = entries;
            byte oldLgCurSize = lgCurSize;

            // Create new table
            entries = new ulong[1 << newLgCurSize];
            lgCurSize = newLgCurSize;
            numEntries = 0;

            // Re-insert entries from old table
            foreach (var entry in oldEntries)
            {
                if (entry != 0 && entry < theta)
                {
                    // Find empty slot in new table
                    uint size = (uint)(1 << lgCurSize);
                    uint index = (uint)(entry & (size - 1));
                    uint stride = GetStride(entry, lgCurSize);
                    uint startIndex = index;
                    uint probeCount = 0;

                    while (entries[index] != 0)
                    {
                        index = (index + stride) & (size - 1);
                        probeCount++;
                        if (probeCount > size || index == startIndex)
                        {
                            throw new InvalidOperationException($"Resize failed: cannot find empty slot. Size={size}, NumEntries={numEntries}, OldEntries={oldEntries.Length}");
                        }
                    }

                    entries[index] = entry;
                    numEntries++;
                }
            }
        }

        private void Rebuild()
        {
            // Only rebuild if we have more than nominal entries
            uint nominalSize = (uint)(1 << lgNomSize);
            if (numEntries <= nominalSize)
                return;


            // Find the (k+1)th smallest hash value
            var sortedHashes = new List<ulong>();
            foreach (var entry in entries)
            {
                if (entry != 0)
                    sortedHashes.Add(entry);
            }
            sortedHashes.Sort();

            int k = 1 << lgNomSize;
            if (sortedHashes.Count > k)
            {
                theta = sortedHashes[k];
            }

            // Rebuild hash table with new theta
            ulong[] oldEntries = entries;
            entries = new ulong[1 << lgCurSize];
            numEntries = 0;

            // Re-insert entries that are still below theta
            foreach (var entry in oldEntries)
            {
                if (entry != 0 && entry < theta)
                {
                    // Find empty slot in new table
                    uint size = (uint)(1 << lgCurSize);
                    uint index = (uint)(entry & (size - 1));
                    uint stride = GetStride(entry, lgCurSize);

                    while (entries[index] != 0)
                    {
                        index = (index + stride) & (size - 1);
                    }

                    entries[index] = entry;
                    numEntries++;
                }
            }
        }

        private static uint GetCapacity(byte lgCurSize, byte lgNomSize)
        {
            // Apply appropriate threshold based on whether we're resizing or rebuilding
            double fraction = (lgCurSize <= lgNomSize) ? ResizeThreshold : RebuildThreshold;
            return (uint)Math.Floor(fraction * (1 << lgCurSize));
        }

        private static uint GetStride(ulong key, byte lgSize)
        {
            uint stride = (uint)((key >> lgSize) & StrideMask);
            return (stride == 0) ? 1 : stride;
        }

        private static long CanonicalDouble(double value)
        {
            if (value == 0.0)
                return BitConverter.DoubleToInt64Bits(0.0);
            if (double.IsNaN(value))
                return 0x7ff8000000000000L; // Java's canonical NaN
            return BitConverter.DoubleToInt64Bits(value);
        }

        /// <summary>
        /// Builder for Update Theta Sketch.
        /// </summary>
        public class Builder
        {
            private byte lgK = ThetaConstants.DefaultLgK;
            private CommonDefs.ResizeFactor rf = ThetaConstants.DefaultResizeFactor;
            private float p = 1.0f;
            private ulong seed = CommonDefs.DEFAULT_SEED;

            /// <summary>
            /// Sets log2(k), where k is the nominal number of entries.
            /// </summary>
            public Builder SetLgK(byte lgK)
            {
                if (lgK < ThetaConstants.MinLgK || lgK > ThetaConstants.MaxLgK)
                    throw new ArgumentException($"lgK must be between {ThetaConstants.MinLgK} and {ThetaConstants.MaxLgK}");
                this.lgK = lgK;
                return this;
            }

            /// <summary>
            /// Sets the resize factor for the internal hash table.
            /// </summary>
            public Builder SetResizeFactor(CommonDefs.ResizeFactor rf)
            {
                this.rf = rf;
                return this;
            }

            /// <summary>
            /// Sets the sampling probability (initial theta).
            /// </summary>
            public Builder SetP(float p)
            {
                if (p <= 0.0f || p > 1.0f)
                    throw new ArgumentException("p must be in (0, 1]");
                this.p = p;
                return this;
            }

            /// <summary>
            /// Sets the seed for the hash function.
            /// </summary>
            public Builder SetSeed(ulong seed)
            {
                this.seed = seed;
                return this;
            }

            /// <summary>
            /// Builds the Update Theta Sketch.
            /// </summary>
            public UpdateThetaSketch Build()
            {
                ulong theta = ThetaBuildHelper.StartingThetaFromP(p);
                byte lgCurSize = StartingLgSize();
                return new UpdateThetaSketch(lgCurSize, lgK, rf, p, theta, seed);
            }

            private byte StartingLgSize()
            {
                byte lgRf = (byte)rf;
                return ThetaBuildHelper.StartingSubMultiple(lgK, ThetaConstants.MinLgK, lgRf);
            }
        }
    }
}

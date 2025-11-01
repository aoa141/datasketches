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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using DataSketches.Common;

namespace DataSketches.Theta
{
    /// <summary>
    /// Compact Theta sketch - an immutable form that can be serialized.
    /// </summary>
    public class CompactThetaSketch : ThetaSketch
    {
        public const byte UncompressedSerialVersion = 3;
        public const byte CompressedSerialVersion = 4;
        public const byte SketchType = 3;

        private readonly bool isEmpty;
        private readonly bool isOrdered;
        private readonly ushort seedHash;
        private readonly ulong theta;
        private readonly ulong[] entries;

        /// <summary>
        /// Constructs a compact sketch from another sketch.
        /// </summary>
        public CompactThetaSketch(ThetaSketch other, bool ordered)
        {
            isEmpty = other.IsEmpty;
            seedHash = other.GetSeedHash();
            theta = other.GetTheta64();
            isOrdered = ordered;

            var entryList = new List<ulong>();
            foreach (var entry in other)
            {
                if (entry != 0 && entry < theta)
                {
                    entryList.Add(entry);
                }
            }

            if (ordered)
            {
                entryList.Sort();
            }

            entries = entryList.ToArray();
        }

        /// <summary>
        /// Internal constructor for creating from components.
        /// </summary>
        internal CompactThetaSketch(bool isEmpty, bool isOrdered, ushort seedHash, ulong theta, ulong[] entries)
        {
            this.isEmpty = isEmpty;
            this.isOrdered = isOrdered;
            this.seedHash = seedHash;
            this.theta = theta;
            this.entries = entries;
        }

        public override bool IsEmpty => isEmpty;
        public override bool IsOrdered => isOrdered;
        public override ushort GetSeedHash() => seedHash;
        public override ulong GetTheta64() => theta;
        public override uint GetNumRetained() => (uint)entries.Length;

        /// <summary>
        /// Computes maximum serialized size in bytes.
        /// </summary>
        public static long GetMaxSerializedSizeBytes(byte lgK)
        {
            int k = 1 << lgK;
            return (3 + k) * 8L; // preamble + entries
        }

        /// <summary>
        /// Computes size in bytes required to serialize the current state.
        /// </summary>
        public long GetSerializedSizeBytes(bool compressed = false)
        {
            if (compressed && IsSuitableForCompression())
            {
                byte entryBits = ComputeEntryBits();
                byte numEntriesBytes = GetNumEntriesBytes();
                return GetCompressedSerializedSizeBytes(entryBits, numEntriesBytes);
            }
            return GetUncompressedSerializedSizeBytes();
        }

        /// <summary>
        /// Serializes the sketch to a stream.
        /// </summary>
        public void Serialize(Stream stream)
        {
            using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, true))
            {
                SerializeUncompressed(writer);
            }
        }

        /// <summary>
        /// Serializes the sketch to a byte array.
        /// </summary>
        public byte[] Serialize(uint headerSizeBytes = 0)
        {
            long size = GetSerializedSizeBytes(false);
            byte[] bytes = new byte[headerSizeBytes + size];
            using (var ms = new MemoryStream(bytes, (int)headerSizeBytes, (int)size))
            {
                Serialize(ms);
            }
            return bytes;
        }

        /// <summary>
        /// Deserializes a sketch from a stream.
        /// </summary>
        public static CompactThetaSketch Deserialize(Stream stream, ulong seed = CommonDefs.DEFAULT_SEED)
        {
            using (var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, true))
            {
                return DeserializeInternal(reader, seed);
            }
        }

        /// <summary>
        /// Deserializes a sketch from a byte array.
        /// </summary>
        public static CompactThetaSketch Deserialize(byte[] bytes, int offset, int size, ulong seed = CommonDefs.DEFAULT_SEED)
        {
            using (var ms = new MemoryStream(bytes, offset, size))
            {
                return Deserialize(ms, seed);
            }
        }

        public override IEnumerator<ulong> GetEnumerator()
        {
            return ((IEnumerable<ulong>)entries).GetEnumerator();
        }

        protected override void PrintSpecifics(StringBuilder sb)
        {
            sb.AppendLine($"  Sketch Type: Compact");
        }

        private void SerializeUncompressed(BinaryWriter writer)
        {
            byte preambleLongs = GetPreambleLongs(false);

            // First byte: preamble longs
            writer.Write(preambleLongs);

            // Second byte: serial version
            writer.Write(UncompressedSerialVersion);

            // Third byte: family ID (Theta = 3)
            writer.Write((byte)3);

            // Fourth byte: flags
            byte flags = 0;
            if (isEmpty) flags |= (1 << 2); // IS_EMPTY flag
            flags |= (1 << 3); // IS_COMPACT flag
            if (isOrdered) flags |= (1 << 4); // IS_ORDERED flag
            writer.Write(flags);

            // Seed hash (2 bytes)
            writer.Write(seedHash);

            // Padding (2 bytes)
            writer.Write((ushort)0);

            // Number of retained entries
            writer.Write((uint)entries.Length);

            // Padding to 8-byte alignment
            writer.Write((uint)0);

            // Theta
            writer.Write(theta);

            // Entries
            foreach (var entry in entries)
            {
                writer.Write(entry);
            }
        }

        private static CompactThetaSketch DeserializeInternal(BinaryReader reader, ulong seed)
        {
            byte preambleLongs = reader.ReadByte();
            byte serialVersion = reader.ReadByte();
            byte familyId = reader.ReadByte();
            byte flags = reader.ReadByte();

            ushort seedHash = reader.ReadUInt16();
            ushort computedSeedHash = MurmurHash3.ComputeSeedHash(seed);
            Checker.CheckSeedHash(seedHash, computedSeedHash);

            reader.ReadUInt16(); // padding

            uint numEntries = reader.ReadUInt32();
            reader.ReadUInt32(); // padding

            ulong theta = reader.ReadUInt64();

            bool isEmpty = (flags & (1 << 2)) != 0;
            bool isOrdered = (flags & (1 << 4)) != 0;

            ulong[] entries = new ulong[numEntries];
            for (int i = 0; i < numEntries; i++)
            {
                entries[i] = reader.ReadUInt64();
            }

            return new CompactThetaSketch(isEmpty, isOrdered, seedHash, theta, entries);
        }

        private byte GetPreambleLongs(bool compressed)
        {
            if (isEmpty) return 1;
            if (entries.Length == 1 && !compressed) return 2;
            return 3;
        }

        private bool IsSuitableForCompression()
        {
            return isOrdered && entries.Length > 1;
        }

        private byte ComputeEntryBits()
        {
            if (entries.Length == 0) return 0;

            ulong maxDelta = 0;
            for (int i = 0; i < entries.Length; i++)
            {
                ulong current = entries[i];
                ulong previous = i > 0 ? entries[i - 1] : 0;
                ulong delta = current - previous;
                if (delta > maxDelta) maxDelta = delta;
            }

            // Count bits needed
            byte bits = 0;
            while (maxDelta > 0)
            {
                bits++;
                maxDelta >>= 1;
            }
            return bits;
        }

        private byte GetNumEntriesBytes()
        {
            uint num = (uint)entries.Length;
            if (num <= 0xFF) return 1;
            if (num <= 0xFFFF) return 2;
            return 4;
        }

        private long GetCompressedSerializedSizeBytes(byte entryBits, byte numEntriesBytes)
        {
            long size = 8; // preamble
            size += numEntriesBytes; // num entries
            size += 8; // theta
            size += (entries.Length * entryBits + 7) / 8; // packed entries
            return size;
        }

        private long GetUncompressedSerializedSizeBytes()
        {
            byte preambleLongs = GetPreambleLongs(false);
            return (preambleLongs * 8L) + (entries.Length * 8L); // preamble + entries
        }
    }
}

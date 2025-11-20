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
using System.Text;
using Apache.DataSketches.Common;
using Apache.DataSketches.ThetaCommon;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// This class defines the preamble data structure and provides basic utilities for some of the key
    /// fields.
    /// <para>
    /// The intent of the design of this class was to isolate the detailed knowledge of the bit and
    /// byte layout of the serialized form of the sketches derived from the Sketch class into one place.
    /// This allows the possibility of the introduction of different serialization
    /// schemes with minimal impact on the rest of the library.
    /// </para>
    /// <para>
    /// MAP: Low significance bytes of this <i>long</i> data structure are on the right. However, the
    /// multi-byte integers (<i>int</i> and <i>long</i>) are stored in native byte order. The
    /// <i>byte</i> values are treated as unsigned.
    /// </para>
    /// <para>
    /// An empty CompactSketch only requires 8 bytes.
    /// Flags: notSI, Ordered*, Compact, Empty*, ReadOnly, LE.
    /// (*) Earlier versions did not set these.
    /// </para>
    /// <para>
    /// Preamble Layout:
    /// </para>
    /// <code>
    /// Long || Start Byte Adr:
    /// Adr:
    ///      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
    ///  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 1   |
    /// </code>
    /// <para>
    /// A SingleItemSketch (extends CompactSketch) requires an 8 byte preamble plus a single
    /// hash item of 8 bytes. Flags: SingleItem*, Ordered, Compact, notEmpty, ReadOnly, LE.
    /// (*) Earlier versions did not set these.
    /// </para>
    /// <code>
    /// Long || Start Byte Adr:
    /// Adr:
    ///      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
    ///  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 1   |
    ///
    ///      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
    ///  1   ||---------------------------Single long hash----------------------------------------|
    /// </code>
    /// <para>
    /// An exact (non-estimating) CompactSketch requires 16 bytes of preamble plus a compact array of
    /// longs.
    /// </para>
    /// <code>
    /// Long || Start Byte Adr:
    /// Adr:
    ///      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
    ///  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 2   |
    ///
    ///      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
    ///  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
    ///
    ///      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
    ///  2   ||----------------------Start of Compact Long Array----------------------------------|
    /// </code>
    /// <para>
    /// An estimating CompactSketch requires 24 bytes of preamble plus a compact array of longs.
    /// </para>
    /// <code>
    /// Long || Start Byte Adr:
    /// Adr:
    ///      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
    ///  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 3   |
    ///
    ///      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
    ///  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
    ///
    ///      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
    ///  2   ||------------------------------THETA_LONG-------------------------------------------|
    ///
    ///      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
    ///  3   ||----------------------Start of Compact Long Array----------------------------------|
    /// </code>
    /// <para>
    /// The UpdateSketch and AlphaSketch require 24 bytes of preamble followed by a non-compact
    /// array of longs representing a hash table.
    /// </para>
    /// <para>
    /// The following table applies to both the Theta UpdateSketch and the Alpha Sketch
    /// </para>
    /// <code>
    /// Long || Start Byte Adr:
    /// Adr:
    ///      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
    ///  0   ||    Seed Hash    | Flags  |  LgArr |  lgNom | FamID  | SerVer | RF, PreLongs = 3   |
    ///
    ///      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
    ///  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
    ///
    ///      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
    ///  2   ||------------------------------THETA_LONG-------------------------------------------|
    ///
    ///      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
    ///  3   ||----------------------Start of Hash Table of longs---------------------------------|
    /// </code>
    /// <para>
    /// Union objects require 32 bytes of preamble plus a non-compact array of longs representing a
    /// hash table.
    /// </para>
    /// <code>
    /// Long || Start Byte Adr:
    /// Adr:
    ///      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
    ///  0   ||    Seed Hash    | Flags  |  LgArr |  lgNom | FamID  | SerVer | RF, PreLongs = 4   |
    ///
    ///      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
    ///  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
    ///
    ///      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
    ///  2   ||------------------------------THETA_LONG-------------------------------------------|
    ///
    ///      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
    ///  3   ||---------------------------UNION THETA LONG----------------------------------------|
    ///
    ///      ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |    32              |
    ///  4   ||----------------------Start of Hash Table of longs---------------------------------|
    /// </code>
    /// </summary>
    internal static class PreambleUtil
    {
        // Preamble byte Addresses
        internal const int PREAMBLE_LONGS_BYTE = 0; //lower 6 bits in byte.
        internal const int LG_RESIZE_FACTOR_BIT = 6; //upper 2 bits in byte. Not used by compact, direct
        internal const int SER_VER_BYTE = 1;
        internal const int FAMILY_BYTE = 2; //SerVer1,2 was SKETCH_TYPE_BYTE
        internal const int LG_NOM_LONGS_BYTE = 3; //not used by compact
        internal const int LG_ARR_LONGS_BYTE = 4; //not used by compact
        internal const int FLAGS_BYTE = 5;
        internal const int SEED_HASH_SHORT = 6;  //byte 6,7
        internal const int RETAINED_ENTRIES_INT = 8;  //8 byte aligned
        internal const int P_FLOAT = 12; //4 byte aligned, not used by compact
        internal const int THETA_LONG = 16; //8-byte aligned
        internal const int UNION_THETA_LONG = 24; //8-byte aligned, only used by Union

        // flag bit masks
        internal const int BIG_ENDIAN_FLAG_MASK = 1; //SerVer 1, 2, 3
        internal const int READ_ONLY_FLAG_MASK = 2; //Set but not read. Reserved. SerVer 1, 2, 3
        internal const int EMPTY_FLAG_MASK = 4; //SerVer 2, 3
        internal const int COMPACT_FLAG_MASK = 8; //SerVer 2 was NO_REBUILD_FLAG_MASK, 3
        internal const int ORDERED_FLAG_MASK = 16;//SerVer 2 was UNORDERED_FLAG_MASK, 3
        internal const int SINGLEITEM_FLAG_MASK = 32;//SerVer 3
        //The last 2 bits of the flags byte are reserved and assumed to be zero, for now.

        //Backward compatibility: SerVer1 preamble always 3 longs, SerVer2 preamble: 1, 2, 3 longs
        //               SKETCH_TYPE_BYTE             2  //SerVer1, SerVer2
        //  V1, V2 types:  Alpha = 1, QuickSelect = 2, SetSketch = 3; V3 only: Buffered QS = 4
        internal const int LG_RESIZE_RATIO_BYTE_V1 = 5; //used by SerVer 1
        internal const int FLAGS_BYTE_V1 = 6; //used by SerVer 1

        //Other constants
        internal const int SER_VER = 3;

        // serial version 4 compressed ordered sketch, not empty, not single item
        internal const int ENTRY_BITS_BYTE_V4 = 3; // number of bits packed in deltas between hashes
        internal const int NUM_ENTRIES_BYTES_BYTE_V4 = 4; // number of bytes used for the number of entries
        internal const int THETA_LONG_V4 = 8; //8-byte aligned

        internal static readonly bool NATIVE_ORDER_IS_BIG_ENDIAN =
            !BitConverter.IsLittleEndian;

        /// <summary>
        /// Computes the number of bytes required for a non-full sized sketch in hash-table form.
        /// This can be used to compute current storage size for heap sketches, or current off-heap memory
        /// required for off-heap (direct) sketches. This does not apply for compact sketches.
        /// </summary>
        /// <param name="lgArrLongs">log2(current hash-table size)</param>
        /// <param name="preambleLongs">current preamble size</param>
        /// <returns>the size in bytes</returns>
        internal static int GetMemBytes(int lgArrLongs, int preambleLongs)
        {
            return (8 << lgArrLongs) + (preambleLongs << 3);
        }

        // STRINGS

        /// <summary>
        /// Returns a human readable string summary of the preamble state of the given byte array.
        /// Used primarily in testing.
        /// </summary>
        /// <param name="byteArr">the given byte array.</param>
        /// <returns>the summary preamble string.</returns>
        internal static string PreambleToString(byte[] byteArr)
        {
            return PreambleToString(new ReadOnlySpan<byte>(byteArr));
        }

        /// <summary>
        /// Returns a human readable string summary of the preamble state of the given Memory.
        /// Note: other than making sure that the given Memory size is large
        /// enough for just the preamble, this does not do much value checking of the contents of the
        /// preamble as this is primarily a tool for debugging the preamble visually.
        /// </summary>
        /// <param name="mem">the given Memory.</param>
        /// <returns>the summary preamble string.</returns>
        internal static string PreambleToString(ReadOnlySpan<byte> mem)
        {
            int preLongs = GetAndCheckPreLongs(mem);
            int rfId = ExtractLgResizeFactor(mem);
            ResizeFactor rf = ResizeFactorExtensions.GetRF(rfId);
            int serVer = ExtractSerVer(mem);
            int familyId = ExtractFamilyID(mem);
            Family family = FamilyExtensions.IdToFamily(familyId);
            int lgNomLongs = ExtractLgNomLongs(mem);
            int lgArrLongs = ExtractLgArrLongs(mem);

            //Flags
            int flags = ExtractFlags(mem);
            string flagsStr = $"{flags}, 0x{flags:X}, {Convert.ToString(flags, 2).PadLeft(8, '0')}";
            string nativeOrder = BitConverter.IsLittleEndian ? "LITTLE_ENDIAN" : "BIG_ENDIAN";
            bool bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
            bool readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
            bool empty = (flags & EMPTY_FLAG_MASK) > 0;
            bool compact = (flags & COMPACT_FLAG_MASK) > 0;
            bool ordered = (flags & ORDERED_FLAG_MASK) > 0;
            bool singleItem = (flags & SINGLEITEM_FLAG_MASK) > 0;

            int seedHash = ExtractSeedHash(mem);

            //assumes preLongs == 1; empty or singleItem
            int curCount = singleItem ? 1 : 0;
            float p = 1.0f;            //preLongs 1 or 2
            long thetaLong = long.MaxValue;  //preLongs 1 or 2
            long thetaULong = thetaLong;      //preLongs 1, 2 or 3

            if (preLongs == 2)
            { //exact (non-estimating) CompactSketch
                curCount = ExtractCurCount(mem);
                p = ExtractP(mem);
            }
            else if (preLongs == 3)
            { //Update Sketch
                curCount = ExtractCurCount(mem);
                p = ExtractP(mem);
                thetaLong = ExtractThetaLong(mem);
                thetaULong = thetaLong;
            }
            else if (preLongs == 4)
            { //Union
                curCount = ExtractCurCount(mem);
                p = ExtractP(mem);
                thetaLong = ExtractThetaLong(mem);
                thetaULong = ExtractUnionThetaLong(mem);
            }
            //else the same as an empty sketch or singleItem

            double thetaDbl = thetaLong / Util.LONG_MAX_VALUE_AS_DOUBLE;
            string thetaHex = thetaLong.ToString("X16");
            double thetaUDbl = thetaULong / Util.LONG_MAX_VALUE_AS_DOUBLE;
            string thetaUHex = thetaULong.ToString("X16");

            var sb = new StringBuilder();
            sb.AppendLine();
            sb.AppendLine("### SKETCH PREAMBLE SUMMARY:");
            sb.AppendLine($"Native Byte Order             : {nativeOrder}");
            sb.AppendLine($"Byte  0: Preamble Longs       : {preLongs}");
            sb.AppendLine($"Byte  0: ResizeFactor         : {rfId}, {rf}");
            sb.AppendLine($"Byte  1: Serialization Version: {serVer}");
            sb.AppendLine($"Byte  2: Family               : {familyId}, {family}");
            sb.AppendLine($"Byte  3: LgNomLongs           : {lgNomLongs}");
            sb.AppendLine($"Byte  4: LgArrLongs           : {lgArrLongs}");
            sb.AppendLine($"Byte  5: Flags Field          : {flagsStr}");
            sb.AppendLine("  Bit Flag Name               : State:");
            sb.AppendLine($"    0 BIG_ENDIAN_STORAGE      : {bigEndian}");
            sb.AppendLine($"    1 READ_ONLY               : {readOnly}");
            sb.AppendLine($"    2 EMPTY                   : {empty}");
            sb.AppendLine($"    3 COMPACT                 : {compact}");
            sb.AppendLine($"    4 ORDERED                 : {ordered}");
            sb.AppendLine($"    5 SINGLE_ITEM             : {singleItem}");
            sb.AppendLine($"Bytes 6-7  : Seed Hash Hex    : {seedHash:X}");
            if (preLongs == 1)
            {
                sb.AppendLine(" --ABSENT FIELDS, ASSUMED:");
                sb.AppendLine($"Bytes 8-11 : CurrentCount     : {curCount}");
                sb.AppendLine($"Bytes 12-15: P                : {p}");
                sb.AppendLine($"Bytes 16-23: Theta (double)   : {thetaDbl}");
                sb.AppendLine($"             Theta (long)     : {thetaLong}");
                sb.AppendLine($"             Theta (long,hex) : {thetaHex}");
            }
            else if (preLongs == 2)
            {
                sb.AppendLine($"Bytes 8-11 : CurrentCount     : {curCount}");
                sb.AppendLine($"Bytes 12-15: P                : {p}");
                sb.AppendLine(" --ABSENT, ASSUMED:");
                sb.AppendLine($"Bytes 16-23: Theta (double)   : {thetaDbl}");
                sb.AppendLine($"             Theta (long)     : {thetaLong}");
                sb.AppendLine($"             Theta (long,hex) : {thetaHex}");
            }
            else if (preLongs == 3)
            {
                sb.AppendLine($"Bytes 8-11 : CurrentCount     : {curCount}");
                sb.AppendLine($"Bytes 12-15: P                : {p}");
                sb.AppendLine($"Bytes 16-23: Theta (double)   : {thetaDbl}");
                sb.AppendLine($"             Theta (long)     : {thetaLong}");
                sb.AppendLine($"             Theta (long,hex) : {thetaHex}");
            }
            else
            { //preLongs == 4
                sb.AppendLine($"Bytes 8-11 : CurrentCount     : {curCount}");
                sb.AppendLine($"Bytes 12-15: P                : {p}");
                sb.AppendLine($"Bytes 16-23: Theta (double)   : {thetaDbl}");
                sb.AppendLine($"             Theta (long)     : {thetaLong}");
                sb.AppendLine($"             Theta (long,hex) : {thetaHex}");
                sb.AppendLine($"Bytes 25-31: ThetaU (double)  : {thetaUDbl}");
                sb.AppendLine($"             ThetaU (long)    : {thetaULong}");
                sb.AppendLine($"             ThetaU (long,hex): {thetaUHex}");
            }
            sb.AppendLine($"Preamble Bytes                : {preLongs * 8}");
            sb.AppendLine($"Data Bytes                    : {curCount * 8}");
            sb.AppendLine($"TOTAL Sketch Bytes            : {(preLongs + curCount) * 8}");
            sb.AppendLine($"TOTAL Capacity Bytes          : {mem.Length}");
            sb.AppendLine("### END SKETCH PREAMBLE SUMMARY");
            return sb.ToString();
        }

        internal static int ExtractPreLongs(ReadOnlySpan<byte> mem)
        {
            return mem[PREAMBLE_LONGS_BYTE] & 0x3F;
        }

        internal static int ExtractLgResizeFactor(ReadOnlySpan<byte> mem)
        {
            return (mem[PREAMBLE_LONGS_BYTE] >> LG_RESIZE_FACTOR_BIT) & 0x3;
        }

        internal static int ExtractLgResizeRatioV1(ReadOnlySpan<byte> mem)
        {
            return mem[LG_RESIZE_RATIO_BYTE_V1] & 0x3;
        }

        internal static int ExtractSerVer(ReadOnlySpan<byte> mem)
        {
            return mem[SER_VER_BYTE] & 0xFF;
        }

        internal static int ExtractFamilyID(ReadOnlySpan<byte> mem)
        {
            return mem[FAMILY_BYTE] & 0xFF;
        }

        internal static int ExtractLgNomLongs(ReadOnlySpan<byte> mem)
        {
            return mem[LG_NOM_LONGS_BYTE] & 0xFF;
        }

        internal static int ExtractLgArrLongs(ReadOnlySpan<byte> mem)
        {
            return mem[LG_ARR_LONGS_BYTE] & 0xFF;
        }

        internal static int ExtractFlags(ReadOnlySpan<byte> mem)
        {
            return mem[FLAGS_BYTE] & 0xFF;
        }

        internal static int ExtractFlagsV1(ReadOnlySpan<byte> mem)
        {
            return mem[FLAGS_BYTE_V1] & 0xFF;
        }

        internal static int ExtractSeedHash(ReadOnlySpan<byte> mem)
        {
            return BinaryPrimitives.ReadUInt16LittleEndian(mem.Slice(SEED_HASH_SHORT));
        }

        internal static int ExtractCurCount(ReadOnlySpan<byte> mem)
        {
            return BinaryPrimitives.ReadInt32LittleEndian(mem.Slice(RETAINED_ENTRIES_INT));
        }

        internal static float ExtractP(ReadOnlySpan<byte> mem)
        {
            return BinaryPrimitives.ReadSingleLittleEndian(mem.Slice(P_FLOAT));
        }

        internal static long ExtractThetaLong(ReadOnlySpan<byte> mem)
        {
            return BinaryPrimitives.ReadInt64LittleEndian(mem.Slice(THETA_LONG));
        }

        internal static long ExtractUnionThetaLong(ReadOnlySpan<byte> mem)
        {
            return BinaryPrimitives.ReadInt64LittleEndian(mem.Slice(UNION_THETA_LONG));
        }

        internal static int ExtractEntryBitsV4(ReadOnlySpan<byte> mem)
        {
            return mem[ENTRY_BITS_BYTE_V4] & 0xFF;
        }

        internal static int ExtractNumEntriesBytesV4(ReadOnlySpan<byte> mem)
        {
            return mem[NUM_ENTRIES_BYTES_BYTE_V4] & 0xFF;
        }

        internal static long ExtractThetaLongV4(ReadOnlySpan<byte> mem)
        {
            return BinaryPrimitives.ReadInt64LittleEndian(mem.Slice(THETA_LONG_V4));
        }

        /// <summary>
        /// Sets PreLongs in the low 6 bits and sets LgRF in the upper 2 bits = 0.
        /// </summary>
        /// <param name="wmem">the target WritableMemory</param>
        /// <param name="preLongs">the given number of preamble longs</param>
        internal static void InsertPreLongs(Span<byte> wmem, int preLongs)
        {
            wmem[PREAMBLE_LONGS_BYTE] = (byte)(preLongs & 0x3F);
        }

        /// <summary>
        /// Sets the top 2 lgRF bits and does not affect the lower 6 bits (PreLongs).
        /// To work properly, this should be called after insertPreLongs().
        /// </summary>
        /// <param name="wmem">the target WritableMemory</param>
        /// <param name="rf">the given lgRF bits</param>
        internal static void InsertLgResizeFactor(Span<byte> wmem, int rf)
        {
            int curByte = wmem[PREAMBLE_LONGS_BYTE] & 0xFF;
            int shift = LG_RESIZE_FACTOR_BIT; // shift in bits
            int mask = 3;
            byte newByte = (byte)(((rf & mask) << shift) | (~(mask << shift) & curByte));
            wmem[PREAMBLE_LONGS_BYTE] = newByte;
        }

        internal static void InsertSerVer(Span<byte> wmem, int serVer)
        {
            wmem[SER_VER_BYTE] = (byte)serVer;
        }

        internal static void InsertFamilyID(Span<byte> wmem, int famId)
        {
            wmem[FAMILY_BYTE] = (byte)famId;
        }

        internal static void InsertLgNomLongs(Span<byte> wmem, int lgNomLongs)
        {
            wmem[LG_NOM_LONGS_BYTE] = (byte)lgNomLongs;
        }

        internal static void InsertLgArrLongs(Span<byte> wmem, int lgArrLongs)
        {
            wmem[LG_ARR_LONGS_BYTE] = (byte)lgArrLongs;
        }

        internal static void InsertFlags(Span<byte> wmem, int flags)
        {
            wmem[FLAGS_BYTE] = (byte)flags;
        }

        internal static void InsertSeedHash(Span<byte> wmem, int seedHash)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(wmem.Slice(SEED_HASH_SHORT), (ushort)seedHash);
        }

        internal static void InsertCurCount(Span<byte> wmem, int curCount)
        {
            BinaryPrimitives.WriteInt32LittleEndian(wmem.Slice(RETAINED_ENTRIES_INT), curCount);
        }

        internal static void InsertP(Span<byte> wmem, float p)
        {
            BinaryPrimitives.WriteSingleLittleEndian(wmem.Slice(P_FLOAT), p);
        }

        internal static void InsertThetaLong(Span<byte> wmem, long thetaLong)
        {
            BinaryPrimitives.WriteInt64LittleEndian(wmem.Slice(THETA_LONG), thetaLong);
        }

        internal static void InsertUnionThetaLong(Span<byte> wmem, long unionThetaLong)
        {
            BinaryPrimitives.WriteInt64LittleEndian(wmem.Slice(UNION_THETA_LONG), unionThetaLong);
        }

        internal static void SetEmpty(Span<byte> wmem)
        {
            int flags = wmem[FLAGS_BYTE] & 0xFF;
            flags |= EMPTY_FLAG_MASK;
            wmem[FLAGS_BYTE] = (byte)flags;
        }

        internal static void ClearEmpty(Span<byte> wmem)
        {
            int flags = wmem[FLAGS_BYTE] & 0xFF;
            flags &= ~EMPTY_FLAG_MASK;
            wmem[FLAGS_BYTE] = (byte)flags;
        }

        internal static bool IsEmptyFlag(ReadOnlySpan<byte> mem)
        {
            return (ExtractFlags(mem) & EMPTY_FLAG_MASK) > 0;
        }

        /// <summary>
        /// Checks Memory for capacity to hold the preamble and returns the extracted preLongs.
        /// </summary>
        /// <param name="mem">the given Memory</param>
        /// <returns>the extracted prelongs value.</returns>
        internal static int GetAndCheckPreLongs(ReadOnlySpan<byte> mem)
        {
            long cap = mem.Length;
            if (cap < 8)
            {
                ThrowNotBigEnough(cap, 8);
            }
            int preLongs = ExtractPreLongs(mem);
            int required = Math.Max(preLongs << 3, 8);
            if (cap < required)
            {
                ThrowNotBigEnough(cap, required);
            }
            return preLongs;
        }

        internal static short CheckMemorySeedHash(ReadOnlySpan<byte> mem, long seed)
        {
            short seedHashMem = (short)ExtractSeedHash(mem);
            ThetaUtil.CheckSeedHashes(seedHashMem, ThetaUtil.ComputeSeedHash(seed)); //throws if bad seedHash
            return seedHashMem;
        }

        private static void ThrowNotBigEnough(long cap, int required)
        {
            throw new SketchesArgumentException(
                $"Possible Corruption: Size of byte array or Memory not large enough: Size: {cap}, Required: {required}");
        }
    }
}

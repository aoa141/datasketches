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
using System.Text;
using Apache.DataSketches.Common;
using Apache.DataSketches.ThetaCommon;
using static Apache.DataSketches.Common.Family;
using static Apache.DataSketches.Common.Util;
using static Apache.DataSketches.Theta.PreambleUtil;
using static Apache.DataSketches.ThetaCommon.HashOperations;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// The top-level class for all theta sketches. This class is never constructed directly.
    /// Use the UpdateSketch.Builder() methods to create UpdateSketches.
    /// </summary>
    public abstract class Sketch
    {
        internal const int DEFAULT_LG_RESIZE_FACTOR = 3;   //Unique to Heap

        protected Sketch() { }

        //public static factory constructor-type methods

        /// <summary>
        /// Heapify takes the sketch image in Memory and instantiates an on-heap Sketch.
        /// <para>
        /// The resulting sketch will not retain any link to the source Memory.
        /// </para>
        /// <para>
        /// For Update Sketches this method checks if the Default Update Seed was used to create the source Memory image.
        /// </para>
        /// <para>
        /// For Compact Sketches this method assumes that the sketch image was created with the
        /// correct hash seed, so it is not checked.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a Sketch.</param>
        /// <returns>a Sketch on the heap.</returns>
        public static Sketch Heapify(ReadOnlySpan<byte> srcMem)
        {
            byte familyID = srcMem[FAMILY_BYTE];
            Family family = FamilyExtensions.IdToFamily(familyID);
            if (family == Family.COMPACT)
            {
                return CompactSketch.Heapify(srcMem);
            }
            return HeapifyUpdateFromMemory(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
        }

        /// <summary>
        /// Heapify takes the sketch image in Memory and instantiates an on-heap Sketch.
        /// <para>
        /// The resulting sketch will not retain any link to the source Memory.
        /// </para>
        /// <para>
        /// For Update and Compact Sketches this method checks if the given expectedSeed was used to
        /// create the source Memory image.  However, SerialVersion 1 sketches cannot be checked.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a Sketch that was created using the given expectedSeed.</param>
        /// <param name="expectedSeed">the seed used to validate the given Memory image.
        /// Compact sketches store a 16-bit hash of the seed, but not the seed itself.</param>
        /// <returns>a Sketch on the heap.</returns>
        public static Sketch Heapify(ReadOnlySpan<byte> srcMem, long expectedSeed)
        {
            byte familyID = srcMem[FAMILY_BYTE];
            Family family = FamilyExtensions.IdToFamily(familyID);
            if (family == Family.COMPACT)
            {
                return CompactSketch.Heapify(srcMem, expectedSeed);
            }
            return HeapifyUpdateFromMemory(srcMem, expectedSeed);
        }

        /// <summary>
        /// Wrap takes the sketch image in the given Memory and refers to it directly.
        /// There is no data copying onto the java heap.
        /// The wrap operation enables fast read-only merging and access to all the public read-only API.
        /// <para>
        /// Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
        /// been explicitly stored as direct sketches can be wrapped.
        /// Wrapping earlier serial version sketches will result in a on-heap CompactSketch
        /// where all data will be copied to the heap. These early versions were never designed to
        /// "wrap".
        /// </para>
        /// <para>
        /// Wrapping any subclass of this class that is empty or contains only a single item will
        /// result in on-heap equivalent forms of empty and single item sketch respectively.
        /// This is actually faster and consumes less overall memory.
        /// </para>
        /// <para>
        /// For Update Sketches this method checks if the Default Update Seed was used to create the source Memory image.
        /// </para>
        /// <para>
        /// For Compact Sketches this method assumes that the sketch image was created with the
        /// correct hash seed, so it is not checked.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a Sketch.</param>
        /// <returns>a Sketch backed by the given Memory</returns>
        public static Sketch Wrap(ReadOnlySpan<byte> srcMem)
        {
            int preLongs = srcMem[PREAMBLE_LONGS_BYTE] & 0x3F;
            int serVer = srcMem[SER_VER_BYTE] & 0xFF;
            int familyID = srcMem[FAMILY_BYTE] & 0xFF;
            Family family = FamilyExtensions.IdToFamily(familyID);
            if (family == Family.QUICKSELECT)
            {
                if (serVer == 3 && preLongs == 3)
                {
                    // Note: DirectQuickSelectSketchR not yet translated
                    throw new SketchesArgumentException(
                        "Direct sketches not yet supported in this C# implementation");
                    //return DirectQuickSelectSketchR.ReadOnlyWrap(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
                }
                else
                {
                    throw new SketchesArgumentException(
                        $"Corrupted: {family} family image: must have SerVer = 3 and preLongs = 3");
                }
            }
            if (family == Family.COMPACT)
            {
                return CompactSketch.Wrap(srcMem);
            }
            throw new SketchesArgumentException(
                $"Cannot wrap family: {family} as a Sketch");
        }

        /// <summary>
        /// Wrap takes the sketch image in the given Memory and refers to it directly.
        /// There is no data copying onto the java heap.
        /// The wrap operation enables fast read-only merging and access to all the public read-only API.
        /// <para>
        /// Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
        /// been explicitly stored as direct sketches can be wrapped.
        /// Wrapping earlier serial version sketches will result in a on-heap CompactSketch
        /// where all data will be copied to the heap. These early versions were never designed to
        /// "wrap".
        /// </para>
        /// <para>
        /// Wrapping any subclass of this class that is empty or contains only a single item will
        /// result in on-heap equivalent forms of empty and single item sketch respectively.
        /// This is actually faster and consumes less overall memory.
        /// </para>
        /// <para>
        /// For Update and Compact Sketches this method checks if the given expectedSeed was used to
        /// create the source Memory image.  However, SerialVersion 1 sketches cannot be checked.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a Sketch.</param>
        /// <param name="expectedSeed">the seed used to validate the given Memory image.</param>
        /// <returns>a UpdateSketch backed by the given Memory except as above.</returns>
        public static Sketch Wrap(ReadOnlySpan<byte> srcMem, long expectedSeed)
        {
            int preLongs = srcMem[PREAMBLE_LONGS_BYTE] & 0x3F;
            int serVer = srcMem[SER_VER_BYTE] & 0xFF;
            int familyID = srcMem[FAMILY_BYTE] & 0xFF;
            Family family = FamilyExtensions.IdToFamily(familyID);
            if (family == Family.QUICKSELECT)
            {
                if (serVer == 3 && preLongs == 3)
                {
                    // Note: DirectQuickSelectSketchR not yet translated
                    throw new SketchesArgumentException(
                        "Direct sketches not yet supported in this C# implementation");
                    //return DirectQuickSelectSketchR.ReadOnlyWrap(srcMem, expectedSeed);
                }
                else
                {
                    throw new SketchesArgumentException(
                        $"Corrupted: {family} family image: must have SerVer = 3 and preLongs = 3");
                }
            }
            if (family == Family.COMPACT)
            {
                return CompactSketch.Wrap(srcMem, expectedSeed);
            }
            throw new SketchesArgumentException(
                $"Cannot wrap family: {family} as a Sketch");
        }

        //Sketch interface

        /// <summary>
        /// Converts this sketch to a ordered CompactSketch.
        /// <para>
        /// If this.IsCompact() == true this method returns this,
        /// otherwise, this method is equivalent to Compact(true, null).
        /// </para>
        /// <para>
        /// A CompactSketch is always immutable.
        /// </para>
        /// </summary>
        /// <returns>this sketch as an ordered CompactSketch.</returns>
        public CompactSketch Compact()
        {
            return IsCompact() ? (CompactSketch)this : Compact(true, null);
        }

        /// <summary>
        /// Convert this sketch to a CompactSketch.
        /// <para>
        /// If this sketch is a type of UpdateSketch, the compacting process converts the hash table
        /// of the UpdateSketch to a simple list of the valid hash values.
        /// Any hash values of zero or equal-to or greater than theta will be discarded.
        /// The number of valid values remaining in the CompactSketch depends on a number of factors,
        /// but may be larger or smaller than Nominal Entries (or k).
        /// It will never exceed 2k.
        /// If it is critical to always limit the size to no more than k,
        /// then Rebuild() should be called on the UpdateSketch prior to calling this method.
        /// </para>
        /// <para>
        /// A CompactSketch is always immutable.
        /// </para>
        /// <para>
        /// A new CompactSketch object is created:
        /// </para>
        /// <list type="bullet">
        /// <item>if dstMem != null</item>
        /// <item>if dstMem == null and this.HasMemory() == true</item>
        /// <item>if dstMem == null and this has more than 1 item and this.IsOrdered() == false
        /// and dstOrdered == true.</item>
        /// </list>
        /// <para>
        /// Otherwise, this operation returns this.
        /// </para>
        /// </summary>
        /// <param name="dstOrdered">assumed true if this sketch is empty or has only one value</param>
        /// <param name="dstMem">destination Memory (or null for heap)</param>
        /// <returns>this sketch as a CompactSketch.</returns>
        public abstract CompactSketch Compact(bool dstOrdered, Span<byte> dstMem);

        /// <summary>
        /// Returns the number of storage bytes required for this Sketch if its current state were
        /// compacted. It this sketch is already in the compact form this is equivalent to
        /// calling GetCurrentBytes().
        /// </summary>
        /// <returns>number of compact bytes</returns>
        public abstract int GetCompactBytes();

        /// <summary>
        /// Gets the number of hash values less than the given theta expressed as a long.
        /// </summary>
        /// <param name="thetaLong">the given theta as a long between zero and long.MaxValue.</param>
        /// <returns>the number of hash values less than the given thetaLong.</returns>
        public virtual int GetCountLessThanThetaLong(long thetaLong)
        {
            return Count(GetCache(), thetaLong);
        }

        /// <summary>
        /// Returns the number of storage bytes required for this sketch in its current state.
        /// </summary>
        /// <returns>the number of storage bytes required for this sketch</returns>
        public abstract int GetCurrentBytes();

        /// <summary>
        /// Gets the unique count estimate.
        /// </summary>
        /// <returns>the sketch's best estimate of the cardinality of the input stream.</returns>
        public abstract double GetEstimate();

        /// <summary>
        /// Returns the Family that this sketch belongs to
        /// </summary>
        /// <returns>the Family that this sketch belongs to</returns>
        public abstract Family GetFamily();

        /// <summary>
        /// Gets the approximate lower error bound given the specified number of Standard Deviations.
        /// This will return GetEstimate() if IsEmpty() is true.
        /// </summary>
        /// <param name="numStdDev">Number of Standard Deviations</param>
        /// <returns>the lower bound.</returns>
        public virtual double GetLowerBound(int numStdDev)
        {
            return IsEstimationMode()
                ? LowerBound(GetRetainedEntries(true), GetThetaLong(), numStdDev, IsEmpty())
                : GetRetainedEntries(true);
        }

        /// <summary>
        /// Returns the maximum number of storage bytes required for a CompactSketch with the given
        /// number of actual entries.
        /// </summary>
        /// <param name="numberOfEntries">the actual number of retained entries stored in the sketch.</param>
        /// <returns>the maximum number of storage bytes required for a CompactSketch with the given number
        /// of retained entries.</returns>
        public static int GetMaxCompactSketchBytes(int numberOfEntries)
        {
            if (numberOfEntries == 0) { return 8; }
            if (numberOfEntries == 1) { return 16; }
            return (numberOfEntries << 3) + 24;
        }

        /// <summary>
        /// Returns the maximum number of storage bytes required for a CompactSketch given the configured
        /// log_base2 of the number of nominal entries, which is a power of 2.
        /// </summary>
        /// <param name="lgNomEntries">Nominal Entries (log base 2)</param>
        /// <returns>the maximum number of storage bytes required for a CompactSketch with the given
        /// lgNomEntries.</returns>
        public static int GetCompactSketchMaxBytes(int lgNomEntries)
        {
            return (int)((2 << lgNomEntries) * ThetaUtil.REBUILD_THRESHOLD
                + Family.QUICKSELECT.GetMaxPreLongs()) * sizeof(long);
        }

        /// <summary>
        /// Returns the maximum number of storage bytes required for an UpdateSketch with the given
        /// number of nominal entries (power of 2).
        /// </summary>
        /// <param name="nomEntries">Nominal Entries
        /// This will become the ceiling power of 2 if it is not.</param>
        /// <returns>the maximum number of storage bytes required for a UpdateSketch with the given
        /// nomEntries</returns>
        public static int GetMaxUpdateSketchBytes(int nomEntries)
        {
            int nomEnt = CeilingPowerOf2(nomEntries);
            return (nomEnt << 4) + (Family.QUICKSELECT.GetMaxPreLongs() << 3);
        }

        /// <summary>
        /// Returns the number of valid entries that have been retained by the sketch.
        /// </summary>
        /// <returns>the number of valid retained entries</returns>
        public int GetRetainedEntries()
        {
            return GetRetainedEntries(true);
        }

        /// <summary>
        /// Returns the number of entries that have been retained by the sketch.
        /// </summary>
        /// <param name="valid">if true, returns the number of valid entries, which are less than theta and used
        /// for estimation.
        /// Otherwise, return the number of all entries, valid or not, that are currently in the internal
        /// sketch cache.</param>
        /// <returns>the number of retained entries</returns>
        public abstract int GetRetainedEntries(bool valid);

        /// <summary>
        /// Returns the serialization version from the given Memory
        /// </summary>
        /// <param name="mem">the sketch Memory</param>
        /// <returns>the serialization version from the Memory</returns>
        public static int GetSerializationVersion(ReadOnlySpan<byte> mem)
        {
            return mem[SER_VER_BYTE];
        }

        /// <summary>
        /// Gets the value of theta as a double with a value between zero and one
        /// </summary>
        /// <returns>the value of theta as a double</returns>
        public double GetTheta()
        {
            return GetThetaLong() / LONG_MAX_VALUE_AS_DOUBLE;
        }

        /// <summary>
        /// Gets the value of theta as a long
        /// </summary>
        /// <returns>the value of theta as a long</returns>
        public abstract long GetThetaLong();

        /// <summary>
        /// Gets the approximate upper error bound given the specified number of Standard Deviations.
        /// This will return GetEstimate() if IsEmpty() is true.
        /// </summary>
        /// <param name="numStdDev">Number of Standard Deviations</param>
        /// <returns>the upper bound.</returns>
        public virtual double GetUpperBound(int numStdDev)
        {
            return IsEstimationMode()
                ? UpperBound(GetRetainedEntries(true), GetThetaLong(), numStdDev, IsEmpty())
                : GetRetainedEntries(true);
        }

        /// <summary>
        /// Returns true if this sketch is in compact form.
        /// </summary>
        /// <returns>true if this sketch is in compact form.</returns>
        public abstract bool IsCompact();

        /// <summary>
        /// Returns true if empty.
        /// </summary>
        /// <returns>true if empty.</returns>
        public abstract bool IsEmpty();

        /// <summary>
        /// Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
        /// This is true if theta &lt; 1.0 AND IsEmpty() is false.
        /// </summary>
        /// <returns>true if the sketch is in estimation mode.</returns>
        public bool IsEstimationMode()
        {
            return EstMode(GetThetaLong(), IsEmpty());
        }

        /// <summary>
        /// Returns true if internal cache is ordered
        /// </summary>
        /// <returns>true if internal cache is ordered</returns>
        public abstract bool IsOrdered();

        /// <summary>
        /// Returns true if this sketch is backed by memory
        /// </summary>
        public abstract bool HasMemory();

        /// <summary>
        /// Returns true if this sketch is read-only
        /// </summary>
        public abstract bool IsReadOnly();

        /// <summary>
        /// Returns a HashIterator that can be used to iterate over the retained hash values of the
        /// Theta sketch.
        /// </summary>
        /// <returns>a HashIterator that can be used to iterate over the retained hash values of the
        /// Theta sketch.</returns>
        public abstract HashIterator Iterator();

        /// <summary>
        /// Serialize this sketch to a byte array form.
        /// </summary>
        /// <returns>byte array of this sketch</returns>
        public abstract byte[] ToByteArray();

        /// <summary>
        /// Returns a human readable summary of the sketch.  This method is equivalent to the parameterized
        /// call: Sketch.ToString(sketch, true, false, 8, true);
        /// </summary>
        /// <returns>summary</returns>
        public override string ToString()
        {
            return ToString(true, false, 8, true);
        }

        /// <summary>
        /// Gets a human readable listing of contents and summary of the given sketch.
        /// This can be a very long string.  If this sketch is in a "dirty" state there
        /// may be values in the dataDetail view that are &gt;= theta.
        /// </summary>
        /// <param name="sketchSummary">If true the sketch summary will be output at the end.</param>
        /// <param name="dataDetail">If true, includes all valid hash values in the sketch.</param>
        /// <param name="width">The number of columns of hash values. Default is 8.</param>
        /// <param name="hexMode">If true, hashes will be output in hex.</param>
        /// <returns>The result string, which can be very long.</returns>
        public string ToString(bool sketchSummary, bool dataDetail, int width, bool hexMode)
        {
            var sb = new StringBuilder();

            long[] cache = GetCache();
            int nomLongs = 0;
            int arrLongs = cache.Length;
            float p = 0;
            int rf = 0;
            bool updateSketch = this is UpdateSketch;

            long thetaLong = GetThetaLong();
            int curCount = GetRetainedEntries(true);

            if (updateSketch)
            {
                UpdateSketch uis = (UpdateSketch)this;
                nomLongs = 1 << uis.GetLgNomLongs();
                arrLongs = 1 << uis.GetLgArrLongs();
                p = uis.GetP();
                rf = uis.GetResizeFactor().GetValue();
            }

            if (dataDetail)
            {
                int w = width > 0 ? width : 8; // default is 8 wide
                if (curCount > 0)
                {
                    sb.Append("### SKETCH DATA DETAIL");
                    for (int i = 0, j = 0; i < arrLongs; i++)
                    {
                        long h = cache[i];
                        if (h <= 0 || h >= thetaLong)
                        {
                            continue;
                        }
                        if (j % w == 0)
                        {
                            sb.Append(Environment.NewLine).Append($"   {j + 1,6}");
                        }
                        if (hexMode)
                        {
                            sb.Append($" {h:X16},");
                        }
                        else
                        {
                            sb.Append($" {h,20},");
                        }
                        j++;
                    }
                    sb.Append(Environment.NewLine).Append("### END DATA DETAIL").Append(Environment.NewLine).Append(Environment.NewLine);
                }
            }

            if (sketchSummary)
            {
                double thetaDbl = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
                string thetaHex = thetaLong.ToString("X16");
                string thisSimpleName = GetType().Name;
                int seedHash = (ushort)GetSeedHash();

                sb.AppendLine();
                sb.AppendLine($"### {thisSimpleName} SUMMARY: ");
                if (updateSketch)
                {
                    sb.AppendLine($"   Nominal Entries (k)     : {nomLongs}");
                }
                sb.AppendLine($"   Estimate                : {GetEstimate()}");
                sb.AppendLine($"   Upper Bound, 95% conf   : {GetUpperBound(2)}");
                sb.AppendLine($"   Lower Bound, 95% conf   : {GetLowerBound(2)}");
                if (updateSketch)
                {
                    sb.AppendLine($"   p                       : {p}");
                }
                sb.AppendLine($"   Theta (double)          : {thetaDbl}");
                sb.AppendLine($"   Theta (long)            : {thetaLong}");
                sb.AppendLine($"   Theta (long) hex        : {thetaHex}");
                sb.AppendLine($"   EstMode?                : {IsEstimationMode()}");
                sb.AppendLine($"   Empty?                  : {IsEmpty()}");
                sb.AppendLine($"   Ordered?                : {IsOrdered()}");
                if (updateSketch)
                {
                    sb.AppendLine($"   Resize Factor           : {rf}");
                    sb.AppendLine($"   Array Size Entries      : {arrLongs}");
                }
                sb.AppendLine($"   Retained Entries        : {curCount}");
                sb.AppendLine($"   Seed Hash               : {seedHash:X} | {seedHash}");
                sb.AppendLine("### END SKETCH SUMMARY");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a human readable string of the preamble of a byte array image of a Theta Sketch.
        /// </summary>
        /// <param name="byteArr">the given byte array</param>
        /// <returns>a human readable string of the preamble of a byte array image of a Theta Sketch.</returns>
        public static string ToString(byte[] byteArr)
        {
            return PreambleUtil.PreambleToString(byteArr);
        }

        /// <summary>
        /// Returns a human readable string of the preamble of a Memory image of a Theta Sketch.
        /// </summary>
        /// <param name="mem">the given Memory object</param>
        /// <returns>a human readable string of the preamble of a Memory image of a Theta Sketch.</returns>
        public static string ToString(ReadOnlySpan<byte> mem)
        {
            return PreambleUtil.PreambleToString(mem);
        }

        //Restricted methods

        /// <summary>
        /// Gets the internal cache array. For on-heap sketches this will return a reference to the actual
        /// cache array. For Memory-based sketches this returns a copy.
        /// </summary>
        /// <returns>the internal cache array.</returns>
        internal abstract long[] GetCache();

        /// <summary>
        /// Gets preamble longs if stored in compact form. If this sketch is already in compact form,
        /// this is identical to the call GetCurrentPreambleLongs().
        /// </summary>
        /// <returns>preamble longs if stored in compact form.</returns>
        internal abstract int GetCompactPreambleLongs();

        /// <summary>
        /// Gets the number of data longs if stored in current state.
        /// </summary>
        /// <returns>the number of data longs if stored in current state.</returns>
        internal abstract int GetCurrentDataLongs();

        /// <summary>
        /// Returns preamble longs if stored in current state.
        /// </summary>
        /// <returns>number of preamble longs if stored.</returns>
        internal abstract int GetCurrentPreambleLongs();

        /// <summary>
        /// Returns the backing Memory object if it exists, otherwise null.
        /// </summary>
        /// <returns>the backing Memory object if it exists, otherwise null.</returns>
        internal abstract ReadOnlyMemory<byte>? GetMemory();

        /// <summary>
        /// Gets the 16-bit seed hash
        /// </summary>
        /// <returns>the seed hash</returns>
        internal abstract short GetSeedHash();

        /// <summary>
        /// Returns true if given Family id is one of the theta sketches
        /// </summary>
        /// <param name="id">the given Family id</param>
        /// <returns>true if given Family id is one of the theta sketches</returns>
        internal static bool IsValidSketchID(int id)
        {
            return id == Family.ALPHA.GetID()
                || id == Family.QUICKSELECT.GetID()
                || id == Family.COMPACT.GetID();
        }

        /// <summary>
        /// Checks Ordered and Compact flags for integrity between sketch and Memory
        /// </summary>
        /// <param name="sketch">the given sketch</param>
        internal static void CheckSketchAndMemoryFlags(Sketch sketch)
        {
            ReadOnlyMemory<byte>? mem = sketch.GetMemory();
            if (mem == null) { return; }
            int flags = PreambleUtil.ExtractFlags(mem.Value.Span);
            if (((flags & COMPACT_FLAG_MASK) > 0) ^ sketch.IsCompact())
            {
                throw new SketchesArgumentException("Possible corruption: " +
                    "Memory Compact Flag inconsistent with Sketch");
            }
            if (((flags & ORDERED_FLAG_MASK) > 0) ^ sketch.IsOrdered())
            {
                throw new SketchesArgumentException("Possible corruption: " +
                    "Memory Ordered Flag inconsistent with Sketch");
            }
        }

        internal static double Estimate(long thetaLong, int curCount)
        {
            return curCount * (LONG_MAX_VALUE_AS_DOUBLE / thetaLong);
        }

        internal static double LowerBound(int curCount, long thetaLong, int numStdDev, bool empty)
        {
            double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
            return BinomialBoundsN.GetLowerBound(curCount, theta, numStdDev, empty);
        }

        internal static double UpperBound(int curCount, long thetaLong, int numStdDev, bool empty)
        {
            double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
            return BinomialBoundsN.GetUpperBound(curCount, theta, numStdDev, empty);
        }

        private static bool EstMode(long thetaLong, bool empty)
        {
            return thetaLong < long.MaxValue && !empty;
        }

        /// <summary>
        /// Instantiates a Heap Update Sketch from Memory. Only SerVer3. SerVer 1 & 2 already handled.
        /// </summary>
        /// <param name="srcMem">Source Memory</param>
        /// <param name="expectedSeed">the seed used to validate the given Memory image.</param>
        /// <returns>a Sketch</returns>
        private static Sketch HeapifyUpdateFromMemory(ReadOnlySpan<byte> srcMem, long expectedSeed)
        {
            long cap = srcMem.Length;
            if (cap < 8)
            {
                throw new SketchesArgumentException(
                    "Corrupted: valid sketch must be at least 8 bytes.");
            }
            byte familyID = srcMem[FAMILY_BYTE];
            Family family = FamilyExtensions.IdToFamily(familyID);

            if (family == Family.ALPHA)
            {
                int flags = PreambleUtil.ExtractFlags(srcMem);
                bool compactFlag = (flags & COMPACT_FLAG_MASK) != 0;
                if (compactFlag)
                {
                    throw new SketchesArgumentException(
                        "Corrupted: ALPHA family image: cannot be compact");
                }
                // Note: HeapAlphaSketch not being translated
                throw new SketchesArgumentException(
                    "ALPHA family sketches not supported in this C# implementation");
                //return HeapAlphaSketch.HeapifyInstance(srcMem, expectedSeed);
            }
            if (family == Family.QUICKSELECT)
            {
                return HeapQuickSelectSketch.HeapifyInstance(srcMem, expectedSeed);
            }
            throw new SketchesArgumentException(
                $"Sketch cannot heapify family: {family} as a Sketch");
        }
    }
}

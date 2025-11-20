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
using Apache.DataSketches.Hash;
using Apache.DataSketches.ThetaCommon;
using static Apache.DataSketches.Common.Util;
using static Apache.DataSketches.Theta.CompactOperations;
using static Apache.DataSketches.Theta.PreambleUtil;
using static Apache.DataSketches.Theta.UpdateReturnState;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// The parent class for the Update Sketch families, such as QuickSelect and Alpha.
    /// The primary task of an Update Sketch is to consider datums presented via the update() methods
    /// for inclusion in its internal cache. This is the sketch building process.
    /// </summary>
    public abstract class UpdateSketch : Sketch
    {
        protected UpdateSketch() { }

        /// <summary>
        /// Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
        /// the heap. Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
        /// been explicitly stored as direct objects can be wrapped. This method assumes the
        /// Default Update Seed.
        /// </summary>
        /// <param name="srcMem">an image of a Sketch where the image seed hash matches the default seed hash.
        /// It must have a size of at least 24 bytes.</param>
        /// <returns>a Sketch backed by the given Memory</returns>
        public static UpdateSketch Wrap(Span<byte> srcMem)
        {
            return Wrap(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
        }

        /// <summary>
        /// Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
        /// the heap. Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
        /// been explicitly stored as direct objects can be wrapped.
        /// An attempt to "wrap" earlier version sketches will result in a "heapified", normal
        /// heap version of the sketch where all data will be copied to the heap.
        /// </summary>
        /// <param name="srcMem">an image of a Sketch where the image seed hash matches the given seed hash.
        /// It must have a size of at least 24 bytes.</param>
        /// <param name="expectedSeed">the seed used to validate the given Memory image.
        /// Compact sketches store a 16-bit hash of the seed, but not the seed itself.</param>
        /// <returns>a UpdateSketch backed by the given Memory</returns>
        public static UpdateSketch Wrap(Span<byte> srcMem, long expectedSeed)
        {
            if (srcMem == null) throw new ArgumentNullException(nameof(srcMem), "Source Memory must not be null");
            CheckBounds(0, 24, srcMem.Length); //need min 24 bytes
            int preLongs = srcMem[PREAMBLE_LONGS_BYTE] & 0x3F;
            int serVer = srcMem[SER_VER_BYTE] & 0xFF;
            int familyID = srcMem[FAMILY_BYTE] & 0xFF;
            Family family = FamilyExtensions.IdToFamily(familyID);
            if (family != Family.QUICKSELECT)
            {
                throw new SketchesArgumentException(
                    $"A {family} sketch cannot be wrapped as an UpdateSketch.");
            }
            if (serVer == 3 && preLongs == 3)
            {
                // Note: DirectQuickSelectSketch not yet translated
                throw new SketchesArgumentException(
                    "Direct sketches not yet supported in this C# implementation");
                //return DirectQuickSelectSketch.WritableWrap(srcMem, expectedSeed);
            }
            else
            {
                throw new SketchesArgumentException(
                    "Corrupted: An UpdateSketch image must have SerVer = 3 and preLongs = 3");
            }
        }

        /// <summary>
        /// Instantiates an on-heap UpdateSketch from Memory. This method assumes the
        /// Default Update Seed.
        /// </summary>
        /// <param name="srcMem">Source Memory. It must have a size of at least 24 bytes.</param>
        /// <returns>an UpdateSketch</returns>
        public new static UpdateSketch Heapify(ReadOnlySpan<byte> srcMem)
        {
            return Heapify(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
        }

        /// <summary>
        /// Instantiates an on-heap UpdateSketch from Memory.
        /// </summary>
        /// <param name="srcMem">Source Memory. It must have a size of at least 24 bytes.</param>
        /// <param name="expectedSeed">the seed used to validate the given Memory image.</param>
        /// <returns>an UpdateSketch</returns>
        public new static UpdateSketch Heapify(ReadOnlySpan<byte> srcMem, long expectedSeed)
        {
            if (srcMem == null) throw new ArgumentNullException(nameof(srcMem), "Source Memory must not be null");
            CheckBounds(0, 24, srcMem.Length); //need min 24 bytes
            Family family = FamilyExtensions.IdToFamily(srcMem[FAMILY_BYTE]);
            if (family == Family.ALPHA)
            {
                // Note: HeapAlphaSketch not being translated
                throw new SketchesArgumentException(
                    "ALPHA family sketches not supported in this C# implementation");
                //return HeapAlphaSketch.HeapifyInstance(srcMem, expectedSeed);
            }
            return HeapQuickSelectSketch.HeapifyInstance(srcMem, expectedSeed);
        }

        //Sketch interface

        public override CompactSketch Compact(bool dstOrdered, Span<byte> dstMem)
        {
            return ComponentsToCompact(GetThetaLong(), GetRetainedEntries(true), GetSeedHash(), IsEmpty(),
                false, false, dstOrdered, dstMem, GetCache());
        }

        public override int GetCompactBytes()
        {
            int preLongs = GetCompactPreambleLongs();
            int dataLongs = GetRetainedEntries(true);
            return (preLongs + dataLongs) << 3;
        }

        internal override int GetCurrentDataLongs()
        {
            return 1 << GetLgArrLongs();
        }

        public override bool IsCompact()
        {
            return false;
        }

        public override bool IsOrdered()
        {
            return false;
        }

        //UpdateSketch interface

        /// <summary>
        /// Returns a new builder
        /// </summary>
        /// <returns>a new builder</returns>
        public static UpdateSketchBuilder Builder()
        {
            return new UpdateSketchBuilder();
        }

        /// <summary>
        /// Returns the configured ResizeFactor
        /// </summary>
        /// <returns>the configured ResizeFactor</returns>
        public abstract ResizeFactor GetResizeFactor();

        /// <summary>
        /// Gets the configured sampling probability, p.
        /// </summary>
        /// <returns>the sampling probability, p</returns>
        internal abstract float GetP();

        /// <summary>
        /// Gets the configured seed
        /// </summary>
        /// <returns>the configured seed</returns>
        internal abstract long GetSeed();

        /// <summary>
        /// Resets this sketch back to a virgin empty state.
        /// </summary>
        public abstract void Reset();

        /// <summary>
        /// Rebuilds the hash table to remove dirty values or to reduce the size
        /// to nominal entries.
        /// </summary>
        /// <returns>this sketch</returns>
        public abstract UpdateSketch Rebuild();

        /// <summary>
        /// Present this sketch with a long.
        /// </summary>
        /// <param name="datum">The given long datum.</param>
        /// <returns>Update Return State</returns>
        public UpdateReturnState Update(long datum)
        {
            long[] data = { datum };
            return HashUpdate(MurmurHash3.Hash(data, GetSeed())[0] >> 1);
        }

        /// <summary>
        /// Present this sketch with the given double (or float) datum.
        /// The double will be converted to a long using BitConverter.DoubleToInt64Bits(datum),
        /// which normalizes all NaN values to a single NaN representation.
        /// Plus and minus zero will be normalized to plus zero.
        /// The special floating-point values NaN and +/- Infinity are treated as distinct.
        /// </summary>
        /// <param name="datum">The given double datum.</param>
        /// <returns>Update Return State</returns>
        public UpdateReturnState Update(double datum)
        {
            double d = datum == 0.0 ? 0.0 : datum; // canonicalize -0.0, 0.0
            long[] data = { BitConverter.DoubleToInt64Bits(d) }; // canonicalize all NaN & +/- infinity forms
            return HashUpdate(MurmurHash3.Hash(data, GetSeed())[0] >> 1);
        }

        /// <summary>
        /// Present this sketch with the given String.
        /// The string is converted to a byte array using UTF8 encoding.
        /// If the string is null or empty no update attempt is made and the method returns.
        /// <para>
        /// Note: this will not produce the same output hash values as the Update(char[])
        /// method and will generally be a little slower depending on the complexity of the UTF8 encoding.
        /// </para>
        /// </summary>
        /// <param name="datum">The given String.</param>
        /// <returns>Update Return State</returns>
        public UpdateReturnState Update(string datum)
        {
            if (string.IsNullOrEmpty(datum))
            {
                return RejectedNullOrEmpty;
            }
            byte[] data = Encoding.UTF8.GetBytes(datum);
            return HashUpdate(MurmurHash3.Hash(data, GetSeed())[0] >> 1);
        }

        /// <summary>
        /// Present this sketch with the given byte array.
        /// If the byte array is null or empty no update attempt is made and the method returns.
        /// </summary>
        /// <param name="data">The given byte array.</param>
        /// <returns>Update Return State</returns>
        public UpdateReturnState Update(byte[] data)
        {
            if (data == null || data.Length == 0)
            {
                return RejectedNullOrEmpty;
            }
            return HashUpdate(MurmurHash3.Hash(data, GetSeed())[0] >> 1);
        }

        /// <summary>
        /// Present this sketch with the given char array.
        /// If the char array is null or empty no update attempt is made and the method returns.
        /// <para>
        /// Note: this will not produce the same output hash values as the Update(String)
        /// method but will be a little faster as it avoids the complexity of the UTF8 encoding.
        /// </para>
        /// </summary>
        /// <param name="data">The given char array.</param>
        /// <returns>Update Return State</returns>
        public UpdateReturnState Update(char[] data)
        {
            if (data == null || data.Length == 0)
            {
                return RejectedNullOrEmpty;
            }
            return HashUpdate(MurmurHash3.Hash(data, GetSeed())[0] >> 1);
        }

        /// <summary>
        /// Present this sketch with the given integer array.
        /// If the integer array is null or empty no update attempt is made and the method returns.
        /// </summary>
        /// <param name="data">The given int array.</param>
        /// <returns>Update Return State</returns>
        public UpdateReturnState Update(int[] data)
        {
            if (data == null || data.Length == 0)
            {
                return RejectedNullOrEmpty;
            }
            return HashUpdate(MurmurHash3.Hash(data, GetSeed())[0] >> 1);
        }

        /// <summary>
        /// Present this sketch with the given long array.
        /// If the long array is null or empty no update attempt is made and the method returns.
        /// </summary>
        /// <param name="data">The given long array.</param>
        /// <returns>Update Return State</returns>
        public UpdateReturnState Update(long[] data)
        {
            if (data == null || data.Length == 0)
            {
                return RejectedNullOrEmpty;
            }
            return HashUpdate(MurmurHash3.Hash(data, GetSeed())[0] >> 1);
        }

        //restricted methods

        /// <summary>
        /// All potential updates converge here.
        /// <para>
        /// Don't ever call this unless you really know what you are doing!
        /// </para>
        /// </summary>
        /// <param name="hash">the given input hash value.  A hash of zero or Long.MaxValue is ignored.
        /// A negative hash value will throw an exception.</param>
        /// <returns>Update Return State</returns>
        internal abstract UpdateReturnState HashUpdate(long hash);

        /// <summary>
        /// Gets the Log base 2 of the current size of the internal cache
        /// </summary>
        /// <returns>the Log base 2 of the current size of the internal cache</returns>
        internal abstract int GetLgArrLongs();

        /// <summary>
        /// Gets the Log base 2 of the configured nominal entries
        /// </summary>
        /// <returns>the Log base 2 of the configured nominal entries</returns>
        public abstract int GetLgNomLongs();

        /// <summary>
        /// Returns true if the internal cache contains "dirty" values that are greater than or equal
        /// to thetaLong.
        /// </summary>
        /// <returns>true if the internal cache is dirty.</returns>
        internal abstract bool IsDirty();

        /// <summary>
        /// Returns true if numEntries (curCount) is greater than the hashTableThreshold.
        /// </summary>
        /// <param name="numEntries">the given number of entries (or current count).</param>
        /// <returns>true if numEntries (curCount) is greater than the hashTableThreshold.</returns>
        internal abstract bool IsOutOfSpace(int numEntries);

        internal static void CheckUnionQuickSelectFamily(ReadOnlySpan<byte> mem, int preambleLongs,
            int lgNomLongs)
        {
            //Check Family
            int familyID = ExtractFamilyID(mem);                       //byte 2
            Family family = FamilyExtensions.IdToFamily(familyID);
            if (family == Family.UNION)
            {
                if (preambleLongs != Family.UNION.GetMinPreLongs())
                {
                    throw new SketchesArgumentException(
                        $"Possible corruption: Invalid PreambleLongs value for UNION: {preambleLongs}");
                }
            }
            else if (family == Family.QUICKSELECT)
            {
                if (preambleLongs != Family.QUICKSELECT.GetMinPreLongs())
                {
                    throw new SketchesArgumentException(
                        $"Possible corruption: Invalid PreambleLongs value for QUICKSELECT: {preambleLongs}");
                }
            }
            else
            {
                throw new SketchesArgumentException(
                    $"Possible corruption: Invalid Family: {family}");
            }

            //Check lgNomLongs
            if (lgNomLongs < ThetaUtil.MIN_LG_NOM_LONGS)
            {
                throw new SketchesArgumentException(
                    $"Possible corruption: Current Memory lgNomLongs < min required size: {lgNomLongs} < {ThetaUtil.MIN_LG_NOM_LONGS}");
            }
        }

        internal static void CheckMemIntegrity(ReadOnlySpan<byte> srcMem, long expectedSeed, int preambleLongs,
            int lgNomLongs, int lgArrLongs)
        {
            //Check SerVer
            int serVer = ExtractSerVer(srcMem);                           //byte 1
            if (serVer != SER_VER)
            {
                throw new SketchesArgumentException(
                    $"Possible corruption: Invalid Serialization Version: {serVer}");
            }

            //Check flags
            int flags = ExtractFlags(srcMem);                             //byte 5
            int flagsMask =
                ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
            if ((flags & flagsMask) > 0)
            {
                throw new SketchesArgumentException(
                    "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
            }

            //Check seed hashes
            short seedHash = CheckMemorySeedHash(srcMem, expectedSeed);              //byte 6,7
            ThetaUtil.CheckSeedHashes(seedHash, ThetaUtil.ComputeSeedHash(expectedSeed));

            //Check mem capacity, lgArrLongs
            long curCapBytes = srcMem.Length;
            int minReqBytes = GetMemBytes(lgArrLongs, preambleLongs);
            if (curCapBytes < minReqBytes)
            {
                throw new SketchesArgumentException(
                    $"Possible corruption: Current Memory size < min required size: {curCapBytes} < {minReqBytes}");
            }
            //check Theta, p
            float p = ExtractP(srcMem);                                   //bytes 12-15
            long thetaLong = ExtractThetaLong(srcMem);                    //bytes 16-23
            double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
            //if (lgArrLongs <= lgNomLongs) the sketch is still resizing, thus theta cannot be < p.
            if ((lgArrLongs <= lgNomLongs) && (theta < p))
            {
                throw new SketchesArgumentException(
                    $"Possible corruption: Theta cannot be < p and lgArrLongs <= lgNomLongs. " +
                    $"{lgArrLongs} <= {lgNomLongs}, Theta: {theta}, p: {p}");
            }
        }

        /// <summary>
        /// This checks to see if the memory RF factor was set correctly as early versions may not
        /// have set it.
        /// </summary>
        /// <param name="srcMem">the source memory</param>
        /// <param name="lgNomLongs">the current lgNomLongs</param>
        /// <param name="lgArrLongs">the current lgArrLongs</param>
        /// <returns>true if the the memory RF factor is incorrect and the caller can either
        /// correct it or throw an error.</returns>
        internal static bool IsResizeFactorIncorrect(ReadOnlySpan<byte> srcMem, int lgNomLongs,
            int lgArrLongs)
        {
            int lgT = lgNomLongs + 1;
            int lgA = lgArrLongs;
            int lgR = ExtractLgResizeFactor(srcMem);
            if (lgR == 0) { return lgA != lgT; }
            return !(((lgT - lgA) % lgR) == 0);
        }
    }
}

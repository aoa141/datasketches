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
using static Apache.DataSketches.Common.Family;
using static Apache.DataSketches.Theta.PreambleUtil;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// The parent class of all the CompactSketches. CompactSketches are never created directly.
    /// They are created as a result of the compact() method of an UpdateSketch, a result of a
    /// getResult() of a SetOperation, or from a heapify method.
    /// <para>
    /// A CompactSketch is the simplest form of a Theta Sketch. It consists of a compact list
    /// (i.e., no intervening spaces) of hash values, which may be ordered or not, a value for theta
    /// and a seed hash. A CompactSketch is immutable (read-only),
    /// and the space required when stored is only the space required for the hash values and 8 to 24
    /// bytes of preamble. An empty CompactSketch consumes only 8 bytes.
    /// </para>
    /// </summary>
    public abstract class CompactSketch : Sketch
    {
        /// <summary>
        /// Heapify takes a CompactSketch image in Memory and instantiates an on-heap CompactSketch.
        /// <para>
        /// The resulting sketch will not retain any link to the source Memory and all of its data will be
        /// copied to the heap CompactSketch.
        /// </para>
        /// <para>
        /// This method assumes that the sketch image was created with the correct hash seed, so it is not checked.
        /// The resulting on-heap CompactSketch will be given the seedHash derived from the given sketch image.
        /// However, Serial Version 1 sketch images do not have a seedHash field,
        /// so the resulting heapified CompactSketch will be given the hash of the DEFAULT_UPDATE_SEED.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a CompactSketch.</param>
        /// <returns>a CompactSketch on the heap.</returns>
        public new static CompactSketch Heapify(ReadOnlySpan<byte> srcMem)
        {
            return Heapify(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED, false);
        }

        /// <summary>
        /// Heapify takes a CompactSketch image in Memory and instantiates an on-heap CompactSketch.
        /// <para>
        /// The resulting sketch will not retain any link to the source Memory and all of its data will be
        /// copied to the heap CompactSketch.
        /// </para>
        /// <para>
        /// This method checks if the given expectedSeed was used to create the source Memory image.
        /// However, SerialVersion 1 sketch images cannot be checked as they don't have a seedHash field,
        /// so the resulting heapified CompactSketch will be given the hash of the expectedSeed.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a CompactSketch that was created using the given expectedSeed.</param>
        /// <param name="expectedSeed">the seed used to validate the given Memory image.</param>
        /// <returns>a CompactSketch on the heap.</returns>
        public new static CompactSketch Heapify(ReadOnlySpan<byte> srcMem, long expectedSeed)
        {
            return Heapify(srcMem, expectedSeed, true);
        }

        private static CompactSketch Heapify(ReadOnlySpan<byte> srcMem, long seed, bool enforceSeed)
        {
            int serVer = ExtractSerVer(srcMem);
            int familyID = ExtractFamilyID(srcMem);
            Family family = FamilyExtensions.IdToFamily(familyID);

            if (family != Family.COMPACT)
            {
                throw new ArgumentException($"Corrupted: {family} is not Compact!");
            }

            if (serVer == 4)
            {
                // Note: V4 compression not yet implemented
                throw new NotImplementedException("SerVer 4 compressed format not yet supported");
                //return HeapifyV4(srcMem, seed, enforceSeed);
            }

            if (serVer == 3)
            {
                int flags = ExtractFlags(srcMem);
                bool srcOrdered = (flags & ORDERED_FLAG_MASK) != 0;
                bool empty = (flags & EMPTY_FLAG_MASK) != 0;
                if (enforceSeed && !empty) { CheckMemorySeedHash(srcMem, seed); }
                return CompactOperations.MemoryToCompact(srcMem, srcOrdered, Span<byte>.Empty);
            }

            // Not SerVer 3, assume compact stored form
            short seedHash = ThetaUtil.ComputeSeedHash(seed);
            if (serVer == 1)
            {
                // Note: ForwardCompatibility not yet implemented
                throw new NotImplementedException("SerVer 1 not yet supported - ForwardCompatibility needed");
                //return ForwardCompatibility.Heapify1to3(srcMem, seedHash);
            }

            if (serVer == 2)
            {
                // Note: ForwardCompatibility not yet implemented
                throw new NotImplementedException("SerVer 2 not yet supported - ForwardCompatibility needed");
                //return ForwardCompatibility.Heapify2to3(srcMem,
                //    enforceSeed ? seedHash : (short)ExtractSeedHash(srcMem));
            }

            throw new SketchesArgumentException($"Unknown Serialization Version: {serVer}");
        }

        /// <summary>
        /// Wrap takes the CompactSketch image in given Memory and refers to it directly.
        /// There is no data copying onto the java heap.
        /// The wrap operation enables fast read-only merging and access to all the public read-only API.
        /// <para>
        /// Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
        /// been explicitly stored as direct sketches can be wrapped.
        /// Wrapping earlier serial version sketches will result in a heapify operation.
        /// These early versions were never designed to "wrap".
        /// </para>
        /// <para>
        /// Wrapping any subclass of this class that is empty or contains only a single item will
        /// result in heapified forms of empty and single item sketch respectively.
        /// This is actually faster and consumes less overall memory.
        /// </para>
        /// <para>
        /// This method assumes that the sketch image was created with the correct hash seed, so it is not checked.
        /// However, Serial Version 1 sketch images do not have a seedHash field,
        /// so the resulting on-heap CompactSketch will be given the hash of the DEFAULT_UPDATE_SEED.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a Sketch.</param>
        /// <returns>a CompactSketch backed by the given Memory except as above.</returns>
        public new static CompactSketch Wrap(ReadOnlySpan<byte> srcMem)
        {
            return Wrap(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED, false);
        }

        /// <summary>
        /// Wrap takes the sketch image in the given Memory and refers to it directly.
        /// There is no data copying onto the java heap.
        /// The wrap operation enables fast read-only merging and access to all the public read-only API.
        /// <para>
        /// Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
        /// been explicitly stored as direct sketches can be wrapped.
        /// Wrapping earlier serial version sketches will result in a heapify operation.
        /// These early versions were never designed to "wrap".
        /// </para>
        /// <para>
        /// Wrapping any subclass of this class that is empty or contains only a single item will
        /// result in heapified forms of empty and single item sketch respectively.
        /// This is actually faster and consumes less overall memory.
        /// </para>
        /// <para>
        /// This method checks if the given expectedSeed was used to create the source Memory image.
        /// However, SerialVersion 1 sketches cannot be checked as they don't have a seedHash field,
        /// so the resulting heapified CompactSketch will be given the hash of the expectedSeed.
        /// </para>
        /// </summary>
        /// <param name="srcMem">an image of a Sketch that was created using the given expectedSeed.</param>
        /// <param name="expectedSeed">the seed used to validate the given Memory image.</param>
        /// <returns>a CompactSketch backed by the given Memory except as above.</returns>
        public new static CompactSketch Wrap(ReadOnlySpan<byte> srcMem, long expectedSeed)
        {
            return Wrap(srcMem, expectedSeed, true);
        }

        private static CompactSketch Wrap(ReadOnlySpan<byte> srcMem, long seed, bool enforceSeed)
        {
            int serVer = ExtractSerVer(srcMem);
            int familyID = ExtractFamilyID(srcMem);
            Family family = FamilyExtensions.IdToFamily(familyID);

            if (family != Family.COMPACT)
            {
                throw new ArgumentException($"Corrupted: {family} is not Compact!");
            }

            short seedHash = ThetaUtil.ComputeSeedHash(seed);

            if (serVer == 4)
            {
                // Not wrapping the compressed format since currently we cannot take advantage of
                // decompression during iteration because set operations reach into memory directly
                // Note: HeapifyV4 not yet implemented
                throw new NotImplementedException("SerVer 4 compressed format not yet supported");
                //return HeapifyV4(srcMem, seed, enforceSeed);
            }
            else if (serVer == 3)
            {
                if (IsEmptyFlag(srcMem))
                {
                    return EmptyCompactSketch.GetHeapInstance(srcMem);
                }
                if (SingleItemSketch.OtherCheckForSingleItem(srcMem))
                {
                    return SingleItemSketch.Heapify(srcMem,
                        enforceSeed ? seedHash : (short)ExtractSeedHash(srcMem));
                }
                // Not empty & not singleItem
                int flags = ExtractFlags(srcMem);
                bool compactFlag = (flags & COMPACT_FLAG_MASK) > 0;
                if (!compactFlag)
                {
                    throw new SketchesArgumentException(
                        "Corrupted: COMPACT family sketch image must have compact flag set");
                }
                bool readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
                if (!readOnly)
                {
                    throw new SketchesArgumentException(
                        "Corrupted: COMPACT family sketch image must have Read-Only flag set");
                }
                // Note: DirectCompactSketch not yet fully implemented
                // For now, just heapify it
                return Heapify(srcMem, seed, enforceSeed);
                //return DirectCompactSketch.WrapInstance(srcMem,
                //    enforceSeed ? seedHash : (short)ExtractSeedHash(srcMem));
            }
            else if (serVer == 1)
            {
                // Note: ForwardCompatibility not yet implemented
                throw new NotImplementedException("SerVer 1 not yet supported - ForwardCompatibility needed");
                //return ForwardCompatibility.Heapify1to3(srcMem, seedHash);
            }
            else if (serVer == 2)
            {
                // Note: ForwardCompatibility not yet implemented
                throw new NotImplementedException("SerVer 2 not yet supported - ForwardCompatibility needed");
                //return ForwardCompatibility.Heapify2to3(srcMem,
                //    enforceSeed ? seedHash : (short)ExtractSeedHash(srcMem));
            }

            throw new SketchesArgumentException(
                $"Corrupted: Serialization Version {serVer} not recognized.");
        }

        // Sketch Overrides

        public override abstract CompactSketch Compact(bool dstOrdered, Span<byte> dstMem);

        public override int GetCompactBytes()
        {
            return GetCurrentBytes();
        }

        internal override int GetCurrentDataLongs()
        {
            return GetRetainedEntries(true);
        }

        public override Family GetFamily()
        {
            return Family.COMPACT;
        }

        public override bool IsCompact()
        {
            return true;
        }

        public override bool HasMemory()
        {
            return GetMemory() != null;
        }

        public override bool IsReadOnly()
        {
            return true;
        }

        /// <summary>
        /// Gets the sketch as a compressed byte array (V4 format)
        /// </summary>
        /// <returns>the sketch as a compressed byte array</returns>
        public byte[] ToByteArrayCompressed()
        {
            if (!IsOrdered() || GetRetainedEntries() == 0 || (GetRetainedEntries() == 1 && !IsEstimationMode()))
            {
                return ToByteArray();
            }
            // Note: V4 compression not yet implemented
            throw new NotImplementedException("V4 compression not yet supported");
            //return ToByteArrayV4();
        }
    }
}

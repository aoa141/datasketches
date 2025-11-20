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

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// The parent API for all Set Operations
    /// </summary>
    public abstract class SetOperation : IMemoryStatus
    {
        internal const int CONST_PREAMBLE_LONGS = 3;

        internal SetOperation() { }

        /// <summary>
        /// Makes a new builder
        /// </summary>
        /// <returns>A new builder</returns>
        public static SetOperationBuilder Builder()
        {
            return new SetOperationBuilder();
        }

        /// <summary>
        /// Heapify takes the SetOperations image in Memory and instantiates an on-heap
        /// SetOperation using the Default Update Seed.
        /// The resulting SetOperation will not retain any link to the source Memory.
        /// Note: Only certain set operators during stateful operations can be serialized and thus heapified.
        /// </summary>
        /// <param name="srcMem">An image of a SetOperation where the image seed hash matches the default seed hash.</param>
        /// <returns>A Heap-based SetOperation from the given Memory</returns>
        public static SetOperation Heapify(Memory<byte> srcMem)
        {
            return Heapify(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
        }

        /// <summary>
        /// Heapify takes the SetOperation image in Memory and instantiates an on-heap
        /// SetOperation using the given expectedSeed.
        /// The resulting SetOperation will not retain any link to the source Memory.
        /// Note: Only certain set operators during stateful operations can be serialized and thus heapified.
        /// </summary>
        /// <param name="srcMem">An image of a SetOperation where the hash of the given expectedSeed matches the image seed hash.</param>
        /// <param name="expectedSeed">The seed used to validate the given Memory image.</param>
        /// <returns>A Heap-based SetOperation from the given Memory</returns>
        public static SetOperation Heapify(Memory<byte> srcMem, long expectedSeed)
        {
            byte famID = srcMem.Span[PreambleUtil.FAMILY_BYTE];
            Family family = FamilyExtensions.IdToFamily(famID);
            switch (family)
            {
                case Family.UNION:
                    return UnionImpl.HeapifyInstance(srcMem, expectedSeed);
                case Family.INTERSECTION:
                    return IntersectionImpl.HeapifyInstance(srcMem, expectedSeed);
                default:
                    throw new SketchesArgumentException($"SetOperation cannot heapify family: {family}");
            }
        }

        /// <summary>
        /// Wrap takes the SetOperation image in Memory and refers to it directly.
        /// There is no data copying onto the java heap.
        /// This method assumes the Default Update Seed.
        /// Note: Only certain set operators during stateful operations can be serialized and thus wrapped.
        /// </summary>
        /// <param name="srcMem">An image of a SetOperation where the image seed hash matches the default seed hash.</param>
        /// <returns>A SetOperation backed by the given Memory</returns>
        public static SetOperation Wrap(Memory<byte> srcMem)
        {
            return Wrap(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
        }

        /// <summary>
        /// Wrap takes the SetOperation image in Memory and refers to it directly.
        /// There is no data copying onto the java heap.
        /// Note: Only certain set operators during stateful operations can be serialized and thus wrapped.
        /// </summary>
        /// <param name="srcMem">An image of a SetOperation where the hash of the given expectedSeed matches the image seed hash.</param>
        /// <param name="expectedSeed">The seed used to validate the given Memory image.</param>
        /// <returns>A SetOperation backed by the given Memory</returns>
        public static SetOperation Wrap(Memory<byte> srcMem, long expectedSeed)
        {
            byte famID = srcMem.Span[PreambleUtil.FAMILY_BYTE];
            Family family = FamilyExtensions.IdToFamily(famID);
            int serVer = srcMem.Span[PreambleUtil.SER_VER_BYTE];
            if (serVer != 3)
            {
                throw new SketchesArgumentException($"SerVer must be 3: {serVer}");
            }
            switch (family)
            {
                case Family.UNION:
                    return UnionImpl.WrapInstance(srcMem, expectedSeed, readOnly: true);
                case Family.INTERSECTION:
                    return IntersectionImpl.WrapInstance(srcMem, expectedSeed, readOnly: true);
                default:
                    throw new SketchesArgumentException($"SetOperation cannot wrap family: {family}");
            }
        }

        /// <summary>
        /// Returns the maximum required storage bytes given a nomEntries parameter for Union operations
        /// </summary>
        /// <param name="nomEntries">Nominal Entries. This will become the ceiling power of 2 if it is not.</param>
        /// <returns>The maximum required storage bytes given a nomEntries parameter</returns>
        public static int GetMaxUnionBytes(int nomEntries)
        {
            int nomEnt = Util.CeilingPowerOf2(nomEntries);
            return (nomEnt << 4) + (Family.UNION.GetMaxPreLongs() << 3);
        }

        /// <summary>
        /// Returns the maximum required storage bytes given a nomEntries parameter for Intersection operations
        /// </summary>
        /// <param name="nomEntries">Nominal Entries. This will become the ceiling power of 2 if it is not.</param>
        /// <returns>The maximum required storage bytes given a nomEntries parameter</returns>
        public static int GetMaxIntersectionBytes(int nomEntries)
        {
            int nomEnt = Util.CeilingPowerOf2(nomEntries);
            int bytes = (nomEnt << 4) + (Family.INTERSECTION.GetMaxPreLongs() << 3);
            return bytes;
        }

        /// <summary>
        /// Returns the maximum number of bytes for the returned CompactSketch, given the
        /// value of nomEntries of the first sketch A of AnotB.
        /// </summary>
        /// <param name="nomEntries">This value must be a power of 2.</param>
        /// <returns>The maximum number of bytes.</returns>
        public static int GetMaxAnotBResultBytes(int nomEntries)
        {
            int ceil = Util.CeilingPowerOf2(nomEntries);
            return 24 + (15 * ceil);
        }

        /// <summary>
        /// Gets the Family of this SetOperation
        /// </summary>
        /// <returns>The Family of this SetOperation</returns>
        public abstract Family GetFamily();

        // Restricted methods

        /// <summary>
        /// Gets the hash array in compact form.
        /// This is only useful during stateful operations.
        /// This should never be made public.
        /// </summary>
        /// <returns>The hash array</returns>
        internal abstract long[] GetCache();

        /// <summary>
        /// Gets the current count of retained entries.
        /// This is only useful during stateful operations.
        /// Intentionally not made public because behavior will be confusing to end user.
        /// </summary>
        /// <returns>The current count of retained entries.</returns>
        internal abstract int GetRetainedEntries();

        /// <summary>
        /// Returns the seedHash established during class construction.
        /// </summary>
        /// <returns>The seedHash.</returns>
        internal abstract short GetSeedHash();

        /// <summary>
        /// Gets the current value of ThetaLong.
        /// Only useful during stateful operations.
        /// Intentionally not made public because behavior will be confusing to end user.
        /// </summary>
        /// <returns>The current value of ThetaLong.</returns>
        internal abstract long GetThetaLong();

        /// <summary>
        /// Returns true if this set operator is empty.
        /// Only useful during stateful operations.
        /// Intentionally not made public because behavior will be confusing to end user.
        /// </summary>
        /// <returns>True if this set operator is empty.</returns>
        internal abstract bool IsEmpty();

        // IMemoryStatus interface implementation
        public abstract bool HasMemory();
        public abstract bool IsDirect();
        public abstract bool IsSameResource(Memory<byte> that);
    }
}

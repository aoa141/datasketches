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
using System.Numerics;
using Apache.DataSketches.Common;
using Apache.DataSketches.Hash;

namespace Apache.DataSketches.ThetaCommon
{
    /// <summary>
    /// Utility methods for the Theta Family of sketches
    /// </summary>
    public static class ThetaUtil
    {
        /// <summary>
        /// The smallest Log2 nom entries allowed: 4.
        /// </summary>
        public const int MIN_LG_NOM_LONGS = 4;

        /// <summary>
        /// The largest Log2 nom entries allowed: 26.
        /// </summary>
        public const int MAX_LG_NOM_LONGS = 26;

        /// <summary>
        /// The hash table rebuild threshold = 15.0/16.0.
        /// </summary>
        public const double REBUILD_THRESHOLD = 15.0 / 16.0;

        /// <summary>
        /// The resize threshold = 0.5; tuned for speed.
        /// </summary>
        public const double RESIZE_THRESHOLD = 0.5;

        /// <summary>
        /// The default nominal entries is provided as a convenience for those cases where the
        /// nominal sketch size in number of entries is not provided.
        /// A sketch of 4096 entries has a Relative Standard Error (RSE) of +/- 1.56% at a confidence of
        /// 68%; or equivalently, a Relative Error of +/- 3.1% at a confidence of 95.4%.
        /// </summary>
        public const int DEFAULT_NOMINAL_ENTRIES = 4096;

        /// <summary>
        /// The seed 9001 used in the sketch update methods is a prime number that
        /// was chosen very early on in experimental testing. Choosing a seed is somewhat arbitrary, and
        /// the author cannot prove that this particular seed is somehow superior to other seeds.  There
        /// was some early Internet discussion that a seed of 0 did not produce as clean avalanche diagrams
        /// as non-zero seeds, but this may have been more related to the MurmurHash2 release, which did
        /// have some issues. As far as the author can determine, MurmurHash3 does not have these problems.
        ///
        /// In order to perform set operations on two sketches it is critical that the same hash
        /// function and seed are identical for both sketches, otherwise the assumed 1:1 relationship
        /// between the original source key value and the hashed bit string would be violated. Once
        /// you have developed a history of stored sketches you are stuck with it.
        ///
        /// WARNING: This seed is used internally by library sketches in different
        /// packages and thus must be declared public. However, this seed value must not be used by library
        /// users with the MurmurHash3 function. It should be viewed as existing for exclusive, private
        /// use by the library.
        /// </summary>
        public const long DEFAULT_UPDATE_SEED = 9001L;

        /// <summary>
        /// The smallest Log2 cache size allowed: 5.
        /// </summary>
        public const int MIN_LG_ARR_LONGS = 5;

        /// <summary>
        /// Check if the two seed hashes are equal. If not, throw an SketchesArgumentException.
        /// </summary>
        /// <param name="seedHashA">the seedHash A</param>
        /// <param name="seedHashB">the seedHash B</param>
        /// <returns>seedHashA if they are equal</returns>
        public static short CheckSeedHashes(short seedHashA, short seedHashB)
        {
            if (seedHashA != seedHashB)
            {
                throw new SketchesArgumentException(
                    $"Incompatible Seed Hashes. {(seedHashA & 0xFFFF):X}, {(seedHashB & 0xFFFF):X}");
            }
            return seedHashA;
        }

        /// <summary>
        /// Computes and checks the 16-bit seed hash from the given long seed.
        /// The seed hash may not be zero in order to maintain compatibility with older serialized
        /// versions that did not have this concept.
        /// </summary>
        /// <param name="seed">The update hash seed</param>
        /// <returns>the seed hash.</returns>
        public static short ComputeSeedHash(long seed)
        {
            long[] seedArr = { seed };
            short seedHash = (short)(MurmurHash3.Hash(seedArr, 0L)[0] & 0xFFFFL);
            if (seedHash == 0)
            {
                throw new SketchesArgumentException(
                    $"The given seed: {seed} produced a seedHash of zero. " +
                    "You must choose a different seed.");
            }
            return seedHash;
        }

        /// <summary>
        /// Gets the smallest allowed exponent of 2 that it is a sub-multiple of the target by zero,
        /// one or more resize factors.
        /// </summary>
        /// <param name="lgTarget">Log2 of the target size</param>
        /// <param name="lgRF">Log_base2 of Resize Factor.</param>
        /// <param name="lgMin">Log2 of the minimum allowed starting size</param>
        /// <returns>The Log2 of the starting size</returns>
        public static int StartingSubMultiple(int lgTarget, int lgRF, int lgMin)
        {
            return lgTarget <= lgMin ? lgMin : lgRF == 0 ? lgTarget : (lgTarget - lgMin) % lgRF + lgMin;
        }

        /// <summary>
        /// Checks that the given nomLongs is within bounds and returns the Log2 of the ceiling power of 2
        /// of the given nomLongs.
        /// </summary>
        /// <param name="nomLongs">the given number of nominal longs.  This can be any value from 16 to
        /// 67108864, inclusive.</param>
        /// <returns>The Log2 of the ceiling power of 2 of the given nomLongs.</returns>
        public static int CheckNomLongs(int nomLongs)
        {
            int lgNomLongs = BitOperations.TrailingZeroCount((uint)Util.CeilingPowerOf2(nomLongs));
            if (lgNomLongs > MAX_LG_NOM_LONGS || lgNomLongs < MIN_LG_NOM_LONGS)
            {
                throw new SketchesArgumentException(
                    $"Nominal Entries must be >= 16 and <= 67108864: {nomLongs}");
            }
            return lgNomLongs;
        }
    }
}

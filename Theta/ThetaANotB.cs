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
using System.Linq;
using DataSketches.Common;

namespace DataSketches.Theta
{
    /// <summary>
    /// Theta A-not-B (set difference) operation.
    /// Computes the set difference A - B.
    /// </summary>
    public class ThetaANotB
    {
        private readonly ulong seed;
        private readonly ushort seedHash;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="seed">Seed for the hash function</param>
        public ThetaANotB(ulong seed = CommonDefs.DEFAULT_SEED)
        {
            this.seed = seed;
            this.seedHash = MurmurHash3.ComputeSeedHash(seed);
        }

        /// <summary>
        /// Computes the A-not-B set operation given two sketches.
        /// Returns entries that are in A but not in B.
        /// </summary>
        /// <param name="a">Sketch A</param>
        /// <param name="b">Sketch B</param>
        /// <param name="ordered">If true, produce an ordered sketch</param>
        /// <returns>The result of A-not-B as a compact sketch</returns>
        public CompactThetaSketch Compute(ThetaSketch a, ThetaSketch b, bool ordered = true)
        {
            if (a == null)
                throw new ArgumentNullException(nameof(a));

            // Check seed hash compatibility
            if (a.GetSeedHash() != seedHash)
            {
                throw new ArgumentException("Sketch A has incompatible seed hash");
            }

            // If A is empty, result is empty
            if (a.IsEmpty)
            {
                return new CompactThetaSketch(true, ordered, seedHash, ThetaConstants.MaxTheta, Array.Empty<ulong>());
            }

            // If B is null or empty, result is A
            if (b == null || b.IsEmpty)
            {
                return new CompactThetaSketch(a, ordered);
            }

            // Check seed hash compatibility for B
            if (b.GetSeedHash() != seedHash)
            {
                throw new ArgumentException("Sketch B has incompatible seed hash");
            }

            // Compute theta as minimum of both sketches
            ulong thetaA = a.GetTheta64();
            ulong thetaB = b.GetTheta64();
            ulong minTheta = Math.Min(thetaA, thetaB);

            // Build set of entries from B
            var bEntries = new HashSet<ulong>();
            foreach (var hash in b)
            {
                if (hash != 0 && hash < minTheta)
                {
                    bEntries.Add(hash);
                }
            }

            // Collect entries from A that are not in B
            var resultEntries = new List<ulong>();
            foreach (var hash in a)
            {
                if (hash != 0 && hash < minTheta && !bEntries.Contains(hash))
                {
                    resultEntries.Add(hash);
                }
            }

            if (ordered)
            {
                resultEntries.Sort();
            }

            bool resultIsEmpty = (resultEntries.Count == 0);

            return new CompactThetaSketch(resultIsEmpty, ordered, seedHash, minTheta, resultEntries.ToArray());
        }
    }
}

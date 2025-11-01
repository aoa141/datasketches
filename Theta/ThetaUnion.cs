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
    /// Theta Union operation - computes union of Theta sketches.
    /// </summary>
    public class ThetaUnion
    {
        private readonly UpdateThetaSketch unionSketch;
        private ulong unionTheta;
        private readonly ulong seed;

        /// <summary>
        /// Private constructor. Use Builder to create instances.
        /// </summary>
        private ThetaUnion(byte lgK, CommonDefs.ResizeFactor rf, float p, ulong seed)
        {
            this.seed = seed;
            this.unionTheta = ThetaConstants.MaxTheta;

            var builder = new UpdateThetaSketch.Builder()
                .SetLgK(lgK)
                .SetResizeFactor(rf)
                .SetP(p)
                .SetSeed(seed);

            this.unionSketch = builder.Build();
        }

        /// <summary>
        /// Updates the union with a given sketch.
        /// </summary>
        public void Update(ThetaSketch sketch)
        {
            if (sketch == null) return;
            if (sketch.IsEmpty) return;

            // Check seed hash compatibility
            if (sketch.GetSeedHash() != unionSketch.GetSeedHash())
            {
                throw new ArgumentException("Incompatible seed hash");
            }

            // Update union theta to minimum
            ulong sketchTheta = sketch.GetTheta64();
            if (sketchTheta < unionTheta)
            {
                unionTheta = sketchTheta;
            }

            // Insert all entries from the sketch that pass both theta checks
            ulong unionSketchTheta = unionSketch.GetTheta64();
            foreach (var hash in sketch)
            {
                if (hash != 0 && hash < unionTheta && hash < unionSketchTheta)
                {
                    unionSketch.Insert(hash);
                }
            }

            // Update union theta to account for union sketch's theta
            unionTheta = Math.Min(unionTheta, unionSketch.GetTheta64());
        }

        /// <summary>
        /// Produces a copy of the current state of the union as a compact sketch.
        /// </summary>
        /// <param name="ordered">If true, produce an ordered sketch</param>
        public CompactThetaSketch GetResult(bool ordered = true)
        {
            if (unionSketch.IsEmpty)
            {
                return new CompactThetaSketch(true, true, unionSketch.GetSeedHash(), unionTheta, new ulong[0]);
            }

            // Compute effective theta as minimum of union theta and sketch theta
            ulong unionSketchTheta = unionSketch.GetTheta64();
            ulong theta = Math.Min(unionTheta, unionSketchTheta);
            uint nominalNum = (uint)(1 << unionSketch.GetLgK());

            // Collect entries - filter by theta only if union_theta < table_.theta_
            var resultEntries = new List<ulong>();
            if (unionTheta >= unionSketchTheta)
            {
                // Copy all non-zero entries without filtering
                foreach (var hash in unionSketch)
                {
                    if (hash != 0)
                    {
                        resultEntries.Add(hash);
                    }
                }
            }
            else
            {
                // Filter by theta
                foreach (var hash in unionSketch)
                {
                    if (hash != 0 && hash < theta)
                    {
                        resultEntries.Add(hash);
                    }
                }
            }

            // If entries exceed nominal size, truncate and update theta
            if (resultEntries.Count > nominalNum)
            {
                resultEntries.Sort();
                theta = resultEntries[(int)nominalNum];
                resultEntries.RemoveRange((int)nominalNum, resultEntries.Count - (int)nominalNum);
            }
            else if (ordered)
            {
                resultEntries.Sort();
            }

            return new CompactThetaSketch(
                false,
                ordered,
                unionSketch.GetSeedHash(),
                theta,
                resultEntries.ToArray()
            );
        }

        /// <summary>
        /// Resets the union to the initial empty state.
        /// </summary>
        public void Reset()
        {
            unionSketch.Reset();
            unionTheta = ThetaConstants.MaxTheta;
        }

        /// <summary>
        /// Builder for Theta Union.
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
            /// Builds the Theta Union.
            /// </summary>
            public ThetaUnion Build()
            {
                return new ThetaUnion(lgK, rf, p, seed);
            }
        }
    }
}

// <copyright file="ThetaIntersection.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.Collections.Generic;
using System.Linq;
using DataSketches.Common;

namespace DataSketches.Theta
{
    /// <summary>
    /// Theta Intersection operation - computes intersection of Theta sketches.
    /// </summary>
    public class ThetaIntersection
    {
        private readonly ulong seed;
        private readonly ushort seedHash;
        private bool hasResult;
        private bool isEmpty;
        private ulong theta;
        private HashSet<ulong> entries;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="seed">Seed for the hash function</param>
        public ThetaIntersection(ulong seed = CommonDefs.DEFAULT_SEED)
        {
            this.seed = seed;
            this.seedHash = MurmurHash3.ComputeSeedHash(seed);
            this.hasResult = false;
            this.isEmpty = false;
            this.theta = ThetaConstants.MaxTheta;
            this.entries = new HashSet<ulong>();
        }

        /// <summary>
        /// Updates the intersection with a given sketch.
        /// The intersection starts from the "universe" set, and every update
        /// reduces the current set to leave the overlapping subset only.
        /// </summary>
        public void Update(ThetaSketch sketch)
        {
            if (sketch == null)
                throw new ArgumentNullException(nameof(sketch));

            // Check seed hash compatibility
            if (sketch.GetSeedHash() != seedHash)
            {
                throw new ArgumentException("Incompatible seed hash");
            }

            if (!hasResult)
            {
                // First sketch - initialize with all its entries
                hasResult = true;
                isEmpty = sketch.IsEmpty;
                theta = sketch.GetTheta64();

                foreach (var hash in sketch)
                {
                    if (hash != 0)
                    {
                        entries.Add(hash);
                    }
                }
            }
            else
            {
                // Subsequent sketches - intersect
                if (sketch.IsEmpty)
                {
                    isEmpty = true;
                    entries.Clear();
                    return;
                }

                // Update theta to minimum
                ulong sketchTheta = sketch.GetTheta64();
                if (sketchTheta < theta)
                {
                    theta = sketchTheta;
                    // Remove entries >= new theta
                    entries.RemoveWhere(e => e >= theta);
                }

                // Build set of sketch entries
                var sketchEntries = new HashSet<ulong>();
                foreach (var hash in sketch)
                {
                    if (hash != 0 && hash < theta)
                    {
                        sketchEntries.Add(hash);
                    }
                }

                // Keep only common entries
                entries.IntersectWith(sketchEntries);

                // If no common entries remain, mark as empty
                if (entries.Count == 0)
                {
                    isEmpty = true;
                }
            }
        }

        /// <summary>
        /// Produces a copy of the current state of the intersection.
        /// </summary>
        /// <param name="ordered">If true, produce an ordered sketch</param>
        /// <returns>The result of the intersection</returns>
        /// <exception cref="InvalidOperationException">Thrown if Update was never called</exception>
        public CompactThetaSketch GetResult(bool ordered = true)
        {
            if (!hasResult)
            {
                throw new InvalidOperationException("Intersection state is undefined. Call Update at least once.");
            }

            var resultEntries = entries.ToArray();
            if (ordered)
            {
                Array.Sort(resultEntries);
            }

            return new CompactThetaSketch(isEmpty, ordered, seedHash, theta, resultEntries);
        }

        /// <summary>
        /// Returns true if the state of the intersection is defined (not infinite "universe").
        /// </summary>
        public bool HasResult()
        {
            return hasResult;
        }
    }
}

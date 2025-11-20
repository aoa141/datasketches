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

namespace Apache.DataSketches.Common
{
    /// <summary>
    /// Defines the various families of sketch and set operation classes. A family defines a set of
    /// classes that share fundamental algorithms and behaviors. The classes within a family may
    /// still differ by how they are stored and accessed.
    /// </summary>
    /// <remarks>
    /// Author: Lee Rhodes
    /// </remarks>
    public enum Family
    {
        /// <summary>
        /// The Alpha Sketch family is a member of the Theta Sketch Framework.
        /// </summary>
        ALPHA = 1,

        /// <summary>
        /// The QuickSelect Sketch family is a member of the Theta Sketch Framework.
        /// </summary>
        QUICKSELECT = 2,

        /// <summary>
        /// The Compact Sketch family is a member of the Theta Sketch Framework.
        /// </summary>
        COMPACT = 3,

        /// <summary>
        /// The Union family is an operation for the Theta Sketch Framework.
        /// </summary>
        UNION = 4,

        /// <summary>
        /// The Intersection family is an operation for the Theta Sketch Framework.
        /// </summary>
        INTERSECTION = 5,

        /// <summary>
        /// The A and not B family is an operation for the Theta Sketch Framework.
        /// </summary>
        A_NOT_B = 6,

        /// <summary>
        /// The HLL family of sketches. (Not part of TSF.)
        /// </summary>
        HLL = 7,

        /// <summary>
        /// The Quantiles family of sketches. (Not part of TSF.)
        /// </summary>
        QUANTILES = 8,

        /// <summary>
        /// The Tuple family of sketches is a large family of sketches that are extensions of the Theta Sketch Framework.
        /// </summary>
        TUPLE = 9,

        /// <summary>
        /// The Frequency family of sketches. (Not part of TSF.)
        /// </summary>
        FREQUENCY = 10,

        /// <summary>
        /// The Reservoir family of sketches. (Not part of TSF.)
        /// </summary>
        RESERVOIR = 11,

        /// <summary>
        /// The reservoir sampling family of Union operations. (Not part of TSF.)
        /// </summary>
        RESERVOIR_UNION = 12,

        /// <summary>
        /// The VarOpt family of sketches. (Not part of TSF.)
        /// </summary>
        VAROPT = 13,

        /// <summary>
        /// The VarOpt Union family of sketches. (Not part of TSF.)
        /// </summary>
        VAROPT_UNION = 14,

        /// <summary>
        /// KLL quantiles sketch
        /// </summary>
        KLL = 15,

        /// <summary>
        /// Compressed Probabilistic Counting (CPC) Sketch
        /// </summary>
        CPC = 16,

        /// <summary>
        /// Relative Error Quantiles Sketch
        /// </summary>
        REQ = 17,

        /// <summary>
        /// CountMin Sketch
        /// </summary>
        COUNTMIN = 18,

        /// <summary>
        /// Exact and Bounded, Probability Proportional to Size (EBPPS)
        /// </summary>
        EBPPS = 19,

        /// <summary>
        /// t-Digest for estimating quantiles and ranks
        /// </summary>
        TDIGEST = 20,

        /// <summary>
        /// Bloom Filter
        /// </summary>
        BLOOMFILTER = 21
    }

    /// <summary>
    /// Extension methods and static helpers for Family enum
    /// </summary>
    public static class FamilyExtensions
    {
        private static readonly Dictionary<int, (string Name, int MinPreLongs, int MaxPreLongs)> FamilyData =
            new Dictionary<int, (string, int, int)>
            {
                { 1, ("ALPHA", 3, 3) },
                { 2, ("QUICKSELECT", 3, 3) },
                { 3, ("COMPACT", 1, 3) },
                { 4, ("UNION", 4, 4) },
                { 5, ("INTERSECTION", 3, 3) },
                { 6, ("ANOTB", 3, 3) },
                { 7, ("HLL", 1, 1) },
                { 8, ("QUANTILES", 1, 2) },
                { 9, ("TUPLE", 1, 3) },
                { 10, ("FREQUENCY", 1, 4) },
                { 11, ("RESERVOIR", 1, 2) },
                { 12, ("RESERVOIR_UNION", 1, 1) },
                { 13, ("VAROPT", 1, 4) },
                { 14, ("VAROPT_UNION", 1, 4) },
                { 15, ("KLL", 1, 2) },
                { 16, ("CPC", 1, 5) },
                { 17, ("REQ", 1, 2) },
                { 18, ("COUNTMIN", 2, 2) },
                { 19, ("EBPPS", 1, 5) },
                { 20, ("TDIGEST", 1, 2) },
                { 21, ("BLOOMFILTER", 3, 4) }
            };

        private static readonly Dictionary<int, Family> LookupID = new Dictionary<int, Family>();
        private static readonly Dictionary<string, Family> LookupFamName = new Dictionary<string, Family>();

        static FamilyExtensions()
        {
            foreach (Family f in Enum.GetValues(typeof(Family)))
            {
                int id = (int)f;
                LookupID[id] = f;
                if (FamilyData.TryGetValue(id, out var data))
                {
                    LookupFamName[data.Name.ToUpperInvariant()] = f;
                }
            }
        }

        /// <summary>
        /// Returns the byte ID for this family
        /// </summary>
        /// <param name="family">The family</param>
        /// <returns>The byte ID for this family</returns>
        public static int GetID(this Family family)
        {
            return (int)family;
        }

        /// <summary>
        /// Checks if the given ID matches this family, throws exception if not.
        /// </summary>
        /// <param name="family">The family</param>
        /// <param name="id">The given id, a value &lt; 128.</param>
        public static void CheckFamilyID(this Family family, int id)
        {
            if (id != (int)family)
            {
                throw new SketchesArgumentException(
                    $"Possible Corruption: This Family {family.GetFamilyName()} " +
                    $"does not match the ID of the given Family: {IdToFamily(id).GetFamilyName()}");
            }
        }

        /// <summary>
        /// Returns the name for this family
        /// </summary>
        /// <param name="family">The family</param>
        /// <returns>The name for this family</returns>
        public static string GetFamilyName(this Family family)
        {
            int id = (int)family;
            return FamilyData.TryGetValue(id, out var data) ? data.Name : family.ToString();
        }

        /// <summary>
        /// Returns the minimum preamble size for this family in longs
        /// </summary>
        /// <param name="family">The family</param>
        /// <returns>The minimum preamble size for this family in longs</returns>
        public static int GetMinPreLongs(this Family family)
        {
            int id = (int)family;
            return FamilyData.TryGetValue(id, out var data) ? data.MinPreLongs : 0;
        }

        /// <summary>
        /// Returns the maximum preamble size for this family in longs
        /// </summary>
        /// <param name="family">The family</param>
        /// <returns>The maximum preamble size for this family in longs</returns>
        public static int GetMaxPreLongs(this Family family)
        {
            int id = (int)family;
            return FamilyData.TryGetValue(id, out var data) ? data.MaxPreLongs : 0;
        }

        /// <summary>
        /// Returns the Family given the ID
        /// </summary>
        /// <param name="id">The given ID</param>
        /// <returns>The Family given the ID</returns>
        public static Family IdToFamily(int id)
        {
            if (LookupID.TryGetValue(id, out var family))
            {
                return family;
            }
            throw new SketchesArgumentException($"Possible Corruption: Illegal Family ID: {id}");
        }

        /// <summary>
        /// Returns the Family given the family name
        /// </summary>
        /// <param name="famName">The family name</param>
        /// <returns>The Family given the family name</returns>
        public static Family StringToFamily(string famName)
        {
            if (LookupFamName.TryGetValue(famName.ToUpperInvariant(), out var family))
            {
                return family;
            }
            throw new SketchesArgumentException($"Possible Corruption: Illegal Family Name: {famName}");
        }
    }
}

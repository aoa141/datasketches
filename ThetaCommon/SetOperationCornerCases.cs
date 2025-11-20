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
using Apache.DataSketches.Common;

namespace Apache.DataSketches.ThetaCommon
{
    /// <summary>
    /// Simplifies and speeds up set operations by resolving specific corner cases.
    /// </summary>
    public class SetOperationCornerCases
    {
        private const long MAX = long.MaxValue;

        /// <summary>
        /// Intersection actions
        /// </summary>
        public enum IntersectAction
        {
            DEGEN_MIN_0_F,  // D - Degenerate{MinTheta, 0, F}
            EMPTY_1_0_T,    // E - Empty{1.0, 0, T}
            FULL_INTERSECT  // I - Full Intersect
        }

        /// <summary>
        /// A not B actions
        /// </summary>
        public enum AnotBAction
        {
            SKETCH_A,       // A - Sketch A Exactly
            TRIM_A,         // TA - Trim Sketch A by MinTheta
            DEGEN_MIN_0_F,  // D - Degenerate{MinTheta, 0, F}
            DEGEN_THA_0_F,  // DA - Degenerate{ThetaA, 0, F}
            EMPTY_1_0_T,    // E - Empty{1.0, 0, T}
            FULL_ANOTB      // N - Full AnotB
        }

        /// <summary>
        /// Union actions
        /// </summary>
        public enum UnionAction
        {
            SKETCH_A,       // A - Sketch A Exactly
            TRIM_A,         // TA - Trim Sketch A by MinTheta
            SKETCH_B,       // B - Sketch B Exactly
            TRIM_B,         // TB - Trim Sketch B by MinTheta
            DEGEN_MIN_0_F,  // D - Degenerate{MinTheta, 0, F}
            DEGEN_THA_0_F,  // DA - Degenerate{ThetaA, 0, F}
            DEGEN_THB_0_F,  // DB - Degenerate{ThetaB, 0, F}
            EMPTY_1_0_T,    // E - Empty{1.0, 0, T}
            FULL_UNION      // N - Full Union
        }

        /// <summary>
        /// Corner case enum
        /// </summary>
        public enum CornerCase
        {
            Empty_Empty = 055,           // A{ 1.0, 0, T} ; B{ 1.0, 0, T}
            Empty_Exact = 056,           // A{ 1.0, 0, T} ; B{ 1.0,>0, F}
            Empty_Estimation = 052,      // A{ 1.0, 0, T} ; B{<1.0,>0, F}
            Empty_Degen = 050,           // A{ 1.0, 0, T} ; B{<1.0, 0, F}

            Exact_Empty = 065,           // A{ 1.0,>0, F} ; B{ 1.0, 0, T}
            Exact_Exact = 066,           // A{ 1.0,>0, F} ; B{ 1.0,>0, F}
            Exact_Estimation = 062,      // A{ 1.0,>0, F} ; B{<1.0,>0, F}
            Exact_Degen = 060,           // A{ 1.0,>0, F} ; B{<1.0, 0, F}

            Estimation_Empty = 025,      // A{<1.0,>0, F} ; B{ 1.0, 0, T}
            Estimation_Exact = 026,      // A{<1.0,>0, F} ; B{ 1.0,>0, F}
            Estimation_Estimation = 022, // A{<1.0,>0, F} ; B{<1.0,>0, F}
            Estimation_Degen = 020,      // A{<1.0,>0, F} ; B{<1.0, 0, F}

            Degen_Empty = 005,           // A{<1.0, 0, F} ; B{ 1.0, 0, T}
            Degen_Exact = 006,           // A{<1.0, 0, F} ; B{ 1.0,>0, F}
            Degen_Estimation = 002,      // A{<1.0, 0, F} ; B{<1.0,>0, F}
            Degen_Degen = 000            // A{<1.0, 0, F} ; B{<1.0, 0, F}
        }

        private static readonly Dictionary<int, (CornerCase, IntersectAction, AnotBAction, UnionAction)> caseIdToActionMap =
            new Dictionary<int, (CornerCase, IntersectAction, AnotBAction, UnionAction)>
            {
                { 055, (CornerCase.Empty_Empty, IntersectAction.EMPTY_1_0_T, AnotBAction.EMPTY_1_0_T, UnionAction.EMPTY_1_0_T) },
                { 056, (CornerCase.Empty_Exact, IntersectAction.EMPTY_1_0_T, AnotBAction.EMPTY_1_0_T, UnionAction.SKETCH_B) },
                { 052, (CornerCase.Empty_Estimation, IntersectAction.EMPTY_1_0_T, AnotBAction.EMPTY_1_0_T, UnionAction.SKETCH_B) },
                { 050, (CornerCase.Empty_Degen, IntersectAction.EMPTY_1_0_T, AnotBAction.EMPTY_1_0_T, UnionAction.DEGEN_THB_0_F) },

                { 065, (CornerCase.Exact_Empty, IntersectAction.EMPTY_1_0_T, AnotBAction.SKETCH_A, UnionAction.SKETCH_A) },
                { 066, (CornerCase.Exact_Exact, IntersectAction.FULL_INTERSECT, AnotBAction.FULL_ANOTB, UnionAction.FULL_UNION) },
                { 062, (CornerCase.Exact_Estimation, IntersectAction.FULL_INTERSECT, AnotBAction.FULL_ANOTB, UnionAction.FULL_UNION) },
                { 060, (CornerCase.Exact_Degen, IntersectAction.DEGEN_MIN_0_F, AnotBAction.TRIM_A, UnionAction.TRIM_A) },

                { 025, (CornerCase.Estimation_Empty, IntersectAction.EMPTY_1_0_T, AnotBAction.SKETCH_A, UnionAction.SKETCH_A) },
                { 026, (CornerCase.Estimation_Exact, IntersectAction.FULL_INTERSECT, AnotBAction.FULL_ANOTB, UnionAction.FULL_UNION) },
                { 022, (CornerCase.Estimation_Estimation, IntersectAction.FULL_INTERSECT, AnotBAction.FULL_ANOTB, UnionAction.FULL_UNION) },
                { 020, (CornerCase.Estimation_Degen, IntersectAction.DEGEN_MIN_0_F, AnotBAction.TRIM_A, UnionAction.TRIM_A) },

                { 005, (CornerCase.Degen_Empty, IntersectAction.EMPTY_1_0_T, AnotBAction.DEGEN_THA_0_F, UnionAction.DEGEN_THA_0_F) },
                { 006, (CornerCase.Degen_Exact, IntersectAction.DEGEN_MIN_0_F, AnotBAction.DEGEN_THA_0_F, UnionAction.TRIM_B) },
                { 002, (CornerCase.Degen_Estimation, IntersectAction.DEGEN_MIN_0_F, AnotBAction.DEGEN_MIN_0_F, UnionAction.TRIM_B) },
                { 000, (CornerCase.Degen_Degen, IntersectAction.DEGEN_MIN_0_F, AnotBAction.DEGEN_MIN_0_F, UnionAction.DEGEN_MIN_0_F) }
            };

        public static int CreateCornerCaseId(
            long thetaLongA, int countA, bool emptyA,
            long thetaLongB, int countB, bool emptyB)
        {
            return (SketchStateId(emptyA, countA, thetaLongA) << 3) | SketchStateId(emptyB, countB, thetaLongB);
        }

        public static int SketchStateId(bool isEmpty, int numRetained, long thetaLong)
        {
            // assume thetaLong = MAX if empty
            return (((thetaLong == MAX) || isEmpty) ? 4 : 0) | ((numRetained > 0) ? 2 : 0) | (isEmpty ? 1 : 0);
        }

        public static CornerCase CaseIdToCornerCase(int id)
        {
            if (caseIdToActionMap.TryGetValue(id, out var result))
            {
                return result.Item1;
            }
            throw new SketchesArgumentException($"Possible Corruption: Illegal CornerCase ID: {Convert.ToString(id, 8)}");
        }

        public static IntersectAction GetIntersectAction(int id)
        {
            if (caseIdToActionMap.TryGetValue(id, out var result))
            {
                return result.Item2;
            }
            throw new SketchesArgumentException($"Possible Corruption: Illegal CornerCase ID: {Convert.ToString(id, 8)}");
        }

        public static AnotBAction GetAnotBAction(int id)
        {
            if (caseIdToActionMap.TryGetValue(id, out var result))
            {
                return result.Item3;
            }
            throw new SketchesArgumentException($"Possible Corruption: Illegal CornerCase ID: {Convert.ToString(id, 8)}");
        }

        public static UnionAction GetUnionAction(int id)
        {
            if (caseIdToActionMap.TryGetValue(id, out var result))
            {
                return result.Item4;
            }
            throw new SketchesArgumentException($"Possible Corruption: Illegal CornerCase ID: {Convert.ToString(id, 8)}");
        }
    }
}

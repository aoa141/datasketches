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

using Apache.DataSketches.Common;

namespace Apache.DataSketches.ThetaCommon
{
    /// <summary>
    /// This class is used to compute the bounds on the estimate of the ratio B / A, where:
    /// A is a Theta Sketch of population PopA.
    /// B is a Theta Sketch of population PopB that is a subset of A,
    /// obtained by an intersection of A with some other Theta Sketch C,
    /// which acts like a predicate or selection clause.
    /// The estimate of the ratio PopB/PopA is BoundsOnRatiosInThetaSketchedSets.GetEstimateOfBoverA(A, B).
    /// The Upper Bound estimate on the ratio PopB/PopA is BoundsOnRatiosInThetaSketchedSets.GetUpperBoundForBoverA(A, B).
    /// The Lower Bound estimate on the ratio PopB/PopA is BoundsOnRatiosInThetaSketchedSets.GetLowerBoundForBoverA(A, B).
    /// Note: The theta of A cannot be greater than the theta of B.
    /// If B is formed as an intersection of A and some other set C,
    /// then the theta of B is guaranteed to be less than or equal to the theta of B.
    /// </summary>
    public static class BoundsOnRatiosInThetaSketchedSets
    {
        /// <summary>
        /// Gets the approximate lower bound for B over A based on a 95% confidence interval
        /// </summary>
        /// <param name="sketchA">the sketch A</param>
        /// <param name="sketchB">the sketch B</param>
        /// <returns>the approximate lower bound for B over A</returns>
        public static double GetLowerBoundForBoverA(Theta.Sketch sketchA, Theta.Sketch sketchB)
        {
            long thetaLongA = sketchA.GetThetaLong();
            long thetaLongB = sketchB.GetThetaLong();
            CheckThetas(thetaLongA, thetaLongB);

            int countB = sketchB.GetRetainedEntries(true);
            int countA = (thetaLongB == thetaLongA)
                ? sketchA.GetRetainedEntries(true)
                : sketchA.GetCountLessThanThetaLong(thetaLongB);

            if (countA <= 0) { return 0; }
            double f = thetaLongB / Util.LONG_MAX_VALUE_AS_DOUBLE;
            return BoundsOnRatiosInSampledSets.GetLowerBoundForBoverA(countA, countB, f);
        }

        /// <summary>
        /// Gets the approximate upper bound for B over A based on a 95% confidence interval
        /// </summary>
        /// <param name="sketchA">the sketch A</param>
        /// <param name="sketchB">the sketch B</param>
        /// <returns>the approximate upper bound for B over A</returns>
        public static double GetUpperBoundForBoverA(Theta.Sketch sketchA, Theta.Sketch sketchB)
        {
            long thetaLongA = sketchA.GetThetaLong();
            long thetaLongB = sketchB.GetThetaLong();
            CheckThetas(thetaLongA, thetaLongB);

            int countB = sketchB.GetRetainedEntries(true);
            int countA = (thetaLongB == thetaLongA)
                ? sketchA.GetRetainedEntries(true)
                : sketchA.GetCountLessThanThetaLong(thetaLongB);

            if (countA <= 0) { return 1.0; }
            double f = thetaLongB / Util.LONG_MAX_VALUE_AS_DOUBLE;
            return BoundsOnRatiosInSampledSets.GetUpperBoundForBoverA(countA, countB, f);
        }

        /// <summary>
        /// Gets the estimate for B over A
        /// </summary>
        /// <param name="sketchA">the sketch A</param>
        /// <param name="sketchB">the sketch B</param>
        /// <returns>the estimate for B over A</returns>
        public static double GetEstimateOfBoverA(Theta.Sketch sketchA, Theta.Sketch sketchB)
        {
            long thetaLongA = sketchA.GetThetaLong();
            long thetaLongB = sketchB.GetThetaLong();
            CheckThetas(thetaLongA, thetaLongB);

            int countB = sketchB.GetRetainedEntries(true);
            int countA = (thetaLongB == thetaLongA)
                ? sketchA.GetRetainedEntries(true)
                : sketchA.GetCountLessThanThetaLong(thetaLongB);

            if (countA <= 0) { return 0.5; }

            return (double)countB / (double)countA;
        }

        internal static void CheckThetas(long thetaLongA, long thetaLongB)
        {
            if (thetaLongB > thetaLongA)
            {
                throw new SketchesArgumentException("ThetaLongB cannot be > ThetaLongA.");
            }
        }
    }
}

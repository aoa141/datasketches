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

namespace Apache.DataSketches.Common
{
    /// <summary>
    /// This class is used to compute the bounds on the estimate of the ratio <i>|B| / |A|</i>, where:
    /// <list type="bullet">
    /// <item><description><i>|A|</i> is the unknown size of a set <i>A</i> of unique identifiers.</description></item>
    /// <item><description><i>|B|</i> is the unknown size of a subset <i>B</i> of <i>A</i>.</description></item>
    /// <item><description><i>a</i> = <i>|S<sub>A</sub>|</i> is the observed size of a sample of <i>A</i>
    /// that was obtained by Bernoulli sampling with a known inclusion probability <i>f</i>.</description></item>
    /// <item><description><i>b</i> = <i>|S<sub>A</sub> ∩ B|</i> is the observed size of a subset
    /// of <i>S<sub>A</sub></i>.</description></item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// Author: Kevin Lang
    /// </remarks>
    public static class BoundsOnRatiosInSampledSets
    {
        private const double NUM_STD_DEVS = 2.0; // made a constant to simplify interface.

        /// <summary>
        /// Return the approximate lower bound based on a 95% confidence interval
        /// </summary>
        /// <param name="a">See class summary</param>
        /// <param name="b">See class summary</param>
        /// <param name="f">the inclusion probability used to produce the set with size <i>a</i> and should
        /// generally be less than 0.5. Above this value, the results not be reliable.
        /// When <i>f</i> = 1.0 this returns the estimate.</param>
        /// <returns>the approximate lower bound</returns>
        public static double GetLowerBoundForBoverA(long a, long b, double f)
        {
            CheckInputs(a, b, f);
            if (a == 0) { return 0.0; }
            if (f == 1.0) { return (double)b / a; }
            return BoundsOnBinomialProportions.ApproximateLowerBoundOnP(a, b, NUM_STD_DEVS * HackyAdjuster(f));
        }

        /// <summary>
        /// Return the approximate upper bound based on a 95% confidence interval
        /// </summary>
        /// <param name="a">See class summary</param>
        /// <param name="b">See class summary</param>
        /// <param name="f">the inclusion probability used to produce the set with size <i>a</i>.</param>
        /// <returns>the approximate upper bound</returns>
        public static double GetUpperBoundForBoverA(long a, long b, double f)
        {
            CheckInputs(a, b, f);
            if (a == 0) { return 1.0; }
            if (f == 1.0) { return (double)b / a; }
            return BoundsOnBinomialProportions.ApproximateUpperBoundOnP(a, b, NUM_STD_DEVS * HackyAdjuster(f));
        }

        /// <summary>
        /// Return the estimate of b over a
        /// </summary>
        /// <param name="a">See class summary</param>
        /// <param name="b">See class summary</param>
        /// <returns>the estimate of b over a</returns>
        public static double GetEstimateOfBoverA(long a, long b)
        {
            CheckInputs(a, b, 0.3);
            if (a == 0) { return 0.5; }
            return (double)b / a;
        }

        /// <summary>
        /// Return the estimate of A. See class summary.
        /// </summary>
        /// <param name="a">See class summary</param>
        /// <param name="f">the inclusion probability used to produce the set with size <i>a</i>.</param>
        /// <returns>the estimate of A</returns>
        public static double GetEstimateOfA(long a, double f)
        {
            CheckInputs(a, 1, f);
            return a / f;
        }

        /// <summary>
        /// Return the estimate of B. See class summary.
        /// </summary>
        /// <param name="b">See class summary</param>
        /// <param name="f">the inclusion probability used to produce the set with size <i>b</i>.</param>
        /// <returns>the estimate of B</returns>
        public static double GetEstimateOfB(long b, double f)
        {
            CheckInputs(b + 1, b, f);
            return b / f;
        }

        /// <summary>
        /// This hackyAdjuster is tightly coupled with the width of the confidence interval normally
        /// specified with number of standard deviations. To simplify this interface the number of
        /// standard deviations has been fixed to 2.0, which corresponds to a confidence interval of
        /// 95%.
        /// </summary>
        /// <param name="f">the inclusion probability used to produce the set with size <i>a</i>.</param>
        /// <returns>the hacky Adjuster</returns>
        private static double HackyAdjuster(double f)
        {
            double tmp = Math.Sqrt(1.0 - f);
            return (f <= 0.5) ? tmp : tmp + (0.01 * (f - 0.5));
        }

        internal static void CheckInputs(long a, long b, double f)
        {
            if (((a - b) | a | b) < 0)  // if any group goes negative
            {
                throw new SketchesArgumentException(
                    $"a must be >= b and neither a nor b can be < 0: a = {a}, b = {b}");
            }
            if ((f > 1.0) || (f <= 0.0))
            {
                throw new SketchesArgumentException($"Required: ((f <= 1.0) && (f > 0.0)): {f}");
            }
        }
    }
}

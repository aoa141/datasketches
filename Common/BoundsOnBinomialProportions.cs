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
    /// Confidence intervals for binomial proportions.
    /// This class computes an approximation to the Clopper-Pearson confidence interval
    /// for a binomial proportion. Exact Clopper-Pearson intervals are strictly
    /// conservative, but these approximations are not.
    /// </summary>
    /// <remarks>
    /// Author: Kevin Lang
    /// </remarks>
    public static class BoundsOnBinomialProportions
    {
        /// <summary>
        /// Computes lower bound of approximate Clopper-Pearson confidence interval for a binomial proportion.
        /// </summary>
        /// <param name="n">is the number of trials. Must be non-negative.</param>
        /// <param name="k">is the number of successes. Must be non-negative, and cannot exceed n.</param>
        /// <param name="numStdDevs">the number of standard deviations defining the confidence interval</param>
        /// <returns>the lower bound of the approximate Clopper-Pearson confidence interval for the unknown success probability.</returns>
        public static double ApproximateLowerBoundOnP(long n, long k, double numStdDevs)
        {
            CheckInputs(n, k);
            if (n == 0) { return 0.0; } // the coin was never flipped, so we know nothing
            else if (k == 0) { return 0.0; }
            else if (k == 1) { return ExactLowerBoundOnPForKequalsOne(n, DeltaOfNumStdevs(numStdDevs)); }
            else if (k == n) { return ExactLowerBoundOnPForKequalsN(n, DeltaOfNumStdevs(numStdDevs)); }
            else
            {
                double x = AbramowitzStegunFormula26p5p22((n - k) + 1, k, (-1.0 * numStdDevs));
                return (1.0 - x); // which is p
            }
        }

        /// <summary>
        /// Computes upper bound of approximate Clopper-Pearson confidence interval for a binomial proportion.
        /// </summary>
        /// <param name="n">is the number of trials. Must be non-negative.</param>
        /// <param name="k">is the number of successes. Must be non-negative, and cannot exceed n.</param>
        /// <param name="numStdDevs">the number of standard deviations defining the confidence interval</param>
        /// <returns>the upper bound of the approximate Clopper-Pearson confidence interval for the unknown success probability.</returns>
        public static double ApproximateUpperBoundOnP(long n, long k, double numStdDevs)
        {
            CheckInputs(n, k);
            if (n == 0) { return 1.0; } // the coin was never flipped, so we know nothing
            else if (k == n) { return 1.0; }
            else if (k == (n - 1))
            {
                return ExactUpperBoundOnPForKequalsNminusOne(n, DeltaOfNumStdevs(numStdDevs));
            }
            else if (k == 0)
            {
                return ExactUpperBoundOnPForKequalsZero(n, DeltaOfNumStdevs(numStdDevs));
            }
            else
            {
                double x = AbramowitzStegunFormula26p5p22(n - k, k + 1, numStdDevs);
                return (1.0 - x); // which is p
            }
        }

        /// <summary>
        /// Computes an estimate of an unknown binomial proportion.
        /// </summary>
        /// <param name="n">is the number of trials. Must be non-negative.</param>
        /// <param name="k">is the number of successes. Must be non-negative, and cannot exceed n.</param>
        /// <returns>the estimate of the unknown binomial proportion.</returns>
        public static double EstimateUnknownP(long n, long k)
        {
            CheckInputs(n, k);
            if (n == 0) { return 0.5; } // the coin was never flipped, so we know nothing
            else { return ((double)k / (double)n); }
        }

        private static void CheckInputs(long n, long k)
        {
            if (n < 0) { throw new SketchesArgumentException("N must be non-negative"); }
            if (k < 0) { throw new SketchesArgumentException("K must be non-negative"); }
            if (k > n) { throw new SketchesArgumentException("K cannot exceed N"); }
        }

        /// <summary>
        /// Computes an approximation to the erf() function.
        /// </summary>
        /// <param name="x">is the input to the erf function</param>
        /// <returns>returns erf(x), accurate to roughly 7 decimal digits.</returns>
        public static double Erf(double x)
        {
            if (x < 0.0) { return (-1.0 * (ErfOfNonneg(-1.0 * x))); }
            else { return ErfOfNonneg(x); }
        }

        /// <summary>
        /// Computes an approximation to normalCDF(x).
        /// </summary>
        /// <param name="x">is the input to the normalCDF function</param>
        /// <returns>returns the approximation to normalCDF(x).</returns>
        public static double NormalCDF(double x)
        {
            return (0.5 * (1.0 + (Erf(x / (Math.Sqrt(2.0))))));
        }

        // Abramowitz and Stegun formula 7.1.28, p. 88; Claims accuracy of about 7 decimal digits
        private static double ErfOfNonneg(double x)
        {
            // The constants from the book
            const double a1 = 0.0705230784;
            const double a3 = 0.0092705272;
            const double a5 = 0.0002765672;
            const double a2 = 0.0422820123;
            const double a4 = 0.0001520143;
            const double a6 = 0.0000430638;
            double x2 = x * x; // x squared, x cubed, etc.
            double x3 = x2 * x;
            double x4 = x2 * x2;
            double x5 = x2 * x3;
            double x6 = x3 * x3;
            double sum = (1.0
                        + (a1 * x)
                        + (a2 * x2)
                        + (a3 * x3)
                        + (a4 * x4)
                        + (a5 * x5)
                        + (a6 * x6));
            double sum2 = sum * sum; // raise the sum to the 16th power
            double sum4 = sum2 * sum2;
            double sum8 = sum4 * sum4;
            double sum16 = sum8 * sum8;
            return (1.0 - (1.0 / sum16));
        }

        private static double DeltaOfNumStdevs(double kappa)
        {
            return NormalCDF(-1.0 * kappa);
        }

        // Formula 26.5.22 on page 945 of Abramowitz & Stegun, which is an approximation
        // of the inverse of the incomplete beta function I_x(a,b) = delta
        // viewed as a scalar function of x.
        // In other words, we specify delta, and it gives us x (with a and b held constant).
        // However, delta is specified in an indirect way through yp which
        // is the number of stdDevs that leaves delta probability in the right
        // tail of a standard gaussian distribution.
        private static double AbramowitzStegunFormula26p5p22(double a, double b, double yp)
        {
            double b2m1 = (2.0 * b) - 1.0;
            double a2m1 = (2.0 * a) - 1.0;
            double lambda = ((yp * yp) - 3.0) / 6.0;
            double htmp = (1.0 / a2m1) + (1.0 / b2m1);
            double h = 2.0 / htmp;
            double term1 = (yp * (Math.Sqrt(h + lambda))) / h;
            double term2 = (1.0 / b2m1) - (1.0 / a2m1);
            double term3 = (lambda + (5.0 / 6.0)) - (2.0 / (3.0 * h));
            double w = term1 - (term2 * term3);
            double xp = a / (a + (b * (Math.Exp(2.0 * w))));
            return xp;
        }

        // Formulas for some special cases.
        private static double ExactUpperBoundOnPForKequalsZero(double n, double delta)
        {
            return (1.0 - Math.Pow(delta, (1.0 / n)));
        }

        private static double ExactLowerBoundOnPForKequalsN(double n, double delta)
        {
            return Math.Pow(delta, (1.0 / n));
        }

        private static double ExactLowerBoundOnPForKequalsOne(double n, double delta)
        {
            return (1.0 - Math.Pow((1.0 - delta), (1.0 / n)));
        }

        private static double ExactUpperBoundOnPForKequalsNminusOne(double n, double delta)
        {
            return Math.Pow((1.0 - delta), (1.0 / n));
        }
    }
}

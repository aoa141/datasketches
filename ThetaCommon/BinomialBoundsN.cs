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
using System.Diagnostics;
using Apache.DataSketches.Common;

namespace Apache.DataSketches.ThetaCommon
{
    /// <summary>
    /// This class enables the estimation of error bounds given a sample set size, the sampling
    /// probability theta, the number of standard deviations and a simple noDataSeen flag.  This can
    /// be used to estimate error bounds for fixed threshold sampling as well as the error bounds
    /// calculations for sketches.
    /// </summary>
    public static class BinomialBoundsN
    {
        private static readonly double[] deltaOfNumSDev =
        {
            0.5000000000000000000, // = 0.5 (1 + erf(0)
            0.1586553191586026479, // = 0.5 (1 + erf((-1/sqrt(2))))
            0.0227502618904135701, // = 0.5 (1 + erf((-2/sqrt(2))))
            0.0013498126861731796  // = 0.5 (1 + erf((-3/sqrt(2))))
        };

        // our "classic" bounds, but now with continuity correction

        private static double ContClassicLB(double numSamplesF, double theta, double numSDev)
        {
            double nHat = (numSamplesF - 0.5) / theta;
            double b = numSDev * Math.Sqrt((1.0 - theta) / theta);
            double d = 0.5 * b * Math.Sqrt((b * b) + (4.0 * nHat));
            double center = nHat + (0.5 * (b * b));
            return (center - d);
        }

        private static double ContClassicUB(double numSamplesF, double theta, double numSDev)
        {
            double nHat = (numSamplesF + 0.5) / theta;
            double b = numSDev * Math.Sqrt((1.0 - theta) / theta);
            double d = 0.5 * b * Math.Sqrt((b * b) + (4.0 * nHat));
            double center = nHat + (0.5 * (b * b));
            return (center + d);
        }

        private static long SpecialNStar(long numSamplesI, double p, double delta)
        {
            double q, numSamplesF;
            double tot, curTerm;
            long m;
            Debug.Assert(numSamplesI >= 1);
            Debug.Assert((0.0 < p) && (p < 1.0));
            Debug.Assert((0.0 < delta) && (delta < 1.0));
            q = 1.0 - p;
            numSamplesF = numSamplesI;
            Debug.Assert((numSamplesF / p) < 500.0);
            curTerm = Math.Pow(p, numSamplesF);
            Debug.Assert(curTerm > 1e-100);
            tot = curTerm;
            m = numSamplesI;
            while (tot <= delta)
            {
                curTerm = (curTerm * q * (m)) / ((m + 1) - numSamplesI);
                tot += curTerm;
                m += 1;
            }
            return (m - 1);
        }

        private static long SpecialNPrimeB(long numSamplesI, double p, double delta)
        {
            double q, numSamplesF, oneMinusDelta;
            double tot, curTerm;
            long m;
            Debug.Assert(numSamplesI >= 1);
            Debug.Assert((0.0 < p) && (p < 1.0));
            Debug.Assert((0.0 < delta) && (delta < 1.0));
            q = 1.0 - p;
            oneMinusDelta = 1.0 - delta;
            numSamplesF = numSamplesI;
            curTerm = Math.Pow(p, numSamplesF);
            Debug.Assert(curTerm > 1e-100);
            tot = curTerm;
            m = numSamplesI;
            while (tot < oneMinusDelta)
            {
                curTerm = (curTerm * q * (m)) / ((m + 1) - numSamplesI);
                tot += curTerm;
                m += 1;
            }
            return (m);
        }

        private static long SpecialNPrimeF(long numSamplesI, double p, double delta)
        {
            Debug.Assert(((numSamplesI) / p) < 500.0);
            return (SpecialNPrimeB(numSamplesI + 1, p, delta));
        }

        private static double ComputeApproxBinoLB(long numSamplesI, double theta, int numSDev)
        {
            if (theta == 1.0)
            {
                return (numSamplesI);
            }
            else if (numSamplesI == 0)
            {
                return (0.0);
            }
            else if (numSamplesI == 1)
            {
                double delta = deltaOfNumSDev[numSDev];
                double rawLB = (Math.Log(1.0 - delta)) / (Math.Log(1.0 - theta));
                return (Math.Floor(rawLB));
            }
            else if (numSamplesI > 120)
            {
                double rawLB = ContClassicLB(numSamplesI, theta, numSDev);
                return (rawLB - 0.5);
            }
            else if (theta > (1.0 - 1e-5))
            {
                return (numSamplesI);
            }
            else if (theta < ((numSamplesI) / 360.0))
            {
                int index;
                double rawLB;
                index = (3 * ((int)numSamplesI)) + (numSDev - 1);
                rawLB = ContClassicLB(numSamplesI, theta, EquivTables.GetLB(index));
                return (rawLB - 0.5);
            }
            else
            {
                double delta = deltaOfNumSDev[numSDev];
                long nstar = SpecialNStar(numSamplesI, theta, delta);
                return (nstar);
            }
        }

        private static double ComputeApproxBinoUB(long numSamplesI, double theta, int numSDev)
        {
            if (theta == 1.0)
            {
                return (numSamplesI);
            }
            else if (numSamplesI == 0)
            {
                double delta = deltaOfNumSDev[numSDev];
                double rawUB = (Math.Log(delta)) / (Math.Log(1.0 - theta));
                return (Math.Ceiling(rawUB));
            }
            else if (numSamplesI > 120)
            {
                double rawUB = ContClassicUB(numSamplesI, theta, numSDev);
                return (rawUB + 0.5);
            }
            else if (theta > (1.0 - 1e-5))
            {
                return (numSamplesI + 1);
            }
            else if (theta < ((numSamplesI) / 360.0))
            {
                int index;
                double rawUB;
                index = (3 * ((int)numSamplesI)) + (numSDev - 1);
                rawUB = ContClassicUB(numSamplesI, theta, EquivTables.GetUB(index));
                return (rawUB + 0.5);
            }
            else
            {
                double delta = deltaOfNumSDev[numSDev];
                long nprimef = SpecialNPrimeF(numSamplesI, theta, delta);
                return (nprimef);
            }
        }

        /// <summary>
        /// Returns the approximate lower bound value
        /// </summary>
        /// <param name="numSamples">the number of samples in the sample set</param>
        /// <param name="theta">the sampling probability</param>
        /// <param name="numSDev">the number of "standard deviations" from the mean for the tail bounds.
        /// This must be an integer value of 1, 2 or 3.</param>
        /// <param name="noDataSeen">this is normally false. However, in the case where you have zero samples
        /// and a theta less than 1.0, this flag enables the distinction between a virgin case when no actual
        /// data has been seen and the case where the estimate may be zero but an upper error bound may
        /// still exist.</param>
        /// <returns>the approximate lower bound value</returns>
        public static double GetLowerBound(long numSamples, double theta, int numSDev, bool noDataSeen)
        {
            if (noDataSeen) { return 0.0; }
            CheckArgs(numSamples, theta, numSDev);
            double lb = ComputeApproxBinoLB(numSamples, theta, numSDev);
            double numSamplesF = numSamples;
            double est = numSamplesF / theta;
            return (Math.Min(est, Math.Max(numSamplesF, lb)));
        }

        /// <summary>
        /// Returns the approximate upper bound value
        /// </summary>
        /// <param name="numSamples">the number of samples in the sample set</param>
        /// <param name="theta">the sampling probability</param>
        /// <param name="numSDev">the number of "standard deviations" from the mean for the tail bounds.
        /// This must be an integer value of 1, 2 or 3.</param>
        /// <param name="noDataSeen">this is normally false. However, in the case where you have zero samples
        /// and a theta less than 1.0, this flag enables the distinction between a virgin case when no actual
        /// data has been seen and the case where the estimate may be zero but an upper error bound may
        /// still exist.</param>
        /// <returns>the approximate upper bound value</returns>
        public static double GetUpperBound(long numSamples, double theta, int numSDev, bool noDataSeen)
        {
            if (noDataSeen) { return 0.0; }
            CheckArgs(numSamples, theta, numSDev);
            double ub = ComputeApproxBinoUB(numSamples, theta, numSDev);
            double numSamplesF = numSamples;
            double est = numSamplesF / theta;
            return (Math.Max(est, ub));
        }

        internal static void CheckArgs(long numSamples, double theta, int numSDev)
        {
            if ((numSDev | (numSDev - 1) | (3 - numSDev) | (int)numSamples) < 0)
            {
                throw new SketchesArgumentException(
                    $"numSDev must only be 1,2, or 3 and numSamples must >= 0: numSDev={numSDev}, numSamples={numSamples}");
            }
            if ((theta < 0.0) || (theta > 1.0))
            {
                throw new SketchesArgumentException($"0.0 < theta <= 1.0: {theta}");
            }
        }
    }
}

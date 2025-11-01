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
using System.Text;
using DataSketches.Common;

namespace DataSketches.Theta
{
    /// <summary>
    /// Abstract base class for all Theta sketches.
    /// </summary>
    public abstract class BaseThetaSketch
    {
        /// <summary>
        /// Returns true if this sketch represents an empty set (not the same as no retained entries).
        /// </summary>
        public abstract bool IsEmpty { get; }

        /// <summary>
        /// Gets the estimate of the distinct count of the input stream.
        /// </summary>
        public double GetEstimate()
        {
            return GetNumRetained() / GetTheta();
        }

        /// <summary>
        /// Returns the approximate lower error bound given a number of standard deviations.
        /// </summary>
        /// <param name="numStdDevs">Number of standard deviations (1, 2, or 3)</param>
        /// <returns>The lower bound</returns>
        public double GetLowerBound(byte numStdDevs)
        {
            if (!IsEstimationMode())
                return GetNumRetained();
            return BinomialBounds.GetLowerBound(GetNumRetained(), GetTheta(), numStdDevs);
        }

        /// <summary>
        /// Returns the approximate upper error bound given a number of standard deviations.
        /// </summary>
        /// <param name="numStdDevs">Number of standard deviations (1, 2, or 3)</param>
        /// <returns>The upper bound</returns>
        public double GetUpperBound(byte numStdDevs)
        {
            if (!IsEstimationMode())
                return GetNumRetained();
            return BinomialBounds.GetUpperBound(GetNumRetained(), GetTheta(), numStdDevs);
        }

        /// <summary>
        /// Returns true if the sketch is in estimation mode (as opposed to exact mode).
        /// </summary>
        public bool IsEstimationMode()
        {
            return GetTheta64() < ThetaConstants.MaxTheta;
        }

        /// <summary>
        /// Gets theta as a fraction from 0 to 1 (effective sampling rate).
        /// </summary>
        public double GetTheta()
        {
            return GetTheta64() / (double)ThetaConstants.MaxTheta;
        }

        /// <summary>
        /// Gets theta as a positive integer between 0 and long.MaxValue.
        /// </summary>
        public abstract ulong GetTheta64();

        /// <summary>
        /// Gets the number of retained entries in the sketch.
        /// </summary>
        public abstract uint GetNumRetained();

        /// <summary>
        /// Gets the hash of the seed that was used to hash the input.
        /// </summary>
        public abstract ushort GetSeedHash();

        /// <summary>
        /// Returns true if retained entries are ordered.
        /// </summary>
        public abstract bool IsOrdered { get; }

        /// <summary>
        /// Provides a human-readable summary of this sketch as a string.
        /// </summary>
        /// <param name="printItems">If true, include the list of items retained by the sketch</param>
        public virtual string ToString(bool printItems = false)
        {
            var sb = new StringBuilder();
            sb.AppendLine("### Theta Sketch Summary ###");
            sb.AppendLine($"  Empty: {IsEmpty}");
            sb.AppendLine($"  Ordered: {IsOrdered}");
            sb.AppendLine($"  Estimation Mode: {IsEstimationMode()}");
            sb.AppendLine($"  Theta (fraction): {GetTheta():F6}");
            sb.AppendLine($"  Theta (raw): {GetTheta64()}");
            sb.AppendLine($"  Num Retained: {GetNumRetained()}");
            sb.AppendLine($"  Estimate: {GetEstimate():F2}");
            sb.AppendLine($"  Lower Bound (95%): {GetLowerBound(2):F2}");
            sb.AppendLine($"  Upper Bound (95%): {GetUpperBound(2):F2}");
            sb.AppendLine($"  Seed Hash: 0x{GetSeedHash():X4}");
            PrintSpecifics(sb);
            if (printItems)
            {
                PrintItems(sb);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Prints sketch-specific details.
        /// </summary>
        protected abstract void PrintSpecifics(StringBuilder sb);

        /// <summary>
        /// Prints retained items.
        /// </summary>
        protected abstract void PrintItems(StringBuilder sb);
    }

    /// <summary>
    /// Base class for Theta sketch with iteration support.
    /// </summary>
    public abstract class ThetaSketch : BaseThetaSketch, IEnumerable<ulong>
    {
        /// <summary>
        /// Gets an enumerator over hash values in this sketch.
        /// </summary>
        public abstract IEnumerator<ulong> GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Prints retained items.
        /// </summary>
        protected override void PrintItems(StringBuilder sb)
        {
            sb.AppendLine("  Retained Entries:");
            int count = 0;
            foreach (var hash in this)
            {
                sb.AppendLine($"    [{count++}]: {hash}");
            }
        }
    }
}

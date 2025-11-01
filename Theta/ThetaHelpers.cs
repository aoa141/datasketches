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

namespace DataSketches.Theta
{
    /// <summary>
    /// Helper utilities for theta sketches.
    /// </summary>
    internal static class ThetaHelpers
    {
        /// <summary>
        /// Checks that a value matches the expected value.
        /// </summary>
        /// <typeparam name="T">The type of value to check</typeparam>
        /// <param name="actual">Actual value</param>
        /// <param name="expected">Expected value</param>
        /// <param name="description">Description of what is being checked</param>
        /// <exception cref="ArgumentException">Thrown when values don't match</exception>
        public static void CheckValue<T>(T actual, T expected, string description) where T : IEquatable<T>
        {
            if (!actual.Equals(expected))
            {
                throw new ArgumentException(
                    $"{description} mismatch: expected {expected}, actual {actual}");
            }
        }
    }

    /// <summary>
    /// Checker utilities for validating sketch data.
    /// </summary>
    public static class Checker
    {
        /// <summary>
        /// Checks the serial version.
        /// </summary>
        /// <param name="actual">Actual version</param>
        /// <param name="expected">Expected version</param>
        /// <exception cref="ArgumentException">Thrown when versions don't match</exception>
        public static void CheckSerialVersion(byte actual, byte expected)
        {
            ThetaHelpers.CheckValue(actual, expected, "serial version");
        }

        /// <summary>
        /// Checks the sketch family.
        /// </summary>
        /// <param name="actual">Actual family</param>
        /// <param name="expected">Expected family</param>
        /// <exception cref="ArgumentException">Thrown when families don't match</exception>
        public static void CheckSketchFamily(byte actual, byte expected)
        {
            ThetaHelpers.CheckValue(actual, expected, "sketch family");
        }

        /// <summary>
        /// Checks the sketch type.
        /// </summary>
        /// <param name="actual">Actual type</param>
        /// <param name="expected">Expected type</param>
        /// <exception cref="ArgumentException">Thrown when types don't match</exception>
        public static void CheckSketchType(byte actual, byte expected)
        {
            ThetaHelpers.CheckValue(actual, expected, "sketch type");
        }

        /// <summary>
        /// Checks the seed hash.
        /// </summary>
        /// <param name="actual">Actual seed hash</param>
        /// <param name="expected">Expected seed hash</param>
        /// <exception cref="ArgumentException">Thrown when seed hashes don't match</exception>
        public static void CheckSeedHash(ushort actual, ushort expected)
        {
            ThetaHelpers.CheckValue(actual, expected, "seed hash");
        }
    }

    /// <summary>
    /// Helper utilities for building theta sketches.
    /// </summary>
    public static class ThetaBuildHelper
    {
        /// <summary>
        /// Computes the starting theta value from a sampling probability.
        /// Avoids multiplication if p == 1 since it might not yield MAX_THETA exactly.
        /// </summary>
        /// <param name="p">Sampling probability (0.0 to 1.0)</param>
        /// <returns>Starting theta value</returns>
        public static ulong StartingThetaFromP(float p)
        {
            if (p < 1.0f)
                return (ulong)((double)ThetaConstants.MaxTheta * p);
            return ThetaConstants.MaxTheta;
        }

        /// <summary>
        /// Computes the starting sub-multiple for hash table sizing.
        /// </summary>
        /// <param name="lgTgt">Log2 of target size</param>
        /// <param name="lgMin">Log2 of minimum size</param>
        /// <param name="lgRf">Log2 of resize factor</param>
        /// <returns>Starting sub-multiple</returns>
        public static byte StartingSubMultiple(byte lgTgt, byte lgMin, byte lgRf)
        {
            return (byte)((lgTgt <= lgMin) ? lgMin : (lgRf == 0) ? lgTgt : ((lgTgt - lgMin) % lgRf) + lgMin);
        }
    }
}

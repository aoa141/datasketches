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

namespace DataSketches.Hll
{
    /// <summary>
    /// Relative error tables for HLL sketch
    /// This is a simplified version - full tables would be larger
    /// </summary>
    internal static class RelativeErrorTables
    {
        // Simplified error tables for lgK <= 12
        // In production, these would be comprehensive empirically-derived tables
        private static readonly double[,] HipRelErrorTable = new double[,]
        {
            // lgK: 4,     5,     6,     7,     8,     9,     10,    11,    12
            { 0.205, 0.155, 0.115, 0.085, 0.065, 0.048, 0.037, 0.028, 0.022 }, // 1 std dev, lower
            { 0.205, 0.155, 0.115, 0.085, 0.065, 0.048, 0.037, 0.028, 0.022 }, // 1 std dev, upper
            { 0.410, 0.310, 0.230, 0.170, 0.130, 0.096, 0.074, 0.056, 0.044 }, // 2 std dev, lower
            { 0.410, 0.310, 0.230, 0.170, 0.130, 0.096, 0.074, 0.056, 0.044 }, // 2 std dev, upper
            { 0.615, 0.465, 0.345, 0.255, 0.195, 0.144, 0.111, 0.084, 0.066 }, // 3 std dev, lower
            { 0.615, 0.465, 0.345, 0.255, 0.195, 0.144, 0.111, 0.084, 0.066 }  // 3 std dev, upper
        };

        private static readonly double[,] NonHipRelErrorTable = new double[,]
        {
            // lgK: 4,     5,     6,     7,     8,     9,     10,    11,    12
            { 0.256, 0.193, 0.143, 0.106, 0.081, 0.060, 0.046, 0.035, 0.027 }, // 1 std dev, lower
            { 0.256, 0.193, 0.143, 0.106, 0.081, 0.060, 0.046, 0.035, 0.027 }, // 1 std dev, upper
            { 0.512, 0.386, 0.286, 0.212, 0.162, 0.120, 0.092, 0.070, 0.054 }, // 2 std dev, lower
            { 0.512, 0.386, 0.286, 0.212, 0.162, 0.120, 0.092, 0.070, 0.054 }, // 2 std dev, upper
            { 0.768, 0.579, 0.429, 0.318, 0.243, 0.180, 0.138, 0.105, 0.081 }, // 3 std dev, lower
            { 0.768, 0.579, 0.429, 0.318, 0.243, 0.180, 0.138, 0.105, 0.081 }  // 3 std dev, upper
        };

        public static double GetRelErr(bool upperBound, bool unioned, byte lgConfigK, byte numStdDev)
        {
            if (lgConfigK < 4 || lgConfigK > 12)
            {
                throw new ArgumentException($"lgConfigK must be between 4 and 12, got {lgConfigK}");
            }

            if (numStdDev < 1 || numStdDev > 3)
            {
                throw new ArgumentException($"numStdDev must be between 1 and 3, got {numStdDev}");
            }

            int lgKIndex = lgConfigK - 4; // Map lgK 4-12 to indices 0-8
            int rowIndex = (numStdDev - 1) * 2 + (upperBound ? 1 : 0);

            var table = unioned ? NonHipRelErrorTable : HipRelErrorTable;

            if (lgKIndex >= table.GetLength(1))
            {
                // Fallback for edge cases
                return 0.05; // 5% default error
            }

            double error = table[rowIndex, lgKIndex];
            return upperBound ? error : -error;
        }
    }
}

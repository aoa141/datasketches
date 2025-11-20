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

namespace Apache.DataSketches.Common
{
    /// <summary>
    /// For the Families that accept this configuration parameter, it controls the size multiple that
    /// affects how fast the internal cache grows, when more space is required.
    /// </summary>
    /// <remarks>
    /// Author: Lee Rhodes
    /// </remarks>
    public enum ResizeFactor
    {
        /// <summary>
        /// Do not resize. Sketch will be configured to full size.
        /// </summary>
        X1 = 0,

        /// <summary>
        /// Resize factor is 2.
        /// </summary>
        X2 = 1,

        /// <summary>
        /// Resize factor is 4.
        /// </summary>
        X4 = 2,

        /// <summary>
        /// Resize factor is 8.
        /// </summary>
        X8 = 3
    }

    /// <summary>
    /// Extension methods for ResizeFactor enum
    /// </summary>
    public static class ResizeFactorExtensions
    {
        /// <summary>
        /// Returns the Log-base 2 of the Resize Factor
        /// </summary>
        /// <param name="rf">The resize factor</param>
        /// <returns>The Log-base 2 of the Resize Factor</returns>
        public static int Lg(this ResizeFactor rf)
        {
            return (int)rf;
        }

        /// <summary>
        /// Returns the Log-base 2 of the Resize Factor (alias for Lg)
        /// </summary>
        /// <param name="rf">The resize factor</param>
        /// <returns>The Log-base 2 of the Resize Factor</returns>
        public static int GetLg(this ResizeFactor rf)
        {
            return (int)rf;
        }

        /// <summary>
        /// Returns the Resize Factor given the Log-base 2 of the Resize Factor
        /// </summary>
        /// <param name="lg">A value between zero and 3, inclusive.</param>
        /// <returns>The Resize Factor given the Log-base 2 of the Resize Factor</returns>
        public static ResizeFactor GetRF(int lg)
        {
            return lg switch
            {
                0 => ResizeFactor.X1,
                1 => ResizeFactor.X2,
                2 => ResizeFactor.X4,
                _ => ResizeFactor.X8
            };
        }

        /// <summary>
        /// Returns the Resize Factor value
        /// </summary>
        /// <param name="rf">The resize factor</param>
        /// <returns>The Resize Factor value</returns>
        public static int GetValue(this ResizeFactor rf)
        {
            return 1 << (int)rf;
        }
    }
}

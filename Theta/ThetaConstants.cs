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

using DataSketches.Common;

namespace DataSketches.Theta
{
    /// <summary>
    /// Constants used by Theta sketches.
    /// </summary>
    public static class ThetaConstants
    {
        /// <summary>
        /// Maximum theta value - signed max for compatibility with Java.
        /// </summary>
        public const ulong MaxTheta = long.MaxValue;

        /// <summary>
        /// Minimum log2 of K (minimum sketch size).
        /// </summary>
        public const byte MinLgK = 5;

        /// <summary>
        /// Maximum log2 of K (maximum sketch size).
        /// </summary>
        public const byte MaxLgK = 26;

        /// <summary>
        /// Default log2 of K (default sketch size = 4096).
        /// </summary>
        public const byte DefaultLgK = 12;

        /// <summary>
        /// Default resize factor for hash tables.
        /// </summary>
        public const CommonDefs.ResizeFactor DefaultResizeFactor = CommonDefs.ResizeFactor.X8;
    }
}

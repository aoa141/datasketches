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

namespace DataSketches.Common
{
    /// <summary>
    /// Utility to compute ceiling power of 2.
    /// </summary>
    public static class CeilingPowerOf2
    {
        /// <summary>
        /// Computes the next highest power of 2 of a 32-bit unsigned integer.
        /// Algorithm taken from https://graphics.stanford.edu/~seander/bithacks.html
        /// </summary>
        /// <param name="n">Input value</param>
        /// <returns>Next power of 2 greater than or equal to n</returns>
        public static uint Compute(uint n)
        {
            --n;
            n |= n >> 1;
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;
            return ++n;
        }
    }
}

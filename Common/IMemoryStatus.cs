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
    /// Methods for inquiring the status of a backing Memory object.
    /// </summary>
    public interface IMemoryStatus
    {
        /// <summary>
        /// Returns true if this object's internal data is backed by a Memory object,
        /// which may be on-heap or off-heap.
        /// </summary>
        /// <returns>True if this object's internal data is backed by a Memory object.</returns>
        bool HasMemory() => false;

        /// <summary>
        /// Returns true if this object's internal data is backed by direct (off-heap) Memory.
        /// </summary>
        /// <returns>True if this object's internal data is backed by direct (off-heap) Memory.</returns>
        bool IsDirect() => false;

        /// <summary>
        /// Returns true if the backing resource of this is identical with the backing resource
        /// of that. The capacities must be the same. If this is a region,
        /// the region offset must also be the same.
        /// </summary>
        /// <param name="that">A different non-null Memory object.</param>
        /// <returns>True if the backing resource of this is identical with the backing resource of that.</returns>
        /// <exception cref="SketchesArgumentException">If that is not valid (already disposed).</exception>
        bool IsSameResource(Memory<byte> that) => false;
    }
}

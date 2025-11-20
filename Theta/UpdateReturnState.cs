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

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Update Return State enum for tracking sketch update operations
    /// </summary>
    public enum UpdateReturnState
    {
        /// <summary>
        /// The hash was accepted into the sketch and the retained count was incremented.
        /// </summary>
        InsertedCountIncremented, //all UpdateSketches

        /// <summary>
        /// The hash was accepted into the sketch, the retained count was incremented.
        /// The current cache was out of room and resized larger based on the Resize Factor.
        /// </summary>
        InsertedCountIncrementedResized, //used by HeapQuickSelectSketch

        /// <summary>
        /// The hash was accepted into the sketch, the retained count was incremented.
        /// The current cache was out of room and at maximum size, so the cache was rebuilt.
        /// </summary>
        InsertedCountIncrementedRebuilt, //used by HeapQuickSelectSketch

        /// <summary>
        /// The hash was accepted into the sketch and the retained count was not incremented.
        /// </summary>
        InsertedCountNotIncremented, //used by enhancedHashInsert for Alpha

        /// <summary>
        /// The hash was inserted into the local concurrent buffer,
        /// but has not yet been propagated to the concurrent shared sketch.
        /// </summary>
        ConcurrentBufferInserted, //used by ConcurrentHeapThetaBuffer

        /// <summary>
        /// The hash has been propagated to the concurrent shared sketch.
        /// This does not reflect the action taken by the shared sketch.
        /// </summary>
        ConcurrentPropagated,  //used by ConcurrentHeapThetaBuffer

        /// <summary>
        /// The hash was rejected as a duplicate.
        /// </summary>
        RejectedDuplicate, //all UpdateSketches hashUpdate(), enhancedHashInsert

        /// <summary>
        /// The hash was rejected because it was null or empty.
        /// </summary>
        RejectedNullOrEmpty, //UpdateSketch.update(arr[])

        /// <summary>
        /// The hash was rejected because the value was negative, zero or
        /// greater than theta.
        /// </summary>
        RejectedOverTheta //all UpdateSketches.hashUpdate()
    }
}

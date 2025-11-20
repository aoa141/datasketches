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
using Apache.DataSketches.Common;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// Computes a set difference, A-AND-NOT-B, of two theta sketches.
    /// This class includes both stateful and stateless operations.
    ///
    /// The stateful operation is as follows:
    /// <code>
    /// AnotB anotb = SetOperation.Builder().BuildANotB();
    ///
    /// anotb.SetA(Sketch skA); // The first argument.
    /// anotb.NotB(Sketch skB); // The second (subtraction) argument.
    /// anotb.NotB(Sketch skC); // ...any number of additional subtractions...
    /// anotb.GetResult(false); // Get an interim result.
    /// anotb.NotB(Sketch skD); // Additional subtractions.
    /// anotb.GetResult(true);  // Final result and resets the AnotB operator.
    /// </code>
    ///
    /// The stateless operation is as follows:
    /// <code>
    /// AnotB anotb = SetOperation.Builder().BuildANotB();
    ///
    /// CompactSketch csk = anotb.ANotB(Sketch skA, Sketch skB);
    /// </code>
    ///
    /// Calling the SetA operation a second time essentially clears the internal state and loads the new sketch.
    ///
    /// The stateless and stateful operations are independent of each other with the exception of
    /// sharing the same update hash seed loaded as the default seed or specified by the user as an
    /// argument to the builder.
    /// </summary>
    public abstract class AnotB : SetOperation
    {
        public override Family GetFamily()
        {
            return Family.A_NOT_B;
        }

        /// <summary>
        /// This is part of a multistep, stateful AnotB operation and sets the given Theta sketch as the
        /// first argument A of A-AND-NOT-B. This overwrites the internal state of this
        /// AnotB operator with the contents of the given sketch.
        /// This sets the stage for multiple following NotB steps.
        ///
        /// An input argument of null will throw an exception.
        ///
        /// Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
        /// That is distinctly different from the C# null, which represents a nonexistent object.
        /// In most cases it is a programming error due to some object that was not properly initialized.
        /// With a null as the first argument, we cannot know what the user's intent is.
        /// Since it is very likely that a null is a programming error, we throw an exception.
        ///
        /// An empty input argument will set the internal state to empty.
        ///
        /// Rationale: An empty set is a mathematically legal concept. Although it makes any subsequent,
        /// valid argument for B irrelevant, we must allow this and assume the user knows what they are doing.
        ///
        /// Performing GetResult(boolean) just after this step will return a compact form of the given argument.
        /// </summary>
        /// <param name="skA">The incoming sketch for the first argument, A.</param>
        public abstract void SetA(Sketch skA);

        /// <summary>
        /// This is part of a multistep, stateful AnotB operation and sets the given Theta sketch as the
        /// second (or n+1th) argument B of A-AND-NOT-B.
        /// Performs an AND NOT operation with the existing internal state of this AnotB operator.
        ///
        /// An input argument of null or empty is ignored.
        ///
        /// Rationale: A null for the second or following arguments is more tolerable because
        /// A NOT null is still A even if we don't know exactly what the null represents. It
        /// clearly does not have any content that overlaps with A. Also, because this can be part of
        /// a multistep operation with multiple NotB steps. Other following steps can still produce
        /// a valid result.
        ///
        /// Use GetResult(boolean) to obtain the result.
        /// </summary>
        /// <param name="skB">The incoming Theta sketch for the second (or following) argument B.</param>
        public abstract void NotB(Sketch skB);

        /// <summary>
        /// Gets the result of the multistep, stateful operation AnotB that have been executed with calls
        /// to SetA(Sketch) and NotB(Sketch).
        /// </summary>
        /// <param name="reset">If true, clears this operator to the empty state after this result is
        /// returned. Set this to false if you wish to obtain an intermediate result.</param>
        /// <returns>The result of this operation as an ordered, on-heap CompactSketch.</returns>
        public abstract CompactSketch GetResult(bool reset);

        /// <summary>
        /// Gets the result of the multistep, stateful operation AnotB that have been executed with calls
        /// to SetA(Sketch) and NotB(Sketch).
        /// </summary>
        /// <param name="dstOrdered">If true, the result will be an ordered CompactSketch.</param>
        /// <param name="dstMem">If not null, the given Memory will be the target location of the result.</param>
        /// <param name="reset">If true, clears this operator to the empty state after this result is
        /// returned. Set this to false if you wish to obtain an intermediate result.</param>
        /// <returns>The result of this operation as a CompactSketch in the given dstMem.</returns>
        public abstract CompactSketch GetResult(bool dstOrdered, Memory<byte>? dstMem, bool reset);

        /// <summary>
        /// Perform A-and-not-B set operation on the two given sketches and return the result as an
        /// ordered CompactSketch on the heap.
        ///
        /// This a stateless operation and has no impact on the internal state of this operator.
        /// Thus, this is not an accumulating update and does not interact with the SetA(Sketch),
        /// NotB(Sketch), GetResult(boolean), or GetResult(boolean, Memory, boolean) methods.
        ///
        /// If either argument is null an exception is thrown.
        ///
        /// Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
        /// That is distinctly different from the C# null, which represents a nonexistent object.
        /// In most cases null is a programming error due to a non-initialized object.
        ///
        /// With a null as the first argument we cannot know what the user's intent is and throw an
        /// exception. With a null as the second argument for this method we must return a result and
        /// there is no following possible viable arguments for the second argument so we throw an exception.
        /// </summary>
        /// <param name="skA">The incoming sketch for the first argument. It must not be null.</param>
        /// <param name="skB">The incoming sketch for the second argument. It must not be null.</param>
        /// <returns>An ordered CompactSketch on the heap</returns>
        public CompactSketch ANotB(Sketch skA, Sketch skB)
        {
            return ANotB(skA, skB, true, null);
        }

        /// <summary>
        /// Perform A-and-not-B set operation on the two given sketches and return the result as a CompactSketch.
        ///
        /// This a stateless operation and has no impact on the internal state of this operator.
        /// Thus, this is not an accumulating update and does not interact with the SetA(Sketch),
        /// NotB(Sketch), GetResult(boolean), or GetResult(boolean, Memory, boolean) methods.
        ///
        /// If either argument is null an exception is thrown.
        ///
        /// Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
        /// That is distinctly different from the C# null, which represents a nonexistent object.
        /// In most cases null is a programming error due to a non-initialized object.
        ///
        /// With a null as the first argument we cannot know what the user's intent is and throw an
        /// exception. With a null as the second argument for this method we must return a result and
        /// there is no following possible viable arguments for the second argument so we throw an exception.
        /// </summary>
        /// <param name="skA">The incoming sketch for the first argument. It must not be null.</param>
        /// <param name="skB">The incoming sketch for the second argument. It must not be null.</param>
        /// <param name="dstOrdered">If true, the result will be ordered</param>
        /// <param name="dstMem">If not null, the result will be stored in this Memory</param>
        /// <returns>The result as a CompactSketch.</returns>
        public abstract CompactSketch ANotB(Sketch skA, Sketch skB, bool dstOrdered,
            Memory<byte>? dstMem);
    }
}

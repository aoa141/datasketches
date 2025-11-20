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
    /// Compute the union of two or more theta sketches.
    /// A new instance represents an empty set.
    /// </summary>
    public abstract class Union : SetOperation
    {
        /// <summary>
        /// Returns the number of storage bytes required for this union in its current state.
        /// </summary>
        /// <returns>The number of storage bytes required for this union in its current state.</returns>
        public abstract int GetCurrentBytes();

        /// <summary>
        /// Gets the Family of this Union
        /// </summary>
        public override Family GetFamily()
        {
            return Family.UNION;
        }

        /// <summary>
        /// Returns the maximum required storage bytes for this union.
        /// </summary>
        /// <returns>The maximum required storage bytes for this union.</returns>
        public abstract int GetMaxUnionBytes();

        /// <summary>
        /// Gets the result of this operation as an ordered CompactSketch on the heap.
        /// This does not disturb the underlying data structure of the union.
        /// Therefore, it is OK to continue updating the union after this operation.
        /// </summary>
        /// <returns>The result of this operation as an ordered CompactSketch on the heap</returns>
        public abstract CompactSketch GetResult();

        /// <summary>
        /// Gets the result of this operation as a CompactSketch of the chosen form.
        /// This does not disturb the underlying data structure of the union.
        /// Therefore, it is OK to continue updating the union after this operation.
        /// </summary>
        /// <param name="dstOrdered">If true, the result will be ordered</param>
        /// <param name="dstMem">If not null, the result will be placed in this Memory</param>
        /// <returns>The result of this operation as a CompactSketch of the chosen form</returns>
        public abstract CompactSketch GetResult(bool dstOrdered, Memory<byte>? dstMem);

        /// <summary>
        /// Resets this Union. The seed remains intact, everything else reverts back to its virgin state.
        /// </summary>
        public abstract void Reset();

        /// <summary>
        /// Returns a byte array image of this Union object
        /// </summary>
        /// <returns>A byte array image of this Union object</returns>
        public abstract byte[] ToByteArray();

        /// <summary>
        /// This implements a stateless, pair-wise union operation. The returned sketch will be cut back to
        /// the smaller of the two k values if required.
        /// Nulls and empty sketches are ignored.
        /// </summary>
        /// <param name="sketchA">The first argument</param>
        /// <param name="sketchB">The second argument</param>
        /// <returns>The result ordered CompactSketch on the heap.</returns>
        public CompactSketch Combine(Sketch sketchA, Sketch sketchB)
        {
            return Combine(sketchA, sketchB, true, null);
        }

        /// <summary>
        /// This implements a stateless, pair-wise union operation. The returned sketch will be cut back to
        /// k if required, similar to the regular Union operation.
        /// Nulls and empty sketches are ignored.
        /// </summary>
        /// <param name="sketchA">The first argument</param>
        /// <param name="sketchB">The second argument</param>
        /// <param name="dstOrdered">If true, the returned CompactSketch will be ordered.</param>
        /// <param name="dstMem">If not null, the returned CompactSketch will be placed in this Memory.</param>
        /// <returns>The result CompactSketch.</returns>
        public abstract CompactSketch Combine(Sketch sketchA, Sketch sketchB, bool dstOrdered,
            Memory<byte>? dstMem);

        /// <summary>
        /// Perform a Union operation with this union and the given on-heap sketch of the Theta Family.
        /// This method is not valid for the older SetSketch, which was prior to Open Source (August, 2015).
        /// This method can be repeatedly called.
        /// Nulls and empty sketches are ignored.
        /// </summary>
        /// <param name="sketchIn">The incoming sketch.</param>
        public abstract void Update(Sketch sketchIn);

        /// <summary>
        /// Perform a Union operation with this union and the given Memory image of any sketch of the
        /// Theta Family. The input image may be from earlier versions of the Theta Compact Sketch,
        /// called the SetSketch (circa 2014), which was prior to Open Source and are compact and ordered.
        /// This method can be repeatedly called.
        /// Nulls and empty sketches are ignored.
        /// </summary>
        /// <param name="mem">Memory image of sketch to be merged</param>
        public abstract void Update(Memory<byte> mem);

        /// <summary>
        /// Update this union with the given long data item.
        /// </summary>
        /// <param name="datum">The given long datum.</param>
        public abstract void Update(long datum);

        /// <summary>
        /// Update this union with the given double (or float) data item.
        /// The double will be converted to a long using BitConverter.DoubleToInt64Bits,
        /// which normalizes all NaN values to a single NaN representation.
        /// Plus and minus zero will be normalized to plus zero.
        /// Each of the special floating-point values NaN and +/- Infinity are treated as distinct.
        /// </summary>
        /// <param name="datum">The given double datum.</param>
        public abstract void Update(double datum);

        /// <summary>
        /// Update this union with the given String data item.
        /// The string is converted to a byte array using UTF8 encoding.
        /// If the string is null or empty no update attempt is made and the method returns.
        /// Note: this will not produce the same output hash values as the Update(char[]) method
        /// and will generally be a little slower depending on the complexity of the UTF8 encoding.
        /// Note: this is not a Sketch Union operation. This treats the given string as a data item.
        /// </summary>
        /// <param name="datum">The given String.</param>
        public abstract void Update(string datum);

        /// <summary>
        /// Update this union with the given byte array item.
        /// If the byte array is null or empty no update attempt is made and the method returns.
        /// Note: this is not a Sketch Union operation. This treats the given byte array as a data item.
        /// </summary>
        /// <param name="data">The given byte array.</param>
        public abstract void Update(byte[] data);

        /// <summary>
        /// Update this union with the given byte span item.
        /// If the byte span is empty no update attempt is made and the method returns.
        /// Note: this is not a Sketch Union operation. This treats the given byte span as a data item.
        /// </summary>
        /// <param name="data">The given byte span.</param>
        public abstract void Update(ReadOnlySpan<byte> data);

        /// <summary>
        /// Update this union with the given integer array item.
        /// If the integer array is null or empty no update attempt is made and the method returns.
        /// Note: this is not a Sketch Union operation. This treats the given integer array as a data item.
        /// </summary>
        /// <param name="data">The given int array.</param>
        public abstract void Update(int[] data);

        /// <summary>
        /// Update this union with the given char array item.
        /// If the char array is null or empty no update attempt is made and the method returns.
        /// Note: this will not produce the same output hash values as the Update(String) method
        /// but will be a little faster as it avoids the complexity of the UTF8 encoding.
        /// Note: this is not a Sketch Union operation. This treats the given char array as a data item.
        /// </summary>
        /// <param name="data">The given char array.</param>
        public abstract void Update(char[] data);

        /// <summary>
        /// Update this union with the given long array item.
        /// If the long array is null or empty no update attempt is made and the method returns.
        /// Note: this is not a Sketch Union operation. This treats the given long array as a data item.
        /// </summary>
        /// <param name="data">The given long array.</param>
        public abstract void Update(long[] data);
    }
}

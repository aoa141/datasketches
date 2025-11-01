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
using System.Runtime.InteropServices;

namespace DataSketches.Common
{
    /// <summary>
    /// Provides memory operation utilities for copying data.
    /// </summary>
    public static class MemoryOperations
    {
        /// <summary>
        /// Ensures that sufficient memory is available.
        /// </summary>
        /// <param name="bytesAvailable">Number of bytes available</param>
        /// <param name="minNeeded">Minimum number of bytes needed</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when insufficient memory is available</exception>
        public static void EnsureMinimumMemory(long bytesAvailable, long minNeeded)
        {
            if (bytesAvailable < minNeeded)
            {
                throw new ArgumentOutOfRangeException(
                    $"Insufficient buffer size detected: bytes available {bytesAvailable}, minimum needed {minNeeded}");
            }
        }

        /// <summary>
        /// Checks that a requested index is within capacity bounds.
        /// </summary>
        /// <param name="requestedIndex">The index being accessed</param>
        /// <param name="capacity">The maximum capacity</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when index exceeds capacity</exception>
        public static void CheckMemorySize(long requestedIndex, long capacity)
        {
            if (requestedIndex > capacity)
            {
                throw new ArgumentOutOfRangeException(
                    $"Attempt to access memory beyond limits: requested index {requestedIndex}, capacity {capacity}");
            }
        }

        /// <summary>
        /// Copies data from source to destination.
        /// </summary>
        /// <param name="src">Source span</param>
        /// <param name="dst">Destination span</param>
        /// <returns>Number of bytes copied</returns>
        public static int CopyFromMemory(ReadOnlySpan<byte> src, Span<byte> dst)
        {
            src.CopyTo(dst);
            return src.Length;
        }

        /// <summary>
        /// Copies data from source to destination.
        /// </summary>
        /// <param name="src">Source span</param>
        /// <param name="dst">Destination span</param>
        /// <returns>Number of bytes copied</returns>
        public static int CopyToMemory(ReadOnlySpan<byte> src, Span<byte> dst)
        {
            src.CopyTo(dst);
            return src.Length;
        }

        /// <summary>
        /// Copies a value from memory span.
        /// </summary>
        /// <typeparam name="T">The type to copy</typeparam>
        /// <param name="src">Source span</param>
        /// <param name="item">Output item</param>
        /// <returns>Number of bytes copied</returns>
        public static int CopyFromMemory<T>(ReadOnlySpan<byte> src, out T item) where T : struct
        {
            item = MemoryMarshal.Read<T>(src);
            return Marshal.SizeOf<T>();
        }

        /// <summary>
        /// Copies a value to memory span.
        /// </summary>
        /// <typeparam name="T">The type to copy</typeparam>
        /// <param name="item">Item to copy</param>
        /// <param name="dst">Destination span</param>
        /// <returns>Number of bytes copied</returns>
        public static int CopyToMemory<T>(T item, Span<byte> dst) where T : struct
        {
            MemoryMarshal.Write(dst, ref item);
            return Marshal.SizeOf<T>();
        }
    }
}

// <copyright file="MemoryOperations.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

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

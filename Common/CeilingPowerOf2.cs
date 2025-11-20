// <copyright file="CeilingPowerOf2.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

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

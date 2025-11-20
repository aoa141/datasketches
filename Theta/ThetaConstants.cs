// <copyright file="ThetaConstants.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

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

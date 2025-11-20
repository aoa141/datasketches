// <copyright file="TargetHllType.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

namespace DataSketches.Hll
{
    /// <summary>
    /// Specifies the target type of HLL sketch to be created. It is a target in that the actual
    /// allocation of the HLL array is deferred until sufficient number of items have been received by
    /// the warm-up phases.
    ///
    /// <para>These three target types are isomorphic representations of the same underlying HLL algorithm.
    /// Thus, given the same value of lgConfigK and the same input, all three HLL target types
    /// will produce identical estimates and have identical error distributions.</para>
    ///
    /// <para>The memory (and also the serialization) of the sketch during this early warmup phase starts
    /// out very small (8 bytes, when empty) and then grows in increments of 4 bytes as required
    /// until the full HLL array is allocated. This transition point occurs at about 10% of K for
    /// sketches where lgConfigK is greater than 8.</para>
    /// </summary>
    public enum TargetHllType
    {
        /// <summary>
        /// 4 bits per entry (most compact, size may vary)
        /// This uses a 4-bit field per HLL bucket and for large counts may require
        /// the use of a small internal auxiliary array for storing statistical exceptions, which are rare.
        /// For the values of lgConfigK greater than 13 (K = 8192),
        /// this additional array adds about 3% to the overall storage. It is generally the slowest in
        /// terms of update time, but has the smallest storage footprint of about
        /// K/2 * 1.03 bytes.
        /// </summary>
        Hll4 = 0,

        /// <summary>
        /// 6 bits per entry (fixed size)
        /// This uses a 6-bit field per HLL bucket. It is generally the next fastest
        /// in terms of update time with a storage footprint of about 3/4 * K bytes.
        /// </summary>
        Hll6 = 1,

        /// <summary>
        /// 8 bits per entry (fastest, fixed size)
        /// This uses an 8-bit byte per HLL bucket. It is generally the
        /// fastest in terms of update time, but has the largest storage footprint of about
        /// K bytes.
        /// </summary>
        Hll8 = 2
    }
}

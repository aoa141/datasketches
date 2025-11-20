// <copyright file="HllSketch.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.IO;
using System.Text;
using DataSketches.Common;

namespace DataSketches.Hll
{
    /// <summary>
    /// This is a high performance implementation of Phillipe Flajolet's HLL sketch but with
    /// significantly improved error behavior. If the ONLY use case for sketching is counting
    /// uniques and merging, the HLL sketch is a reasonable choice, although the highest performing in terms of accuracy for
    /// storage space consumed is CPC (Compressed Probabilistic Counting). For large enough counts, this HLL version (with Hll4) can be 2 to
    /// 16 times smaller than the Theta sketch family for the same accuracy.
    ///
    /// <para>This implementation offers three different types of HLL sketch, each with different
    /// trade-offs with accuracy, space and performance. These types are specified with the
    /// TargetHllType parameter.</para>
    ///
    /// <para>In terms of accuracy, all three types, for the same lgConfigK, have the same error
    /// distribution as a function of n, the number of unique values fed to the sketch.
    /// The configuration parameter lgConfigK is the log-base-2 of K,
    /// where K is the number of buckets or slots for the sketch.</para>
    ///
    /// <para>During warmup, when the sketch has only received a small number of unique items
    /// (up to about 10% of K), this implementation leverages a new class of estimator
    /// algorithms with significantly better accuracy.</para>
    /// </summary>
    public class HllSketch
    {
        private HllSketchImpl _impl;

        /// <summary>
        /// Constructs a new HLL sketch.
        /// </summary>
        /// <param name="lgConfigK">Sketch can hold 2^lgConfigK rows (must be 4-21)</param>
        /// <param name="tgtType">The HLL mode to use, if/when the sketch reaches that state</param>
        /// <param name="startFullSize">Indicates whether to start in HLL mode,
        /// keeping memory use constant (if Hll6 or Hll8) at the cost of
        /// starting out using much more memory</param>
        public HllSketch(byte lgConfigK, TargetHllType tgtType = TargetHllType.Hll4, bool startFullSize = false)
        {
            HllUtil.CheckLgK(lgConfigK);
            _impl = HllSketchImplFactory.NewInstance(lgConfigK, tgtType, startFullSize);
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public HllSketch(HllSketch that)
        {
            _impl = that._impl.Copy();
        }

        /// <summary>
        /// Copy constructor to a new target type
        /// </summary>
        public HllSketch(HllSketch that, TargetHllType tgtType)
        {
            _impl = that._impl.CopyAs(tgtType);
        }

        private HllSketch(HllSketchImpl impl)
        {
            _impl = impl;
        }

        /// <summary>
        /// Reconstructs a sketch from a serialized binary stream.
        /// </summary>
        public static HllSketch Deserialize(Stream stream)
        {
            var impl = HllSketchImplFactory.Deserialize(stream);
            return new HllSketch(impl);
        }

        /// <summary>
        /// Reconstructs a sketch from a serialized byte array.
        /// </summary>
        public static HllSketch Deserialize(byte[] bytes)
        {
            using var stream = new MemoryStream(bytes);
            return Deserialize(stream);
        }

        /// <summary>
        /// Resets the sketch to an empty state in coupon collection mode.
        /// </summary>
        public void Reset()
        {
            _impl = _impl.Reset();
        }

        /// <summary>
        /// Serializes the sketch to a byte array, compacting data structures
        /// where feasible to eliminate unused storage in the serialized image.
        /// </summary>
        public byte[] SerializeCompact(int headerSizeBytes = 0)
        {
            return _impl.Serialize(true, headerSizeBytes);
        }

        /// <summary>
        /// Serializes the sketch to a byte array, retaining all internal
        /// data structures in their current form.
        /// </summary>
        public byte[] SerializeUpdatable()
        {
            return _impl.Serialize(false, 0);
        }

        /// <summary>
        /// Serializes the sketch to a stream, compacting data structures
        /// where feasible to eliminate unused storage in the serialized image.
        /// </summary>
        public void SerializeCompact(Stream stream)
        {
            _impl.Serialize(stream, true);
        }

        /// <summary>
        /// Serializes the sketch to a stream, retaining all internal data
        /// structures in their current form.
        /// </summary>
        public void SerializeUpdatable(Stream stream)
        {
            _impl.Serialize(stream, false);
        }

        /// <summary>
        /// Present the given string as a potential unique item.
        /// </summary>
        public void Update(string datum)
        {
            if (string.IsNullOrEmpty(datum))
                return;
            Update(Encoding.UTF8.GetBytes(datum));
        }

        /// <summary>
        /// Present the given unsigned 64-bit integer as a potential unique item.
        /// </summary>
        public void Update(ulong datum)
        {
            var bytes = BitConverter.GetBytes(datum);
            Update(bytes);
        }

        /// <summary>
        /// Present the given unsigned 32-bit integer as a potential unique item.
        /// </summary>
        public void Update(uint datum)
        {
            Update((ulong)datum);
        }

        /// <summary>
        /// Present the given unsigned 16-bit integer as a potential unique item.
        /// </summary>
        public void Update(ushort datum)
        {
            Update((ulong)datum);
        }

        /// <summary>
        /// Present the given unsigned 8-bit integer as a potential unique item.
        /// </summary>
        public void Update(byte datum)
        {
            Update((ulong)datum);
        }

        /// <summary>
        /// Present the given signed 64-bit integer as a potential unique item.
        /// </summary>
        public void Update(long datum)
        {
            Update((ulong)datum);
        }

        /// <summary>
        /// Present the given signed 32-bit integer as a potential unique item.
        /// </summary>
        public void Update(int datum)
        {
            Update((ulong)(long)datum);
        }

        /// <summary>
        /// Present the given signed 16-bit integer as a potential unique item.
        /// </summary>
        public void Update(short datum)
        {
            Update((ulong)(long)datum);
        }

        /// <summary>
        /// Present the given signed 8-bit integer as a potential unique item.
        /// </summary>
        public void Update(sbyte datum)
        {
            Update((ulong)(long)datum);
        }

        /// <summary>
        /// Present the given 64-bit floating point value as a potential unique item.
        /// </summary>
        public void Update(double datum)
        {
            var bytes = BitConverter.GetBytes(datum);
            Update(bytes);
        }

        /// <summary>
        /// Present the given 32-bit floating point value as a potential unique item.
        /// </summary>
        public void Update(float datum)
        {
            var bytes = BitConverter.GetBytes(datum);
            Update(bytes);
        }

        /// <summary>
        /// Present the given data array as a potential unique item.
        /// </summary>
        public void Update(byte[] data)
        {
            if (data == null || data.Length == 0)
                return;

            var hashState = HllUtil.Hash(data);
            var coupon = HllUtil.Coupon(hashState);
            _impl = _impl.CouponUpdate(coupon);
        }

        /// <summary>
        /// Returns the current cardinality estimate
        /// </summary>
        public double GetEstimate()
        {
            return _impl.GetEstimate();
        }

        /// <summary>
        /// This is less accurate than the GetEstimate() method
        /// and is automatically used when the sketch has gone through
        /// union operations where the more accurate HIP estimator cannot
        /// be used.
        /// </summary>
        public double GetCompositeEstimate()
        {
            return _impl.GetCompositeEstimate();
        }

        /// <summary>
        /// Returns the approximate lower error bound given the specified
        /// number of standard deviations.
        /// </summary>
        /// <param name="numStdDev">Number of standard deviations, an integer from the set {1, 2, 3}.</param>
        public double GetLowerBound(byte numStdDev)
        {
            return _impl.GetLowerBound(numStdDev);
        }

        /// <summary>
        /// Returns the approximate upper error bound given the specified
        /// number of standard deviations.
        /// </summary>
        /// <param name="numStdDev">Number of standard deviations, an integer from the set {1, 2, 3}.</param>
        public double GetUpperBound(byte numStdDev)
        {
            return _impl.GetUpperBound(numStdDev);
        }

        /// <summary>
        /// Returns sketch's configured lgK value.
        /// </summary>
        public byte GetLgConfigK()
        {
            return _impl.LgConfigK;
        }

        /// <summary>
        /// Returns the sketch's target HLL mode.
        /// </summary>
        public TargetHllType GetTargetType()
        {
            return _impl.TgtHllType;
        }

        /// <summary>
        /// Indicates if the sketch is currently stored compacted.
        /// </summary>
        public bool IsCompact()
        {
            return _impl.IsCompact();
        }

        /// <summary>
        /// Indicates if the sketch is currently empty.
        /// </summary>
        public bool IsEmpty()
        {
            return _impl.IsEmpty();
        }

        /// <summary>
        /// Returns the size of the sketch serialized in compact form.
        /// </summary>
        public int GetCompactSerializationBytes()
        {
            return (int)_impl.GetCompactSerializationBytes();
        }

        /// <summary>
        /// Returns the size of the sketch serialized without compaction.
        /// </summary>
        public int GetUpdatableSerializationBytes()
        {
            return (int)_impl.GetUpdatableSerializationBytes();
        }

        /// <summary>
        /// Returns the maximum size in bytes that this sketch can grow to
        /// given lgConfigK. However, for the Hll4 sketch type, this
        /// value can be exceeded in extremely rare cases. If exceeded, it
        /// will be larger by only a few percent.
        /// </summary>
        public static int GetMaxUpdatableSerializationBytes(byte lgK, TargetHllType tgtType)
        {
            HllUtil.CheckLgK(lgK);
            int arrBytes = HllArray.HllArrBytes(tgtType, lgK);
            int auxBytes = (tgtType == TargetHllType.Hll4) ? (4 << HllConstants.LgAuxArrInts[lgK]) : 0;
            return HllConstants.HllByteArrStart + arrBytes + auxBytes;
        }

        /// <summary>
        /// Gets the current (approximate) Relative Error (RE) asymptotic values given several
        /// parameters. This is used primarily for testing.
        /// </summary>
        public static double GetRelErr(bool upperBound, bool unioned, byte lgConfigK, byte numStdDev)
        {
            return HllUtil.GetRelErr(upperBound, unioned, lgConfigK, numStdDev);
        }

        /// <summary>
        /// Returns a human-readable string representation of the sketch
        /// </summary>
        public override string ToString()
        {
            return _impl.ToString();
        }
    }
}

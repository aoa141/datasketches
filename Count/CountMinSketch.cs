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
using System.IO;
using System.Text;

using System.Numerics;
using DataSketches.Common;
namespace DataSketches.Count
{
    /// <summary>
    /// C# implementation of the CountMin sketch data structure of Cormode and Muthukrishnan.
    /// See: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
    /// </summary>
    /// <typeparam name="T">The weight type (must be numeric)</typeparam>
    public class CountMinSketch<T> where T : struct, IComparable<T>, INumber<T>
    {
        private const byte SerialVersion = 1;
        private const byte FamilyId = 18;
        private const byte PreambleShortLength = 2;
        private const byte PreambleFullLength = 3;

        private readonly byte _numHashes;
        private readonly uint _numBuckets;
        private readonly T[] _sketchArray;
        private readonly ulong _seed;
        private T _totalWeight;
        private readonly ulong[] _hashSeeds;

        /// <summary>
        /// Creates an instance of the sketch given parameters.
        /// </summary>
        /// <param name="numHashes">Number of hash functions (rows in the array)</param>
        /// <param name="numBuckets">Number of buckets (columns in the array)</param>
        /// <param name="seed">Hash seed (default: 9001)</param>
        public CountMinSketch(byte numHashes, uint numBuckets, ulong seed = 9001)
        {
            if (numHashes == 0) throw new ArgumentException("numHashes must be positive");
            if (numBuckets == 0) throw new ArgumentException("numBuckets must be positive");

            _numHashes = numHashes;
            _numBuckets = numBuckets;
            _seed = seed;
            _sketchArray = new T[numHashes * numBuckets];
            _totalWeight = default(T);

            // Initialize hash seeds
            _hashSeeds = new ulong[numHashes];
            var random = new Random((int)seed);
            for (int i = 0; i < numHashes; i++)
            {
                _hashSeeds[i] = (ulong)random.Next() | ((ulong)random.Next() << 32);
            }
        }

        /// <summary>
        /// Gets the number of hash functions configured for this sketch.
        /// </summary>
        public byte NumHashes => _numHashes;

        /// <summary>
        /// Gets the number of buckets configured for this sketch.
        /// </summary>
        public uint NumBuckets => _numBuckets;

        /// <summary>
        /// Gets the hash seed configured for this sketch.
        /// </summary>
        public ulong Seed => _seed;

        /// <summary>
        /// Gets the total weight of all items in the sketch.
        /// </summary>
        public T TotalWeight => _totalWeight;

        /// <summary>
        /// Returns true if the sketch is empty.
        /// </summary>
        public bool IsEmpty => EqualityComparer<T>.Default.Equals(_totalWeight, default(T));

        /// <summary>
        /// Gets the relative error (epsilon) for this sketch.
        /// epsilon = e / numBuckets (approximately 2.71828 / numBuckets)
        /// </summary>
        public double RelativeError => Math.E / _numBuckets;

        /// <summary>
        /// Suggests the number of buckets required to achieve a given relative error.
        /// </summary>
        /// <param name="relativeError">Desired accuracy (e.g., 0.05 for 5% error)</param>
        /// <returns>Number of buckets needed</returns>
        public static uint SuggestNumBuckets(double relativeError)
        {
            if (relativeError <= 0 || relativeError >= 1)
                throw new ArgumentException("relativeError must be between 0 and 1");

            return (uint)Math.Ceiling(Math.E / relativeError);
        }

        /// <summary>
        /// Suggests the number of hash functions required for a given confidence level.
        /// </summary>
        /// <param name="confidence">Desired confidence (e.g., 0.95 for 95% confidence)</param>
        /// <returns>Number of hash functions needed</returns>
        public static byte SuggestNumHashes(double confidence)
        {
            if (confidence <= 0 || confidence >= 1)
                throw new ArgumentException("confidence must be between 0 and 1");

            return (byte)Math.Ceiling(Math.Log(1.0 / (1.0 - confidence)));
        }

        /// <summary>
        /// Updates the sketch with a given item and weight.
        /// </summary>
        /// <param name="item">Item to update</param>
        /// <param name="weight">Weight to add</param>
        public void Update(ulong item, T weight)
        {
            var bytes = BitConverter.GetBytes(item);
            Update(bytes, weight);
        }

        /// <summary>
        /// Updates the sketch with a given item and weight.
        /// </summary>
        /// <param name="item">Item to update</param>
        /// <param name="weight">Weight to add</param>
        public void Update(long item, T weight)
        {
            var bytes = BitConverter.GetBytes(item);
            Update(bytes, weight);
        }

        /// <summary>
        /// Updates the sketch with a given string and weight.
        /// </summary>
        /// <param name="item">String to update</param>
        /// <param name="weight">Weight to add</param>
        public void Update(string item, T weight)
        {
            var bytes = Encoding.UTF8.GetBytes(item);
            Update(bytes, weight);
        }

        /// <summary>
        /// Updates the sketch with given data and weight.
        /// </summary>
        /// <param name="data">Byte array to hash</param>
        /// <param name="weight">Weight to add</param>
        public void Update(byte[] data, T weight)
        {
            var hashes = GetHashes(data);

            for (int i = 0; i < _numHashes; i++)
            {
                var index = (i * _numBuckets) + (long)hashes[i];
                _sketchArray[index] = _sketchArray[index] + weight;
            }

            _totalWeight = _totalWeight + weight;
        }

        /// <summary>
        /// Gets the frequency estimate for an item.
        /// </summary>
        public T GetEstimate(ulong item)
        {
            var bytes = BitConverter.GetBytes(item);
            return GetEstimate(bytes);
        }

        /// <summary>
        /// Gets the frequency estimate for an item.
        /// </summary>
        public T GetEstimate(long item)
        {
            var bytes = BitConverter.GetBytes(item);
            return GetEstimate(bytes);
        }

        /// <summary>
        /// Gets the frequency estimate for a string.
        /// </summary>
        public T GetEstimate(string item)
        {
            var bytes = Encoding.UTF8.GetBytes(item);
            return GetEstimate(bytes);
        }

        /// <summary>
        /// Gets the frequency estimate for given data.
        /// </summary>
        private T GetEstimate(byte[] data)
        {
            var hashes = GetHashes(data);
            T minEstimate = _sketchArray[hashes[0]];

            for (int i = 1; i < _numHashes; i++)
            {
                var index = (i * _numBuckets) + (long)hashes[i];
                var value = _sketchArray[index];
                if (value.CompareTo(minEstimate) < 0)
                    minEstimate = value;
            }

            return minEstimate;
        }

        /// <summary>
        /// Gets the upper bound for an item's frequency.
        /// </summary>
        public T GetUpperBound(byte[] data)
        {
            var estimate = GetEstimate(data);
            var error = T.CreateChecked(RelativeError) * _totalWeight;
            return estimate + error;
        }

        /// <summary>
        /// Gets the lower bound for an item's frequency.
        /// </summary>
        public T GetLowerBound(byte[] data)
        {
            return GetEstimate(data);
        }

        /// <summary>
        /// Merges another sketch into this one.
        /// </summary>
        public void Merge(CountMinSketch<T> other)
        {
            if (other._numHashes != _numHashes)
                throw new ArgumentException("Cannot merge sketches with different numHashes");
            if (other._numBuckets != _numBuckets)
                throw new ArgumentException("Cannot merge sketches with different numBuckets");
            if (other._seed != _seed)
                throw new ArgumentException("Cannot merge sketches with different seeds");

            for (int i = 0; i < _sketchArray.Length; i++)
            {
                _sketchArray[i] = _sketchArray[i] + other._sketchArray[i];
            }

            _totalWeight = _totalWeight + other._totalWeight;
        }

        private ulong[] GetHashes(byte[] data)
        {
            var hashes = new ulong[_numHashes];

            for (int i = 0; i < _numHashes; i++)
            {
                HashState hashState;
                MurmurHash3.Hash128(data, _hashSeeds[i], out hashState);
                hashes[i] = hashState.H1 % _numBuckets;
            }

            return hashes;
        }

        /// <summary>
        /// Returns a string representation of this sketch.
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("### CountMin Sketch Summary ###");
            sb.AppendLine($"  Num Hashes     : {_numHashes}");
            sb.AppendLine($"  Num Buckets    : {_numBuckets}");
            sb.AppendLine($"  Total Weight   : {_totalWeight}");
            sb.AppendLine($"  Relative Error : {RelativeError:F6}");
            sb.AppendLine($"  Is Empty       : {IsEmpty}");
            return sb.ToString();
        }

        /// <summary>
        /// Serializes the sketch to a byte array.
        /// </summary>
        public byte[] Serialize()
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                // Preamble
                writer.Write(IsEmpty ? PreambleShortLength : PreambleFullLength);
                writer.Write(SerialVersion);
                writer.Write(FamilyId);
                writer.Write((byte)(IsEmpty ? 1 : 0)); // flags

                // Parameters
                writer.Write((uint)0); // unused
                writer.Write(_numBuckets);
                writer.Write(_numHashes);
                writer.Write((ushort)_seed);
                writer.Write((ushort)0); // padding

                if (!IsEmpty)
                {
                    // Write total weight
                    WriteValue(writer, _totalWeight);

                    // Write sketch array
                    foreach (var value in _sketchArray)
                    {
                        WriteValue(writer, value);
                    }
                }

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Deserializes a sketch from a byte array.
        /// </summary>
        public static CountMinSketch<T> Deserialize(byte[] bytes, ulong seed = 9001)
        {
            using (var ms = new MemoryStream(bytes))
            using (var reader = new BinaryReader(ms))
            {
                var preambleLongs = reader.ReadByte();
                var serVer = reader.ReadByte();
                var familyId = reader.ReadByte();
                var flags = reader.ReadByte();

                if (serVer != SerialVersion)
                    throw new ArgumentException($"Unsupported serial version: {serVer}");
                if (familyId != FamilyId)
                    throw new ArgumentException($"Invalid family ID: {familyId}");

                reader.ReadUInt32(); // skip unused
                var numBuckets = reader.ReadUInt32();
                var numHashes = reader.ReadByte();
                reader.ReadUInt16(); // skip seed hash
                reader.ReadUInt16(); // skip padding

                var sketch = new CountMinSketch<T>(numHashes, numBuckets, seed);

                if (preambleLongs >= PreambleFullLength)
                {
                    sketch._totalWeight = ReadValue(reader);

                    for (int i = 0; i < sketch._sketchArray.Length; i++)
                    {
                        sketch._sketchArray[i] = ReadValue(reader);
                    }
                }

                return sketch;
            }
        }

        private static void WriteValue(BinaryWriter writer, T value)
        {
            if (typeof(T) == typeof(long))
                writer.Write((long)(object)value);
            else if (typeof(T) == typeof(ulong))
                writer.Write((ulong)(object)value);
            else if (typeof(T) == typeof(double))
                writer.Write((double)(object)value);
            else
                throw new NotSupportedException($"Type {typeof(T)} not supported for serialization");
        }

        private static T ReadValue(BinaryReader reader)
        {
            if (typeof(T) == typeof(long))
                return (T)(object)reader.ReadInt64();
            else if (typeof(T) == typeof(ulong))
                return (T)(object)reader.ReadUInt64();
            else if (typeof(T) == typeof(double))
                return (T)(object)reader.ReadDouble();
            else
                throw new NotSupportedException($"Type {typeof(T)} not supported for deserialization");
        }
    }
}

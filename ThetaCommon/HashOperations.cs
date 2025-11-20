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
using System.Buffers.Binary;
using Apache.DataSketches.Common;

namespace Apache.DataSketches.ThetaCommon
{
    /// <summary>
    /// Helper class for the common hash table methods.
    /// </summary>
    public static class HashOperations
    {
        private const int STRIDE_HASH_BITS = 7;
        private const long EMPTY = 0;

        /// <summary>
        /// The stride mask for the Open Address, Double Hashing (OADH) hash table algorithm.
        /// </summary>
        public const int STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

        //Make odd and independent of index assuming lgArrLongs lowest bits of the hash were used for
        //  index. This results in a 8 bit value that is always odd.
        private static int GetStride(long hash, int lgArrLongs)
        {
            return (2 * (int)((hash >> lgArrLongs) & STRIDE_MASK)) + 1;
        }

        //ON-HEAP

        /// <summary>
        /// This is a classical Knuth-style Open Addressing, Double Hash (OADH) search scheme for on-heap.
        /// Returns the index if found, -1 if not found.
        /// </summary>
        /// <param name="hashTable">The hash table to search. Its size must be a power of 2.</param>
        /// <param name="lgArrLongs">The log_base2(hashTable.length).</param>
        /// <param name="hash">The hash value to search for. It must not be zero.</param>
        /// <returns>Current probe index if found, -1 if not found.</returns>
        public static int HashSearch(long[] hashTable, int lgArrLongs, long hash)
        {
            if (hash == 0)
            {
                throw new SketchesArgumentException($"Given hash must not be zero: {hash}");
            }
            int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
            int stride = GetStride(hash, lgArrLongs);
            int curProbe = (int)(hash & arrayMask);

            // search for duplicate or empty slot
            int loopIndex = curProbe;
            do
            {
                long arrVal = hashTable[curProbe];
                if (arrVal == EMPTY)
                {
                    return -1; // not found
                }
                else if (arrVal == hash)
                {
                    return curProbe; // found
                }
                curProbe = (curProbe + stride) & arrayMask;
            } while (curProbe != loopIndex);
            return -1;
        }

        /// <summary>
        /// This is a classical Knuth-style Open Addressing, Double Hash (OADH) insert scheme for on-heap.
        /// This method assumes that the input hash is not a duplicate.
        /// Useful for rebuilding tables to avoid unnecessary comparisons.
        /// Returns the index of insertion, which is always positive or zero.
        /// Throws an exception if the table has no empty slot.
        /// </summary>
        /// <param name="hashTable">the hash table to insert into. Its size must be a power of 2.</param>
        /// <param name="lgArrLongs">The log_base2(hashTable.length).</param>
        /// <param name="hash">The hash value to be potentially inserted into an empty slot. It must not be zero.</param>
        /// <returns>index of insertion.  Always positive or zero.</returns>
        public static int HashInsertOnly(long[] hashTable, int lgArrLongs, long hash)
        {
            int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
            int stride = GetStride(hash, lgArrLongs);
            int curProbe = (int)(hash & arrayMask);

            long loopIndex = curProbe;
            do
            {
                long arrVal = hashTable[curProbe];
                if (arrVal == EMPTY)
                {
                    hashTable[curProbe] = hash;
                    return curProbe;
                }
                curProbe = (curProbe + stride) & arrayMask;
            } while (curProbe != loopIndex);
            throw new SketchesArgumentException("No empty slot in table!");
        }

        /// <summary>
        /// This is a classical Knuth-style Open Addressing, Double Hash (OADH) insert scheme for on-heap.
        /// Returns index >= 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
        /// Throws an exception if the value is not found and table has no empty slot.
        /// </summary>
        /// <param name="hashTable">The hash table to insert into. Its size must be a power of 2.</param>
        /// <param name="lgArrLongs">The log_base2(hashTable.length).</param>
        /// <param name="hash">The hash value to be potentially inserted into an empty slot only if it is not
        /// a duplicate of any other hash value in the table. It must not be zero.</param>
        /// <returns>index >= 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).</returns>
        public static int HashSearchOrInsert(long[] hashTable, int lgArrLongs, long hash)
        {
            int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
            int stride = GetStride(hash, lgArrLongs);
            int curProbe = (int)(hash & arrayMask);

            // search for duplicate or zero
            int loopIndex = curProbe;
            do
            {
                long arrVal = hashTable[curProbe];
                if (arrVal == EMPTY)
                {
                    hashTable[curProbe] = hash; // insert value
                    return ~curProbe;
                }
                else if (arrVal == hash)
                {
                    return curProbe; // found a duplicate
                }
                curProbe = (curProbe + stride) & arrayMask;
            } while (curProbe != loopIndex);
            throw new SketchesArgumentException("Hash not found and no empty slots!");
        }

        /// <summary>
        /// Inserts the given long array into the given OADH hashTable of the target size,
        /// ignores duplicates and counts the values inserted.
        /// The hash values must not be negative, zero values and values >= thetaLong are ignored.
        /// The given hash table may have values, but they must have been inserted by this method or one
        /// of the other OADH insert methods in this class.
        /// This method performs additional checks against potentially invalid hash values or theta values.
        /// Returns the count of values actually inserted.
        /// </summary>
        /// <param name="srcArr">the source hash array to be potentially inserted</param>
        /// <param name="hashTable">The hash table to insert into. Its size must be a power of 2.</param>
        /// <param name="lgArrLongs">The log_base2(hashTable.length).</param>
        /// <param name="thetaLong">The theta value that all input hash values are compared against.
        /// It must greater than zero.</param>
        /// <returns>the count of values actually inserted</returns>
        public static int HashArrayInsert(long[] srcArr, long[] hashTable, int lgArrLongs, long thetaLong)
        {
            int count = 0;
            int arrLen = srcArr.Length;
            CheckThetaCorruption(thetaLong);
            for (int i = 0; i < arrLen; i++)
            { // scan source array, build target array
                long hash = srcArr[i];
                CheckHashCorruption(hash);
                if (ContinueCondition(thetaLong, hash))
                {
                    continue;
                }
                if (HashSearchOrInsert(hashTable, lgArrLongs, hash) < 0)
                {
                    count++;
                }
            }
            return count;
        }

        //With Memory or WritableMemory

        /// <summary>
        /// This is a classical Knuth-style Open Addressing, Double Hash (OADH) search scheme for Memory.
        /// Returns the index if found, -1 if not found.
        /// </summary>
        /// <param name="mem">The Memory containing the hash table to search.
        /// The hash table portion must be a power of 2 in size.</param>
        /// <param name="lgArrLongs">The log_base2(hashTable.length).</param>
        /// <param name="hash">The hash value to search for. Must not be zero.</param>
        /// <param name="memOffsetBytes">offset in the memory where the hashTable starts</param>
        /// <returns>Current probe index if found, -1 if not found.</returns>
        public static int HashSearchMemory(ReadOnlySpan<byte> mem, int lgArrLongs, long hash, int memOffsetBytes)
        {
            if (hash == 0)
            {
                throw new SketchesArgumentException($"Given hash must not be zero: {hash}");
            }
            int arrayMask = (1 << lgArrLongs) - 1;
            int stride = GetStride(hash, lgArrLongs);
            int curProbe = (int)(hash & arrayMask);
            int loopIndex = curProbe;
            do
            {
                int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
                long curArrayHash = BinaryPrimitives.ReadInt64LittleEndian(mem.Slice(curProbeOffsetBytes, 8));
                if (curArrayHash == EMPTY) { return -1; }
                else if (curArrayHash == hash) { return curProbe; }
                curProbe = (curProbe + stride) & arrayMask;
            } while (curProbe != loopIndex);
            return -1;
        }

        /// <summary>
        /// This is a classical Knuth-style Open Addressing, Double Hash (OADH) insert scheme for Memory.
        /// This method assumes that the input hash is not a duplicate.
        /// Useful for rebuilding tables to avoid unnecessary comparisons.
        /// Returns the index of insertion, which is always positive or zero.
        /// Throws an exception if table has no empty slot.
        /// </summary>
        /// <param name="wmem">The WritableMemory that contains the hashTable to insert into.
        /// The size of the hashTable portion must be a power of 2.</param>
        /// <param name="lgArrLongs">The log_base2(hashTable.length.</param>
        /// <param name="hash">value that must not be zero and will be inserted into the array into an empty slot.</param>
        /// <param name="memOffsetBytes">offset in the WritableMemory where the hashTable starts</param>
        /// <returns>index of insertion.  Always positive or zero.</returns>
        public static int HashInsertOnlyMemory(Span<byte> wmem, int lgArrLongs, long hash, int memOffsetBytes)
        {
            int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
            int stride = GetStride(hash, lgArrLongs);
            int curProbe = (int)(hash & arrayMask);
            // search for duplicate or zero
            int loopIndex = curProbe;
            do
            {
                int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
                long curArrayHash = BinaryPrimitives.ReadInt64LittleEndian(wmem.Slice(curProbeOffsetBytes, 8));
                if (curArrayHash == EMPTY)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(wmem.Slice(curProbeOffsetBytes, 8), hash);
                    return curProbe;
                }
                curProbe = (curProbe + stride) & arrayMask;
            } while (curProbe != loopIndex);
            throw new SketchesArgumentException("No empty slot in table!");
        }

        /// <summary>
        /// This is a classical Knuth-style Open Addressing, Double Hash insert scheme, but inserts
        /// values directly into a Memory.
        /// Returns index >= 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).
        /// Throws an exception if the value is not found and table has no empty slot.
        /// </summary>
        /// <param name="wmem">The WritableMemory that contains the hashTable to insert into.</param>
        /// <param name="lgArrLongs">The log_base2(hashTable.length).</param>
        /// <param name="hash">The hash value to be potentially inserted into an empty slot only if it is not
        /// a duplicate of any other hash value in the table. It must not be zero.</param>
        /// <param name="memOffsetBytes">offset in the WritableMemory where the hash array starts</param>
        /// <returns>index >= 0 if found (duplicate); &lt; 0 if inserted, inserted at -(index + 1).</returns>
        public static int HashSearchOrInsertMemory(Span<byte> wmem, int lgArrLongs, long hash, int memOffsetBytes)
        {
            int arrayMask = (1 << lgArrLongs) - 1; // current Size -1
            int stride = GetStride(hash, lgArrLongs);
            int curProbe = (int)(hash & arrayMask);
            // search for duplicate or zero
            int loopIndex = curProbe;
            do
            {
                int curProbeOffsetBytes = (curProbe << 3) + memOffsetBytes;
                long curArrayHash = BinaryPrimitives.ReadInt64LittleEndian(wmem.Slice(curProbeOffsetBytes, 8));
                if (curArrayHash == EMPTY)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(wmem.Slice(curProbeOffsetBytes, 8), hash);
                    return ~curProbe;
                }
                else if (curArrayHash == hash) { return curProbe; } // curArrayHash is a duplicate
                // curArrayHash is not a duplicate and not zero, continue searching
                curProbe = (curProbe + stride) & arrayMask;
            } while (curProbe != loopIndex);
            throw new SketchesArgumentException("Key not found and no empty slot in table!");
        }

        //Other related methods

        /// <summary>
        /// Checks that thetaLong must be greater than zero otherwise throws an exception.
        /// </summary>
        /// <param name="thetaLong">must be greater than zero otherwise throws an exception.</param>
        public static void CheckThetaCorruption(long thetaLong)
        {
            //if any one of the groups go negative it fails.
            if ((thetaLong | (thetaLong - 1)) < 0L)
            {
                throw new SketchesStateException(
                    $"Data Corruption: thetaLong was negative or zero: ThetaLong: {thetaLong}");
            }
        }

        /// <summary>
        /// Checks that hash must be greater than -1 otherwise throws an exception.
        /// Note a hash of zero is normally ignored, but a negative hash is never allowed.
        /// </summary>
        /// <param name="hash">must be greater than -1 otherwise throws an exception.</param>
        public static void CheckHashCorruption(long hash)
        {
            if (hash < 0L)
            {
                throw new SketchesArgumentException(
                    $"Data Corruption: hash was negative: Hash: {hash}");
            }
        }

        /// <summary>
        /// Return true (continue) if hash is greater than or equal to thetaLong, or if hash == 0,
        /// or if hash == Long.MAX_VALUE.
        /// </summary>
        /// <param name="thetaLong">must be greater than the hash value</param>
        /// <param name="hash">must be less than thetaLong and not less than or equal to zero.</param>
        /// <returns>true (continue) if hash is greater than or equal to thetaLong, or if hash == 0,
        /// or if hash == Long.MAX_VALUE.</returns>
        public static bool ContinueCondition(long thetaLong, long hash)
        {
            //if any one of the groups go negative it returns true
            return (((hash - 1L) | (thetaLong - hash - 1L)) < 0L);
        }

        /// <summary>
        /// Converts the given array to a hash table.
        /// </summary>
        /// <param name="hashArr">The given array of hashes. Gaps are OK.</param>
        /// <param name="count">The number of valid hashes in the array</param>
        /// <param name="thetaLong">Any hashes equal to or greater than thetaLong will be ignored</param>
        /// <param name="rebuildThreshold">The fill fraction for the hash table forcing a rebuild or resize.</param>
        /// <returns>a HashTable</returns>
        public static long[] ConvertToHashTable(
            long[] hashArr,
            int count,
            long thetaLong,
            double rebuildThreshold)
        {
            int lgArrLongs = MinLgHashTableSize(count, rebuildThreshold);
            int arrLongs = 1 << lgArrLongs;
            long[] hashTable = new long[arrLongs];
            HashArrayInsert(hashArr, hashTable, lgArrLongs, thetaLong);
            return hashTable;
        }

        /// <summary>
        /// Returns the smallest log hash table size given the count of items and the rebuild threshold.
        /// </summary>
        /// <param name="count">the given count of items</param>
        /// <param name="rebuildThreshold">the rebuild threshold as a fraction between zero and one.</param>
        /// <returns>the smallest log hash table size</returns>
        public static int MinLgHashTableSize(int count, double rebuildThreshold)
        {
            int upperCount = (int)Math.Ceiling(count / rebuildThreshold);
            int arrLongs = Math.Max(Util.CeilingPowerOf2(upperCount), 1 << ThetaUtil.MIN_LG_ARR_LONGS);
            int newLgArrLongs = System.Numerics.BitOperations.TrailingZeroCount((uint)arrLongs);
            return newLgArrLongs;
        }

        /// <summary>
        /// Counts the cardinality of the first Log2 values of the given source array.
        /// </summary>
        /// <param name="srcArr">the given source array</param>
        /// <param name="lgArrLongs">log2 array longs</param>
        /// <param name="thetaLong">theta long</param>
        /// <returns>the cardinality</returns>
        public static int CountPart(long[] srcArr, int lgArrLongs, long thetaLong)
        {
            int cnt = 0;
            int len = 1 << lgArrLongs;
            for (int i = len; i-- > 0;)
            {
                long hash = srcArr[i];
                if (ContinueCondition(thetaLong, hash))
                {
                    continue;
                }
                cnt++;
            }
            return cnt;
        }

        /// <summary>
        /// Counts the cardinality of the given source array.
        /// </summary>
        /// <param name="srcArr">the given source array</param>
        /// <param name="thetaLong">theta long</param>
        /// <returns>the cardinality</returns>
        public static int Count(long[] srcArr, long thetaLong)
        {
            int cnt = 0;
            int len = srcArr.Length;
            for (int i = len; i-- > 0;)
            {
                long hash = srcArr[i];
                if (ContinueCondition(thetaLong, hash))
                {
                    continue;
                }
                cnt++;
            }
            return cnt;
        }
    }
}

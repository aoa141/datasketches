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
using System.Collections.Generic;

namespace DataSketches.Theta
{
    /// <summary>
    /// Comparer that compares entries by extracting and comparing their keys.
    /// </summary>
    /// <typeparam name="TEntry">Entry type</typeparam>
    /// <typeparam name="TKey">Key type</typeparam>
    public class CompareByKey<TEntry, TKey> : IComparer<TEntry> where TKey : IComparable<TKey>
    {
        private readonly Func<TEntry, TKey> extractKey;

        /// <summary>
        /// Creates a new comparer.
        /// </summary>
        /// <param name="extractKey">Function to extract key from entry</param>
        public CompareByKey(Func<TEntry, TKey> extractKey)
        {
            this.extractKey = extractKey;
        }

        /// <summary>
        /// Compares two entries by their keys.
        /// </summary>
        public int Compare(TEntry a, TEntry b)
        {
            return extractKey(a).CompareTo(extractKey(b));
        }
    }

    /// <summary>
    /// Predicate that checks if an entry's key is less than a target key.
    /// </summary>
    /// <typeparam name="TEntry">Entry type</typeparam>
    /// <typeparam name="TKey">Key type</typeparam>
    public class KeyLessThan<TEntry, TKey> where TKey : IComparable<TKey>
    {
        private readonly TKey key;
        private readonly Func<TEntry, TKey> extractKey;

        /// <summary>
        /// Creates a new predicate.
        /// </summary>
        /// <param name="key">Target key</param>
        /// <param name="extractKey">Function to extract key from entry</param>
        public KeyLessThan(TKey key, Func<TEntry, TKey> extractKey)
        {
            this.key = key;
            this.extractKey = extractKey;
        }

        /// <summary>
        /// Checks if the entry's key is less than the target key.
        /// </summary>
        public bool Test(TEntry entry)
        {
            return extractKey(entry).CompareTo(key) < 0;
        }
    }
}

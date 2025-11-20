// <copyright file="ThetaComparators.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

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

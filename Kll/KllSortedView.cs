// <copyright file="KllSortedView.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.Collections.Generic;
using System.Linq;

namespace DataSketches.Kll
{
    /// <summary>
    /// Sorted view of KLL sketch for quantile queries
    /// </summary>
    public class KllSortedView<T> where T : IComparable<T>
    {
        private readonly T[] _quantiles;
        private readonly ulong[] _cumWeights;
        private readonly ulong _totalWeight;
        private readonly IComparer<T> _comparer;

        internal KllSortedView(KllSketch<T> sketch)
        {
            if (sketch.IsEmpty())
            {
                throw new InvalidOperationException("Sketch is empty");
            }

            _comparer = sketch.Comparer ?? Comparer<T>.Default;
            _totalWeight = sketch.GetN();

            // Build sorted view from sketch - extract all items with their weights
            var itemsList = new List<(T item, ulong weight)>();

            var items = sketch.Items;
            var levels = sketch.Levels;
            var numLevels = sketch.NumLevels;

            // Extract items from each level
            // Level 0 items have weight 1, level 1 items have weight 2, etc.
            for (byte level = 0; level < numLevels; level++)
            {
                uint levelStart = levels[level];
                uint levelEnd = levels[level + 1];
                ulong weight = 1UL << level; // Weight is 2^level

                for (uint i = levelStart; i < levelEnd; i++)
                {
                    itemsList.Add((items[i], weight));
                }
            }

            // Sort items
            itemsList.Sort((a, b) => _comparer.Compare(a.item, b.item));

            // Build cumulative weights array
            _quantiles = new T[itemsList.Count];
            _cumWeights = new ulong[itemsList.Count];

            ulong cumWeight = 0;
            for (int i = 0; i < itemsList.Count; i++)
            {
                _quantiles[i] = itemsList[i].item;
                cumWeight += itemsList[i].weight;
                _cumWeights[i] = cumWeight;
            }
        }

        public T GetQuantile(double rank, bool inclusive)
        {
            if (rank < 0.0 || rank > 1.0)
            {
                throw new ArgumentException("Rank must be between 0 and 1");
            }

            if (_quantiles.Length == 0)
            {
                throw new InvalidOperationException("No quantiles available");
            }

            // Calculate target weight based on rank
            // For rank 0.5 (median), we want the item at position n/2
            ulong targetWeight = (ulong)Math.Ceiling(rank * _totalWeight);

            if (targetWeight == 0)
            {
                return _quantiles[0];
            }

            // Binary search for the appropriate quantile
            int index = Array.BinarySearch(_cumWeights, targetWeight);

            if (index < 0)
            {
                // If not found, BinarySearch returns bitwise complement of next larger element
                index = ~index;
            }

            // Ensure index is within bounds
            if (index >= _quantiles.Length)
            {
                index = _quantiles.Length - 1;
            }

            return _quantiles[index];
        }

        public double GetRank(T item, bool inclusive)
        {
            if (_quantiles.Length == 0)
            {
                return 0.0;
            }

            // Binary search for the item
            int index = Array.BinarySearch(_quantiles, item, _comparer);

            if (index >= 0)
            {
                // Exact match found
                // For inclusive, return cumulative weight at this position
                // For exclusive, return cumulative weight before this position
                if (inclusive)
                {
                    return (double)_cumWeights[index] / _totalWeight;
                }
                else
                {
                    if (index == 0)
                    {
                        return 0.0;
                    }
                    return (double)_cumWeights[index - 1] / _totalWeight;
                }
            }
            else
            {
                // Not found - index is the bitwise complement of the insertion point
                index = ~index;

                if (index == 0)
                {
                    return 0.0;
                }

                if (index >= _cumWeights.Length)
                {
                    return 1.0;
                }

                // Return rank of the item just before the insertion point
                return (double)_cumWeights[index - 1] / _totalWeight;
            }
        }

        public double[] GetPMF(T[] splitPoints, bool inclusive)
        {
            if (splitPoints == null || splitPoints.Length == 0)
            {
                return new double[] { 1.0 };
            }

            var result = new double[splitPoints.Length + 1];
            var cdf = GetCDF(splitPoints, inclusive);

            result[0] = cdf[0];
            for (int i = 1; i < cdf.Length; i++)
            {
                result[i] = cdf[i] - cdf[i - 1];
            }

            return result;
        }

        public double[] GetCDF(T[] splitPoints, bool inclusive)
        {
            if (splitPoints == null || splitPoints.Length == 0)
            {
                return new double[] { 1.0 };
            }

            var result = new double[splitPoints.Length + 1];

            for (int i = 0; i < splitPoints.Length; i++)
            {
                result[i] = GetRank(splitPoints[i], inclusive);
            }

            result[splitPoints.Length] = 1.0;

            return result;
        }

        public ulong GetTotalWeight()
        {
            return _totalWeight;
        }

        public int GetNumRetained()
        {
            return _quantiles.Length;
        }
    }
}

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

            _comparer = Comparer<T>.Default;

            // Build sorted view from sketch
            var items = new List<(T item, ulong weight)>();

            // This is a simplified implementation
            // A full implementation would extract all items with their weights
            // from all levels of the sketch and sort them

            _totalWeight = sketch.GetN();

            // For now, create a simple sorted view
            _quantiles = new T[1];
            _cumWeights = new ulong[1];
            _quantiles[0] = sketch.GetMinItem();
            _cumWeights[0] = _totalWeight;
        }

        public T GetQuantile(double rank, bool inclusive)
        {
            if (rank < 0.0 || rank > 1.0)
            {
                throw new ArgumentException("Rank must be between 0 and 1");
            }

            ulong targetWeight = (ulong)(rank * _totalWeight);

            // Binary search for the appropriate quantile
            int index = 0;
            for (int i = 0; i < _cumWeights.Length; i++)
            {
                if (_cumWeights[i] >= targetWeight)
                {
                    index = i;
                    break;
                }
            }

            return _quantiles[index];
        }

        public double GetRank(T item, bool inclusive)
        {
            // Binary search for rank
            int index = Array.BinarySearch(_quantiles, item, _comparer);

            if (index < 0)
            {
                index = ~index;
            }

            if (index == 0)
            {
                return 0.0;
            }

            if (index >= _cumWeights.Length)
            {
                return 1.0;
            }

            return (double)_cumWeights[index] / _totalWeight;
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

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

namespace Apache.DataSketches.ThetaCommon
{
    /// <summary>
    /// QuickSelect algorithm improved from Sedgewick. Gets the kth order value
    /// (1-based or 0-based) from the array.
    /// Warning! This changes the ordering of elements in the given array!
    /// Also see: blog.teamleadnet.com/2012/07/quick-select-algorithm-find-kth-element.html
    /// </summary>
    public static class QuickSelect
    {
        /// <summary>
        /// Gets the 0-based kth order statistic from the array. Warning! This changes the ordering
        /// of elements in the given array!
        /// </summary>
        /// <param name="arr">The array to be re-arranged.</param>
        /// <param name="lo">The lowest 0-based index to be considered.</param>
        /// <param name="hi">The highest 0-based index to be considered.</param>
        /// <param name="pivot">The 0-based index of the value to pivot on.</param>
        /// <returns>The value of the smallest (n)th element where n is 0-based.</returns>
        public static long Select(long[] arr, int lo, int hi, int pivot)
        {
            while (hi > lo)
            {
                int j = Partition(arr, lo, hi);
                if (j == pivot)
                {
                    return arr[pivot];
                }
                if (j > pivot)
                {
                    hi = j - 1;
                }
                else
                {
                    lo = j + 1;
                }
            }
            return arr[pivot];
        }

        /// <summary>
        /// Gets the 1-based kth order statistic from the array including any zero values in the
        /// array. Warning! This changes the ordering of elements in the given array!
        /// </summary>
        /// <param name="arr">The hash array.</param>
        /// <param name="pivot">The 1-based index of the value that is chosen as the pivot for the array.
        /// After the operation all values below this 1-based index will be less than this value
        /// and all values above this index will be greater. The 0-based index of the pivot will be
        /// pivot-1.</param>
        /// <returns>The value of the smallest (N)th element including zeros, where N is 1-based.</returns>
        public static long SelectIncludingZeros(long[] arr, int pivot)
        {
            int arrSize = arr.Length;
            int adj = pivot - 1;
            return Select(arr, 0, arrSize - 1, adj);
        }

        /// <summary>
        /// Gets the 1-based kth order statistic from the array excluding any zero values in the
        /// array. Warning! This changes the ordering of elements in the given array!
        /// </summary>
        /// <param name="arr">The hash array.</param>
        /// <param name="nonZeros">The number of non-zero values in the array.</param>
        /// <param name="pivot">The 1-based index of the value that is chosen as the pivot for the array.
        /// After the operation all values below this 1-based index will be less than this value
        /// and all values above this index will be greater. The 0-based index of the pivot will be
        /// pivot+arr.length-nonZeros-1.</param>
        /// <returns>The value of the smallest (N)th element excluding zeros, where N is 1-based.</returns>
        public static long SelectExcludingZeros(long[] arr, int nonZeros, int pivot)
        {
            if (pivot > nonZeros)
            {
                return 0L;
            }
            int arrSize = arr.Length;
            int zeros = arrSize - nonZeros;
            int adjK = (pivot + zeros) - 1;
            return Select(arr, 0, arrSize - 1, adjK);
        }

        /// <summary>
        /// Partition arr[] into arr[lo .. i-1], arr[i], arr[i+1,hi]
        /// </summary>
        /// <param name="arr">The given array to partition</param>
        /// <param name="lo">the low index</param>
        /// <param name="hi">the high index</param>
        /// <returns>the next partition value.  Ultimately, the desired pivot.</returns>
        private static int Partition(long[] arr, int lo, int hi)
        {
            int i = lo, j = hi + 1; //left and right scan indices
            long v = arr[lo]; //partitioning item value
            while (true)
            {
                //Scan right, scan left, check for scan complete, and exchange
                while (arr[++i] < v)
                {
                    if (i == hi)
                    {
                        break;
                    }
                }
                while (v < arr[--j])
                {
                    if (j == lo)
                    {
                        break;
                    }
                }
                if (i >= j)
                {
                    break;
                }
                long x = arr[i];
                arr[i] = arr[j];
                arr[j] = x;
            }
            //put v=arr[j] into position with a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
            long temp = arr[lo];
            arr[lo] = arr[j];
            arr[j] = temp;
            return j;
        }

        //For double arrays

        /// <summary>
        /// Gets the 0-based kth order statistic from the array. Warning! This changes the ordering
        /// of elements in the given array!
        /// </summary>
        /// <param name="arr">The array to be re-arranged.</param>
        /// <param name="lo">The lowest 0-based index to be considered.</param>
        /// <param name="hi">The highest 0-based index to be considered.</param>
        /// <param name="pivot">The 0-based smallest value to pivot on.</param>
        /// <returns>The value of the smallest (n)th element where n is 0-based.</returns>
        public static double Select(double[] arr, int lo, int hi, int pivot)
        {
            while (hi > lo)
            {
                int j = Partition(arr, lo, hi);
                if (j == pivot)
                {
                    return arr[pivot];
                }
                if (j > pivot)
                {
                    hi = j - 1;
                }
                else
                {
                    lo = j + 1;
                }
            }
            return arr[pivot];
        }

        /// <summary>
        /// Gets the 1-based kth order statistic from the array including any zero values in the
        /// array. Warning! This changes the ordering of elements in the given array!
        /// </summary>
        /// <param name="arr">The hash array.</param>
        /// <param name="pivot">The 1-based index of the value that is chosen as the pivot for the array.
        /// After the operation all values below this 1-based index will be less than this value
        /// and all values above this index will be greater. The 0-based index of the pivot will be
        /// pivot-1.</param>
        /// <returns>The value of the smallest (N)th element including zeros, where N is 1-based.</returns>
        public static double SelectIncludingZeros(double[] arr, int pivot)
        {
            int arrSize = arr.Length;
            int adj = pivot - 1;
            return Select(arr, 0, arrSize - 1, adj);
        }

        /// <summary>
        /// Gets the 1-based kth order statistic from the array excluding any zero values in the
        /// array. Warning! This changes the ordering of elements in the given array!
        /// </summary>
        /// <param name="arr">The hash array.</param>
        /// <param name="nonZeros">The number of non-zero values in the array.</param>
        /// <param name="pivot">The 1-based index of the value that is chosen as the pivot for the array.
        /// After the operation all values below this 1-based index will be less than this value
        /// and all values above this index will be greater. The 0-based index of the pivot will be
        /// pivot+arr.length-nonZeros-1.</param>
        /// <returns>The value of the smallest (N)th element excluding zeros, where N is 1-based.</returns>
        public static double SelectExcludingZeros(double[] arr, int nonZeros, int pivot)
        {
            if (pivot > nonZeros)
            {
                return 0L;
            }
            int arrSize = arr.Length;
            int zeros = arrSize - nonZeros;
            int adjK = (pivot + zeros) - 1;
            return Select(arr, 0, arrSize - 1, adjK);
        }

        /// <summary>
        /// Partition arr[] into arr[lo .. i-1], arr[i], arr[i+1,hi]
        /// </summary>
        /// <param name="arr">The given array to partition</param>
        /// <param name="lo">the low index</param>
        /// <param name="hi">the high index</param>
        /// <returns>the next partition value.  Ultimately, the desired pivot.</returns>
        private static int Partition(double[] arr, int lo, int hi)
        {
            int i = lo, j = hi + 1; //left and right scan indices
            double v = arr[lo]; //partitioning item value
            while (true)
            {
                //Scan right, scan left, check for scan complete, and exchange
                while (arr[++i] < v)
                {
                    if (i == hi)
                    {
                        break;
                    }
                }
                while (v < arr[--j])
                {
                    if (j == lo)
                    {
                    break;
                    }
                }
                if (i >= j)
                {
                    break;
                }
                double x = arr[i];
                arr[i] = arr[j];
                arr[j] = x;
            }
            //put v=arr[j] into position with a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
            double temp = arr[lo];
            arr[lo] = arr[j];
            arr[j] = temp;
            return j;
        }
    }
}

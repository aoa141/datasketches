// <copyright file="Example.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;

namespace DataSketches.Cpc
{
    /// <summary>
    /// Example usage of CPC (Compressed Probabilistic Counting) Sketch
    /// </summary>
    public static class Example
    {
        public static void Main()
        {
            Console.WriteLine("=== CPC Sketch Example ===\n");

            // Example 1: Basic distinct counting
            BasicDistinctCounting();

            // Example 2: Accuracy demonstration
            AccuracyDemonstration();

            // Example 3: Serialization
            SerializationExample();

            // Example 4: Memory efficiency
            MemoryEfficiencyExample();
        }

        private static void BasicDistinctCounting()
        {
            Console.WriteLine("Example 1: Basic Distinct Counting");
            Console.WriteLine("----------------------------------");

            // Create a CPC sketch with default lgK=11 (K=2048)
            var sketch = new CpcSketch();

            // Add some values
            for (int i = 0; i < 10000; i++)
            {
                sketch.Update(i);
            }

            // Add duplicates
            for (int i = 0; i < 10000; i++)
            {
                sketch.Update(i); // Same values again
            }

            Console.WriteLine($"Added 20,000 values (10,000 unique)");
            Console.WriteLine($"Estimate: {sketch.Estimate:F0}");
            Console.WriteLine($"Actual unique: 10,000");
            Console.WriteLine($"Error: {Math.Abs(sketch.Estimate - 10000) / 10000.0:P2}");
            Console.WriteLine($"Lower Bound (95%): {sketch.GetLowerBound(2):F0}");
            Console.WriteLine($"Upper Bound (95%): {sketch.GetUpperBound(2):F0}");
            Console.WriteLine();
        }

        private static void AccuracyDemonstration()
        {
            Console.WriteLine("Example 2: Accuracy at Different Scales");
            Console.WriteLine("---------------------------------------");

            var lgK = (byte)12; // K=4096
            var sketch = new CpcSketch(lgK);

            var scales = new[] { 100, 1000, 10000, 100000, 1000000 };

            foreach (var n in scales)
            {
                sketch = new CpcSketch(lgK); // Reset

                for (int i = 0; i < n; i++)
                {
                    sketch.Update($"user_{i}");
                }

                var estimate = sketch.Estimate;
                var error = Math.Abs(estimate - n) / n;

                Console.WriteLine($"N={n,8:N0}: Estimate={estimate,10:F0}, Error={error,7:P2}");
            }

            Console.WriteLine();
        }

        private static void SerializationExample()
        {
            Console.WriteLine("Example 3: Serialization");
            Console.WriteLine("-----------------------");

            // Create and populate a sketch
            var sketch1 = new CpcSketch();
            for (int i = 0; i < 50000; i++)
            {
                sketch1.Update($"event_{i}");
            }

            Console.WriteLine($"Original sketch estimate: {sketch1.Estimate:F0}");

            // Serialize
            var bytes = sketch1.Serialize();
            Console.WriteLine($"Serialized size: {bytes.Length:N0} bytes");

            // Deserialize
            var sketch2 = CpcSketch.Deserialize(bytes);
            Console.WriteLine($"Deserialized sketch estimate: {sketch2.Estimate:F0}");

            Console.WriteLine($"Estimates match: {Math.Abs(sketch1.Estimate - sketch2.Estimate) < 1}");
            Console.WriteLine();
        }

        private static void MemoryEfficiencyExample()
        {
            Console.WriteLine("Example 4: Memory Efficiency Comparison");
            Console.WriteLine("---------------------------------------");

            // Compare CPC sketch memory usage at different lgK values
            var lgKValues = new byte[] { 10, 11, 12, 13, 14 };

            Console.WriteLine("lgK | K      | Max Serialized Size | Typical Error");
            Console.WriteLine("----+--------+--------------------+-------------");

            foreach (var lgK in lgKValues)
            {
                var k = 1 << lgK;
                var maxSize = CpcSketch.GetMaxSerializedSizeBytes(lgK);
                var typicalError = 0.833 / Math.Sqrt(k);

                Console.WriteLine($"{lgK,3} | {k,6} | {maxSize,18:N0} | {typicalError,11:P2}");
            }

            Console.WriteLine();
            Console.WriteLine("CPC sketches are significantly more space-efficient");
            Console.WriteLine("than other distinct count sketches, especially for");
            Console.WriteLine("small to medium cardinalities.");
            Console.WriteLine();
        }
    }
}

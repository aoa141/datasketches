// <copyright file="ThetaSketchExample.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using System;
using System.Diagnostics.CodeAnalysis;
using DataSketches.Common;
using DataSketches.Theta;

namespace DataSketches.Examples
{
    /// <summary>
    /// Example usage of Theta sketches.
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class ThetaSketchExample
    {
        public static void Main(string[] args)
        {
            BasicExample();
            Console.WriteLine("\n" + new string('-', 80) + "\n");
            SetOperationsExample();
        }

        /// <summary>
        /// Basic theta sketch usage - counting distinct values.
        /// </summary>
        public static void BasicExample()
        {
            Console.WriteLine("=== Basic Theta Sketch Example ===\n");

            // Create a sketch with default parameters (k=4096)
            var sketch = new UpdateThetaSketch.Builder().Build();

            // Update with some values
            Console.WriteLine("Adding 10,000 unique values...");
            for (int i = 0; i < 10000; i++)
            {
                Console.WriteLine($"Adding value: {i}");
                sketch.Update(i);
            }

            // Add some duplicates
            Console.WriteLine("Adding 5,000 duplicate values...");
            for (int i = 0; i < 5000; i++)
            {
                Console.WriteLine($"Adding value: {i}");
                sketch.Update(i); // These are duplicates
            }

            // Get the estimate
            Console.WriteLine($"\nEstimate of unique values: {sketch.GetEstimate():F0}");
            Console.WriteLine($"Actual unique values: 10000");
            Console.WriteLine($"Error: {Math.Abs(sketch.GetEstimate() - 10000):F0}");
            Console.WriteLine($"\nLower bound (95% confidence): {sketch.GetLowerBound(2):F0}");
            Console.WriteLine($"Upper bound (95% confidence): {sketch.GetUpperBound(2):F0}");

            // Convert to compact sketch for serialization
            var compact = sketch.Compact(ordered: true);
            Console.WriteLine($"\nCompact sketch is ordered: {compact.IsOrdered}");
            Console.WriteLine($"Compact sketch size: {compact.GetSerializedSizeBytes()} bytes");

            // Serialize and deserialize
            byte[] serialized = compact.Serialize();
            var deserialized = CompactThetaSketch.Deserialize(serialized, 0, serialized.Length);
            Console.WriteLine($"\nDeserialized estimate: {deserialized.GetEstimate():F0}");
        }

        /// <summary>
        /// Demonstrates set operations: union, intersection, and A-not-B.
        /// </summary>
        public static void SetOperationsExample()
        {
            Console.WriteLine("=== Set Operations Example ===\n");

            // Create two sketches with overlapping data
            var sketchA = new UpdateThetaSketch.Builder().Build();
            var sketchB = new UpdateThetaSketch.Builder().Build();

            Console.WriteLine("Sketch A: values 0-9999");
            for (int i = 0; i < 10000; i++)
            {
                sketchA.Update(i);
            }

            Console.WriteLine("Sketch B: values 5000-14999 (5000 overlap)");
            for (int i = 5000; i < 15000; i++)
            {
                sketchB.Update(i);
            }

            Console.WriteLine($"\nSketch A estimate: {sketchA.GetEstimate():F0}");
            Console.WriteLine($"Sketch B estimate: {sketchB.GetEstimate():F0}");

            // Union: A ∪ B = 15000 unique values
            Console.WriteLine("\n--- Union Operation ---");
            var union = new ThetaUnion.Builder().Build();
            union.Update(sketchA);
            union.Update(sketchB);
            var unionResult = union.GetResult();
            Console.WriteLine($"Union estimate (A ∪ B): {unionResult.GetEstimate():F0}");
            Console.WriteLine($"Expected: 15000");

            // Intersection: A ∩ B = 5000 common values
            Console.WriteLine("\n--- Intersection Operation ---");
            var intersection = new ThetaIntersection();
            intersection.Update(sketchA);
            intersection.Update(sketchB);
            var intersectionResult = intersection.GetResult();
            Console.WriteLine($"Intersection estimate (A ∩ B): {intersectionResult.GetEstimate():F0}");
            Console.WriteLine($"Expected: 5000");

            // A-not-B: values only in A = 5000 values
            Console.WriteLine("\n--- A-not-B Operation ---");
            var aNotB = new ThetaANotB();
            var aNotBResult = aNotB.Compute(sketchA, sketchB);
            Console.WriteLine($"A-not-B estimate (A - B): {aNotBResult.GetEstimate():F0}");
            Console.WriteLine($"Expected: 5000");

            // Verify set identity: |A ∪ B| = |A| + |B| - |A ∩ B|
            double calculatedUnion = sketchA.GetEstimate() + sketchB.GetEstimate() - intersectionResult.GetEstimate();
            Console.WriteLine($"\n--- Set Identity Verification ---");
            Console.WriteLine($"|A| + |B| - |A ∩ B| = {calculatedUnion:F0}");
            Console.WriteLine($"|A ∪ B| = {unionResult.GetEstimate():F0}");
            Console.WriteLine($"Difference: {Math.Abs(calculatedUnion - unionResult.GetEstimate()):F0}");
        }

        /// <summary>
        /// Example with custom sketch parameters.
        /// </summary>
        public static void CustomParametersExample()
        {
            Console.WriteLine("=== Custom Parameters Example ===\n");

            // Create a smaller sketch with k=512 (2^9) for less memory usage
            var smallSketch = new UpdateThetaSketch.Builder()
                .SetLgK(9)  // k = 512
                .SetResizeFactor(CommonDefs.ResizeFactor.X2)
                .Build();

            Console.WriteLine($"Sketch with k=512 (lgK={smallSketch.GetLgK()})");
            Console.WriteLine($"Resize factor: {smallSketch.GetResizeFactor()}");

            // Add many values
            for (int i = 0; i < 100000; i++)
            {
                smallSketch.Update(i);
            }

            Console.WriteLine($"\nAdded 100,000 unique values");
            Console.WriteLine($"Estimate: {smallSketch.GetEstimate():F0}");
            Console.WriteLine($"Error: {Math.Abs(smallSketch.GetEstimate() - 100000):F0} ({Math.Abs((smallSketch.GetEstimate() - 100000) / 100000 * 100):F2}%)");
            Console.WriteLine($"95% confidence interval: [{smallSketch.GetLowerBound(2):F0}, {smallSketch.GetUpperBound(2):F0}]");
        }
    }
}

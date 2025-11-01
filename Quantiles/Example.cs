using System;
using DataSketches.Quantiles;

namespace DataSketches.Examples
{
    /// <summary>
    /// Example usage of Quantiles Sketch.
    /// </summary>
    public class QuantilesExample
    {
        public static void Run()
        {
            Console.WriteLine("=== Quantiles Sketch Example ===\n");

            // Create sketch with K=128 (default)
            var sketch = new QuantilesSketch<double>(128);

            Console.WriteLine("Created Quantiles sketch with K=128");
            Console.WriteLine($"Expected rank error: {sketch.GetNormalizedRankError(false):P2}\n");

            // Simulate streaming temperature measurements
            var random = new Random(42);
            int numReadings = 100000;

            Console.WriteLine($"Streaming {numReadings} temperature readings...\n");

            for (int i = 0; i < numReadings; i++)
            {
                // Generate temperatures with normal distribution (mean=70°F, stddev=10°F)
                double temp = 70 + (random.NextDouble() - 0.5 + random.NextDouble() - 0.5 +
                                   random.NextDouble() - 0.5) * 20; // Approximates normal
                sketch.Update(temp);
            }

            Console.WriteLine($"N (count)      : {sketch.N}");
            Console.WriteLine($"Num Retained   : {sketch.NumRetained}");
            Console.WriteLine($"Is Estimation  : {sketch.IsEstimationMode}\n");

            // Query quantiles
            Console.WriteLine("Temperature Quantiles:");
            double[] ranks = { 0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99 };

            foreach (var rank in ranks)
            {
                var temp = sketch.GetQuantile(rank);
                Console.WriteLine($"  P{rank * 100,5:F1}: {temp:F1}°F");
            }

            Console.WriteLine($"\n  Min  : {sketch.MinValue:F1}°F");
            Console.WriteLine($"  Max  : {sketch.MaxValue:F1}°F");

            // Get rank of specific temperature
            double targetTemp = 75.0;
            double tempRank = sketch.GetRank(targetTemp);
            Console.WriteLine($"\nRank of {targetTemp}°F: {tempRank:P2}");

            // Get PMF for temperature ranges
            double[] splitPoints = { 50, 60, 70, 80, 90 };
            var pmf = sketch.GetPMF(splitPoints);

            Console.WriteLine("\nTemperature Distribution (PMF):");
            Console.WriteLine($"  < {splitPoints[0],4}°F: {pmf[0]:P2}");
            for (int i = 0; i < splitPoints.Length - 1; i++)
            {
                Console.WriteLine($"  {splitPoints[i],4}-{splitPoints[i + 1],4}°F: {pmf[i + 1]:P2}");
            }
            Console.WriteLine($"  > {splitPoints[splitPoints.Length - 1],4}°F: {pmf[pmf.Length - 1]:P2}");

            // Get CDF
            var cdf = sketch.GetCDF(splitPoints);
            Console.WriteLine("\nCumulative Distribution (CDF):");
            for (int i = 0; i < splitPoints.Length; i++)
            {
                Console.WriteLine($"  ≤ {splitPoints[i],4}°F: {cdf[i]:P2}");
            }

            // Serialize and deserialize
            var bytes = sketch.Serialize();
            Console.WriteLine($"\nSerialized size: {bytes.Length} bytes");

            var sketch2 = QuantilesSketch<double>.Deserialize(bytes);
            Console.WriteLine($"Deserialized sketch has N={sketch2.N}");

            // Merge two sketches
            var sketch3 = new QuantilesSketch<double>(128);
            for (int i = 0; i < 10000; i++)
            {
                sketch3.Update(65 + random.NextDouble() * 20);
            }

            sketch.Merge(sketch3);
            Console.WriteLine($"\nAfter merge: N={sketch.N}");
            Console.WriteLine($"New median: {sketch.GetQuantile(0.5):F1}°F");
        }
    }
}

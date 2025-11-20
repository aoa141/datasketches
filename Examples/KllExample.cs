// <copyright file="KllExample.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using DataSketches.Kll;
using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;

namespace DataSketches.Examples
{
    /// <summary>
    /// Comprehensive examples for KLL quantiles sketch usage
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class KllExample
    {
        /// <summary>
        /// Basic usage example: response time quantiles
        /// </summary>
        public static void BasicQuantilesExample()
        {
            Console.WriteLine("=== KLL Basic Quantiles Example ===\n");

            // Create a KLL sketch with default K=200
            var sketch = new KllSketch<double>(k: 200);

            // Simulate API response times in milliseconds
            var random = new Random(42);
            for (int i = 0; i < 100000; i++)
            {
                // Generate response times with some pattern
                // Most are fast (20-100ms), some are slow (100-500ms), rare very slow (500-2000ms)
                double responseTime;
                double r = random.NextDouble();
                if (r < 0.80)
                {
                    // 80% fast responses
                    responseTime = 20 + random.NextDouble() * 80;
                }
                else if (r < 0.95)
                {
                    // 15% medium responses
                    responseTime = 100 + random.NextDouble() * 400;
                }
                else
                {
                    // 5% slow responses
                    responseTime = 500 + random.NextDouble() * 1500;
                }

                sketch.Update(responseTime);
            }

            Console.WriteLine($"Total requests: {sketch.GetN()}");
            Console.WriteLine($"Retained samples: {sketch.GetNumRetained()}");
            Console.WriteLine($"Min response time: {sketch.GetMinItem():F2} ms");
            Console.WriteLine($"Max response time: {sketch.GetMaxItem():F2} ms");

            Console.WriteLine("\nQuantiles (percentiles):");
            double[] percentiles = { 0.50, 0.75, 0.90, 0.95, 0.99 };
            foreach (double p in percentiles)
            {
                double quantile = sketch.GetQuantile(p);
                Console.WriteLine($"  P{p * 100,2:F0}: {quantile,8:F2} ms");
            }

            Console.WriteLine($"\nSketch size: {sketch.Serialize().Length} bytes");
            Console.WriteLine($"Compression ratio: {sketch.GetN() / (double)sketch.GetNumRetained():F1}x");
        }

        /// <summary>
        /// Rank query example
        /// </summary>
        public static void RankQueryExample()
        {
            Console.WriteLine("\n=== KLL Rank Query Example ===\n");

            var sketch = new KllSketch<int>(k: 200);

            // Add test scores (0-100)
            var random = new Random(42);
            for (int i = 0; i < 10000; i++)
            {
                // Generate scores with normal distribution around 75
                double score = 75 + random.NextDouble() * 20 - 10;
                score = Math.Max(0, Math.Min(100, score)); // Clamp to 0-100
                sketch.Update((int)score);
            }

            Console.WriteLine($"Total students: {sketch.GetN()}");
            Console.WriteLine();

            // Find ranks for specific scores
            int[] scores = { 50, 60, 70, 80, 90 };
            foreach (int score in scores)
            {
                double rank = sketch.GetRank(score);
                Console.WriteLine($"Score {score,3}: {rank * 100:F1}th percentile");
            }

            Console.WriteLine("\nInterpretation:");
            Console.WriteLine("A student with score 80 is in the ~75th percentile");
            Console.WriteLine("meaning they scored better than ~75% of students");
        }

        /// <summary>
        /// PMF (Probability Mass Function) example
        /// </summary>
        public static void PMFExample()
        {
            Console.WriteLine("\n=== KLL PMF Example ===\n");

            var sketch = new KllSketch<double>(k: 200);

            // Simulate salary data
            var random = new Random(42);
            for (int i = 0; i < 50000; i++)
            {
                // Generate salaries with log-normal distribution
                double salary = Math.Exp(random.NextDouble() * 1.5 + 10.5) * 1000;
                sketch.Update(salary);
            }

            // Define salary ranges
            double[] splitPoints = { 30000, 50000, 75000, 100000, 150000, 200000 };

            double[] pmf = sketch.GetPMF(splitPoints);

            Console.WriteLine("Salary distribution:");
            Console.WriteLine($"  < $30K:             {pmf[0] * 100:F1}%");
            Console.WriteLine($"  $30K - $50K:        {pmf[1] * 100:F1}%");
            Console.WriteLine($"  $50K - $75K:        {pmf[2] * 100:F1}%");
            Console.WriteLine($"  $75K - $100K:       {pmf[3] * 100:F1}%");
            Console.WriteLine($"  $100K - $150K:      {pmf[4] * 100:F1}%");
            Console.WriteLine($"  $150K - $200K:      {pmf[5] * 100:F1}%");
            Console.WriteLine($"  > $200K:            {pmf[6] * 100:F1}%");

            double sum = pmf.Sum();
            Console.WriteLine($"\nSum of probabilities: {sum:F4} (should be ~1.0)");
        }

        /// <summary>
        /// CDF (Cumulative Distribution Function) example
        /// </summary>
        public static void CDFExample()
        {
            Console.WriteLine("\n=== KLL CDF Example ===\n");

            var sketch = new KllSketch<int>(k: 200);

            // Simulate age distribution
            var random = new Random(42);
            for (int i = 0; i < 100000; i++)
            {
                // Generate ages: mostly 18-65, some outside
                int age;
                double r = random.NextDouble();
                if (r < 0.05)
                {
                    age = random.Next(1, 18); // Children
                }
                else if (r < 0.85)
                {
                    age = random.Next(18, 65); // Working age
                }
                else
                {
                    age = random.Next(65, 100); // Seniors
                }
                sketch.Update(age);
            }

            // Define age boundaries
            int[] ageBoundaries = { 18, 25, 35, 45, 55, 65, 75 };

            double[] cdf = sketch.GetCDF(ageBoundaries);

            Console.WriteLine("Cumulative age distribution:");
            Console.WriteLine($"  Age <= 18:          {cdf[0] * 100:F1}%");
            Console.WriteLine($"  Age <= 25:          {cdf[1] * 100:F1}%");
            Console.WriteLine($"  Age <= 35:          {cdf[2] * 100:F1}%");
            Console.WriteLine($"  Age <= 45:          {cdf[3] * 100:F1}%");
            Console.WriteLine($"  Age <= 55:          {cdf[4] * 100:F1}%");
            Console.WriteLine($"  Age <= 65:          {cdf[5] * 100:F1}%");
            Console.WriteLine($"  Age <= 75:          {cdf[6] * 100:F1}%");
            Console.WriteLine($"  All:                {cdf[7] * 100:F1}%");
        }

        /// <summary>
        /// Serialization example
        /// </summary>
        public static void SerializationExample()
        {
            Console.WriteLine("\n=== KLL Serialization Example ===\n");

            // Create and populate a sketch
            var sketch1 = new KllSketch<float>(k: 200);

            var random = new Random(42);
            for (int i = 0; i < 100000; i++)
            {
                sketch1.Update((float)(random.NextDouble() * 1000));
            }

            Console.WriteLine($"Original sketch:");
            Console.WriteLine($"  N: {sketch1.GetN()}");
            Console.WriteLine($"  Retained: {sketch1.GetNumRetained()}");
            Console.WriteLine($"  Median: {sketch1.GetQuantile(0.5):F2}");

            // Serialize to byte array
            byte[] bytes = sketch1.Serialize();
            Console.WriteLine($"\nSerialized size: {bytes.Length} bytes");

            // Deserialize
            using var stream = new MemoryStream(bytes);
            var sketch2 = KllSketch<float>.Deserialize(stream);

            Console.WriteLine($"\nDeserialized sketch:");
            Console.WriteLine($"  N: {sketch2.GetN()}");
            Console.WriteLine($"  Retained: {sketch2.GetNumRetained()}");
            Console.WriteLine($"  Median: {sketch2.GetQuantile(0.5):F2}");

            // Save to file
            string filename = "kll_sketch.bin";
            File.WriteAllBytes(filename, bytes);
            Console.WriteLine($"\nSaved to: {filename}");

            // Load from file
            byte[] loadedBytes = File.ReadAllBytes(filename);
            using var loadStream = new MemoryStream(loadedBytes);
            var sketch3 = KllSketch<float>.Deserialize(loadStream);
            Console.WriteLine($"Loaded sketch median: {sketch3.GetQuantile(0.5):F2}");

            // Cleanup
            File.Delete(filename);
        }

        /// <summary>
        /// Merge sketches example
        /// </summary>
        public static void MergeSketchesExample()
        {
            Console.WriteLine("\n=== KLL Merge Sketches Example ===\n");

            // Create sketches for different data centers
            var dc1 = new KllSketch<double>(k: 200);
            var dc2 = new KllSketch<double>(k: 200);
            var dc3 = new KllSketch<double>(k: 200);

            var random = new Random(42);

            // DC1: Low latency region (Asia)
            for (int i = 0; i < 30000; i++)
            {
                dc1.Update(10 + random.NextDouble() * 50); // 10-60ms
            }

            // DC2: Medium latency region (Europe)
            for (int i = 0; i < 40000; i++)
            {
                dc2.Update(50 + random.NextDouble() * 80); // 50-130ms
            }

            // DC3: High latency region (distant)
            for (int i = 0; i < 30000; i++)
            {
                dc3.Update(100 + random.NextDouble() * 150); // 100-250ms
            }

            Console.WriteLine("Individual data centers:");
            Console.WriteLine($"DC1: N={dc1.GetN(),6}, P50={dc1.GetQuantile(0.5):F1}ms, P95={dc1.GetQuantile(0.95):F1}ms");
            Console.WriteLine($"DC2: N={dc2.GetN(),6}, P50={dc2.GetQuantile(0.5):F1}ms, P95={dc2.GetQuantile(0.95):F1}ms");
            Console.WriteLine($"DC3: N={dc3.GetN(),6}, P50={dc3.GetQuantile(0.5):F1}ms, P95={dc3.GetQuantile(0.95):F1}ms");

            // Merge all sketches
            var global = new KllSketch<double>(k: 200);
            global.Merge(dc1);
            global.Merge(dc2);
            global.Merge(dc3);

            Console.WriteLine($"\nGlobal (merged):");
            Console.WriteLine($"  N={global.GetN(),6}, P50={global.GetQuantile(0.5):F1}ms, P95={global.GetQuantile(0.95):F1}ms");
            Console.WriteLine($"\nMerged sketch represents worldwide latency distribution");
        }

        /// <summary>
        /// Streaming data example
        /// </summary>
        public static void StreamingDataExample()
        {
            Console.WriteLine("\n=== KLL Streaming Data Example ===\n");

            var sketch = new KllSketch<int>(k: 200);
            var random = new Random(42);

            Console.WriteLine("Processing streaming data...");
            Console.WriteLine();
            Console.WriteLine($"{"Items",-10} {"P50",-8} {"P95",-8} {"P99",-8} {"Retained",-10}");
            Console.WriteLine(new string('-', 50));

            int[] checkpoints = { 1000, 5000, 10000, 50000, 100000, 500000, 1000000 };
            int checkpointIndex = 0;
            int count = 0;

            // Process 1 million values
            for (int i = 0; i < 1000000; i++)
            {
                // Simulate sensor readings (temperature in Fahrenheit)
                int temperature = 65 + random.Next(-10, 25); // 55-90°F
                sketch.Update(temperature);
                count++;

                // Report at checkpoints
                if (checkpointIndex < checkpoints.Length && count >= checkpoints[checkpointIndex])
                {
                    Console.WriteLine($"{count,-10} {sketch.GetQuantile(0.50),-8} {sketch.GetQuantile(0.95),-8} " +
                                    $"{sketch.GetQuantile(0.99),-8} {sketch.GetNumRetained(),-10}");
                    checkpointIndex++;
                }
            }

            Console.WriteLine($"\nFinal sketch uses only {sketch.GetNumRetained()} samples to represent {sketch.GetN()} items");
            Console.WriteLine($"Memory saved: {(1 - (double)sketch.GetNumRetained() / sketch.GetN()) * 100:F1}%");
        }

        /// <summary>
        /// Accuracy analysis example
        /// </summary>
        public static void AccuracyAnalysisExample()
        {
            Console.WriteLine("\n=== KLL Accuracy Analysis ===\n");

            const int numItems = 100000;
            ushort[] kValues = { 100, 200, 400, 800 };

            Console.WriteLine($"Analyzing accuracy with {numItems} items:\n");
            Console.WriteLine($"{"K",-6} {"Retained",-10} {"Size",-12} {"Error",-10} {"Rank Error",-12}");
            Console.WriteLine(new string('-', 60));

            foreach (ushort k in kValues)
            {
                var sketch = new KllSketch<int>(k: k);

                // Add sequential integers
                for (int i = 0; i < numItems; i++)
                {
                    sketch.Update(i);
                }

                // Test accuracy at median
                int trueMedian = numItems / 2;
                int estimatedMedian = sketch.GetQuantile(0.5);
                int error = Math.Abs(estimatedMedian - trueMedian);
                double rankError = sketch.GetNormalizedRankError(pmf: false);

                int size = sketch.Serialize().Length;

                Console.WriteLine($"{k,-6} {sketch.GetNumRetained(),-10} {size,-12} {error,-10} {rankError,-12:F4}");
            }

            Console.WriteLine("\nNote: Larger K = more accuracy but more memory");
            Console.WriteLine("      K=200 is a good default choice");
        }

        /// <summary>
        /// Real-world application: SLA monitoring
        /// </summary>
        public static void SLAMonitoringExample()
        {
            Console.WriteLine("\n=== SLA Monitoring Example ===\n");

            var sketch = new KllSketch<double>(k: 200);
            var random = new Random(42);

            // Simulate one day of API requests
            int totalRequests = 0;
            int slaViolations = 0;
            const double slaThreshold = 200.0; // 200ms SLA

            for (int hour = 0; hour < 24; hour++)
            {
                // Different traffic patterns by hour
                int requestsThisHour = 1000 + random.Next(3000);

                for (int i = 0; i < requestsThisHour; i++)
                {
                    // Generate latency (worse during peak hours)
                    double baseLatency = (hour >= 9 && hour <= 17) ? 100 : 50;
                    double latency = baseLatency + random.NextDouble() * 150;

                    // Occasional spikes
                    if (random.NextDouble() < 0.02)
                    {
                        latency += 200 + random.NextDouble() * 300;
                    }

                    sketch.Update(latency);
                    totalRequests++;

                    if (latency > slaThreshold)
                    {
                        slaViolations++;
                    }
                }
            }

            Console.WriteLine($"24-hour SLA Report:");
            Console.WriteLine($"  Total requests: {totalRequests:N0}");
            Console.WriteLine($"  SLA threshold: {slaThreshold}ms");
            Console.WriteLine();

            Console.WriteLine("Latency percentiles:");
            double[] percentiles = { 0.50, 0.90, 0.95, 0.99, 0.999 };
            foreach (double p in percentiles)
            {
                double latency = sketch.GetQuantile(p);
                string status = latency <= slaThreshold ? "✓" : "✗";
                Console.WriteLine($"  P{p * 100,5:F1}: {latency,7:F2}ms {status}");
            }

            Console.WriteLine();
            double slaCompliance = (1.0 - (double)slaViolations / totalRequests) * 100;
            Console.WriteLine($"SLA compliance: {slaCompliance:F2}%");
            Console.WriteLine($"SLA violations: {slaViolations:N0} ({(double)slaViolations / totalRequests * 100:F2}%)");

            // Determine if we meet "4 nines" (99.99% availability)
            bool meetsSLA = slaCompliance >= 99.99;
            Console.WriteLine($"\nMeets 99.99% SLA: {(meetsSLA ? "YES ✓" : "NO ✗")}");
        }

        /// <summary>
        /// Run all examples
        /// </summary>
        public static void RunAllExamples()
        {
            BasicQuantilesExample();
            RankQueryExample();
            PMFExample();
            CDFExample();
            SerializationExample();
            MergeSketchesExample();
            StreamingDataExample();
            AccuracyAnalysisExample();
            SLAMonitoringExample();
        }
    }
}

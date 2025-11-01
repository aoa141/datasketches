using System;
using System.Linq;
using DataSketches.TDigest;

namespace DataSketches.Examples
{
    /// <summary>
    /// Example usage of T-Digest for streaming quantile estimation.
    /// </summary>
    public class TDigestExample
    {
        public static void Run()
        {
            Console.WriteLine("=== T-Digest Example ===\n");

            // Create a T-Digest with default compression (200)
            var tdigest = new TDigest(200);

            Console.WriteLine("Created T-Digest with K=200 (compression parameter)\n");

            // Simulate streaming latency measurements (in milliseconds)
            var random = new Random(42);
            int numSamples = 100000;

            Console.WriteLine($"Streaming {numSamples} latency measurements...");

            for (int i = 0; i < numSamples; i++)
            {
                // Generate latencies with a log-normal distribution
                double latency = Math.Exp(random.NextDouble() * 2 + 2); // 7-403ms range
                tdigest.Update(latency);
            }

            Console.WriteLine($"Total samples: {tdigest.TotalWeight}\n");

            // Query percentiles
            Console.WriteLine("Latency Percentiles:");
            double[] percentiles = { 0.50, 0.75, 0.90, 0.95, 0.99, 0.999 };

            foreach (var p in percentiles)
            {
                var value = tdigest.GetQuantile(p);
                Console.WriteLine($"  P{p * 100,5:F1}: {value,8:F2} ms");
            }

            Console.WriteLine($"\n  Min  : {tdigest.MinValue,8:F2} ms");
            Console.WriteLine($"  Max  : {tdigest.MaxValue,8:F2} ms");

            // Get rank of a specific value
            double targetLatency = 100.0;
            double rank = tdigest.GetRank(targetLatency);
            Console.WriteLine($"\nRank of {targetLatency}ms: {rank:P2}");

            // Get PMF for latency buckets
            double[] splitPoints = { 20, 50, 100, 200, 500 };
            var pmf = tdigest.GetPMF(splitPoints);

            Console.WriteLine("\nLatency Distribution (PMF):");
            Console.WriteLine($"  < {splitPoints[0],5} ms: {pmf[0]:P2}");
            for (int i = 0; i < splitPoints.Length - 1; i++)
            {
                Console.WriteLine($"  {splitPoints[i],5} - {splitPoints[i + 1],5} ms: {pmf[i + 1]:P2}");
            }
            Console.WriteLine($"  > {splitPoints[splitPoints.Length - 1],5} ms: {pmf[pmf.Length - 1]:P2}");

            // Serialize and deserialize
            var bytes = tdigest.Serialize();
            Console.WriteLine($"\nSerialized size: {bytes.Length} bytes");

            var tdigest2 = TDigest.Deserialize(bytes);
            Console.WriteLine($"Deserialized T-Digest has {tdigest2.TotalWeight} samples");

            // Merge two T-Digests
            var tdigest3 = new TDigest(200);
            for (int i = 0; i < 10000; i++)
            {
                tdigest3.Update(random.NextDouble() * 100);
            }

            tdigest.Merge(tdigest3);
            Console.WriteLine($"\nAfter merge: {tdigest.TotalWeight} total samples");
            Console.WriteLine($"New P95: {tdigest.GetQuantile(0.95):F2} ms");
        }
    }
}

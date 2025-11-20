// <copyright file="HllExample.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>

using DataSketches.Hll;
using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace DataSketches.Examples
{
    /// <summary>
    /// Comprehensive examples for HLL (HyperLogLog) sketch usage
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class HllExample
    {
        /// <summary>
        /// Basic usage example: counting unique visitors
        /// </summary>
        public static void BasicUniqueCount()
        {
            Console.WriteLine("=== HLL Basic Unique Count Example ===\n");

            // Create an HLL sketch with lgK=12 (K=4096)
            // This gives approximately 1.63% error at 99% confidence
            var sketch = new HllSketch(lgConfigK: 12, tgtType: TargetHllType.Hll4);

            // Simulate user IDs visiting a website
            for (int i = 0; i < 100000; i++)
            {
                sketch.Update($"user_{i}");
            }

            // Simulate some duplicate visits
            for (int i = 0; i < 10000; i++)
            {
                sketch.Update($"user_{i}"); // These are duplicates
            }

            // Get the cardinality estimate
            double estimate = sketch.GetEstimate();
            Console.WriteLine($"Total updates: 110,000");
            Console.WriteLine($"Unique users estimate: {estimate:F0}");
            Console.WriteLine($"Actual unique users: 100,000");
            Console.WriteLine($"Error: {Math.Abs(estimate - 100000):F0} ({Math.Abs(estimate - 100000) / 100000 * 100:F2}%)");

            // Get error bounds
            Console.WriteLine($"\nError bounds:");
            Console.WriteLine($"  Lower bound (1σ): {sketch.GetLowerBound(1):F0}");
            Console.WriteLine($"  Upper bound (1σ): {sketch.GetUpperBound(1):F0}");
            Console.WriteLine($"  Lower bound (2σ): {sketch.GetLowerBound(2):F0}");
            Console.WriteLine($"  Upper bound (2σ): {sketch.GetUpperBound(2):F0}");

            Console.WriteLine($"\nSketch size: {sketch.GetUpdatableSerializationBytes()} bytes");
        }

        /// <summary>
        /// Serialization and deserialization example
        /// </summary>
        public static void SerializationExample()
        {
            Console.WriteLine("\n=== HLL Serialization Example ===\n");

            // Create and populate a sketch
            var sketch1 = new HllSketch(lgConfigK: 10);

            for (int i = 0; i < 50000; i++)
            {
                sketch1.Update(i);
            }

            Console.WriteLine($"Original sketch estimate: {sketch1.GetEstimate():F0}");

            // Serialize to byte array (compact form)
            byte[] bytes = sketch1.SerializeCompact();
            Console.WriteLine($"Serialized size: {bytes.Length} bytes");

            // Deserialize
            var sketch2 = HllSketch.Deserialize(bytes);
            Console.WriteLine($"Deserialized sketch estimate: {sketch2.GetEstimate():F0}");

            // Save to file
            string filename = "hll_sketch.bin";
            File.WriteAllBytes(filename, bytes);
            Console.WriteLine($"\nSaved to file: {filename}");

            // Load from file
            byte[] loadedBytes = File.ReadAllBytes(filename);
            var sketch3 = HllSketch.Deserialize(loadedBytes);
            Console.WriteLine($"Loaded sketch estimate: {sketch3.GetEstimate():F0}");

            // Cleanup
            File.Delete(filename);
        }

        /// <summary>
        /// Comparing different HLL types
        /// </summary>
        public static void CompareHllTypes()
        {
            Console.WriteLine("\n=== Comparing HLL Types ===\n");

            const int lgK = 12;
            const int numItems = 100000;

            // Create sketches with different types
            var hll4 = new HllSketch(lgK, TargetHllType.Hll4);
            var hll6 = new HllSketch(lgK, TargetHllType.Hll6);
            var hll8 = new HllSketch(lgK, TargetHllType.Hll8);

            // Update all sketches with the same data
            for (int i = 0; i < numItems; i++)
            {
                hll4.Update(i);
                hll6.Update(i);
                hll8.Update(i);
            }

            Console.WriteLine($"Actual unique items: {numItems}");
            Console.WriteLine();

            Console.WriteLine("HLL-4 (most compact):");
            Console.WriteLine($"  Estimate: {hll4.GetEstimate():F0}");
            Console.WriteLine($"  Size: {hll4.GetCompactSerializationBytes()} bytes");
            Console.WriteLine($"  Error: {Math.Abs(hll4.GetEstimate() - numItems) / numItems * 100:F2}%");
            Console.WriteLine();

            Console.WriteLine("HLL-6 (balanced):");
            Console.WriteLine($"  Estimate: {hll6.GetEstimate():F0}");
            Console.WriteLine($"  Size: {hll6.GetCompactSerializationBytes()} bytes");
            Console.WriteLine($"  Error: {Math.Abs(hll6.GetEstimate() - numItems) / numItems * 100:F2}%");
            Console.WriteLine();

            Console.WriteLine("HLL-8 (fastest):");
            Console.WriteLine($"  Estimate: {hll8.GetEstimate():F0}");
            Console.WriteLine($"  Size: {hll8.GetCompactSerializationBytes()} bytes");
            Console.WriteLine($"  Error: {Math.Abs(hll8.GetEstimate() - numItems) / numItems * 100:F2}%");
        }

        /// <summary>
        /// Distributed counting example
        /// </summary>
        public static void DistributedCountingExample()
        {
            Console.WriteLine("\n=== Distributed Counting Example ===\n");

            const byte lgK = 12;

            // Simulate 5 distributed servers, each tracking users
            var sketches = new HllSketch[5];
            for (int i = 0; i < 5; i++)
            {
                sketches[i] = new HllSketch(lgK);
            }

            // Each server sees different (and some overlapping) users
            // Server 0: users 0-19,999
            for (int i = 0; i < 20000; i++)
            {
                sketches[0].Update($"user_{i}");
            }

            // Server 1: users 10,000-29,999 (overlaps with server 0)
            for (int i = 10000; i < 30000; i++)
            {
                sketches[1].Update($"user_{i}");
            }

            // Server 2: users 20,000-39,999 (overlaps with server 1)
            for (int i = 20000; i < 40000; i++)
            {
                sketches[2].Update($"user_{i}");
            }

            // Server 3: users 30,000-49,999 (overlaps with server 2)
            for (int i = 30000; i < 50000; i++)
            {
                sketches[3].Update($"user_{i}");
            }

            // Server 4: users 40,000-59,999 (overlaps with server 3)
            for (int i = 40000; i < 60000; i++)
            {
                sketches[4].Update($"user_{i}");
            }

            // Print individual estimates
            for (int i = 0; i < 5; i++)
            {
                Console.WriteLine($"Server {i} estimate: {sketches[i].GetEstimate():F0}");
            }

            // Note: Full union functionality would be implemented in HllUnion class
            // For demonstration, we show the concept
            Console.WriteLine($"\nTotal unique users across all servers: 60,000");
            Console.WriteLine("(Union operation would merge all sketches to get this estimate)");
        }

        /// <summary>
        /// Real-time analytics example
        /// </summary>
        public static void RealTimeAnalyticsExample()
        {
            Console.WriteLine("\n=== Real-Time Analytics Example ===\n");

            // Track unique IP addresses per minute
            var currentMinute = new HllSketch(lgConfigK: 10);
            var lastHour = new HllSketch(lgConfigK: 12);
            var last24Hours = new HllSketch(lgConfigK: 14);

            // Simulate traffic
            var random = new Random(42);
            int totalRequests = 0;

            for (int minute = 0; minute < 60; minute++)
            {
                // Simulate requests in this minute
                int requestsThisMinute = random.Next(1000, 2000);
                totalRequests += requestsThisMinute;

                for (int req = 0; req < requestsThisMinute; req++)
                {
                    // Generate IP address
                    string ip = $"{random.Next(256)}.{random.Next(256)}.{random.Next(256)}.{random.Next(256)}";

                    currentMinute.Update(ip);
                    lastHour.Update(ip);
                    last24Hours.Update(ip);
                }

                // Report every 10 minutes
                if ((minute + 1) % 10 == 0)
                {
                    Console.WriteLine($"\nMinute {minute + 1}:");
                    Console.WriteLine($"  Current minute unique IPs: {currentMinute.GetEstimate():F0}");
                    Console.WriteLine($"  Last hour unique IPs: {lastHour.GetEstimate():F0}");
                    Console.WriteLine($"  Total requests: {totalRequests}");

                    // Reset current minute sketch
                    currentMinute.Reset();
                }
            }

            Console.WriteLine($"\n24-hour unique IPs estimate: {last24Hours.GetEstimate():F0}");
        }

        /// <summary>
        /// Accuracy vs size tradeoff example
        /// </summary>
        public static void AccuracySizeTradeoffExample()
        {
            Console.WriteLine("\n=== Accuracy vs Size Tradeoff ===\n");

            const int numItems = 100000;
            byte[] lgKValues = { 8, 10, 12, 14, 16 };

            Console.WriteLine($"Testing with {numItems} unique items:\n");
            Console.WriteLine($"{"lgK",-6} {"K",-8} {"Size (bytes)",-15} {"Estimate",-12} {"Error %",-10}");
            Console.WriteLine(new string('-', 60));

            foreach (byte lgK in lgKValues)
            {
                var sketch = new HllSketch(lgK, TargetHllType.Hll4);

                for (int i = 0; i < numItems; i++)
                {
                    sketch.Update(i);
                }

                int k = 1 << lgK;
                double estimate = sketch.GetEstimate();
                double errorPct = Math.Abs(estimate - numItems) / numItems * 100;
                int size = sketch.GetCompactSerializationBytes();

                Console.WriteLine($"{lgK,-6} {k,-8} {size,-15} {estimate,-12:F0} {errorPct,-10:F3}");
            }

            Console.WriteLine("\nNote: Larger K = more accuracy but more memory");
            Console.WriteLine("      lgK=12 (K=4096) is a good default choice");
        }

        /// <summary>
        /// Run all examples
        /// </summary>
        public static void RunAllExamples()
        {
            BasicUniqueCount();
            SerializationExample();
            CompareHllTypes();
            DistributedCountingExample();
            RealTimeAnalyticsExample();
            AccuracySizeTradeoffExample();
        }
    }
}

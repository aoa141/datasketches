using System;
using DataSketches.Filters;

namespace DataSketches.Examples
{
    /// <summary>
    /// Example usage of Bloom Filter for set membership testing.
    /// </summary>
    public class BloomFilterExample
    {
        public static void Run()
        {
            Console.WriteLine("=== Bloom Filter Example ===\n");

            // Create a Bloom filter for 10,000 expected items with 1% false positive rate
            var filter = BloomFilter.Builder.CreateByAccuracy(10000, 0.01);

            Console.WriteLine($"Created filter with:");
            Console.WriteLine($"  Capacity: {filter.Capacity} bits");
            Console.WriteLine($"  Num Hashes: {filter.NumHashes}");
            Console.WriteLine($"  Expected FPP: 1%\n");

            // Add items to the filter - simulate a blocklist
            string[] blocked = {
                "spam@example.com",
                "malicious@bad.com",
                "phishing@evil.net",
                "scam@fraud.org"
            };

            foreach (var email in blocked)
            {
                filter.Update(email);
                Console.WriteLine($"Added: {email}");
            }

            Console.WriteLine($"\nBits used: {filter.BitsUsed} / {filter.Capacity}");
            Console.WriteLine($"Fill ratio: {(double)filter.BitsUsed / filter.Capacity:P2}\n");

            // Test membership queries
            Console.WriteLine("Testing membership queries:");

            // Should return true (items are in filter)
            foreach (var email in blocked)
            {
                bool present = filter.Query(email);
                Console.WriteLine($"  {email,25}: {(present ? "BLOCKED" : "allowed")}");
            }

            // Should return false (items not in filter)
            string[] allowed = {
                "user@example.com",
                "admin@company.com",
                "info@business.net"
            };

            foreach (var email in allowed)
            {
                bool present = filter.Query(email);
                Console.WriteLine($"  {email,25}: {(present ? "BLOCKED" : "allowed")}");
            }

            // Serialize and deserialize
            var bytes = filter.Serialize();
            Console.WriteLine($"\nSerialized size: {bytes.Length} bytes");

            var filter2 = BloomFilter.Deserialize(bytes);
            Console.WriteLine($"Deserialized filter has {filter2.BitsUsed} bits used");

            // Union operation
            var filter3 = BloomFilter.Builder.CreateBySize(filter.Capacity, filter.NumHashes);
            filter3.Update("another@bad.com");

            filter.UnionWith(filter3);
            Console.WriteLine($"\nAfter union: {filter.BitsUsed} bits used");
            Console.WriteLine($"Union contains 'another@bad.com': {filter.Query("another@bad.com")}");
        }
    }
}

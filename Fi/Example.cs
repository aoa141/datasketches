using System;
using System.Linq;
using DataSketches.Fi;

namespace DataSketches.Examples
{
    /// <summary>
    /// Example usage of Frequent Items Sketch.
    /// </summary>
    public class FrequentItemsExample
    {
        public static void Run()
        {
            Console.WriteLine("=== Frequent Items Sketch Example ===\n");

            // Create sketch with lg_max_map_size = 10 (max 1024 items)
            var sketch = new FrequentItemsSketch<string>(10);

            Console.WriteLine($"Created sketch with epsilon = {sketch.Epsilon:F6}\n");

            // Simulate streaming product purchases
            string[] products = {
                "iPhone", "Samsung Galaxy", "iPad", "MacBook",
                "AirPods", "Surface", "Pixel", "Kindle"
            };

            var random = new Random(42);
            int numPurchases = 50000;

            Console.WriteLine($"Processing {numPurchases} purchases...\n");

            // Generate purchases with Zipfian distribution (some products much more popular)
            for (int i = 0; i < numPurchases; i++)
            {
                int index = (int)(Math.Pow(random.NextDouble(), 2) * products.Length);
                sketch.Update(products[index], 1);
            }

            Console.WriteLine($"Total weight: {sketch.TotalWeight}");
            Console.WriteLine($"Active items: {sketch.NumActiveItems}");
            Console.WriteLine($"Max error: {sketch.MaximumError}\n");

            // Get top items with no false negatives (may include some false positives)
            var threshold = sketch.TotalWeight * 0.05; // Items with > 5% frequency
            var topItems = sketch.GetFrequentItems(FrequentItemsErrorType.NoFalseNegatives, threshold);

            Console.WriteLine($"Products with > 5% purchase frequency:");
            Console.WriteLine("(Using NO_FALSE_NEGATIVES mode)\n");

            foreach (var item in topItems.Take(10))
            {
                var percent = (double)item.Estimate / sketch.TotalWeight * 100;
                Console.WriteLine($"  {item.Item,-20} Est: {item.Estimate,6} ({percent:F2}%)  Bounds: [{item.LowerBound}, {item.UpperBound}]");
            }

            // Query specific items
            Console.WriteLine("\nSpecific product queries:");
            foreach (var product in new[] { "iPhone", "Surface", "Pixel" })
            {
                var estimate = sketch.GetEstimate(product);
                var lower = sketch.GetLowerBound(product);
                var upper = sketch.GetUpperBound(product);
                var percent = (double)estimate / sketch.TotalWeight * 100;

                Console.WriteLine($"  {product,-20} Est: {estimate,6} ({percent:F2}%)  Bounds: [{lower}, {upper}]");
            }

            // Serialize and deserialize
            var bytes = sketch.Serialize();
            Console.WriteLine($"\nSerialized size: {bytes.Length} bytes");

            var sketch2 = FrequentItemsSketch<string>.Deserialize(bytes);
            Console.WriteLine($"Deserialized sketch has {sketch2.TotalWeight} total weight");

            // Merge two sketches
            var sketch3 = new FrequentItemsSketch<string>(10);
            sketch3.Update("iPhone", 1000);
            sketch3.Update("MacBook", 500);

            sketch.Merge(sketch3);
            Console.WriteLine($"\nAfter merge: {sketch.TotalWeight} total weight");
            Console.WriteLine($"iPhone estimate: {sketch.GetEstimate("iPhone")}");
        }
    }
}

using System;
using DataSketches.Count;

namespace DataSketches.Examples
{
    /// <summary>
    /// Example usage of CountMin Sketch for frequency estimation.
    /// </summary>
    public class CountMinExample
    {
        public static void Run()
        {
            Console.WriteLine("=== CountMin Sketch Example ===\n");

            // Create a CountMin sketch with 5% error rate and 95% confidence
            var numBuckets = CountMinSketch<long>.SuggestNumBuckets(0.05); // 5% error
            var numHashes = CountMinSketch<long>.SuggestNumHashes(0.95);   // 95% confidence

            var sketch = new CountMinSketch<long>((byte)numHashes, numBuckets);

            Console.WriteLine($"Created sketch with {numHashes} hashes and {numBuckets} buckets");
            Console.WriteLine($"Expected relative error: {sketch.RelativeError:P2}\n");

            // Simulate streaming data - web page views
            string[] pages = { "home", "about", "products", "contact" };
            var random = new Random(42);

            // Generate 10,000 page views with different frequencies
            for (int i = 0; i < 10000; i++)
            {
                string page;
                int rand = random.Next(100);

                if (rand < 50)      // 50% home page
                    page = "home";
                else if (rand < 75) // 25% products
                    page = "products";
                else if (rand < 90) // 15% about
                    page = "about";
                else               // 10% contact
                    page = "contact";

                sketch.Update(page, 1);
            }

            Console.WriteLine("After processing 10,000 page views:\n");

            // Query frequencies
            foreach (var page in pages)
            {
                var estimate = sketch.GetEstimate(page);
                var lowerBound = sketch.GetLowerBound(page);
                var upperBound = sketch.GetUpperBound(page);

                Console.WriteLine($"{page,10}: estimate = {estimate,5}, range = [{lowerBound,5}, {upperBound,5}]");
            }

            Console.WriteLine($"\nTotal weight: {sketch.TotalWeight}");

            // Serialize and deserialize
            var bytes = sketch.Serialize();
            Console.WriteLine($"\nSerialized size: {bytes.Length} bytes");

            var sketch2 = CountMinSketch<long>.Deserialize(bytes);
            Console.WriteLine($"Deserialized sketch has {sketch2.TotalWeight} total weight");

            // Merge two sketches
            var sketch3 = new CountMinSketch<long>((byte)numHashes, numBuckets);
            sketch3.Update("home", 100);
            sketch3.Update("products", 50);

            sketch.Merge(sketch3);
            Console.WriteLine($"\nAfter merge, total weight: {sketch.TotalWeight}");
            Console.WriteLine($"Home page estimate: {sketch.GetEstimate("home")}");
        }
    }
}

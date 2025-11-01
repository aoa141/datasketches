# DataSketches C# - Quick Start Guide

## Installation

Add the DataSketches files to your C# project:

```bash
# Copy files to your project
cp -r c:/askideas/datasketches/Hll YourProject/DataSketches/
cp -r c:/askideas/datasketches/Kll YourProject/DataSketches/
```

Or reference the namespace in your project.

---

## HLL (HyperLogLog) - Unique Counting

### Quick Example

```csharp
using DataSketches.Hll;

// Count unique users
var sketch = new HllSketch(lgConfigK: 12);

sketch.Update("user_1");
sketch.Update("user_2");
sketch.Update("user_1"); // duplicate

Console.WriteLine($"Unique: {sketch.GetEstimate():F0}"); // ~2
```

### Common Use Cases

#### 1. Web Analytics
```csharp
var dailyVisitors = new HllSketch(12);
foreach (var pageview in GetPageViews())
{
    dailyVisitors.Update(pageview.UserId);
}
Console.WriteLine($"Daily visitors: {dailyVisitors.GetEstimate():F0}");
```

#### 2. Distributed Counting
```csharp
// Server 1
var server1 = new HllSketch(12);
server1.Update("user_A");
server1.Update("user_B");

// Server 2
var server2 = new HllSketch(12);
server2.Update("user_B");
server2.Update("user_C");

// Combine (requires HllUnion - simplified here)
// var combined = HllUnion.Merge(server1, server2);
```

#### 3. Save/Load Sketch
```csharp
// Save
byte[] bytes = sketch.SerializeCompact();
File.WriteAllBytes("sketch.bin", bytes);

// Load
byte[] loaded = File.ReadAllBytes("sketch.bin");
var sketch2 = HllSketch.Deserialize(loaded);
```

### Configuration Guide

| lgK | K (buckets) | Memory | Error (1σ) | Use Case |
|-----|-------------|--------|------------|----------|
| 8   | 256        | ~256B  | ~3.25%     | Testing |
| 10  | 1,024      | ~1KB   | ~1.63%     | Small datasets |
| 12  | 4,096      | ~4KB   | ~0.81%     | Default choice |
| 14  | 16,384     | ~16KB  | ~0.41%     | High accuracy |
| 16  | 65,536     | ~64KB  | ~0.20%     | Very high accuracy |

### HLL Type Comparison

```csharp
// Hll4 - Most compact (~K/2 bytes)
var hll4 = new HllSketch(12, TargetHllType.Hll4);

// Hll6 - Balanced (~3K/4 bytes)
var hll6 = new HllSketch(12, TargetHllType.Hll6);

// Hll8 - Fastest (~K bytes)
var hll8 = new HllSketch(12, TargetHllType.Hll8);
```

---

## KLL - Quantiles and Distributions

### Quick Example

```csharp
using DataSketches.Kll;

// Track response times
var sketch = new KllSketch<double>(k: 200);

sketch.Update(45.2);
sketch.Update(123.7);
sketch.Update(89.1);

// Get median
double p50 = sketch.GetQuantile(0.50);
Console.WriteLine($"Median: {p50:F2}ms");
```

### Common Use Cases

#### 1. Performance Monitoring
```csharp
var latencies = new KllSketch<double>(200);

foreach (var request in GetRequests())
{
    latencies.Update(request.ResponseTime);
}

Console.WriteLine($"P50: {latencies.GetQuantile(0.50):F2}ms");
Console.WriteLine($"P95: {latencies.GetQuantile(0.95):F2}ms");
Console.WriteLine($"P99: {latencies.GetQuantile(0.99):F2}ms");
```

#### 2. Find Outliers
```csharp
var values = new KllSketch<int>(200);
// ... add data ...

// Find what value is at 99th percentile
int threshold = values.GetQuantile(0.99);
Console.WriteLine($"99th percentile threshold: {threshold}");
```

#### 3. Distribution Analysis
```csharp
var ages = new KllSketch<int>(200);
// ... add ages ...

// Get distribution
double[] splitPoints = { 18, 25, 35, 50, 65 };
double[] pmf = ages.GetPMF(splitPoints);

Console.WriteLine($"Under 18: {pmf[0] * 100:F1}%");
Console.WriteLine($"18-25: {pmf[1] * 100:F1}%");
Console.WriteLine($"25-35: {pmf[2] * 100:F1}%");
// ... etc
```

#### 4. Merge Sketches
```csharp
var east = new KllSketch<double>(200);
var west = new KllSketch<double>(200);

// ... populate with data ...

// Combine
var global = new KllSketch<double>(200);
global.Merge(east);
global.Merge(west);

Console.WriteLine($"Global P95: {global.GetQuantile(0.95):F2}");
```

### Configuration Guide

| K   | Memory | Error | Use Case |
|-----|--------|-------|----------|
| 100 | ~1KB   | ~2.3% | Testing |
| 200 | ~2KB   | ~1.6% | Default (recommended) |
| 400 | ~4KB   | ~1.1% | Higher accuracy |
| 800 | ~8KB   | ~0.8% | Very high accuracy |

### Understanding Rank vs Quantile

```csharp
var sketch = new KllSketch<int>(200);
// Add values 0-999
for (int i = 0; i < 1000; i++) sketch.Update(i);

// Quantile: "What value is at rank 0.5?"
int median = sketch.GetQuantile(0.5);  // ~500

// Rank: "What rank is value 500?"
double rank = sketch.GetRank(500);     // ~0.5
```

---

## Choosing Between HLL and KLL

### Use HLL when:
- ✅ You need **cardinality** (count of unique items)
- ✅ You only care about "how many distinct"
- ✅ You need extremely compact size
- ✅ Examples: unique visitors, distinct IPs, unique products

### Use KLL when:
- ✅ You need **quantiles** (percentiles, median)
- ✅ You need **distribution** (PMF, CDF)
- ✅ You want min/max values
- ✅ Examples: latency monitoring, value distributions, ranking

### Both support:
- ✅ Streaming data (handle billions of items)
- ✅ Serialization (save/load)
- ✅ Merging (combine multiple sketches)
- ✅ Error bounds (probabilistic guarantees)

---

## Real-World Examples

### Example 1: API Monitoring Dashboard

```csharp
// Track both unique users AND latency
var uniqueUsers = new HllSketch(12);
var latencies = new KllSketch<double>(200);

foreach (var request in GetApiRequests())
{
    uniqueUsers.Update(request.UserId);
    latencies.Update(request.DurationMs);
}

// Dashboard metrics
Console.WriteLine("=== API Dashboard ===");
Console.WriteLine($"Unique users: {uniqueUsers.GetEstimate():F0}");
Console.WriteLine($"Median latency: {latencies.GetQuantile(0.50):F2}ms");
Console.WriteLine($"P95 latency: {latencies.GetQuantile(0.95):F2}ms");
Console.WriteLine($"P99 latency: {latencies.GetQuantile(0.99):F2}ms");
```

### Example 2: Daily Report

```csharp
public class DailyReport
{
    private HllSketch _uniqueVisitors = new HllSketch(12);
    private KllSketch<double> _pageTimes = new KllSketch<double>(200);
    private KllSketch<double> _checkoutValues = new KllSketch<double>(200);

    public void RecordPageView(string userId, double loadTime)
    {
        _uniqueVisitors.Update(userId);
        _pageTimes.Update(loadTime);
    }

    public void RecordPurchase(double amount)
    {
        _checkoutValues.Update(amount);
    }

    public void PrintReport()
    {
        Console.WriteLine($"Daily Report:");
        Console.WriteLine($"  Visitors: {_uniqueVisitors.GetEstimate():F0}");
        Console.WriteLine($"  Median page load: {_pageTimes.GetQuantile(0.5):F2}s");
        Console.WriteLine($"  Median purchase: ${_checkoutValues.GetQuantile(0.5):F2}");

        // Save for historical analysis
        File.WriteAllBytes($"report_{DateTime.Now:yyyy-MM-dd}.bin",
            _uniqueVisitors.SerializeCompact());
    }
}
```

### Example 3: Multi-Datacenter Aggregation

```csharp
public class GlobalMetrics
{
    public static void AggregateDatacenters()
    {
        var sketches = new[]
        {
            LoadSketch("dc-us-east.bin"),
            LoadSketch("dc-us-west.bin"),
            LoadSketch("dc-eu.bin"),
            LoadSketch("dc-asia.bin")
        };

        // Merge all
        var global = new KllSketch<double>(200);
        foreach (var sketch in sketches)
        {
            global.Merge(sketch);
        }

        Console.WriteLine($"Global P99 latency: {global.GetQuantile(0.99):F2}ms");
    }

    private static KllSketch<double> LoadSketch(string file)
    {
        byte[] bytes = File.ReadAllBytes(file);
        using var stream = new MemoryStream(bytes);
        return KllSketch<double>.Deserialize(stream);
    }
}
```

---

## Error Handling

### HLL Errors
```csharp
try
{
    var sketch = new HllSketch(25); // lgK too large!
}
catch (ArgumentException ex)
{
    Console.WriteLine($"Invalid lgK: {ex.Message}");
}

// Valid range: lgK must be 4-21
var valid = new HllSketch(12);
```

### KLL Errors
```csharp
var sketch = new KllSketch<int>(200);

try
{
    double quantile = sketch.GetQuantile(0.5); // Empty!
}
catch (InvalidOperationException ex)
{
    Console.WriteLine("Sketch is empty");
}

// Always check
if (!sketch.IsEmpty())
{
    double q = sketch.GetQuantile(0.5);
}
```

---

## Performance Tips

### HLL Tips
1. **Choose appropriate lgK**: Default lgK=12 is good for most cases
2. **Use Hll4 for storage**: Most compact when saving to disk
3. **Use Hll8 for speed**: Fastest updates in hot path
4. **Batch updates**: Update multiple items before querying

### KLL Tips
1. **Default K=200**: Good balance of accuracy and memory
2. **Pre-size for known distributions**: Larger K for more accuracy
3. **Merge carefully**: Merge smaller sketches into larger ones
4. **Cache sorted view**: If doing many queries, sorted view is lazy

---

## Common Patterns

### Pattern 1: Rolling Window
```csharp
// Keep last hour of unique visitors
var windows = new Queue<HllSketch>();
var currentMinute = new HllSketch(12);

void OnMinuteTick()
{
    windows.Enqueue(currentMinute);
    if (windows.Count > 60) windows.Dequeue();
    currentMinute = new HllSketch(12);
}

// Approximate hourly uniques (would need HllUnion for exact)
```

### Pattern 2: Hierarchical Aggregation
```csharp
// Minute → Hour → Day
var minuteSketch = new KllSketch<double>(200);
var hourSketch = new KllSketch<double>(200);
var daySketch = new KllSketch<double>(200);

void OnMinuteEnd()
{
    hourSketch.Merge(minuteSketch);
    minuteSketch = new KllSketch<double>(200);
}

void OnHourEnd()
{
    daySketch.Merge(hourSketch);
    hourSketch = new KllSketch<double>(200);
}
```

---

## Troubleshooting

**Q: My HLL estimate seems off**
- A: Check that you're not double-counting items
- A: Verify lgK is appropriate for your cardinality range
- A: Remember error is ±1.63% with 99% confidence (lgK=12)

**Q: KLL quantiles not accurate**
- A: Increase K parameter for better accuracy
- A: Check that items are comparable correctly
- A: Verify you haven't exceeded memory constraints

**Q: Serialization fails**
- A: Check that stream is readable/writable
- A: Verify you're using BinaryReader/Writer correctly
- A: Ensure sketch isn't corrupted

---

## Next Steps

1. See **HLL_KLL_TRANSLATION.md** for detailed technical information
2. Run **HllExample.cs** and **KllExample.cs** for comprehensive examples
3. Explore the source code for advanced usage
4. Read the Apache DataSketches documentation: https://datasketches.apache.org/

---

## Support

For questions about:
- **Algorithm details**: See original papers linked in HLL_KLL_TRANSLATION.md
- **C++ implementation**: https://github.com/apache/datasketches-cpp
- **Java implementation**: https://github.com/apache/datasketches-java

Happy sketching! 📊

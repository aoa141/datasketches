# DataSketches C# Implementation

A C# translation of the Apache DataSketches C++ library, focusing on the Theta sketch module for fast, approximate distinct counting and set operations.

## Overview

Apache DataSketches is a library of stochastic streaming algorithms for processing large data sets. This C# implementation provides:

- **Theta Sketches**: Fast approximate distinct counting with set operations
- **Common Utilities**: Hash functions, memory operations, and mathematical utilities
- **Serialization**: Compatible with C++ and Java implementations

## Features

### Theta Sketches

- **Update Sketch**: Build sketches from streaming data
- **Compact Sketch**: Immutable, serializable form
- **Set Operations**:
  - Union (A ∪ B)
  - Intersection (A ∩ B)
  - A-not-B (A - B)
- **Estimates with Confidence Bounds**: Get lower/upper bounds at 67%, 95%, or 99% confidence

### Key Benefits

- **Memory Efficient**: Configurable size (k parameter)
- **Fast**: O(1) update time
- **Mergeable**: Combine sketches from distributed systems
- **Accurate**: Configurable error bounds
- **Compatible**: Interoperable with C++ and Java implementations

## Quick Start

### Creating and Updating a Sketch

```csharp
using DataSketches.Theta;

// Create a sketch with default size (k=4096)
var sketch = new UpdateThetaSketch.Builder().Build();

// Update with values
for (int i = 0; i < 1000000; i++)
{
    sketch.Update(i);
}

// Get the estimate
double estimate = sketch.GetEstimate();
Console.WriteLine($"Estimated distinct count: {estimate:F0}");

// Get confidence bounds
double lower = sketch.GetLowerBound(2); // 95% confidence
double upper = sketch.GetUpperBound(2);
Console.WriteLine($"95% CI: [{lower:F0}, {upper:F0}]");
```

### Set Operations

```csharp
// Create two sketches
var sketchA = new UpdateThetaSketch.Builder().Build();
var sketchB = new UpdateThetaSketch.Builder().Build();

// Add data
for (int i = 0; i < 10000; i++) sketchA.Update(i);
for (int i = 5000; i < 15000; i++) sketchB.Update(i);

// Union
var union = new ThetaUnion.Builder().Build();
union.Update(sketchA);
union.Update(sketchB);
var unionResult = union.GetResult();
Console.WriteLine($"Union: {unionResult.GetEstimate():F0}"); // ~15000

// Intersection
var intersection = new ThetaIntersection();
intersection.Update(sketchA);
intersection.Update(sketchB);
var intersectResult = intersection.GetResult();
Console.WriteLine($"Intersection: {intersectResult.GetEstimate():F0}"); // ~5000

// A-not-B
var aNotB = new ThetaANotB();
var diffResult = aNotB.Compute(sketchA, sketchB);
Console.WriteLine($"A-not-B: {diffResult.GetEstimate():F0}"); // ~5000
```

### Serialization

```csharp
// Serialize
var compact = sketch.Compact(ordered: true);
byte[] serialized = compact.Serialize();

// Deserialize
var deserialized = CompactThetaSketch.Deserialize(serialized, 0, serialized.Length);
Console.WriteLine($"Deserialized estimate: {deserialized.GetEstimate():F0}");
```

### Custom Parameters

```csharp
// Smaller sketch for less memory usage
var smallSketch = new UpdateThetaSketch.Builder()
    .SetLgK(9)  // k = 512 entries
    .SetResizeFactor(ResizeFactor.X2)
    .Build();

// With sampling probability
var sampledSketch = new UpdateThetaSketch.Builder()
    .SetP(0.5f)  // Sample 50% of data
    .Build();
```

## Project Structure

```
c:/askideas/datasketches/
├── Common/                      # Shared utilities
│   ├── BinomialBounds.cs       # Confidence interval calculations
│   ├── CeilingPowerOf2.cs      # Power-of-2 utilities
│   ├── CommonDefs.cs           # Common definitions
│   ├── CountZeros.cs           # Bit manipulation utilities
│   ├── MathUtils.cs            # Mathematical utilities
│   ├── MemoryOperations.cs     # Memory copy/validation
│   └── MurmurHash3.cs          # Hash function
├── Theta/                       # Theta sketch implementation
│   ├── CompactThetaSketch.cs   # Immutable sketch
│   ├── ThetaANotB.cs           # Set difference operation
│   ├── ThetaComparators.cs     # Comparison utilities
│   ├── ThetaConstants.cs       # Constants
│   ├── ThetaHelpers.cs         # Helper utilities
│   ├── ThetaIntersection.cs    # Set intersection operation
│   ├── ThetaSketch.cs          # Base classes
│   ├── ThetaUnion.cs           # Set union operation
│   └── UpdateThetaSketch.cs    # Mutable sketch
├── Examples/                    # Usage examples
│   └── ThetaSketchExample.cs
├── README.md                    # This file
└── TRANSLATION_NOTES.md         # Detailed translation notes
```

## Configuration

### Sketch Size (k parameter)

The `k` parameter controls sketch size and accuracy:

| lgK | k (entries) | Approximate Error (RSE) | Memory |
|-----|-------------|-------------------------|---------|
| 9   | 512         | ~4.4%                   | ~4 KB   |
| 10  | 1,024       | ~3.1%                   | ~8 KB   |
| 11  | 2,048       | ~2.2%                   | ~16 KB  |
| 12  | 4,096       | ~1.6% (default)         | ~32 KB  |
| 13  | 8,192       | ~1.1%                   | ~64 KB  |
| 14  | 16,384      | ~0.8%                   | ~128 KB |

**RSE** = Relative Standard Error at 68% confidence

### Resize Factor

Controls how the hash table grows:
- `X1` (no resize): Fixed size
- `X2`: Double on resize
- `X4`: 4x on resize
- `X8` (default): 8x on resize

Larger factors = fewer resizes, more memory overhead.

## Algorithm Details

### How Theta Sketches Work

1. **Hash**: Each input value is hashed to a 64-bit integer
2. **Filter**: Only keep hash values below theta (sampling threshold)
3. **Store**: Store unique hashes in a hash table (max k entries)
4. **Adjust**: When table fills, reduce theta and discard larger hashes
5. **Estimate**: Distinct count ≈ (entries retained) / theta

### Key Properties

- **Unbiased**: Estimates converge to true cardinality
- **Mergeable**: Union of sketches = sketch of union
- **Monotonic**: Adding data never decreases estimate
- **Streaming**: Single pass over data, constant memory

## Performance

### Time Complexity

- Update: O(1) average
- GetEstimate: O(1)
- Union: O(k)
- Intersection: O(k)
- Compact: O(k log k) if ordered

### Memory Usage

- Update Sketch: ~32 KB for default k=4096
- Compact Sketch: 8 * (entries + overhead) bytes
- Serialized: Similar to compact sketch size

## Compatibility

This C# implementation is **compatible** with:
- Apache DataSketches C++ library
- Apache DataSketches Java library

Sketches can be serialized in one language and deserialized in another.

## Testing

Run the examples:

```csharp
using DataSketches.Examples;

ThetaSketchExample.Main(null);
```

Expected output shows:
- Basic distinct counting
- Set operations
- Serialization/deserialization
- Estimate accuracy

## Limitations

Current implementation:
- ✅ Update Theta Sketches
- ✅ Compact Theta Sketches
- ✅ Union, Intersection, A-not-B
- ✅ Serialization (uncompressed)
- ⏳ Compressed serialization (structure in place)
- ❌ Wrapped sketches (zero-copy)
- ❌ Other sketch types (CPC, HLL, KLL, etc.)

## References

- [DataSketches Website](https://datasketches.apache.org/)
- [Theta Sketch Paper](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)
- [C++ Implementation](https://github.com/apache/datasketches-cpp)
- [Java Implementation](https://github.com/apache/datasketches-java)

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file for details.

Copyright 2024 The Apache Software Foundation

## Support

For questions, issues, or contributions, please refer to:
- TRANSLATION_NOTES.md for implementation details
- Examples/ThetaSketchExample.cs for usage patterns
- Original Apache DataSketches documentation

## Acknowledgments

This implementation is based on the Apache DataSketches C++ library developed by the Apache DataSketches team. All core algorithms and serialization formats are preserved to ensure compatibility.

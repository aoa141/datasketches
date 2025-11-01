# ThetaUnion Fix Part 2: Conditional Theta Filtering

## Problem
After fixing double-hashing, union returned 0 entries instead of ~15,000.

## Root Cause  
The GetResult() method unconditionally filtered by theta, but C++ has conditional logic:

**C++ code**:
```cpp
if (union_theta_ >= table_.theta_) {
  std::copy_if(..., key_not_zero<EN, EK>());  // Copy ALL
} else {
  std::copy_if(..., key_not_zero_less_than<..>(theta));  // Filter
}
```

## The Fix
Added conditional filtering matching C++:

```csharp
if (unionTheta >= unionSketchTheta)
{
    // Copy ALL non-zero entries (no theta filter)
    foreach (var hash in unionSketch)
    {
        if (hash != 0) resultEntries.Add(hash);
    }
}
else
{
    // Filter by theta
    foreach (var hash in unionSketch)  
    {
        if (hash != 0 && hash < theta) resultEntries.Add(hash);
    }
}
```

## Why It Matters
- unionTheta: minimum theta from input sketches
- unionSketch.theta: internal union sketch theta

If unionTheta >= unionSketch.theta:
  - Union sketch theta is more restrictive
  - All entries in union sketch are already valid
  - Copy ALL without filtering

If unionTheta < unionSketch.theta:
  - Input sketches more restrictive
  - Must filter by smaller theta

## Result
Union now correctly produces ~15,000 estimate instead of 0.

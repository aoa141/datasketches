# ThetaUnion Bug Fix Part 3: Missing isEmpty Flag Update

## Date Fixed
2025-10-31

## Problem
After fixing double-hashing AND conditional filtering, union STILL returned 0 entries.

```
Union estimate (A ∪ B): 0
Expected: 15000
```

## Root Cause

The `isEmpty` flag in `UpdateThetaSketch` was never being set to `false` when entries were inserted via the `Insert()` method!

### The Flow

1. ThetaUnion calls `unionSketch.Insert(hash)` to add entries
2. Insert() adds the hash to the table and increments `numEntries`
3. **But Insert() never sets `isEmpty = false`**
4. unionSketch.IsEmpty remains `true` even after inserting thousands of entries!

### The Check in GetResult()

```csharp
if (unionSketch.IsEmpty)
{
    return new CompactThetaSketch(true, true, unionSketch.GetSeedHash(), unionTheta, new ulong[0]);
}
```

Since `IsEmpty` is still `true`, GetResult() returns an empty sketch with 0 entries!

## C++ Reference

In the C++ union code (line 43):

```cpp
void theta_union_base<EN, EK, P, S, CS, A>::update(SS&& sketch) {
  if (sketch.is_empty()) return;
  if (sketch.get_seed_hash() != compute_seed_hash(table_.seed_)) throw std::invalid_argument("seed hash mismatch");
  table_.is_empty_ = false;  // ← Explicitly sets is_empty to false!
  union_theta_ = std::min(union_theta_, sketch.get_theta64());
  ...
}
```

## The Fix

**File**: `UpdateThetaSketch.cs` line 249

**Before**:
```csharp
internal void Insert(ulong hash)
{
    var (index, found) = Find(hash);
    if (found) return;

    entries[index] = hash;  // ← Never sets isEmpty!
    numEntries++;
    ...
}
```

**After**:
```csharp
internal void Insert(ulong hash)
{
    var (index, found) = Find(hash);
    if (found) return;

    isEmpty = false;  // ← Added!
    entries[index] = hash;
    numEntries++;
    ...
}
```

## Why This Was Missed

The `Update()` method (which hashes raw data) correctly sets `isEmpty = false`:

```csharp
public void Update(ReadOnlySpan<byte> data, int length)
{
    ulong hash = HashAndScreen(data, length);
    if (hash == 0) return;
    isEmpty = false;  // ← Present here
    Insert(hash);
}
```

But when we made `Insert()` `internal` so the union could call it directly, we bypassed the `Update()` method and its `isEmpty = false` line!

## Impact

### Before
- Union inserted entries correctly into hash table
- `numEntries` incremented correctly
- But `isEmpty` remained `true`
- GetResult() checked `IsEmpty` and returned empty sketch
- Result: 0 entries

### After  
- Insert() now sets `isEmpty = false`
- GetResult() correctly processes entries
- Union returns correct cardinality estimate

## Result

Union should now produce correct estimate of ~15,000 instead of 0!

---

## Summary of All ThetaUnion Fixes

1. **Double Hashing**: Changed `Update(hash)` → `Insert(hash)` 
2. **Conditional Filtering**: Added if/else for theta-based filtering in GetResult()
3. **isEmpty Flag**: Added `isEmpty = false` in Insert() method

All three bugs had to be fixed for union to work correctly!

# Bug Fixes: UpdateThetaSketch.cs

## Date Fixed
2025-10-31

## Summary

Found and fixed FOUR critical bugs in the Theta sketch hash table management that were causing infinite loops and table overflow errors. All bugs were translation errors from the C++ source code.

---

## Bug #1: Infinite Loop in Find() Method

### Problem
The `Find()` method at line 266 had an infinite loop issue.

### Original Code (BUGGY)
```csharp
private (int index, bool found) Find(ulong key)
{
    while (true)  // <- INFINITE LOOP RISK!
    {
        ulong entry = entries[index];
        if (entry == 0) return ((int)index, false);
        if (entry == key) return ((int)index, true);
        index = (index + stride) & (size - 1);
    }
}
```

### Fixed Code
```csharp
private (int index, bool found) Find(ulong key)
{
    uint startIndex = index;
    do
    {
        ulong entry = entries[index];
        if (entry == 0) return ((int)index, false);
        if (entry == key) return ((int)index, true);
        index = (index + stride) & (size - 1);
    } while (index != startIndex);
    
    throw new InvalidOperationException("Hash table is full and key not found!");
}
```

---

## Bug #2: Incorrect GetCapacity() Implementation

### Problem
The `GetCapacity()` method didn't apply resize/rebuild thresholds correctly.

### Original Code (BUGGY)
```csharp
private static uint GetCapacity(byte lgCurSize, byte lgNomSize)
{
    uint nomEntries = (uint)(1 << lgNomSize);
    uint curCap = (uint)(1 << lgCurSize);
    return Math.Min(nomEntries, curCap);  // <- Missing threshold!
}
```

### Fixed Code
```csharp
private static uint GetCapacity(byte lgCurSize, byte lgNomSize)
{
    double fraction = (lgCurSize <= lgNomSize) ? ResizeThreshold : RebuildThreshold;
    return (uint)Math.Floor(fraction * (1 << lgCurSize));
}
```

### Impact
- **Before**: Table had to fill to 100% before resizing
- **After**: Table resizes at 50% load factor (ResizeThreshold = 0.5)

---

## Bug #3: Double-Applied Threshold in Insert()

### Problem
The `Insert()` method was multiplying capacity by threshold when GetCapacity already included it.

### Original Code (BUGGY)
```csharp
uint capacity = GetCapacity(lgCurSize, lgNomSize);
if (numEntries > capacity * ResizeThreshold)  // <- Double threshold!
{
    if (numEntries >= capacity)  // <- Always true!
    {
        Rebuild();
    }
    else if (lgCurSize < lgNomSize)
    {
        Resize();
    }
}
```

### Fixed Code
```csharp
uint capacity = GetCapacity(lgCurSize, lgNomSize);
if (numEntries > capacity)
{
    if (lgCurSize <= lgNomSize)
    {
        Resize();
    }
    else
    {
        Rebuild();
    }
}
```

---

## Bug #4: Resize() Clamped to Wrong Size

### Problem
**THIS WAS THE CRITICAL BUG!** The `Resize()` method clamped to `lgNomSize` instead of `lgNomSize + 1`, preventing the table from growing when at nominal size.

### Original Code (BUGGY)
```csharp
private void Resize()
{
    byte lgResizeFactor = (byte)rf;
    byte newLgCurSize = (byte)(lgCurSize + lgResizeFactor);
    if (newLgCurSize > lgNomSize) newLgCurSize = lgNomSize;  // <- WRONG!
    // ...creates new table of size (1 << newLgCurSize)
}
```

### The Problem Scenario
When `lgCurSize = 12` and `lgNomSize = 12`:
- Table size = 4096 entries
- At 50% full (2048 entries), Insert triggers Resize
- Resize calculates: `newLgCurSize = 12 + 1 = 13`
- **BUG**: Clamps to `lgNomSize = 12` (NO GROWTH!)
- Creates "new" table of 4096 entries (same size!)
- Tries to rehash 2048 entries into a 4096-entry table
- But with open addressing and collisions, table fills up
- Find() loops through entire table and throws exception

### Fixed Code (from C++ line 209)
```csharp
private void Resize()
{
    byte lgResizeFactor = (byte)rf;
    byte newLgCurSize = (byte)Math.Min(lgCurSize + lgResizeFactor, lgNomSize + 1);
    // ...creates new table of size (1 << newLgCurSize)
}
```

### Impact
- **Before**: Table couldn't grow beyond `1 << lgNomSize` entries during resize
- **After**: Table can grow to `1 << (lgNomSize + 1)` entries, then triggers rebuild

### C++ Reference (theta_update_sketch_base_impl.hpp:209)
```cpp
const uint8_t lg_new_size = std::min<uint8_t>(
    lg_cur_size_ + static_cast<uint8_t>(rf_), 
    lg_nom_size_ + 1  // <-- Note the +1!
);
```

---

## Bug #5: Rebuild() Double-Applied Threshold

### Problem
The `Rebuild()` method applied RebuildThreshold when checking if rebuild was needed, but GetCapacity already included it.

### Original Code (BUGGY)
```csharp
private void Rebuild()
{
    uint capacity = GetCapacity(lgCurSize, lgNomSize);
    if (numEntries <= capacity * RebuildThreshold)  // <- Double threshold!
        return;
    // ...
}
```

### Fixed Code
```csharp
private void Rebuild()
{
    uint nominalSize = (uint)(1 << lgNomSize);
    if (numEntries <= nominalSize)
        return;
    // ...
}
```

---

## Root Cause Analysis

All bugs stemmed from incorrect translation of the C++ source code in:
`cpp/theta/include/theta_update_sketch_base_impl.hpp`

### Key Differences Between C++ and Buggy C#

1. **Find loop**: C++ uses `do-while` with termination check; C# used `while(true)`
2. **GetCapacity**: C++ applies thresholds; C# returned raw minimum
3. **Insert logic**: C++ simple comparison; C# double-applied threshold
4. **Resize clamping**: C++ uses `lg_nom_size_ + 1`; C# used `lgNomSize`
5. **Rebuild check**: C++ compares to nominal size; C# double-applied threshold

---

## Error Symptoms

### Before Fixes
```
System.InvalidOperationException: Hash table is full and key not found!
  at UpdateThetaSketch.Find(UInt64 key)
  at UpdateThetaSketch.Resize()
  at UpdateThetaSketch.Insert(UInt64 hash)
```

Occurred at approximately 3,960-4,020 insertions with default parameters (lgK=12).

### After Fixes
- Table properly resizes at 50% load factor
- Can grow to 2x nominal size before rebuild
- Rebuild correctly adjusts theta to keep ~K entries
- No infinite loops or overflow errors

---

## Testing Verification

After fixes, the sketch should:
1. ✅ Accept millions of updates without errors
2. ✅ Resize smoothly when reaching 50% capacity
3. ✅ Grow from 4K → 8K entries when at nominal size
4. ✅ Rebuild with theta adjustment when exceeding 2× nominal
5. ✅ Never throw "Hash table is full" in normal usage

The infinite loop protection in Find() now serves as a safety check that should never trigger.

---

# Additional Bug Fix: CompactThetaSketch Serialization

## Date Fixed
2025-10-31

## Bug #6: Incorrect Serialization Size Calculation

### Problem
The `GetUncompressedSerializedSizeBytes()` method in `CompactThetaSketch.cs` calculated the wrong size, causing "Memory stream is not expandable" errors during serialization.

### Original Code (BUGGY)
```csharp
private long GetUncompressedSerializedSizeBytes()
{
    return 8 + 8 + (entries.Length * 8L); // = 16 + (numEntries × 8) bytes
}
```

### The Problem
The method hardcoded the preamble size as 16 bytes (2 longs), but the actual preamble written by `SerializeUncompressed()` is 24 bytes (3 longs) for typical non-empty sketches:

**Bytes written by SerializeUncompressed:**
- **Long 0** (bytes 0-7): preambleLongs + version + family + flags + seedHash + padding
- **Long 1** (bytes 8-15): numEntries + padding  
- **Long 2** (bytes 16-23): theta
- **Entries** (bytes 24+): 8 bytes per entry

**Total: 24 + (numEntries × 8) bytes**

But the size calculation returned: **16 + (numEntries × 8) bytes** ❌

This caused the MemoryStream to be created with insufficient capacity (8 bytes too small), triggering "Memory stream is not expandable" when trying to write beyond the allocated space.

### Fixed Code
```csharp
private long GetUncompressedSerializedSizeBytes()
{
    byte preambleLongs = GetPreambleLongs(false);
    return (preambleLongs * 8L) + (entries.Length * 8L); // preamble + entries
}
```

### Impact
- **Before**: Serialization threw `NotSupportedException: Memory stream is not expandable`
- **After**: Correctly allocates buffer size, serialization works properly

### Preamble Size Depends on Sketch State

The `GetPreambleLongs()` method returns:
- **1 long (8 bytes)** if sketch is empty
- **2 longs (16 bytes)** if sketch has exactly 1 entry (uncompressed)
- **3 longs (24 bytes)** for typical non-empty sketches

The fix now uses the actual preamble size instead of hardcoding it.

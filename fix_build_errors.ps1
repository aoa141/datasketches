# PowerShell script to fix remaining build errors

Write-Host "Fixing MurmurHash3.Hash128 calls in CountMinSketch.cs..."
$content = Get-Content "Count/CountMinSketch.cs" -Raw
$content = $content -replace 'var hash = MurmurHash3\.Hash128\(data, _hashSeeds\[i\]\);[\r\n\s]+hashes\[i\] = hash\.Low % _numBuckets;', @'
MurmurHash3.HashState hashState;
                MurmurHash3.Hash128(data, _hashSeeds[i], out hashState);
                hashes[i] = hashState.H1 % _numBuckets;
'@
$content | Set-Content "Count/CountMinSketch.cs" -NoNewline

Write-Host "Fixing MurmurHash3.Hash128 calls in BloomFilter.cs..."
$content = Get-Content "Filters/BloomFilter.cs" -Raw
$content = $content -replace 'var hash = MurmurHash3\.Hash128\(data, _seed\);[\r\n\s]+return \(hash\.Low, hash\.High\);', @'
MurmurHash3.HashState hashState;
            MurmurHash3.Hash128(data, _seed, out hashState);
            return (hashState.H1, hashState.H2);
'@
$content | Set-Content "Filters/BloomFilter.cs" -NoNewline

Write-Host "Fixing MurmurHash3.Hash128 calls in HllUtil.cs..."
$content = Get-Content "Hll/HllUtil.cs" -Raw
$content = $content -replace 'Hash128\(data, seed\)\.H1', @'
Hash128Wrapper(data, seed).H1
'@
$content | Set-Content "Hll/HllUtil.cs" -NoNewline

# Add helper method to HllUtil
$content = Get-Content "Hll/HllUtil.cs" -Raw
if ($content -notmatch 'private static MurmurHash3\.HashState Hash128Wrapper') {
    $content = $content -replace '(\s+public static ulong HashBytes)', @'

        private static MurmurHash3.HashState Hash128Wrapper(byte[] data, ulong seed)
        {
            MurmurHash3.HashState state;
            MurmurHash3.Hash128(data, seed, out state);
            return state;
        }

$1
'@
    $content | Set-Content "Hll/HllUtil.cs" -NoNewline
}

Write-Host "Fixing MurmurHash3_x64_128 in CpcSketch.cs..."
$content = Get-Content "Cpc/CpcSketch.cs" -Raw
$content = $content -replace 'MurmurHash3\.MurmurHash3_x64_128', 'MurmurHash3.Hash128'
$content | Set-Content "Cpc/CpcSketch.cs" -NoNewline

Write-Host "Fixing operator ambiguity in CountMinSketch.cs..."
$content = Get-Content "Count/CountMinSketch.cs" -Raw
$content = $content -replace '(\(i \* _numBuckets\)) \+ hashes\[i\]', '$1 + (long)hashes[i]'
$content | Set-Content "Count/CountMinSketch.cs" -NoNewline

Write-Host "Fixing operator ambiguity in QuantilesSketch.cs..."
$content = Get-Content "Quantiles/QuantilesSketch.cs" -Raw
$content = $content -replace '(_combinedBufferItemCapacity) >= (0)', '(long)$1 >= $2'
$content | Set-Content "Quantiles/QuantilesSketch.cs" -NoNewline

Write-Host "Done! Run: dotnet build DataSketches.csproj --configuration Release"

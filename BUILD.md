# Building DataSketches C#

## Prerequisites

- .NET 8.0 SDK or later
- Visual Studio 2022 (optional) or VS Code
- Command line tools (dotnet CLI)

## Quick Start

### Build the library

```bash
cd c:/askideas/datasketches
dotnet build DataSketches.csproj
```

### Build everything (library + examples)

```bash
dotnet build DataSketches.sln
```

### Run examples

```bash
cd Examples
dotnet run --project DataSketches.Examples.csproj
```

## Build Configurations

### Debug Build
```bash
dotnet build --configuration Debug
```

### Release Build (optimized)
```bash
dotnet build --configuration Release
```

## Create NuGet Package

```bash
dotnet pack DataSketches.csproj --configuration Release
```

This creates: `bin/Release/Apache.DataSketches.1.0.0.nupkg`

## Visual Studio

Open `DataSketches.sln` in Visual Studio 2022:
1. File → Open → Project/Solution
2. Select `DataSketches.sln`
3. Build → Build Solution (or press F6)

## Project Structure

```
DataSketches/
├── DataSketches.sln          # Solution file
├── DataSketches.csproj       # Main library project
├── Directory.Build.props     # Shared build properties
├── Common/                   # Common utilities
├── Theta/                    # Theta sketches
├── Hll/                      # HyperLogLog
├── Kll/                      # KLL quantiles
├── Cpc/                      # CPC sketches
├── Quantiles/                # Classic quantiles
├── TDigest/                  # T-Digest
├── Fi/                       # Frequent items
├── Count/                    # Count-Min
├── Filters/                  # Bloom filters
└── Examples/                 # Example projects
    └── DataSketches.Examples.csproj
```

## Targets

- **net8.0**: .NET 8.0 (primary target)
- Compatible with: .NET 8.0+, C# 12+

## Features

- **Nullable reference types**: Enabled
- **Unsafe code**: Enabled (for performance-critical sections)
- **XML Documentation**: Generated automatically
- **ImplicitUsings**: Enabled for cleaner code

## Testing

To add unit tests, create a test project:

```bash
dotnet new xunit -n DataSketches.Tests
dotnet add DataSketches.Tests/DataSketches.Tests.csproj reference DataSketches.csproj
dotnet test
```

## Publishing

### Local install
```bash
dotnet pack --configuration Release
dotnet nuget push bin/Release/Apache.DataSketches.1.0.0.nupkg --source local
```

### NuGet.org (requires API key)
```bash
dotnet nuget push bin/Release/Apache.DataSketches.1.0.0.nupkg \
  --api-key YOUR_API_KEY \
  --source https://api.nuget.org/v3/index.json
```

## Troubleshooting

### Build errors

1. **Ensure .NET 8 SDK is installed**:
   ```bash
   dotnet --version
   ```
   Should show 8.0.x or later

2. **Clean and rebuild**:
   ```bash
   dotnet clean
   dotnet build
   ```

3. **Restore NuGet packages**:
   ```bash
   dotnet restore
   ```

### Missing files

If you get "file not found" errors, ensure all .cs files are in the correct directories:
- Common/*.cs
- Theta/*.cs
- Hll/*.cs
- etc.

## IDE Support

### Visual Studio 2022
- Full IntelliSense
- Debugging
- NuGet package management

### Visual Studio Code
- Install C# extension
- Install .NET SDK
- Use integrated terminal for `dotnet` commands

### JetBrains Rider
- Native .NET support
- Open .sln file directly

## Performance

Release builds include:
- Optimizations enabled
- Inline method hints
- Aggressive inlining for hot paths

To verify optimization:
```bash
dotnet build -c Release -v detailed
```

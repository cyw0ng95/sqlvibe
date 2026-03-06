#!/usr/bin/env python3
# benchmark_analyze.py - Analyze benchmark results
# Usage: python3 scripts/benchmark_analyze.py <benchmark.csv>

import csv
import sys
from collections import defaultdict

def analyze_benchmark(csv_file):
    data = defaultdict(dict)
    
    try:
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                workload = row['workload']
                scale = row['scale']
                ratio = float(row['ratio'])
                data[f"{workload}_{scale}"] = ratio
    except FileNotFoundError:
        print(f"ERROR: File not found: {csv_file}")
        sys.exit(1)
    except KeyError as e:
        print(f"ERROR: Missing column in CSV: {e}")
        print("Expected columns: workload, scale, sqlite_ns, sqlvibe_ns, ratio, status")
        sys.exit(1)
    
    if not data:
        print("ERROR: No benchmark data found in file")
        sys.exit(1)
    
    # Sort by slowdown
    sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)
    
    print("=" * 70)
    print("BENCHMARK ANALYSIS - Sorted by Slowdown")
    print("=" * 70)
    print()
    
    for (name, ratio) in sorted_data:
        if ratio > 100:
            status = "🔴 CRITICAL"
        elif ratio > 10:
            status = "🟡 HIGH"
        elif ratio > 1:
            status = "🟢 OK"
        else:
            status = "🟢 FASTER"
        print(f"{name:30s} {ratio:8.1f}×  {status}")
    
    # Calculate priority scores
    print()
    print("=" * 70)
    print("OPTIMIZATION PRIORITIES")
    print("=" * 70)
    
    critical = [(n, r) for n, r in sorted_data if r > 100]
    high = [(n, r) for n, r in sorted_data if 10 < r <= 100]
    ok = [(n, r) for n, r in sorted_data if 1 < r <= 10]
    faster = [(n, r) for n, r in sorted_data if r <= 1]
    
    if critical:
        print()
        print("P0-CRITICAL (100×+ slowdown):")
        for name, ratio in critical:
            print(f"  - {name}: {ratio:.0f}×")
    
    if high:
        print()
        print("P1-HIGH (10-100× slowdown):")
        for name, ratio in high:
            print(f"  - {name}: {ratio:.1f}×")
    
    if ok:
        print()
        print("P2-MODERATE (1-10× slowdown):")
        for name, ratio in ok:
            print(f"  - {name}: {ratio:.1f}×")
    
    if faster:
        print()
        print("✅ FASTER than SQLite:")
        for name, ratio in faster:
            print(f"  - {name}: {1/ratio:.1f}× faster")
    
    # Summary statistics
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    ratios = list(data.values())
    avg = sum(ratios) / len(ratios)
    geo_mean = 1.0
    for r in ratios:
        geo_mean *= r
    geo_mean = geo_mean ** (1.0 / len(ratios))
    
    print(f"Total benchmarks: {len(data)}")
    print(f"Critical (>100×):  {len(critical)}")
    print(f"High (10-100×):    {len(high)}")
    print(f"Moderate (1-10×):  {len(ok)}")
    print(f"Faster (<1×):      {len(faster)}")
    print()
    print(f"Arithmetic mean slowdown: {avg:.1f}×")
    print(f"Geometric mean slowdown:  {geo_mean:.1f}×")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: benchmark_analyze.py <benchmark.csv>")
        print()
        print("Example:")
        print("  python3 scripts/benchmark_analyze.py .build/benchmarks/benchmark_20260306_120000.csv")
        sys.exit(1)
    analyze_benchmark(sys.argv[1])

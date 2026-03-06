#!/usr/bin/env python3
# benchmark_compare.py - Compare before/after optimization
# Usage: python3 scripts/benchmark_compare.py <before.csv> <after.csv>

import csv
import sys
import math

def compare_benchmarks(before_file, after_file):
    before = {}
    after = {}
    
    # Load before data
    try:
        with open(before_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['workload']}_{row['scale']}"
                before[key] = float(row['sqlvibe_ns'])
    except FileNotFoundError:
        print(f"ERROR: File not found: {before_file}")
        sys.exit(1)
    
    # Load after data
    try:
        with open(after_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['workload']}_{row['scale']}"
                after[key] = float(row['sqlvibe_ns'])
    except FileNotFoundError:
        print(f"ERROR: File not found: {after_file}")
        sys.exit(1)
    
    if not before or not after:
        print("ERROR: No benchmark data found in files")
        sys.exit(1)
    
    print("=" * 80)
    print("BENCHMARK COMPARISON - Before vs After Optimization")
    print("=" * 80)
    print(f"Before: {before_file}")
    print(f"After:  {after_file}")
    print("=" * 80)
    print()
    print(f"{'Workload':<30s} {'Before':>12s} {'After':>12s} {'Speedup':>10s} {'Status':<8s}")
    print("-" * 80)
    
    improvements = []
    regressions = []
    unchanged = []
    
    all_keys = set(before.keys()) | set(after.keys())
    
    for key in sorted(all_keys):
        if key not in before or key not in after:
            continue
        
        before_ns = before[key]
        after_ns = after[key]
        speedup = before_ns / after_ns if after_ns > 0 else float('inf')
        
        if speedup > 1.05:  # 5% threshold for significance
            status = "✅"
            improvements.append((key, speedup))
        elif speedup < 0.95:
            status = "⚠️"
            regressions.append((key, 1.0/speedup))
        else:
            status = "➡️"
            unchanged.append((key, speedup))
        
        before_ms = before_ns / 1e6
        after_ms = after_ns / 1e6
        
        if speedup >= 1.0:
            speedup_str = f"{speedup:.2f}×"
        else:
            speedup_str = f"{1.0/speedup:.2f}× slower"
        
        print(f"{key:<30s} {before_ms:>10.2f}ms {after_ms:>10.2f}ms {speedup_str:>10s} {status}")
    
    print("-" * 80)
    
    # Summary sections
    if improvements:
        print()
        print("✅ IMPROVEMENTS:")
        for name, speedup in sorted(improvements, key=lambda x: x[1], reverse=True):
            print(f"  {name}: {speedup:.2f}× faster")
    
    if regressions:
        print()
        print("⚠️ REGRESSIONS:")
        for name, speedup in sorted(regressions, key=lambda x: x[1], reverse=True):
            print(f"  {name}: {speedup:.2f}× slower")
    
    if unchanged:
        print()
        print("➡️ UNCHANGED:")
        for name, _ in unchanged:
            print(f"  {name}")
    
    # Calculate geometric mean speedup
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    speedups = []
    for key in before:
        if key in after:
            ratio = before[key] / after[key]
            speedups.append(ratio)
    
    if speedups:
        geo_mean = math.prod(speedups) ** (1.0 / len(speedups))
        arithmetic_mean = sum(speedups) / len(speedups)
        
        print(f"Total benchmarks compared: {len(speedups)}")
        print(f"Improvements: {len(improvements)}")
        print(f"Regressions:  {len(regressions)}")
        print(f"Unchanged:    {len(unchanged)}")
        print()
        print(f"Arithmetic mean speedup: {arithmetic_mean:.2f}×")
        print(f"Geometric mean speedup:  {geo_mean:.2f}×")
        
        if geo_mean >= 1.5:
            print()
            print("🎉 SIGNIFICANT PERFORMANCE GAIN!")
        elif geo_mean >= 1.1:
            print()
            print("✅ Modest performance improvement")
        elif geo_mean >= 0.95:
            print()
            print("➡️ Roughly equivalent performance")
        else:
            print()
            print("⚠️ PERFORMANCE REGRESSION - investigate!")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: benchmark_compare.py <before.csv> <after.csv>")
        print()
        print("Example:")
        print("  python3 scripts/benchmark_compare.py \\")
        print("    .build/benchmarks/benchmark_before.csv \\")
        print("    .build/benchmarks/benchmark_after.csv")
        sys.exit(1)
    compare_benchmarks(sys.argv[1], sys.argv[2])

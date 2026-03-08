#!/bin/bash
# benchmark_collect.sh - Collect performance data for analysis
# Usage: ./scripts/benchmark_collect.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="$PROJECT_ROOT/.build/benchmarks"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="${OUTPUT_DIR}/benchmark_${TIMESTAMP}.csv"
RAW_OUTPUT="${OUTPUT_FILE%.csv}.txt"

mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "BENCHMARK COLLECTION"
echo "============================================================"
echo "Hardware: $(uname -a)"
echo "Go version: $(go version)"
echo "Output CSV: ${OUTPUT_FILE}"
echo "Raw output: ${RAW_OUTPUT}"
echo "============================================================"
echo ""

# Ensure project is built
echo "Building project..."
cd "$PROJECT_ROOT"
./build.sh > /dev/null 2>&1 || {
    echo "ERROR: Build failed. Please run ./build.sh first."
    exit 1
}

# Set library path for CGO
export LD_LIBRARY_PATH="${PROJECT_ROOT}/.build/cmake/lib:${LD_LIBRARY_PATH:-}"

echo ""
echo "Running benchmarks (5 iterations each, 1s benchtime)..."
echo ""

# Run benchmarks and capture output
go test ./tests/Benchmark/... \
    -bench=BenchmarkCompare_ \
    -benchmem \
    -benchtime=1s \
    -count=5 \
    -run=^$ \
    2>&1 | tee "$RAW_OUTPUT"

# Parse results into CSV
echo ""
echo "Parsing results..."

# CSV Header
echo "workload,scale,sqlite_ns,sqlvibe_ns,ratio,status" > "$OUTPUT_FILE"

# Parse benchmark output
grep -E "BenchmarkCompare_(SelectAll|CountStar|SumAggregate|GroupBy|Insert|InnerJoin|Where|OrderBy)_(1K|10K|100K)_(SQLite|SVBytecode)-" "$RAW_OUTPUT" | \
while read -r line; do
    # Parse: BenchmarkCompare_SelectAll_1K_SQLite-20  1388  441523 ns/op
    name=$(echo "$line" | awk '{print $1}')
    ns=$(echo "$line" | awk '{print $3}')
    
    # Extract workload and scale
    workload=$(echo "$name" | sed 's/BenchmarkCompare_//' | sed 's/_SQLite$//' | sed 's/_SVBytecode$//')
    scale=$(echo "$workload" | grep -oE '(1K|10K|100K)')
    workload_base=$(echo "$workload" | sed 's/_(1K|10K|100K)$//')
    backend=$(echo "$name" | grep -oE '(SQLite|SVBytecode)')
    
    echo "$workload_base,$scale,$backend,$ns"
done | \
awk -F',' '
{
    key = $1 "_" $2
    if ($3 == "SQLite") {
        sqlite[key] = $4
    } else {
        sqlvibe[key] = $4
    }
}
END {
    for (key in sqlite) {
        if (key in sqlvibe) {
            ratio = sqlvibe[key] / sqlite[key]
            status = (ratio > 100) ? "CRITICAL" : (ratio > 10) ? "HIGH" : "OK"
            if (ratio < 1) status = "FASTER"
            print key "," sqlite[key] "," sqlvibe[key] "," ratio "," status
        }
    }
}' >> "$OUTPUT_FILE"

echo ""
echo "============================================================"
echo "Benchmark collection complete: $OUTPUT_FILE"
echo "============================================================"
echo ""
echo "Next steps:"
echo "  1. Analyze: python3 scripts/benchmark_analyze.py $OUTPUT_FILE"
echo "  2. Profile: go test ./tests/Benchmark/... -bench=BenchmarkCompare_InnerJoin_1K -cpuprofile=cpu.prof"

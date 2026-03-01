#!/usr/bin/env bash
# build.sh — Unified build/test/benchmark/fuzz/coverage runner for sqlvibe.
#
# Usage:
#   ./build.sh [options]
#
# Options:
#   -t              Run unit tests
#   -b              Run benchmarks
#   -f              Run fuzz testing (seed-corpus run, not continuous fuzzing)
#   -c              Collect coverage — works with -t and/or -b; generates
#                   .build/coverage.out and .build/coverage.html
#   --fuzz-time D   Duration per fuzz target during -f (default: 30s)
#   -v              Verbose output (passes -v to go test)
#   -h              Print this help message
#
# Output directory:  <project-root>/.build/
#   .build/test.log          — unit-test output
#   .build/bench.log         — benchmark output
#   .build/fuzz/<name>.log   — per-target fuzz output
#   .build/coverage.out      — merged coverage profile (when -c is used)
#   .build/coverage.html     — HTML coverage report (when -c is used)
#
# Examples:
#   ./build.sh                # build with CGO (default)
#   ./build.sh -t             # run all unit tests
#   ./build.sh -t -c          # run tests and generate coverage report
#   ./build.sh -b             # run all benchmarks
#   ./build.sh -t -b -c       # tests + benchmarks + merged coverage
#   ./build.sh -f             # run fuzz seed corpus (30s per target)
#   ./build.sh -f --fuzz-time 5m  # fuzz each target for 5 minutes
#   ./build.sh -t -b -f -c    # everything
#
# Note: CGO is always enabled. C++ libraries are built automatically.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/.build"

# Build tags: CGO always enabled
EXT_TAGS="SVDB_EXT_JSON,SVDB_EXT_MATH,SVDB_EXT_FTS5,SVDB_EXT_PROFILING,SVDB_ENABLE_CGO,SVDB_ENABLE_CGO_DS,SVDB_ENABLE_CGO_VM,SVDB_ENABLE_CGO_QP"

# Defaults
RUN_TESTS=0
RUN_BENCH=0
RUN_FUZZ=0
COVERAGE=0
FUZZ_TIME="30s"
VERBOSE=0

usage() {
    sed -n '2,/^set -euo/{ /^set -euo/d; s/^# \{0,1\}//; p }' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t)           RUN_TESTS=1 ;;
        -b)           RUN_BENCH=1 ;;
        -f)           RUN_FUZZ=1 ;;
        -c)           COVERAGE=1 ;;
        --fuzz-time)  FUZZ_TIME="${2:?'--fuzz-time requires an argument'}"; shift ;;
        -v)           VERBOSE=1 ;;
        -h|--help)    usage; exit 0 ;;
        *)            echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
    shift
done

mkdir -p "$BUILD_DIR"

# Always build C++ extensions (CGO is default)
echo "====> Building C++ extensions (CGO default)..."
echo "      - Extensions: math, json, fts5"
echo "      - Data Storage: B-Tree, SIMD, Roaring bitmap"
echo "      - VM: Hash functions, batch execution, expression eval, dispatch, type conv, strings, datetime, aggregates"
echo "      - QP: Fast tokenizer"
echo "      - CG: Bytecode optimizer, plan cache, expression compiler"
echo "      - Wrapper: Phase 4 invoke chain (hash+filter, batch compare, scan+aggregate)"

# Build C++ extensions
if [[ -f "CMakeLists.txt" ]]; then
    mkdir -p "$BUILD_DIR/cmake"
    cd "$BUILD_DIR/cmake"
    cmake "$SCRIPT_DIR" -DCMAKE_BUILD_TYPE=Release
    cmake --build . -- -j$(nproc)
    cd "$SCRIPT_DIR"
fi

# Set LD_LIBRARY_PATH for CGO
export LD_LIBRARY_PATH="${BUILD_DIR}/cmake/lib:${LD_LIBRARY_PATH:-}"
echo "====> LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

# If no test/bench/fuzz requested, just build the package and binaries
if [[ $RUN_TESTS -eq 0 && $RUN_BENCH -eq 0 && $RUN_FUZZ -eq 0 ]]; then
    echo "====> Building package and binaries..."
    echo "  CMD: go build -tags $EXT_TAGS ./..."
    go build -tags "$EXT_TAGS" ./...

    echo "  CMD: go build -tags $EXT_TAGS -o $BUILD_DIR/bin/sv-cli ./cmd/sv-cli"
    mkdir -p "$BUILD_DIR/bin"
    go build -tags "$EXT_TAGS" -o "$BUILD_DIR/bin/sv-cli" ./cmd/sv-cli

    if [[ -d "./cmd/sv-check" ]]; then
        echo "  CMD: go build -tags $EXT_TAGS -o $BUILD_DIR/bin/sv-check ./cmd/sv-check"
        go build -tags "$EXT_TAGS" -o "$BUILD_DIR/bin/sv-check" ./cmd/sv-check
    fi

    echo "====> Build complete. Binaries in $BUILD_DIR/bin/"
    exit 0
fi

VERBOSE_FLAG=""
[[ $VERBOSE -eq 1 ]] && VERBOSE_FLAG="-v"

# Coverage profile files collected in this run (for later merge)
COVER_PROFILES=()

# ----- unit tests -------------------------------------------------------------

if [[ $RUN_TESTS -eq 1 ]]; then
    echo ""
    echo "====> Running unit tests..."
    TEST_COVER_ARGS=()
    if [[ $COVERAGE -eq 1 ]]; then
        COVER_PROF_TESTS="$BUILD_DIR/coverage_tests.out"
        COVERPKG=$(go list -tags "$EXT_TAGS" -f '{{.ImportPath}}' ./... 2>/dev/null \
            | grep -vE "internal/TS|^github.com/cyw0ng95/sqlvibe/internal/VM/benchdata" \
            | tr '\n' ',' | sed 's/,$//')
        TEST_COVER_ARGS+=(-coverprofile="$COVER_PROF_TESTS" -covermode=atomic -coverpkg="$COVERPKG")
        COVER_PROFILES+=("$COVER_PROF_TESTS")
        # Export LD_LIBRARY_PATH for test binaries to find shared libraries
        export LD_LIBRARY_PATH="${BUILD_DIR}/cmake/lib:${LD_LIBRARY_PATH:-}"
        env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" go test -tags "$EXT_TAGS" \
            "${TEST_COVER_ARGS[@]}" \
            ${VERBOSE_FLAG} \
            ./... 2>&1 | tee "$BUILD_DIR/test.log"
    else
        # Export LD_LIBRARY_PATH for test binaries to find shared libraries
        export LD_LIBRARY_PATH="${BUILD_DIR}/cmake/lib:${LD_LIBRARY_PATH:-}"
        env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" go test -tags "$EXT_TAGS" \
            ${VERBOSE_FLAG} \
            ./... 2>&1 | tee "$BUILD_DIR/test.log"
    fi
    echo "====> Unit tests complete. Log: $BUILD_DIR/test.log"
fi

# ----- benchmarks -------------------------------------------------------------

if [[ $RUN_BENCH -eq 1 ]]; then
    echo ""
    echo "====> Running benchmarks..."
    BENCH_COVER_ARGS=()
    if [[ $COVERAGE -eq 1 ]]; then
        COVER_PROF_BENCH="$BUILD_DIR/coverage_bench.out"
        COVERPKG=$(go list -tags "$EXT_TAGS" -f '{{.ImportPath}}' ./... 2>/dev/null \
            | grep -vE "internal/TS|^github.com/cyw0ng95/sqlvibe/internal/VM/benchdata" \
            | tr '\n' ',' | sed 's/,$//')
        BENCH_COVER_ARGS+=(-coverprofile="$COVER_PROF_BENCH" -covermode=atomic -coverpkg="$COVERPKG")
        COVER_PROFILES+=("$COVER_PROF_BENCH")
    fi
    go test -tags "$EXT_TAGS" \
        -run '^$' \
        -bench '.' \
        -benchmem \
        "${BENCH_COVER_ARGS[@]+"${BENCH_COVER_ARGS[@]}"}" \
        ${VERBOSE_FLAG} \
        ./internal/TS/Benchmark/... 2>&1 | tee "$BUILD_DIR/bench.log"
    echo "====> Benchmarks complete. Log: $BUILD_DIR/bench.log"
fi

# ----- fuzz testing -----------------------------------------------------------

if [[ $RUN_FUZZ -eq 1 ]]; then
    echo ""
    echo "====> Running fuzz tests (seed corpus, $FUZZ_TIME per target)..."
    mkdir -p "$BUILD_DIR/fuzz"

    FUZZ_PKG=(
        "github.com/cyw0ng95/sqlvibe/internal/TS/PlainFuzzer"
        "github.com/cyw0ng95/sqlvibe/internal/TS/PlainFuzzer"
    )
    FUZZ_FUNC=(
        "FuzzSQL"
        "FuzzDBFile"
    )

    for i in "${!FUZZ_PKG[@]}"; do
        PKG="${FUZZ_PKG[$i]}"
        FUNC="${FUZZ_FUNC[$i]}"
        FUZZ_LOG="$BUILD_DIR/fuzz/${FUNC}.log"
        echo "  -> Fuzzing $FUNC ($PKG) for $FUZZ_TIME..."
        go test -tags "$EXT_TAGS" \
            -run '^$' \
            -fuzz "^${FUNC}$" \
            -fuzztime "$FUZZ_TIME" \
            "$PKG" 2>&1 | tee "$FUZZ_LOG" || true
    done
    echo "====> Fuzz runs complete. Logs: $BUILD_DIR/fuzz/"
fi

# ----- coverage report --------------------------------------------------------

if [[ $COVERAGE -eq 1 && ${#COVER_PROFILES[@]} -gt 0 ]]; then
    MERGED="$BUILD_DIR/coverage.out"

    echo ""
    echo "====> Merging ${#COVER_PROFILES[@]} coverage profile(s)..."

    if [[ ${#COVER_PROFILES[@]} -eq 1 ]]; then
        cp "${COVER_PROFILES[0]}" "$MERGED"
    else
        {
            head -1 "${COVER_PROFILES[0]}"
            for p in "${COVER_PROFILES[@]}"; do
                tail -n +2 "$p"
            done
        } > "$MERGED"
    fi

    HTML="$BUILD_DIR/coverage.html"
    go tool cover -html="$MERGED" -o "$HTML"
    echo "====> Coverage profile : $MERGED"
    echo "====> Coverage report  : $HTML"
    go tool cover -func="$MERGED" | tail -1
fi

echo ""
echo "====> Done. All output under: $BUILD_DIR/"

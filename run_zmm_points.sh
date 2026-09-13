#!/usr/bin/env bash
#
# True 512-bit points for the register-width matrix.
#
# WHY THESE ARE SEPARATE FROM run_width_matrix.sh
#   -DARROW_SIMD_LEVEL=AVX512 makes Arrow compile with -march=skylake-avx512.
#   GCC applies -mprefer-vector-width=256 to that target by default (the
#   Skylake-X downclocking heuristic), so the AUTOVECTORIZER never emits zmm.
#   Verified on this box:
#     -march=skylake-avx512                            -> zmm=0 ymm=2
#     -march=skylake-avx512 -mprefer-vector-width=512  -> zmm=2 ymm=2
#   Arrow's own sequential unpacker is explicit intrinsics / generated code, so
#   it reaches 512-bit anyway. The FastLanes interleaved kernel is portable C
#   that depends on the autovectorizer, so under Arrow's stock AVX512 flags it
#   is silently capped at 256-bit. objdump on the O2_AVX512 build confirmed it:
#   ymm=221908, zmm=0.
#
#   So a "512-bit" number from that build is really a 256-bit number. These two
#   points add -mprefer-vector-width=512 to get genuine zmm codegen, which is
#   the only way to test the layout at 512 bits.
#
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export PATH="$HOME/.local/bin:$PATH"

# Don't contend with the matrix run for the pinned core.
while pgrep -f run_width_matrix >/dev/null; do sleep 20; done

BUILD=build-x86-sweep
OUT="$HOME/Projects/pfor_x86_handoff/width_matrix"
mkdir -p "$OUT"
REPS=7
COLS='TpcdsSoldDateSk|TpcdsStoreSk|TpcdsItemSk|TpcdsQuantity|EventDate|ClientIP|CounterID|SortedKeys|MonotoneRowId|RandomWalk'
FILTER="BM_(PforDecode|InterleavedPforDecode|InterleavedPforFlOrderDecode)/($COLS)/"

RUNNER=()
command -v taskset >/dev/null 2>&1 && RUNNER=(taskset -c 2)

for P in "O2_AVX512zmm:-O2 -DNDEBUG" "O3_AVX512zmm:-O3 -DNDEBUG"; do
  NAME="${P%%:*}"; BASEOPT="${P#*:}"
  echo "=================================================================="
  echo "== $NAME : ARROW_SIMD_LEVEL=AVX512 + -mprefer-vector-width=512"
  echo "=================================================================="

  if ! cmake -S cpp -B "$BUILD" -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DARROW_SIMD_LEVEL=AVX512 \
      -DCMAKE_CXX_FLAGS_RELEASE="$BASEOPT -mprefer-vector-width=512" \
      -DARROW_PARQUET=ON -DARROW_BUILD_BENCHMARKS=ON \
      -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON \
      -DARROW_BUILD_TESTS=OFF > "$OUT/configure_$NAME.log" 2>&1; then
    echo "!! configure FAILED -- see $OUT/configure_$NAME.log"; continue
  fi

  if ! cmake --build "$BUILD" --target parquet-pfor-comparison-benchmark -j 14 \
      > "$OUT/build_$NAME.log" 2>&1; then
    echo "!! build FAILED -- see $OUT/build_$NAME.log"; continue
  fi

  BIN="$BUILD/release/parquet-pfor-comparison-benchmark"

  # The whole point of this script: prove zmm is actually present now.
  {
    echo "point=$NAME"
    echo "binary sha256: $(sha256sum "$BIN" | cut -d' ' -f1)"
    for R in xmm ymm zmm; do
      echo "  $R: $(objdump -d --no-show-raw-insn "$BIN" 2>/dev/null | grep -c "%$R")"
    done
  } > "$OUT/verify_$NAME.txt" 2>&1
  cat "$OUT/verify_$NAME.txt"

  if ! grep -qE '^  zmm: [1-9]' "$OUT/verify_$NAME.txt"; then
    echo "!! WARNING: still no zmm in $NAME -- treat as a 256-bit build, not 512"
  fi

  ARROW_USER_SIMD_LEVEL=AVX512 "${RUNNER[@]}" "$BIN" \
    --benchmark_filter="$FILTER" \
    --benchmark_repetitions="$REPS" \
    --benchmark_report_aggregates_only=true \
    --benchmark_out="$OUT/results_$NAME.json" \
    --benchmark_out_format=json > "$OUT/run_$NAME.log" 2>&1 \
    && echo "-> $OUT/results_$NAME.json" \
    || echo "!! run FAILED -- see $OUT/run_$NAME.log"
done
echo "zmm points done"

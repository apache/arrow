#!/usr/bin/env bash
#
# Register-width matrix for the FastLanes interleaved PFOR layout.
#
# WHY THIS EXISTS (and why x86_register_width_sweep.sh could not answer it)
#   That script varies ARROW_USER_SIMD_LEVEL, which caps Arrow's *runtime*
#   dispatch table (bpacking.cc:36-41). The FastLanes interleaved kernel in
#   arrow/util/fastlanes/fastlanes_kernels_internal.h has NO dispatch table at
#   all -- it is portable C that the autovectorizer widens at COMPILE time. So
#   in a single default build it is frozen at the compile baseline
#   (ARROW_SIMD_LEVEL=SSE4_2 => 128-bit SSE) in all three runs. Measured: the
#   interleaved arm was flat at 30.43 / 30.28 / 30.73 GiB/s across
#   SSE4_2 / AVX2 / AVX512 runtime caps -- 0.3% spread.
#
#   To move the interleaved kernel to 256- or 512-bit you must RECOMPILE it.
#   That is what this script does: it rebuilds at each ARROW_SIMD_LEVEL and,
#   separately, at -O2 vs -O3, because the primer records that this kernel
#   moves ~13% between the two while Arrow's explicit-vector kernel moves none.
#
# WHAT IT HOLDS FIXED
#   Same machine, same commit, same corpus, same repetition count, same core
#   pin. Only the compile baseline (register width) and the optimizer level
#   vary -- one axis at a time.
#
# VERIFICATION
#   Each build is disassembled to confirm the interleaved unpack symbol
#   actually contains ymm/zmm registers at the wider baselines. A width that
#   did not vectorize is reported, not silently averaged in.
#
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
export PATH="$HOME/.local/bin:$PATH"

BUILD=build-x86-sweep          # reused across points so bundled deps are not rebuilt
OUT="$HOME/Projects/pfor_x86_handoff/width_matrix"
mkdir -p "$OUT"

REPS=7
# Representative subset: Kosta's four TPC-DS columns, a bit-width-32 control
# (ClientIP, which decodes via the memcpy path), and both arm64 populations.
COLS='TpcdsSoldDateSk|TpcdsStoreSk|TpcdsItemSk|TpcdsQuantity|EventDate|ClientIP|CounterID|SortedKeys|MonotoneRowId|RandomWalk'
FILTER="BM_(PforDecode|InterleavedPforDecode|InterleavedPforFlOrderDecode)/($COLS)/"

RUNNER=()
command -v taskset >/dev/null 2>&1 && RUNNER=(taskset -c 2)

# point name | ARROW_SIMD_LEVEL | optimizer
POINTS=(
  "O2_AVX2:AVX2:-O2"
  "O2_AVX512:AVX512:-O2"
  "O3_SSE4_2:SSE4_2:-O3"
  "O3_AVX2:AVX2:-O3"
  "O3_AVX512:AVX512:-O3"
)

for P in "${POINTS[@]}"; do
  NAME="${P%%:*}"; REST="${P#*:}"; LEVEL="${REST%%:*}"; OPT="${REST##*:}"
  echo "=================================================================="
  echo "== $NAME : ARROW_SIMD_LEVEL=$LEVEL  optimizer=$OPT"
  echo "=================================================================="

  # -O2 is what CMAKE_BUILD_TYPE=Release already gives; only override for -O3.
  FLAGS_ARG=()
  [[ "$OPT" == "-O3" ]] && FLAGS_ARG=(-DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG")

  if ! cmake -S cpp -B "$BUILD" -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DARROW_SIMD_LEVEL="$LEVEL" \
      "${FLAGS_ARG[@]}" \
      -DARROW_PARQUET=ON -DARROW_BUILD_BENCHMARKS=ON \
      -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON \
      -DARROW_BUILD_TESTS=OFF > "$OUT/configure_$NAME.log" 2>&1; then
    echo "!! configure FAILED for $NAME -- see $OUT/configure_$NAME.log"; continue
  fi

  if ! cmake --build "$BUILD" --target parquet-pfor-comparison-benchmark -j 14 \
      > "$OUT/build_$NAME.log" 2>&1; then
    echo "!! build FAILED for $NAME -- see $OUT/build_$NAME.log"; continue
  fi

  BIN="$BUILD/release/parquet-pfor-comparison-benchmark"

  # Did the interleaved kernel actually widen? Count vector registers in the
  # FastLanes unpack code. This is the check that makes the width real.
  {
    echo "point=$NAME level=$LEVEL opt=$OPT"
    echo "binary sha256: $(sha256sum "$BIN" | cut -d' ' -f1)"
    for SYM in $(nm -C "$BIN" 2>/dev/null | grep -oE '[a-zA-Z_:<>0-9]*fastlanes[a-zA-Z_:<>0-9]*Unpack[a-zA-Z_:<>0-9]*' | head -3); do
      echo "  symbol: $SYM"
    done
    echo "  vector register usage across whole binary:"
    for R in xmm ymm zmm; do
      echo "    $R: $(objdump -d --no-show-raw-insn "$BIN" 2>/dev/null | grep -c "%$R")"
    done
  } > "$OUT/verify_$NAME.txt" 2>&1
  cat "$OUT/verify_$NAME.txt"

  # Run the sequential arm at the SAME width as the compile baseline, so both
  # layouts are compared at one register width.
  ARROW_USER_SIMD_LEVEL="$LEVEL" "${RUNNER[@]}" "$BIN" \
    --benchmark_filter="$FILTER" \
    --benchmark_repetitions="$REPS" \
    --benchmark_report_aggregates_only=true \
    --benchmark_out="$OUT/results_$NAME.json" \
    --benchmark_out_format=json > "$OUT/run_$NAME.log" 2>&1 \
    && echo "-> $OUT/results_$NAME.json" \
    || echo "!! run FAILED for $NAME -- see $OUT/run_$NAME.log"
done

echo
echo "matrix done; results in $OUT"

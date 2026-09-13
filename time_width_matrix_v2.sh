#!/usr/bin/env bash
#
# Timing pass for the corrected width x optimizer matrix.
#
# SUPERSEDED FOR COMPARISONS -- use ab_compare.sh instead.
#   This script records ONE process per point and the points are minutes apart.
#   INTEL_RESULTS.md section 10 shows that is not comparable: (1) the
#   ARROW_USER_SIMD_LEVEL it sets on line 49 changes WHICH sequential kernel is
#   timed, worth 3.2x, so a point capped to AVX2 and a point capped to AVX512 are
#   not two widths of one decoder; and (2) the interleaved arm reads either ~36 or
#   ~48 GiB/s at identical build and flags, stable within a batch of consecutive
#   processes and flipping between batches, so consecutive points can sit in
#   different modes. Both are deterministic and both are larger than the width
#   effect this script was written to measure. Its own "+-3% noise floor" claim
#   below is wrong.
#   This script is still fine for producing a single point's absolute numbers.
#
# Separated from the build pass on purpose. These numbers come from a shared
# virtualized cloud container; a concurrent Arrow build on another core moves
# them by more than the effect being measured. v1's "-O3 is a non-factor
# (1.01x / 0.97x)" turned out to be two runs of a bit-identical binary, which
# means +-3% is the noise floor here, not a result. Do not run this while
# anything else on the box is busy.
#
# Each binary is timed at the ARROW_USER_SIMD_LEVEL matching its compile
# baseline, so both layouts are compared at one register width. Arrow's
# sequential decoder has a real runtime dispatch table (bpacking.cc:29-45) and
# obeys that cap; the FastLanes interleaved kernel has no dispatch at all and is
# fixed by its compile flags, which is the whole reason the build matrix exists.
#
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

OUT="$HOME/Projects/pfor_x86_handoff/width_matrix_v2"
REPS=${REPS:-9}          # odd, so the median is a real sample
CORE=${CORE:-2}

# Kosta's four TPC-DS columns, a bit-width-32 control (ClientIP, which Arrow
# decodes via the memcpy path at bpacking_dispatch_internal.h:253), and members
# of both arm64 populations from amd64_handoff.md section 6.
COLS='TpcdsSoldDateSk|TpcdsStoreSk|TpcdsItemSk|TpcdsQuantity|EventDate|ClientIP|CounterID|SortedKeys|MonotoneRowId|RandomWalk'
FILTER="BM_(PforDecode|InterleavedPforDecode|InterleavedPforFlOrderDecode)/($COLS)/"

RUNNER=()
command -v taskset >/dev/null 2>&1 && RUNNER=(taskset -c "$CORE")

# Refuse to produce numbers while the box is loaded -- see header.
LOAD=$(awk '{print int($1)}' /proc/loadavg)
if [ "$LOAD" -gt 2 ]; then
  echo "!! loadavg is $LOAD -- too busy for timing. Numbers would be noise."
  echo "   Re-run when idle, or set FORCE=1 to override."
  [ "${FORCE:-0}" = 1 ] || exit 1
fi

declare -A LVL=( [O2_128]=SSE4_2 [O2_256]=AVX2 [O2_512]=AVX512
                 [O3_128]=SSE4_2 [O3_256]=AVX2 [O3_512]=AVX512 )

for NAME in O2_128 O2_256 O2_512 O3_128 O3_256 O3_512; do
  BIN="$OUT/bin/bench_$NAME"
  [ -x "$BIN" ] || { echo "-- $NAME: no binary, skipping"; continue; }
  echo "== timing $NAME (cap=${LVL[$NAME]}, reps=$REPS, core=$CORE)"
  ARROW_USER_SIMD_LEVEL="${LVL[$NAME]}" "${RUNNER[@]}" "$BIN" \
    --benchmark_filter="$FILTER" \
    --benchmark_repetitions="$REPS" \
    --benchmark_report_aggregates_only=true \
    --benchmark_out="$OUT/results_$NAME.json" \
    --benchmark_out_format=json > "$OUT/run_$NAME.log" 2>&1 \
    && echo "   -> $OUT/results_$NAME.json" \
    || echo "   !! run FAILED -- $OUT/run_$NAME.log"
done

echo
echo "done. analyze with: python3 ~/Projects/pfor_x86_handoff/analyze_matrix_v2.py"

#!/usr/bin/env bash
#
# A/B comparison protocol for pfor_comparison_benchmark.
#
# WHY THIS EXISTS
#   This benchmark has two deterministic confounds, both larger than most
#   effects being measured. Neither is noise; neither shrinks with more
#   repetitions. See INTEL_RESULTS.md section 10.
#
#   (1) ARROW_USER_SIMD_LEVEL caps Arrow's runtime dispatch (bpacking.cc:29-45).
#       It reaches BM_PforDecode only -- the FastLanes kernel has no dispatch.
#       Capping to AVX2 makes Arrow's sequential decoder 3.21x FASTER than
#       leaving it uncapped on Granite Rapids, because the AVX-512 family takes
#       a second pass for the frame bias where the xsimd kernels fold it in.
#       unset == MAX == AVX512, so the DEFAULT selects the slow path.
#
#   (2) The interleaved arm reads either ~36 or ~48 GiB/s (1.33x) at identical
#       binary, cap, filter and reps. It is stable to 1.00-1.03x across
#       consecutive processes but flips between batches minutes apart, and it
#       also moves with how many columns the filter admits (more columns ->
#       more corpora allocated before the hot buffer). fl_order is immune.
#
# THE PROTOCOL
#   Run every variant ALTERNATING inside one batch, several processes each, with
#   the cap pinned. Ratios are then taken within a batch, where confound (2) is
#   constant. Report the cross-process spread, because the within-process stddev
#   is 0.2-1% here and hides both confounds completely.
#
#   The arms come from bench_arms.sh, which also states which of them may be
#   divided by which. This script used to name the production decoder against an
#   exception-free standalone one and call the ratio a layout result.
#
# USAGE
#   ./ab_compare.sh <binA>:<cap> <binB>:<cap> [...]      # binaries under bin/
#   ROUNDS=3 REPS=3 CORE=2 ./ab_compare.sh bench_O2_256:AVX2 fix_O2_256:AVX2
#   ARM_SETS='ARMS_LAYOUT ARMS_DEST' ./ab_compare.sh bench_O2_256:AVX2
#
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
. ./bench_arms.sh

BIN_DIR=${BIN_DIR:-$HOME/Projects/pfor_x86_handoff/width_matrix_v2/bin}
OUT=${OUT:-/tmp/ab}
ROUNDS=${ROUNDS:-3}
REPS=${REPS:-3}
CORE=${CORE:-2}

# Which arm groups to time. Several in one process is fine and preferred, since
# arms timed together share the machine state; dividing across groups is not --
# see bench_arms.sh.
ARM_SETS=${ARM_SETS:-'ARMS_LAYOUT ARMS_ORDER'}
SETS=()
for NAME in $ARM_SETS; do SETS+=("${!NAME}"); done
FILTER=$(bench_filter "${SETS[@]}")

[ $# -ge 1 ] || { sed -n '2,40p' "$0"; exit 1; }

mkdir -p "$OUT"
LOAD=$(awk '{print int($1)}' /proc/loadavg)
if [ "$LOAD" -gt 2 ] && [ "${FORCE:-0}" != 1 ]; then
  echo "!! loadavg is $LOAD -- too busy. Set FORCE=1 to override."; exit 1
fi

for i in $(seq 1 "$ROUNDS"); do
  for SPEC in "$@"; do
    IFS=: read -r B CAP <<<"$SPEC"
    [ -x "$BIN_DIR/$B" ] || { echo "-- no binary $BIN_DIR/$B, skipping"; continue; }
    # Cap pinned per variant. Stated in the output filename so the analysis
    # cannot silently mix caps -- that is confound (1).
    ARROW_USER_SIMD_LEVEL="${CAP:-MAX}" taskset -c "$CORE" "$BIN_DIR/$B" \
      --benchmark_filter="$FILTER" \
      --benchmark_repetitions="$REPS" \
      --benchmark_report_aggregates_only=true \
      --benchmark_out="$OUT/${B}__${CAP:-MAX}__$i.json" \
      --benchmark_out_format=json >/dev/null 2>&1 \
      && echo "  $B cap=${CAP:-MAX} round $i" \
      || echo "  !! $B cap=${CAP:-MAX} round $i FAILED"
  done
done
echo
echo "analyze with: python3 ~/Projects/pfor_x86_handoff/ab_analyze.py $OUT"

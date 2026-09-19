#!/usr/bin/env bash
# Wait for a quiet box, then run the AVX2-transpose A/B in one batch.
# Four variants alternating so all of them see the same placement mode
# (INTEL_RESULTS.md section 10b); cap pinned to AVX2 so the sequential control is
# on Arrow's good kernel and not its AVX-512 pathology (section 10a).
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
./wait_quiet.sh || { echo "GAVE_UP_BUSY"; exit 1; }
rm -rf /tmp/ab_tr; mkdir -p /tmp/ab_tr
ROUNDS=3 REPS=3 CORE=2 OUT=/tmp/ab_tr ./ab_compare.sh \
  fix_O2_256:AVX2 bench_tr_O2_256:AVX2 fix_O3_256:AVX2 bench_tr_O3_256:AVX2
echo "TR_AB_DONE files=$(ls /tmp/ab_tr | wc -l)"

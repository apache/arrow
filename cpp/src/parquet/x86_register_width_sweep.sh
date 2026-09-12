#!/usr/bin/env bash
#
# One-shot benchmark sweep for the register-width question on x86.
#
# WHAT THIS ANSWERS
#   Does the sequential-vs-transposed PFOR decode ratio widen as the SIMD
#   register gets wider (128 -> 256 -> 512 bits), the way it does on the ARM
#   machine this project has on hand? We don't have x86 hardware here, so
#   this script is meant to be handed to someone who does (Kosta), run once,
#   and the resulting tarball sent back.
#
# HOW IT GETS ALL THREE REGISTER WIDTHS FROM ONE BUILD
#   Arrow picks its bit-unpacking kernel at *runtime* via CPUID, and caps
#   that choice with the ARROW_USER_SIMD_LEVEL env var (see
#   cpp/src/arrow/util/cpu_info.cc). A single native build with the default
#   ARROW_RUNTIME_SIMD_LEVEL=MAX compiles the SSE4_2, AVX2, and AVX512
#   dispatch candidates all into the same binary; this script just re-runs
#   that one binary three times, once per env var setting, so the compiler,
#   the flags, the machine, and the binary are held fixed and only the
#   register width the dispatcher is allowed to pick changes.
#
# WHY REPETITIONS ARE FIXED ACROSS ALL THREE WIDTHS
#   An earlier x86 data point compared a 3-repetition median at 256-bit
#   against a single run at 512-bit -- a methodology mismatch that could
#   inflate or deflate either side. This script runs the identical
#   --benchmark_repetitions at every width, so that variable is removed.
#
# PREREQUISITE -- build once, on the target machine, before running this:
#   cmake -S cpp -B build-x86-sweep \
#     -DCMAKE_BUILD_TYPE=Release \
#     -DARROW_PARQUET=ON \
#     -DARROW_BUILD_BENCHMARKS=ON \
#     -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON \
#     -DARROW_BUILD_TESTS=OFF
#   cmake --build build-x86-sweep --target parquet-pfor-comparison-benchmark -j
#
#   Do NOT pass -DARROW_SIMD_LEVEL=AVX512 -- that changes the compile
#   baseline for the *whole* binary and would no longer match how the ARM
#   numbers in this report were built (a default Release build). Leave
#   ARROW_SIMD_LEVEL and ARROW_RUNTIME_SIMD_LEVEL at their defaults; MAX
#   runtime dispatch is already the default and is all this script needs.
#
# USAGE
#   ./x86_register_width_sweep.sh /path/to/build-x86-sweep/release/parquet-pfor-comparison-benchmark
#
# OUTPUT
#   x86_register_width_sweep_<hostname>_<date>.tar.gz in the current
#   directory, containing:
#     - combined_results.json   (all three register widths, one file)
#     - machine.txt             (lscpu, cpuinfo flags, governor, arrow commit)
#     - results_<LEVEL>.json    (the raw per-level benchmark output, kept
#                                 alongside the combined file for the record)
#   Send that one tarball back -- nothing else is needed from this run.

set -euo pipefail

BENCH_BIN="${1:-}"
if [[ -z "${BENCH_BIN}" || ! -x "${BENCH_BIN}" ]]; then
  echo "usage: $0 /path/to/parquet-pfor-comparison-benchmark" >&2
  echo "(build it first -- see the header comment in this script)" >&2
  exit 1
fi

OUTDIR="$(mktemp -d)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo unknown-date)"
HOST="$(hostname -s 2>/dev/null || echo unknown-host)"
TARBALL="x86_register_width_sweep_${HOST}_${STAMP}.tar.gz"

REPS=7
# Only the arms this question is about, across every registered column --
# typical columns and the delta-shaped ones alike, so delta-mode-relevant
# columns are covered by column choice, not a separate flag:
#   - BM_Pfor(64)?Decode           sequential layout, Arrow's shipped decoder
#   - BM_InterleavedPforDecode     interleaved layout, plain PFOR, file order
#   - BM_InterleavedPforFlOrderDecode  interleaved layout, plain PFOR, the
#                                  paper's lane assignment (prices the gather
#                                  it forces; PFOR has no chain for it to help)
#   - BM_TposeApiDecode / TposeFusedDecode / TposeRawDecode / LaneDeltaDecode
#                                  interleaved layout applied to a delta chain
# Encode arms and the other codecs (DBP/zstd/lz4/RLE/BSS) are outside this
# question and are left out to keep the run and the output short.
FILTER='BM_(Pfor(64)?Decode|InterleavedPforDecode|InterleavedPforFlOrderDecode|TposeApiDecode|TposeFusedDecode|TposeRawDecode|LaneDeltaDecode)/'

echo "== machine identification ==" | tee "${OUTDIR}/machine.txt"
{
  echo "--- hostname/date ---"
  echo "host: ${HOST}"
  echo "utc:  ${STAMP}"
  echo
  echo "--- lscpu ---"
  lscpu 2>/dev/null || echo "(lscpu not available)"
  echo
  echo "--- relevant /proc/cpuinfo flags (cpu0) ---"
  grep -m1 '^flags' /proc/cpuinfo 2>/dev/null | tr ' ' '\n' | grep -E '^(sse4_2|avx2?|avx512[a-z]*|bmi[12])$' || echo "(not on Linux / flags unavailable)"
  echo
  echo "--- governor ---"
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "(not exposed)"
  echo
  echo "--- benchmark binary ---"
  echo "path: ${BENCH_BIN}"
  sha256sum "${BENCH_BIN}" 2>/dev/null || true
  echo
  echo "--- repetitions used at every register width ---"
  echo "${REPS}"
} | tee -a "${OUTDIR}/machine.txt"

CPU_FLAGS="$(grep -m1 '^flags' /proc/cpuinfo 2>/dev/null || true)"
have_flag() { echo "${CPU_FLAGS}" | grep -qw "$1"; }

RUNNER=()
if command -v taskset >/dev/null 2>&1; then
  RUNNER=(taskset -c 2)
fi

declare -A LEVEL_SUPPORTED=(
  [SSE4_2]=sse4_2
  [AVX2]=avx2
  [AVX512]=avx512f
)

RESULT_FILES=()
for LEVEL in SSE4_2 AVX2 AVX512; do
  NEEDED_FLAG="${LEVEL_SUPPORTED[${LEVEL}]}"
  if [[ -n "${CPU_FLAGS}" ]] && ! have_flag "${NEEDED_FLAG}"; then
    echo "skipping ${LEVEL}: cpu does not report ${NEEDED_FLAG}" | tee -a "${OUTDIR}/machine.txt"
    continue
  fi
  OUT="${OUTDIR}/results_${LEVEL}.json"
  echo
  echo "== running at ARROW_USER_SIMD_LEVEL=${LEVEL} =="
  ARROW_USER_SIMD_LEVEL="${LEVEL}" "${RUNNER[@]}" "${BENCH_BIN}" \
    --benchmark_filter="${FILTER}" \
    --benchmark_repetitions="${REPS}" \
    --benchmark_report_aggregates_only=true \
    --benchmark_out="${OUT}" \
    --benchmark_out_format=json
  RESULT_FILES+=("${LEVEL}:${OUT}")
done

if [[ ${#RESULT_FILES[@]} -eq 0 ]]; then
  echo "no register width ran -- nothing to combine" >&2
  exit 1
fi

python3 - "${OUTDIR}/combined_results.json" "${RESULT_FILES[@]}" <<'PYEOF'
import json
import sys

out_path = sys.argv[1]
combined = []
for pair in sys.argv[2:]:
    level, path = pair.split(":", 1)
    with open(path) as f:
        data = json.load(f)
    for bench in data.get("benchmarks", []):
        bench = dict(bench)
        bench["simd_level"] = level
        combined.append(bench)

with open(out_path, "w") as f:
    json.dump({"benchmarks": combined}, f, indent=2)

print(f"combined {len(combined)} benchmark results across {len(sys.argv) - 2} register widths")
PYEOF

if command -v git >/dev/null 2>&1 && git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse HEAD >/dev/null 2>&1; then
  {
    echo
    echo "--- arrow commit this binary should have been built from ---"
    git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse HEAD
    git -C "$(dirname "${BASH_SOURCE[0]}")" status --short
  } | tee -a "${OUTDIR}/machine.txt"
fi

tar -czf "${TARBALL}" -C "${OUTDIR}" .
rm -rf "${OUTDIR}"

echo
echo "done: ${TARBALL}"
echo "send this one file back -- it has the combined results and the machine record."

#!/usr/bin/env bash
#
# One-shot benchmark sweep for the register-width question on x86.
#
# The question: does the sequential-vs-interleaved PFOR decode ratio widen as
# the SIMD register gets wider? We have no x86 hardware here, so this script is
# meant to be handed to someone who does, run once, and the tarball sent back.
#
# Register width has to be set per build, at compile time, and the script takes
# one binary per level. Setting it per run with ARROW_USER_SIMD_LEVEL out of a
# single build does not work, and handicaps the interleaved side of the
# comparison this script exists to make:
#
#   - The sequential decoder (bpacking.cc, unpack_bias) has real runtime
#     dispatch: hand-written per-target translation units selected by CPUID and
#     capped by ARROW_USER_SIMD_LEVEL. It responds to the env var.
#
#   - The interleaved and FL_ORDER decoders go through fastlanes::UnpackBlock in
#     fastlanes_kernels_internal.h, which has no intrinsics -- it is portable
#     C++ that the compiler auto-vectorizes -- no target attribute, and no
#     dispatch table (MakeUnpackTable in util/pfor/pfor.cc is indexed by bit
#     width 1..32, with no CPU-feature dimension). Its register width is fixed
#     at compile time by the translation unit'"'"'s flags, that is by
#     ARROW_SIMD_LEVEL, which defaults to SSE4_2 on x86, and
#     ARROW_USER_SIMD_LEVEL cannot reach it.
#
# Out of one default build, the AVX2 leg would therefore compare a hand-written
# AVX2 sequential kernel against a 128-bit interleaved kernel and report the
# ratio as a layout result. ARROW_USER_SIMD_LEVEL is still pinned below, but
# only to hold the sequential decoder at the same width as the interleaved one
# rather than letting it run ahead to its CPUID maximum.
#
# That the flag is what moves the width was checked on Granite Rapids by
# objdump of one source at three flag settings (InterleavedPforDecode, w=12):
#     -march=haswell        -mprefer-vector-width=256   ymm, no zmm
#     -march=sapphirerapids -mprefer-vector-width=256   ymm, no zmm
#     -march=sapphirerapids -mprefer-vector-width=512   1409 zmm, no ymm
#
# One kernel cannot be widened at all, and it is the candidate.
#   UnpackBlockFlToFileOrder in util/fastlanes/interleaved_pfor.h -- the fused
#   FL_ORDER-to-file-order transpose, the only variant that satisfies Parquet'"'"'s
#   positional contract -- is hand-written intrinsics, and they are __m256i. It
#   compiles to an identical 2324 ymm / 0 zmm body whether or not the
#   translation unit is allowed 512-bit registers. A 512-bit leg therefore
#   widens the two controls (plain interleaved, FL_ORDER-raw) and leaves the
#   candidate at 256. Read any 512 column with that asymmetry in mind; it is not
#   a like-for-like widening of both sides.
#
# Why 512 is optional and labelled separately.
#   On the sequential side, 512 is not a data point at all:
#   bpacking_simd_avx512.cc is still driven by the legacy generated kernels,
#   which build their input register from a list of scalar loads. Measured at
#   0.16x of the AVX2 kernel and 0.67x of the scalar one, so dispatch is capped
#   at 256 bits in bpacking.cc and a 512 request resolves back to AVX2. Do not
#   read a 512 sequential column as a 512-bit sequential kernel; it is the AVX2
#   one. The interleaved side has no such defect -- its 512 column is genuine
#   zmm -- which is why the two must not be compared at "512". AVX2 is the level
#   at which both sides are honest, so AVX2 is the headline.
#
# Prerequisite: one build per register width. The AVX2 build is the one that
# matters; do that one first, and stop there if you only have time for one.
#
#     for LEVEL in AVX2 SSE4_2; do
#       cmake -S cpp -B build-x86-$LEVEL \
#         -DCMAKE_BUILD_TYPE=Release \
#         -DARROW_SIMD_LEVEL=$LEVEL \
#         -DARROW_PARQUET=ON \
#         -DARROW_BUILD_BENCHMARKS=ON \
#         -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON \
#         -DARROW_BUILD_TESTS=OFF
#       cmake --build build-x86-$LEVEL --target parquet-pfor-comparison-benchmark -j
#     done
#
#   -DARROW_SIMD_LEVEL is required, not tuning: its default (SSE4_2) is what
#   leaves the interleaved kernel in XMM registers. It does change the compile
#   baseline for the whole binary, so these numbers are not directly comparable
#   to a default-Release build. That is a real cost, and the correct trade,
#   because the alternative is comparing two layouts at two different register
#   widths and calling it a layout result.
#
# Usage
#   ./x86_register_width_sweep.sh AVX2=/path/to/build-x86-AVX2/release/parquet-pfor-comparison-benchmark \
#                                 [SSE4_2=/path/to/build-x86-SSE4_2/release/...]
#
#   Levels may be given in any order and any subset. One LEVEL=PATH pair per
#   build. A single AVX2 pair is a complete, valid run.
#
# Output
#   x86_register_width_sweep_<hostname>_<date>.tar.gz in the current directory:
#     - combined_results.json   (all register widths, one file, each record
#                                 tagged with the level it was built at)
#     - machine.txt             (lscpu, cpuinfo flags, governor, arrow commit,
#                                 plus an objdump register census per binary so
#                                 the width is evidenced rather than asserted)
#     - results_<LEVEL>.json    (the raw per-level benchmark output, kept
#                                 alongside the combined file for the record)
#   Send that one tarball back -- nothing else is needed from this run.

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 LEVEL=/path/to/parquet-pfor-comparison-benchmark [LEVEL=/path ...]" >&2
  echo "       e.g. $0 AVX2=build-x86-AVX2/release/parquet-pfor-comparison-benchmark" >&2
  echo "(one build per level -- see the header comment; AVX2 alone is a valid run)" >&2
  exit 1
fi

declare -A BIN_FOR=()
ORDER=()
for pair in "$@"; do
  if [[ "${pair}" != *=* ]]; then
    echo "error: expected LEVEL=/path/to/binary, got '${pair}'" >&2
    echo "(each level needs its own binary -- see the header)" >&2
    exit 1
  fi
  LEVEL="${pair%%=*}"
  BIN="${pair#*=}"
  case "${LEVEL}" in
    SSE4_2 | AVX2 | AVX512) ;;
    *)
      echo "error: unknown level '${LEVEL}' (want SSE4_2, AVX2 or AVX512)" >&2
      exit 1
      ;;
  esac
  if [[ ! -x "${BIN}" ]]; then
    echo "error: ${LEVEL}: not an executable: ${BIN}" >&2
    exit 1
  fi
  BIN_FOR["${LEVEL}"]="${BIN}"
  ORDER+=("${LEVEL}")
done

if [[ -z "${BIN_FOR[AVX2]:-}" ]]; then
  echo "warning: no AVX2 build given. AVX2 is the level at which both layouts" >&2
  echo "         are honest and is the one the report quotes." >&2
fi

OUTDIR="$(mktemp -d)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo unknown-date)"
HOST="$(hostname -s 2>/dev/null || echo unknown-host)"
TARBALL="x86_register_width_sweep_${HOST}_${STAMP}.tar.gz"

REPS=7
# The four layout benchmarks now sweep five working-set sizes (16 KiB / 400 KiB /
# 1.5 MiB / 4 MiB / 32 MiB of decoded output) instead of the single 400 KiB
# point the first run used -- see LayoutArgs in pfor_comparison_benchmark.cc.
# The grid's advantage is a compute effect, and whether it converts to time at a
# given footprint is part of what is being asked, so one mid-size point cannot
# show either its size or the size of the permutation tax. Expect this run to
# take longer than the first for that reason.
# Only the benchmarks this question is about, across every registered column --
# typical columns and the delta-shaped ones alike, so delta-mode-relevant
# columns are covered by column choice, not a separate flag:
#   - BM_Pfor(64)?Decode           sequential layout, Arrow's shipped decoder
#                                  with its default options, so the planner may
#                                  pick a delta representation per vector
#   - BM_PforPlainSeqDecode / BM_PforPlainInterleavedDecode
#                                  the same production encoder with delta
#                                  declined on both sides, so the ratio between
#                                  these two is the layout on its own. This is
#                                  the pair to quote for the layout question;
#                                  BM_PforDecode against an interleaved one mixes
#                                  the layout with the prefix sum delta forces.
#   - BM_InterleavedPforDecode     interleaved layout, plain PFOR, file order.
#                                  A control: no encoder can ship this, because
#                                  filling the grid in file order is what the
#                                  paper's lane assignment exists to avoid. It
#                                  is here to price the grid's bit-unpacking on
#                                  its own.
#   - BM_InterleavedPforFlOrderRawDecode  the same grid filled the paper's way
#                                  and handed back without the permutation that
#                                  Parquet's positional contract requires. Also
#                                  a control. Without it the two competing
#                                  explanations for a flat result cannot be
#                                  separated: "the grid's unpacking is no
#                                  cheaper" and "the grid's unpacking is much
#                                  cheaper but the permutation eats all of it"
#                                  look identical in the candidate's number.
#   - BM_InterleavedPforFlOrderDecode  the candidate. Same grid, permuted back
#                                  to file order by the fused in-register
#                                  transpose, so it is the only one here that
#                                  satisfies the positional contract and the
#                                  only one that answers the question.
#                                  All three interleaved benchmarks above carry no
#                                  exception handling at all: no patch list on
#                                  the wire, no patch pass in the decoder. Read
#                                  them against each other and never against a
#                                  production benchmark -- the quotient would charge
#                                  one side for patching the other never does.
#                                  They answer the ordering question, not the
#                                  layout one, which is the pair above.
#   - BM_TposeApiDecode / TposeFusedDecode / TposeRawDecode / LaneDeltaDecode
#                                  interleaved layout applied to a delta chain
# Encode benchmarks and the other codecs (DBP/zstd/lz4/RLE/BSS) are outside this
# question and are left out to keep the run and the output short.
FILTER='BM_(Pfor(64)?Decode|PforPlainSeqDecode|PforPlainInterleavedDecode|InterleavedPforDecode|InterleavedPforFlOrderRawDecode|InterleavedPforFlOrderDecode|TposeApiDecode|TposeFusedDecode|TposeRawDecode|LaneDeltaDecode)/'

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
  echo "--- repetitions used at every register width ---"
  echo "${REPS}"
} | tee -a "${OUTDIR}/machine.txt"

# Evidence, not assertion: show what register width each binary's interleaved
# kernel actually got. If a "AVX2" binary reports no ymm here, it was built
# without -DARROW_SIMD_LEVEL=AVX2 and its interleaved numbers are invalid.
{
  echo
  echo "--- objdump register census per binary (interleaved kernel) ---"
  echo "    a build whose interleaved kernel shows only xmm was NOT built with"
  echo "    -DARROW_SIMD_LEVEL set, and its interleaved kernels are handicapped."
  for LEVEL in "${ORDER[@]}"; do
    BIN="${BIN_FOR[${LEVEL}]}"
    echo
    echo "  [${LEVEL}] ${BIN}"
    sha256sum "${BIN}" 2>/dev/null | sed 's/^/    sha256: /' || true
    if ! command -v objdump >/dev/null 2>&1 || ! command -v nm >/dev/null 2>&1; then
      echo "    (objdump/nm unavailable -- cannot evidence register width)"
      continue
    fi
    ADDR="$(nm -C "${BIN}" 2>/dev/null \
      | grep -F 'fastlanes::InterleavedPforDecode' | head -1 | awk '{print $1}')" || true
    if [[ -z "${ADDR:-}" ]]; then
      echo "    (InterleavedPforDecode not found in symbol table -- stripped?)"
    else
      objdump -d --no-show-raw-insn \
        --start-address="0x${ADDR}" \
        --stop-address="$((0x${ADDR} + 0x2000))" "${BIN}" 2>/dev/null > "${OUTDIR}/.dis" || true
      printf '    InterleavedPforDecode: zmm=%s ymm=%s xmm=%s\n' \
        "$(grep -co zmm "${OUTDIR}/.dis" || true)" \
        "$(grep -co ymm "${OUTDIR}/.dis" || true)" \
        "$(grep -co xmm "${OUTDIR}/.dis" || true)"
    fi
    # The candidate kernel, pinned at 256 by its own __m256i intrinsics.
    TADDR="$(nm -C "${BIN}" 2>/dev/null \
      | grep -F 'UnpackBlockFlToFileOrder<12u' | head -1 | awk '{print $1}')" || true
    if [[ -n "${TADDR:-}" ]]; then
      objdump -d --no-show-raw-insn \
        --start-address="0x${TADDR}" \
        --stop-address="$((0x${TADDR} + 0x1400))" "${BIN}" 2>/dev/null > "${OUTDIR}/.dis" || true
      printf '    UnpackBlockFlToFileOrder<12>: zmm=%s ymm=%s xmm=%s (expect ymm at every level)\n' \
        "$(grep -co zmm "${OUTDIR}/.dis" || true)" \
        "$(grep -co ymm "${OUTDIR}/.dis" || true)" \
        "$(grep -co xmm "${OUTDIR}/.dis" || true)"
    fi
  done
  rm -f "${OUTDIR}/.dis"
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
for LEVEL in "${ORDER[@]}"; do
  BIN="${BIN_FOR[${LEVEL}]}"
  NEEDED_FLAG="${LEVEL_SUPPORTED[${LEVEL}]}"
  if [[ -n "${CPU_FLAGS}" ]] && ! have_flag "${NEEDED_FLAG}"; then
    echo "skipping ${LEVEL}: cpu does not report ${NEEDED_FLAG}" | tee -a "${OUTDIR}/machine.txt"
    continue
  fi
  OUT="${OUTDIR}/results_${LEVEL}.json"
  echo
  echo "== running the ${LEVEL} BUILD (ARROW_USER_SIMD_LEVEL pinned to ${LEVEL}) =="
  if [[ "${LEVEL}" == AVX512 ]]; then
    echo "   note: the sequential decoder resolves back to AVX2 here (capped"
    echo "   in bpacking.cc); the interleaved one is genuine zmm. Not"
    echo "   like-for-like."
  fi
  # Pinning the env var holds the sequential decoder at the same width the
  # binary was compiled for, instead of letting CPUID take it to the machine
  # maximum while the interleaved one stays at its compile-time width.
  ARROW_USER_SIMD_LEVEL="${LEVEL}" "${RUNNER[@]}" "${BIN}" \
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
        # The level the binary was compiled at -- this is what sets the
        # interleaved kernel's register width. Named to avoid the old
        # implication that one build was re-dispatched at runtime.
        bench["build_simd_level"] = level
        combined.append(bench)

with open(out_path, "w") as f:
    json.dump({"benchmarks": combined}, f, indent=2)

print(f"combined {len(combined)} benchmark results across {len(sys.argv) - 2} builds")
PYEOF

if command -v git >/dev/null 2>&1 && git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse HEAD >/dev/null 2>&1; then
  {
    echo
    echo "--- arrow commit these binaries should have been built from ---"
    git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse HEAD
    git -C "$(dirname "${BASH_SOURCE[0]}")" status --short
  } | tee -a "${OUTDIR}/machine.txt"
fi

tar -czf "${TARBALL}" -C "${OUTDIR}" .
rm -rf "${OUTDIR}"

echo
echo "done: ${TARBALL}"
echo "send this one file back -- it has the combined results and the machine record."

#!/usr/bin/env bash
#
# Build the AVX2 Transpose32x32 A/B points.
#
# Treatment: transposed_delta.h gained Transpose32x32Avx2 (sixteen in-register
# 8x8 transposes) behind #if defined(__AVX2__). Before this, x86 fell through to
# Transpose32x32Scalar -- 1024 four-byte loads at a 128-byte stride plus 1024
# scalar stores per block, because only NEON had a hand-written path. Isolated,
# the permutation goes 28.67 -> 73.56 GiB/s (2.57x).
#
# Only the kFlOrder decoder calls the transpose, so BM_InterleavedPforDecode (file
# order) and BM_PforDecode are controls this change cannot reach. If either
# moves by more than the cross-process spread, the comparison is contaminated --
# see INTEL_RESULTS.md section 10.
#
# Correctness is not left to inspection: pfor_comparison_benchmark.cc
# round-trips every column through encode+decode and ARROW_CHECKs the result
# before timing starts, for both orders. A wrong transpose aborts the binary.
# Transpose32x32 is also its own inverse on a square grid, so encode and decode
# share the path and a broken permutation cannot cancel itself out.
#
# Build only. Time with ab_compare.sh.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export PATH="$HOME/.local/bin:$PATH"   # cmake must be <4

BUILD=build-x86-sweep
OUT="$HOME/Projects/pfor_x86_handoff/width_matrix_v2"
mkdir -p "$OUT/bin"

# Matched to the existing fix_* binaries so the only difference is the transpose.
POINTS=(
  "tr_O3_256:AVX2:-O3:256"
  "tr_O2_256:AVX2:-O2:256"
)

for P in "${POINTS[@]}"; do
  IFS=: read -r NAME LEVEL OPT PVW <<<"$P"
  echo "== $NAME : ARROW_SIMD_LEVEL=$LEVEL $OPT -mprefer-vector-width=$PVW"
  # Explicit every time: CMAKE_CXX_FLAGS_RELEASE is a CACHE variable and would
  # otherwise inherit the previous point's value.
  if ! cmake -S cpp -B "$BUILD" -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DARROW_SIMD_LEVEL="$LEVEL" \
      -DCMAKE_CXX_FLAGS_RELEASE="$OPT -DNDEBUG -mprefer-vector-width=$PVW" \
      -DCMAKE_C_FLAGS_RELEASE="$OPT -DNDEBUG -mprefer-vector-width=$PVW" \
      -DARROW_PARQUET=ON -DARROW_BUILD_BENCHMARKS=ON \
      -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON \
      -DARROW_BUILD_TESTS=OFF > "$OUT/configure_$NAME.log" 2>&1; then
    echo "!! configure FAILED -- $OUT/configure_$NAME.log"; continue
  fi
  if ! cmake --build "$BUILD" --target parquet-pfor-comparison-benchmark -j 6 \
      > "$OUT/build_$NAME.log" 2>&1; then
    echo "!! build FAILED -- tail:"; tail -30 "$OUT/build_$NAME.log"; continue
  fi
  cp "$BUILD/release/parquet-pfor-comparison-benchmark" "$OUT/bin/bench_$NAME"
  echo "   -> $OUT/bin/bench_$NAME"
  # Prove the AVX2 transpose is actually in the kFlOrder kernel.
  objdump -d --demangle "$OUT/bin/bench_$NAME" 2>/dev/null | awk '
    /^[0-9a-f]+ <.*InterleavedPforDecode<.*>:$/ {
      inside=1
      name=($0 ~ /InterleavedPforOrder\)1/) ? "kFlOrder(1)" : "kFileOrder(0)"
      n=0; y=0; z=0; perm=0; next
    }
    /^[0-9a-f]+ </ { if (inside) printf "      %-14s insns=%-7d ymm=%-6d zmm=%-6d vperm2x128=%d\n", name,n,y,z,perm; inside=0; next }
    inside { nf=split($0,p,"\t"); if (nf<3) next; n++
             y+=gsub(/%ymm[0-9]+/,"&",p[3]); z+=gsub(/%zmm[0-9]+/,"&",p[3])
             if (p[3] ~ /vperm2[fi]128/) perm++ }
    END { if (inside) printf "      %-14s insns=%-7d ymm=%-6d zmm=%-6d vperm2x128=%d\n", name,n,y,z,perm }'
done
echo "TRANSPOSE_BUILDS_DONE"

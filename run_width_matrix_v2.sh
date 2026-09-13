#!/usr/bin/env bash
#
# Register-width x optimizer matrix, CORRECTED.
#
# WHY v2 EXISTS -- the bug in v1
#   v1 assumed CMAKE_BUILD_TYPE=Release means -O2, which is true for upstream
#   Arrow but NOT on this branch. cpp/cmake_modules/SetupCxxFlags.cmake:637
#   carries a local override:
#       "keep -O3 from CMake's default Release flags for the pfor benchmark"
#   so Release keeps CMake's default -O3 -DNDEBUG. v1 passed an explicit -O3
#   only for its "-O3" points and let the "-O2" points default -- which also
#   came out -O3. Proof, from v1's own artifacts:
#       verify_O2_AVX2.txt   sha256 22ae7451045ce1f5...
#       verify_O3_AVX2.txt   sha256 22ae7451045ce1f5...   <- same binary
#       verify_O2_AVX512.txt sha256 440bf51c5ec1f62c...
#       verify_O3_AVX512.txt sha256 440bf51c5ec1f62c...   <- same binary
#   and configure_O2_AVX2.log: "CMAKE_CXX_FLAGS_RELEASE: -O3 -DNDEBUG
#   -ftree-vectorize". So v1's "-O3 is a non-factor (1.01x/0.97x)" was a binary
#   compared against itself -- it measures the shared-VM noise floor (+-3%),
#   not the optimizer. Only v1's 512-bit pair differed for real.
#
#   Consequence for the headline: v1's width scaling 30.32/35.41/38.22 mixed
#   levels -- 128 and 256 were -O3 builds, 512 was the one genuine -O2 build.
#   Not a clean axis. This script fixes it by ALWAYS setting the optimizer
#   explicitly, never inheriting it.
#
# ALSO FIXED
#   - CMAKE_CXX_FLAGS_RELEASE is a CACHE variable, so a value set at one point
#     persists into the next configure of the same build dir. v2 passes it every
#     time, so no point can inherit a neighbour's flags.
#   - -mprefer-vector-width is set explicitly at every point. Arrow's
#     ARROW_SIMD_LEVEL=AVX512 uses -march=skylake-avx512, and GCC defaults that
#     target to -mprefer-vector-width=256, so a nominally-512 build emits zero
#     zmm unless asked. v1 hit exactly this (ymm 221908, zmm 0).
#   - Width is verified INSIDE the kernel function, not across the whole binary.
#     Whole-binary counts are dominated by Arrow's own explicit-vector code and
#     say nothing about what the autovectorizer did to the FastLanes kernel.
#   - Each binary is preserved under $OUT/bin/ so timing can be replayed later
#     without rebuilding, and so builds and timing runs never overlap.
#
# BUILD ONLY. Timing is time_width_matrix_v2.sh, run when the box is quiet --
# these numbers are from a shared virtualized container and a concurrent build
# on another core will move them.
#
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export PATH="$HOME/.local/bin:$PATH"   # cmake must be <4; 3.31.10 lives here

BUILD=build-x86-sweep                  # reused so bundled deps are not rebuilt
OUT="$HOME/Projects/pfor_x86_handoff/width_matrix_v2"
mkdir -p "$OUT/bin"

# The autovectorized FastLanes kernel under test. Order 0 = plain interleaved,
# Order 1 = FL_ORDER. Order 0 is the one the width claim is about.
KERNEL='arrow::util::fastlanes::InterleavedPforDecode<(arrow::util::fastlanes::InterleavedPforOrder)0>'

# name | ARROW_SIMD_LEVEL | optimizer | prefer-vector-width
POINTS=(
  "O2_128:SSE4_2:-O2:128"
  "O2_256:AVX2:-O2:256"
  "O2_512:AVX512:-O2:512"
  "O3_128:SSE4_2:-O3:128"
  "O3_256:AVX2:-O3:256"
  "O3_512:AVX512:-O3:512"
)

for P in "${POINTS[@]}"; do
  IFS=: read -r NAME LEVEL OPT PVW <<<"$P"
  echo "=================================================================="
  echo "== $NAME : ARROW_SIMD_LEVEL=$LEVEL  $OPT  -mprefer-vector-width=$PVW"
  echo "=================================================================="

  # Explicit every time. Nothing inherited, nothing assumed about Release.
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
  # Record the flags cmake actually resolved, so the log proves the level.
  grep -E "CMAKE_CXX_FLAGS(_RELEASE)?:" "$OUT/configure_$NAME.log" | sed 's/^/   /'

  if ! cmake --build "$BUILD" --target parquet-pfor-comparison-benchmark -j 6 \
      > "$OUT/build_$NAME.log" 2>&1; then
    echo "!! build FAILED -- $OUT/build_$NAME.log"; continue
  fi

  SRC="$BUILD/release/parquet-pfor-comparison-benchmark"
  BIN="$OUT/bin/bench_$NAME"
  cp "$SRC" "$BIN"

  # ---- verify the width the autovectorizer gave THIS kernel ----------------
  # Disassemble only the kernel's address range: from its symbol to the next.
  ADDR=$(nm -C "$BIN" | grep -F "$KERNEL" | awk '{print $1}' | head -1)
  {
    echo "point=$NAME level=$LEVEL opt=$OPT prefer=$PVW"
    echo "sha256: $(sha256sum "$BIN" | cut -d' ' -f1)"
    echo "kernel: $KERNEL"
    echo "kernel addr: 0x$ADDR"
    if [ -n "$ADDR" ]; then
      # Boundaries come from objdump's OWN '<addr> <symbol>:' labels, not from
      # sorting nm addresses. The nm approach silently produced an empty range
      # for O2_256 -- kernel_O2_256.asm came out 0 bytes and the point then
      # reported "zmm: 0 ymm: 0", which reads as a failed vectorization rather
      # than a failed extraction. nm lists symbols from the symbol table in an
      # order that need not bracket the function body (aliases, local symbols
      # and ifunc resolvers all land in between), so "next address after this
      # one" is not the function's end. objdump cannot disagree with itself.
      objdump -dC --no-show-raw-insn "$BIN" 2>/dev/null \
        | awk -v s="$KERNEL" '
            /^[0-9a-f]+ </ { inside = index($0, s) > 0 }
            inside' > "$OUT/kernel_$NAME.asm"
      echo "kernel insns: $(grep -cE '^\s+[0-9a-f]+:' "$OUT/kernel_$NAME.asm")"
      echo "IN-KERNEL vector register usage:"
      for R in xmm ymm zmm; do
        echo "    $R: $(grep -c "%$R" "$OUT/kernel_$NAME.asm")"
      done
      echo "IN-KERNEL notable ops:"
      # v?-prefixed: a 128-bit SSE build emits non-VEX psrld/movdqa, so
      # VEX-only patterns reported zero shifts and zero stack traffic for every
      # 128-bit point -- which made the narrow builds look spill-free when
      # O3_128 in fact had the most stack traffic in the matrix.
      for OP in 'v?psrld' 'v?pslld' 'vpsrlvd' 'vpsllvd' 'v?pand' 'v?por' \
                'v?movdqu' 'v?movdqa' 'vpgatherdd' 'vpermd' 'v?pshufb' \
                'vpbroadcastd' 'vzeroupper'; do
        C=$(grep -cE "^\s+[0-9a-f]+:\s+$OP" "$OUT/kernel_$NAME.asm")
        [ "$C" -gt 0 ] && echo "    $OP: $C"
      done
      # Vector moves with a stack-relative operand. Reported as traffic, not
      # spills: a genuine stack scratch buffer matches this too, which is
      # exactly what the pre-fix kFileOrder path was (a 4 KiB grid, ~1984
      # write-only stores with zero matching reloads).
      echo "    stack vector traffic: $(grep -cE '^\s+[0-9a-f]+:\s+v?mov(dqu|dqa|ups|aps).*\((%rsp|%rbp)' "$OUT/kernel_$NAME.asm")"
    else
      echo "!! kernel symbol not found -- cannot verify width"
    fi
  } > "$OUT/verify_$NAME.txt" 2>&1
  cat "$OUT/verify_$NAME.txt"

  # A width that did not materialize is reported, never silently averaged in.
  case "$PVW" in
    512) grep -qE '^    zmm: [1-9]' "$OUT/verify_$NAME.txt" \
           || echo "!! WARNING $NAME: no zmm IN KERNEL -- this is NOT a 512-bit point" ;;
    256) grep -qE '^    ymm: [1-9]' "$OUT/verify_$NAME.txt" \
           || echo "!! WARNING $NAME: no ymm IN KERNEL -- this is NOT a 256-bit point" ;;
  esac
  echo
done

echo "=== builds complete. binaries in $OUT/bin/ ==="
sha256sum "$OUT"/bin/* 2>/dev/null
echo
echo "Distinct binaries (if two points share a sha256, they are the SAME build"
echo "and any difference between their timings is noise -- this is the v1 bug):"
sha256sum "$OUT"/bin/* 2>/dev/null | awk '{print $1}' | sort -u | wc -l
echo
echo "Now run: ./time_width_matrix_v2.sh   (when the box is quiet)"

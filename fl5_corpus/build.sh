#!/bin/bash
# Builds every harness in this directory that is present: fl5_corpus (the 5-arm
# corpus harness), seq_granularity (the per-block call-overhead probe that
# calibrates the sequential arm), and fl5_delta_corpus (the delta ladder) when
# that source is there. Sources that are absent are skipped with a note rather
# than failing the build, so a checkout that carries only some of them still
# produces working binaries. Works on x86-64 and aarch64; the only difference
# is -march.
#
#   ARROW=/path/to/arrow           source checkout (has cpp/src)
#   ARROW_BUILD=/path/to/build     configured+built Arrow (has src/arrow/util/config.h
#                                  and release/libarrow.so)
#   XSIMD=/path/to/xsimd/include   xsimd headers (Arrow vendors them under
#                                  <build>/_deps/xsimd-src/include)
set -euo pipefail
# This script lives at <arrow>/fl5_corpus/, so the checkout is one level up and
# needs no configuring. ARROW_BUILD and XSIMD do: point them at your own build.
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ARROW=${ARROW:-$(dirname "$HERE")}
ARROW_BUILD=${ARROW_BUILD:?set ARROW_BUILD to a configured+built Arrow dir (has src/arrow/util/config.h)}
# Arrow vendors xsimd under the build tree; fall back to a sibling build if the
# caller did not say.
XSIMD=${XSIMD:-$ARROW_BUILD/_deps/xsimd-src/include}
LIBDIR=${LIBDIR:-$ARROW_BUILD/release}

for f in "$ARROW/cpp/src/arrow/util/fastlanes/interleaved_pfor.h" \
         "$ARROW_BUILD/src/arrow/util/config.h" \
         "$XSIMD/xsimd/xsimd.hpp"; do
  [ -f "$f" ] || { echo "build.sh: missing $f" >&2; exit 1; }
done

case "$(uname -m)" in
  x86_64)  ARCH_FLAGS=${ARCH_FLAGS:-"-march=haswell -mprefer-vector-width=256"} ;;
  aarch64) ARCH_FLAGS=${ARCH_FLAGS:-"-march=armv8-a+simd"} ;;
  *)       ARCH_FLAGS=${ARCH_FLAGS:-""} ;;
esac

built=0
skipped=""
for SRC in fl5_corpus fl5_delta_corpus seq_granularity; do
  if [ ! -f "$HERE/$SRC.cpp" ]; then
    skipped="$skipped $SRC"
    continue
  fi
  ( set -x
    ${CXX:-g++} -std=c++20 -O3 $ARCH_FLAGS -DNDEBUG \
      -I"$ARROW/cpp/src" -I"$ARROW_BUILD/src" -I"$ARROW/cpp/build-support" -I"$XSIMD" \
      "$HERE/$SRC.cpp" -o "$HERE/$SRC" \
      -L"$LIBDIR" -larrow -Wl,-rpath,"$LIBDIR" )
  built=$((built + 1))
done

[ -n "$skipped" ] && echo "build.sh: source not present, skipped:$skipped" >&2
[ "$built" -gt 0 ] || { echo "build.sh: no harness sources found in $HERE" >&2; exit 1; }
echo "build.sh: built $built harness(es) in $HERE"

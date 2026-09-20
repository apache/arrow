#!/bin/bash
# Builds one of the bit-unpacking layout harnesses -- layout_benchmark by
# default, or whichever .cpp beside it is named as the first argument. Works on
# x86-64 and aarch64; the only difference is -march.
#
#   ./build.sh                     layout_benchmark, 43 columns x 6 working sets
#   ./build.sh width_ladder        the same layouts with bit width as the axis
#   OPT='-O2 -ftree-vectorize' ./build.sh width_ladder
#                                  Arrow's own Release level. Quote figures from
#                                  this one and bound them with the -O3 default:
#                                  the container arms are inlined from headers and
#                                  so answer to the level given here, while
#                                  seq_simd lives in libarrow.so and does not.
#
#   ARROW=/path/to/arrow           source checkout (has cpp/src)
#   ARROW_BUILD=/path/to/build     configured+built Arrow (has src/arrow/util/config.h
#                                  and release/libarrow.so)
#   XSIMD=/path/to/xsimd/include   xsimd headers (Arrow vendors them under
#                                  <build>/_deps/xsimd-src/include)
#   OPT='-O2 -ftree-vectorize'     optimization level, default -O3
#   OUT=name                       output binary, default the source name
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

SRC=${1:-layout_benchmark}
OUT=${OUT:-$SRC}
OPT=${OPT:--O3}
[ -f "$HERE/$SRC.cpp" ] || { echo "build.sh: missing $HERE/$SRC.cpp" >&2; exit 1; }
( set -x
  ${CXX:-g++} -std=c++20 $OPT $ARCH_FLAGS -DNDEBUG \
    -I"$ARROW/cpp/src" -I"$ARROW_BUILD/src" -I"$ARROW/cpp/build-support" -I"$XSIMD" \
    "$HERE/$SRC.cpp" -o "$HERE/$OUT" \
    -L"$LIBDIR" -larrow -Wl,-rpath,"$LIBDIR" )
echo "build.sh: built $HERE/$OUT"

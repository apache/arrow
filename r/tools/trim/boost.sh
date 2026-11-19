#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Trim the Boost CMake source tarball (boost-X.Y.Z-cmake.tar.gz, ~140 MB)
# down to the Boost libraries that libarrow's bundled Boost build
# (build_boost() in cpp/cmake_modules/ThirdpartyToolchain.cmake) uses when
# the R package builds libarrow, so it is small enough to ship in the R
# package (~5 MB).
#
# Usage: tools/trim/boost.sh IN_TARBALL OUT_TARBALL
#
# Called by tools/vendor_dependencies.sh (`make sync-cpp`) after tools/cpp
# has been synced. Needs cmake and a C++ compiler, because Boost's own CMake
# is used to work out which libraries are needed.
#
# How it works:
#
# 1. Read the libraries build_boost() adds to BOOST_INCLUDE_LIBRARIES from
#    ThirdpartyToolchain.cmake, minus the ones that the R build never needs
#    (EXCLUDED_LIBRARIES below).
# 2. Configure Boost with that BOOST_INCLUDE_LIBRARIES. Boost's CMake adds
#    the transitive dependencies and logs each library it adds.
# 3. Keep only the top-level files, tools/cmake and those libraries, without
#    their tests, docs, examples, etc.
#
# Set BOOST_INCLUDE_LIBRARIES (semicolon-separated) in the environment to
# skip step 1 and use that list instead.

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 IN_TARBALL OUT_TARBALL" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IN="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
OUT="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"

# Libraries that build_boost() can add to BOOST_INCLUDE_LIBRARIES but that
# the R package's libarrow build never needs. If you add a library here,
# say why.
#
# - multiprecision: only needed when ARROW_BOOST_NEED_MULTIPRECISION is
#   true, i.e. with ARROW_TESTING, ARROW_GANDIVA, ARROW_USE_UBSAN or a
#   compiler without a native 128-bit integer (ARROW_USE_NATIVE_INT128).
#   The R package builds none of those, and gcc/clang on the 64-bit
#   platforms that nixlibs.R builds for all have __int128 (32-bit targets
#   don't). Including it would more than double the size of the trimmed
#   tarball (~5 MB, 54 libraries -> ~11 MB, 58 libraries). If a build that
#   needs it uses this tarball, configuring libarrow fails in build_boost()
#   because the boost_multiprecision target doesn't exist. Such builds
#   should set LIBARROW_VENDORED_DEPS=false to use the full upstream
#   tarball instead (see nixlibs.R).
# - beast, variant, xpressive: only for ARROW_FLIGHT_SQL_ODBC.
EXCLUDED_LIBRARIES=(multiprecision beast variant xpressive)

# Print the library names that build_boost() adds to BOOST_INCLUDE_LIBRARIES
# with `set(BOOST_INCLUDE_LIBRARIES ...)` or
# `list(APPEND BOOST_INCLUDE_LIBRARIES ...)`, one per line. Variable
# references such as ${ARROW_BOOST_COMPONENTS} are skipped: those are only
# non-empty when building tests, integration tests, benchmarks or ODBC.
boost_libraries_from_cmake() {
  awk '
    /^function\(build_boost\)/ { inside = 1; next }
    inside && /^endfunction\(\)/ { exit }
    inside {
      sub(/#.*/, "")
      gsub(/\(/, " ( ")
      gsub(/\)/, " ) ")
      for (i = 1; i <= NF; i++) tok[++n] = $i
    }
    END {
      for (i = 1; i < n; i++) {
        if (tok[i + 1] != "(") continue
        cmd = tolower(tok[i])
        j = i + 2
        if (cmd == "set" && tok[j] == "BOOST_INCLUDE_LIBRARIES") {
          j += 1
        } else if (cmd == "list" && tok[j] == "APPEND" &&
                   tok[j + 1] == "BOOST_INCLUDE_LIBRARIES") {
          j += 2
        } else {
          continue
        }
        for (; j <= n && tok[j] != ")"; j++) {
          if (tok[j] !~ /^\$/) print tok[j]
        }
      }
    }' "$1"
}

if [ -z "${BOOST_INCLUDE_LIBRARIES:-}" ]; then
  # tools/cpp is the synced copy of cpp/ (`make sync-cpp`); fall back to the
  # repository's cpp/ so this can also be run by hand from a git checkout.
  TOOLCHAIN=""
  for dir in "${SCRIPT_DIR}/../cpp" "${SCRIPT_DIR}/../../../cpp"; do
    if [ -f "${dir}/cmake_modules/ThirdpartyToolchain.cmake" ]; then
      TOOLCHAIN="${dir}/cmake_modules/ThirdpartyToolchain.cmake"
      break
    fi
  done
  if [ -z "${TOOLCHAIN}" ]; then
    echo "Can't find ThirdpartyToolchain.cmake; run \`make sync-cpp\` first" >&2
    exit 1
  fi

  ALL_LIBRARIES="$(boost_libraries_from_cmake "${TOOLCHAIN}")"
  if [ -z "${ALL_LIBRARIES}" ]; then
    echo "No BOOST_INCLUDE_LIBRARIES found in build_boost() in ${TOOLCHAIN}." >&2
    echo "Has build_boost() changed? Update ${BASH_SOURCE[0]}." >&2
    exit 1
  fi
  for lib in "${EXCLUDED_LIBRARIES[@]}"; do
    if ! printf '%s\n' "${ALL_LIBRARIES}" | grep -qxF "${lib}"; then
      echo "Warning: '${lib}' is in EXCLUDED_LIBRARIES but build_boost() no" \
        "longer uses it; remove it from ${BASH_SOURCE[0]}" >&2
    fi
  done
  BOOST_INCLUDE_LIBRARIES="$(printf '%s\n' "${ALL_LIBRARIES}" |
    grep -vxF "$(printf '%s\n' "${EXCLUDED_LIBRARIES[@]}")" |
    paste -s -d ';' -)"
fi
echo "Trimming Boost to BOOST_INCLUDE_LIBRARIES=${BOOST_INCLUDE_LIBRARIES} and their dependencies"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/arrow-trim-boost.XXXXXX")"
trap 'rm -rf "${WORK}"' EXIT

mkdir "${WORK}/src" "${WORK}/trimmed"
tar -xzf "${IN}" -C "${WORK}/src"
# The tarball has a single top-level directory, e.g. boost-1.92.0
SRC_NAME="$(ls "${WORK}/src")"
SRC="${WORK}/src/${SRC_NAME}"
if [ ! -f "${SRC}/tools/cmake/include/BoostRoot.cmake" ]; then
  echo "${IN} doesn't look like a Boost CMake source tarball" >&2
  exit 1
fi

# Let Boost's CMake resolve the transitive dependencies. With
# --log-level=VERBOSE, BoostRoot.cmake logs "Adding Boost library 'x'" for
# each requested library and "Adding Boost dependency 'x'" for each
# dependency (including 'headers', which is always added).
# BUILD_TESTING=OFF matches prepare_fetchcontent(), so test-only
# dependencies are not pulled in.
cmake -S "${SRC}" -B "${WORK}/build" \
  --log-level=VERBOSE \
  -DBUILD_TESTING=OFF \
  -DBOOST_INCLUDE_LIBRARIES="${BOOST_INCLUDE_LIBRARIES}" \
  > "${WORK}/cmake.log" 2>&1 || {
  cat "${WORK}/cmake.log" >&2
  echo "Configuring Boost failed" >&2
  exit 1
}
if grep -q "has not been found" "${WORK}/cmake.log"; then
  grep "has not been found" "${WORK}/cmake.log" >&2
  echo "A library in BOOST_INCLUDE_LIBRARIES doesn't exist in ${SRC_NAME}" >&2
  exit 1
fi
# (bash 3.2 on macOS has no mapfile)
KEEP_LIBRARIES=()
while IFS= read -r lib; do
  KEEP_LIBRARIES+=("${lib}")
done < <(sed -n "s/.*Adding Boost [a-z]* '\([^']*\)'.*/\1/p" "${WORK}/cmake.log" | LC_ALL=C sort -u)
if [ "${#KEEP_LIBRARIES[@]}" -eq 0 ]; then
  echo "Found no \"Adding Boost ...\" messages in Boost's CMake output." >&2
  echo "Has BoostRoot.cmake changed? Update ${BASH_SOURCE[0]}." >&2
  exit 1
fi
echo "Keeping ${#KEEP_LIBRARIES[@]} Boost libraries: ${KEEP_LIBRARIES[*]}"

# Copy what the bundled build needs into ${WORK}/trimmed/${SRC_NAME}:
# the top-level files (CMakeLists.txt, LICENSE_1_0.txt, ...), tools/cmake
# (BoostRoot.cmake etc.) and the libraries, but not the top-level doc/,
# more/, status/ or the other tools/.
DEST="${WORK}/trimmed/${SRC_NAME}"
mkdir -p "${DEST}/tools"
find "${SRC}" -maxdepth 1 -type f -exec cp -p {} "${DEST}/" \;
cp -pR "${SRC}/tools/cmake" "${DEST}/tools/"
for lib in "${KEEP_LIBRARIES[@]}"; do
  # lib can be a nested library such as numeric/conversion
  mkdir -p "$(dirname "${DEST}/libs/${lib}")"
  cp -pR "${SRC}/libs/${lib}" "${DEST}/libs/${lib}"
  # Only remove these at the top level of each library: deeper down they
  # can be header directories (e.g. include/boost/math/tools/).
  for dir in test doc example examples bench benchmark perf performance meta tools; do
    rm -rf "${DEST}/libs/${lib:?}/${dir}"
  done
done

# COPYFILE_DISABLE stops macOS tar from adding AppleDouble (._*) files.
(cd "${WORK}/trimmed" && COPYFILE_DISABLE=1 tar -czf "${OUT}" "${SRC_NAME}")

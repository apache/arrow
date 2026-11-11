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

# Trim the Boost source archive (boost-X.Y.Z-cmake.tar.gz) to the
# headers that Apache Thrift needs.
#
# The R package only needs Boost for Apache Thrift (for Parquet), and
# Arrow's CMake uses bundled Boost header-only in that case (see
# ARROW_BOOST_FOR_THRIFT_ONLY in cpp/cmake_modules/ThirdpartyToolchain.cmake).
# So we keep only the headers reachable via #include from the Boost
# headers that Apache Thrift includes, and keep the directory layout of
# the upstream archive (boost-X.Y.Z/libs/<library>/include/boost/...).
#
# Builds that aren't ARROW_BOOST_FOR_THRIFT_ONLY (e.g. with
# -DARROW_USE_UBSAN=ON, which turns off native 128-bit integers) run
# Boost's CMake build, which this tarball can't do. Such builds should
# set LIBARROW_VENDORED_DEPS=false to use the full upstream tarball
# instead (see nixlibs.R).
#
# The #include graph is followed without evaluating the preprocessor,
# so headers for all platforms and compilers are kept. boost/config is
# kept entirely because it includes headers via macros.
#
# Usage: tools/trim/boost.sh IN_TARBALL OUT_TARBALL

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 IN_TARBALL OUT_TARBALL" >&2
  exit 1
fi

# The Boost headers that Apache Thrift's C++ library includes (in the
# sources Arrow builds and in the headers Parquet includes). Update
# this when Apache Thrift is upgraded:
#   grep -rh '#include <boost' thrift-*/lib/cpp/src/thrift/{*,protocol/*,transport/*}
SEEDS="boost/uuid/uuid.hpp boost/uuid/uuid_io.hpp boost/uuid/string_generator.hpp"

in="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
out="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
mkdir "${work}/src"
# Only extract headers: much faster than extracting everything.
# GNU tar needs --wildcards for patterns; bsdtar (macOS) always uses them.
wildcards=""
if tar --version 2> /dev/null | grep -q "GNU tar"; then
  wildcards="--wildcards"
fi
tar -xzf "${in}" -C "${work}/src" ${wildcards} \
  '*/libs/*/include/boost/*' \
  '*/LICENSE_1_0.txt'
top="$(ls "${work}/src")"
cd "${work}/src/${top}"

find libs -type f -path '*/include/boost/*' > "${work}/index"

# Breadth-first search of the #include graph. Input: the index of all
# headers. Output: the headers to keep.
awk -v seeds="${SEEDS}" '
function normalize(path,    n, parts, i, out, m, stack) {
  n = split(path, parts, "/")
  m = 0
  for (i = 1; i <= n; i++) {
    if (parts[i] == "" || parts[i] == ".") continue
    if (parts[i] == "..") { if (m > 0) m--; continue }
    stack[++m] = parts[i]
  }
  out = ""
  for (i = 1; i <= m; i++) out = out (i > 1 ? "/" : "") stack[i]
  return out
}
{
  # libs/uuid/include/boost/uuid/uuid.hpp -> boost/uuid/uuid.hpp
  header = substr($0, index($0, "/include/boost/") + length("/include/"))
  path[header] = $0
}
END {
  tail = split(seeds, queue, " ")
  head = 0
  while (head < tail) {
    header = queue[++head]
    if ((header in seen) || !(header in path)) continue
    seen[header] = 1
    print path[header]
    dir = header
    sub(/\/[^\/]*$/, "", dir)
    file = path[header]
    while ((getline line < file) > 0) {
      if (line !~ /^[ \t]*#[ \t]*include[ \t]*[<"]/) continue
      sub(/^[ \t]*#[ \t]*include[ \t]*[<"]/, "", line)
      sub(/[>"].*$/, "", line)
      if (line ~ /^boost\//) {
        queue[++tail] = line
      } else {
        relative = normalize(dir "/" line)
        if (relative in path) queue[++tail] = relative
      }
    }
    close(file)
  }
}' "${work}/index" > "${work}/keep"

if ! grep -q "/uuid/uuid.hpp$" "${work}/keep"; then
  echo "Failed to find Boost headers for Apache Thrift in ${in}" >&2
  exit 1
fi

{
  cat "${work}/keep"
  find libs/config/include -type f
  echo LICENSE_1_0.txt
} | LC_ALL=C sort -u | sed -e "s|^|${top}/|" > "${work}/files"

echo "Keeping $(wc -l < "${work}/files" | tr -d ' ') files from Boost libraries:" \
  "$(sed -n -e 's|^[^/]*/libs/\([^/]*\)/.*|\1|p' "${work}/files" | sort -u | tr '\n' ' ')"

cd "${work}/src"
tar -cf - -T "${work}/files" | gzip -n -9 > "${out}"

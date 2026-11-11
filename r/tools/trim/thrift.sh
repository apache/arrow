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

# Trim the Apache Thrift source archive to what Arrow's minimal
# Apache Thrift build uses: the C++ library sources and the config.h
# template. Arrow doesn't use Apache Thrift's CMake build when Boost is
# only needed for Apache Thrift (see ARROW_BOOST_FOR_THRIFT_ONLY and
# add_thrift_minimal_library() in
# cpp/cmake_modules/ThirdpartyToolchain.cmake), which is the case for
# the R package's default build. Builds that aren't (e.g. with
# -DARROW_USE_UBSAN=ON) should set LIBARROW_VENDORED_DEPS=false to use
# the full upstream tarball instead (see nixlibs.R).
#
# Usage: tools/trim/thrift.sh IN_TARBALL OUT_TARBALL

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 IN_TARBALL OUT_TARBALL" >&2
  exit 1
fi

in="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
out="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
tar -xzf "${in}" -C "${work}"
top="$(ls "${work}")"

cd "${work}"
for path in LICENSE NOTICE build/cmake/config.h.in lib/cpp/src/thrift; do
  if [ ! -e "${top}/${path}" ]; then
    echo "${top}/${path} not found in ${in}" >&2
    exit 1
  fi
done
find "${top}/LICENSE" "${top}/NOTICE" "${top}/build/cmake/config.h.in" \
  "${top}/lib/cpp/src/thrift" -type f | LC_ALL=C sort > files
tar -cf - -T files | gzip -n -9 > "${out}"

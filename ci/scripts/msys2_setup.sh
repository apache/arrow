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

set -eux

target=$1

packages=()
case "${target}" in
  cpp|c_glib|ruby)
    packages+=(${MINGW_PACKAGE_PREFIX}-aws-sdk-cpp)
    packages+=(${MINGW_PACKAGE_PREFIX}-boost)
    packages+=(${MINGW_PACKAGE_PREFIX}-brotli)
    packages+=(${MINGW_PACKAGE_PREFIX}-bzip2)
    packages+=(${MINGW_PACKAGE_PREFIX}-c-ares)
    packages+=(${MINGW_PACKAGE_PREFIX}-cc)
    packages+=(${MINGW_PACKAGE_PREFIX}-ccache)
    packages+=(${MINGW_PACKAGE_PREFIX}-clang)
    packages+=(${MINGW_PACKAGE_PREFIX}-cmake)
    packages+=(${MINGW_PACKAGE_PREFIX}-double-conversion)
    packages+=(${MINGW_PACKAGE_PREFIX}-flatbuffers)
    packages+=(${MINGW_PACKAGE_PREFIX}-gflags)
    packages+=(${MINGW_PACKAGE_PREFIX}-grpc)
    packages+=(${MINGW_PACKAGE_PREFIX}-gtest)
    packages+=(${MINGW_PACKAGE_PREFIX}-libutf8proc)
    packages+=(${MINGW_PACKAGE_PREFIX}-libxml2)
    packages+=(${MINGW_PACKAGE_PREFIX}-lz4)
    packages+=(${MINGW_PACKAGE_PREFIX}-ninja)
    packages+=(${MINGW_PACKAGE_PREFIX}-nlohmann-json)
    packages+=(${MINGW_PACKAGE_PREFIX}-openssl)
    packages+=(${MINGW_PACKAGE_PREFIX}-protobuf)
    packages+=(${MINGW_PACKAGE_PREFIX}-rapidjson)
    packages+=(${MINGW_PACKAGE_PREFIX}-re2)
    packages+=(${MINGW_PACKAGE_PREFIX}-snappy)
    packages+=(${MINGW_PACKAGE_PREFIX}-sqlite3)
    packages+=(${MINGW_PACKAGE_PREFIX}-thrift)
    packages+=(${MINGW_PACKAGE_PREFIX}-xsimd)
    packages+=(${MINGW_PACKAGE_PREFIX}-uriparser)
    packages+=(${MINGW_PACKAGE_PREFIX}-zlib)
    packages+=(${MINGW_PACKAGE_PREFIX}-zstd)
  ;;
esac

case "${target}" in
  c_glib|ruby)
    packages+=(${MINGW_PACKAGE_PREFIX}-gobject-introspection)
    packages+=(${MINGW_PACKAGE_PREFIX}-gtk-doc)
    packages+=(${MINGW_PACKAGE_PREFIX}-meson)
    packages+=(${MINGW_PACKAGE_PREFIX}-vala)
    ;;
esac

case "${target}" in
  cgo)
    packages+=(${MINGW_PACKAGE_PREFIX}-arrow)
    packages+=(${MINGW_PACKAGE_PREFIX}-gcc)
    packages+=(${MINGW_PACKAGE_PREFIX}-toolchain)
    packages+=(base-devel)
    ;;
esac

pacman \
  --needed \
  --noconfirm \
  --refresh \
  --sync \
  "${packages[@]}"

"$(dirname $0)/ccache_setup.sh"
echo "CCACHE_DIR=$(cygpath --absolute --windows ccache)" >> $GITHUB_ENV
echo "PIP_CACHE_DIR=$(pip cache dir)" >> $GITHUB_ENV

#!/usr/bin/env bash

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

# Build the static libarrow that r/configure.win downloads on Windows, using
# Rtools45 (libraries are a fixed set built with MXE): call CMake directly
# against the toolchain's static libraries.
#
# Rtools40 (MSYS2) is built by ci/scripts/r_windows_build_rtools40.sh instead.
# The two zips differ in contents; r/configure.win handles both.

set -ex

: "${ARROW_HOME:=$(pwd)}"
# Make sure it is absolute and exported
ARROW_HOME="$(cd "${ARROW_HOME}" && pwd)"
export ARROW_HOME

: "${RTOOLS_VERSION:=45}"
: "${RTOOLS_HOME:=/c/rtools${RTOOLS_VERSION}}"
TOOLCHAIN="${RTOOLS_HOME}/x86_64-w64-mingw32.static.posix"
export PATH="${TOOLCHAIN}/bin:${RTOOLS_HOME}/usr/bin:${PATH}"

# CMake is a native Windows program, so give it Windows-style paths
TOOLCHAIN_WIN="$(cygpath -am "${TOOLCHAIN}")"
BUILD_DIR="$(pwd)/build"
CPP_BUILD_DIR="${BUILD_DIR}/cpp"
INSTALL_DIR="${BUILD_DIR}/install"
INSTALL_DIR_WIN="$(cygpath -am "${INSTALL_DIR}")"

printenv
gcc --version
g++ --version
cmake --version
make --version | head -n 1
ls "${TOOLCHAIN}/lib"
cat "${TOOLCHAIN}/lib/pkgconfig/libcurl.pc"

# google-cloud-cpp links static cURL. CXXFLAGS, not ARROW_CXXFLAGS, because
# ARROW_CXXFLAGS aren't passed to ExternalProjects.
export CXXFLAGS="${CXXFLAGS:-} -DCURL_STATICLIB"

rm -rf "${CPP_BUILD_DIR}"
mkdir -p "${CPP_BUILD_DIR}" "${INSTALL_DIR}"
pushd "${CPP_BUILD_DIR}"

# * CMAKE_UNITY_BUILD=OFF: otherwise some compute tests segfault.
# * Bundled because Rtools45 doesn't ship them: AWS SDK, Thrift,
#   google-cloud-cpp, snappy, utf8proc.
# * re2 bundled: the Rtools45 re2 would pull abseil onto the R link line.
# * abseil, protobuf bundled: Rtools45 has abseil 20250127 and protobuf
#   29.5.0, which are too old for the bundled google-cloud-cpp.
# * Brotli bundled: Rtools45 has 1.2.0, without the 1.2.1 security fix
#   (GH-55165). cURL's Brotli symbols resolve from the bundled copy too.
# * ARROW_USE_STD_CHRONO=0: the R package supplies the time zone database
#   through tzdb, which only the vendored date library uses (GH-48593).
cmake \
  "$(cygpath -am "${ARROW_HOME}/cpp")" \
  -G "MSYS Makefiles" \
  -DARROW_ACERO=ON \
  -DARROW_AZURE=OFF \
  -DARROW_BUILD_SHARED=OFF \
  -DARROW_BUILD_STATIC=ON \
  -DARROW_BUILD_UTILITIES=OFF \
  -DARROW_COMPUTE=ON \
  -DARROW_CXXFLAGS="-DARROW_USE_STD_CHRONO=0" \
  -DARROW_CSV=ON \
  -DARROW_DATASET=ON \
  -DARROW_FILESYSTEM=ON \
  -DARROW_GCS=ON \
  -DARROW_HDFS=OFF \
  -DARROW_JEMALLOC=OFF \
  -DARROW_JSON=ON \
  -DARROW_LZ4_USE_SHARED=OFF \
  -DARROW_MIMALLOC=ON \
  -DARROW_PACKAGE_PREFIX="${TOOLCHAIN_WIN}" \
  -DARROW_PARQUET=ON \
  -DARROW_S3=ON \
  -DARROW_SNAPPY_USE_SHARED=OFF \
  -DARROW_USE_GLOG=OFF \
  -DARROW_UTF8PROC_USE_SHARED=OFF \
  -DARROW_VERBOSE_THIRDPARTY_BUILD=ON \
  -DARROW_WITH_BROTLI=ON \
  -DARROW_WITH_BZ2=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_RE2=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_WITH_ZSTD=ON \
  -DARROW_ZSTD_USE_SHARED=OFF \
  -DAWSSDK_SOURCE=BUNDLED \
  -DBrotli_SOURCE=BUNDLED \
  -DCMAKE_BUILD_TYPE=release \
  -DCMAKE_DISABLE_PRECOMPILE_HEADERS=ON \
  -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR_WIN}" \
  -DCMAKE_PREFIX_PATH="${TOOLCHAIN_WIN}" \
  -DCMAKE_UNITY_BUILD=OFF \
  -DCMAKE_VERBOSE_MAKEFILE=ON \
  -DProtobuf_SOURCE=BUNDLED \
  -DSnappy_SOURCE=BUNDLED \
  -DThrift_SOURCE=BUNDLED \
  -Dabsl_SOURCE=BUNDLED \
  -Dgoogle_cloud_cpp_storage_SOURCE=BUNDLED \
  -Dre2_SOURCE=BUNDLED \
  -Dutf8proc_SOURCE=BUNDLED

make -j"${NPROC:-$(nproc)}"
make install
popd

ccache --show-stats || echo "ccache not found"

VERSION="$(grep Version "${ARROW_HOME}/r/DESCRIPTION" | cut -d " " -f 2)"
DST_DIR="r-libarrow-windows-x86_64-rtools${RTOOLS_VERSION}-${VERSION}"

# Make the zip layout r/configure.win expects: include/ and lib/x64-ucrt/
pushd "${BUILD_DIR}"
rm -rf "${DST_DIR}"
mkdir -p "${DST_DIR}/lib/x64-ucrt"
cp -r install/include "${DST_DIR}/include"
cp install/lib/*.a "${DST_DIR}/lib/x64-ucrt/"

# Rtools libraries in PKG_LIBS in r/configure.win; keep in sync
for lib in z zstd lz4 bz2 crypto ssl curl; do
  cp "${TOOLCHAIN}/lib/lib${lib}.a" "${DST_DIR}/lib/x64-ucrt/"
done

# cURL's link dependencies vary by Rtools version, so read them from
# libcurl.pc, ship the toolchain ones, and record the flags in libcurl.libs
# for r/configure.win. Static link order matters, so:
# * system libraries (ws2_32, ...) aren't shipped and go last;
# * duplicate flags keep their last occurrence, not their first.
# Brotli is skipped: the bundled copy is used instead (see above).
CURL_LIBS=""
CURL_SYS_LIBS=""
while read -r flag; do
  lib="${flag#-l}"
  case "${lib}" in
    brotli*) continue ;;
    ws2_32 | bcrypt | advapi32 | crypt32 | gdi32 | wldap32 | secur32 | normaliz | pthread | m)
      CURL_SYS_LIBS="${CURL_SYS_LIBS} ${flag}"
      continue
      ;;
    curl | z | zstd | ssl | crypto) ;; # already copied above
    *)
      if [ -f "${TOOLCHAIN}/lib/lib${lib}.a" ]; then
        cp "${TOOLCHAIN}/lib/lib${lib}.a" "${DST_DIR}/lib/x64-ucrt/"
      fi
      ;;
  esac
  CURL_LIBS="${CURL_LIBS} ${flag}"
done < <(grep -E '^Libs(\.private)?:' "${TOOLCHAIN}/lib/pkgconfig/libcurl.pc" |
  grep -o -E '\-l[A-Za-z0-9_+.-]+' |
  awk '{ flag[NR] = $0; last[$0] = NR }
       END { for (i = 1; i <= NR; i++) if (last[flag[i]] == i) print flag[i] }')
case " ${CURL_LIBS} " in
  *" -lcurl "*) ;;
  *)
    echo "Failed to parse link flags from ${TOOLCHAIN}/lib/pkgconfig/libcurl.pc" >&2
    exit 1
    ;;
esac
echo "${CURL_LIBS}${CURL_SYS_LIBS}" >"${DST_DIR}/lib/x64-ucrt/libcurl.libs"
cat "${DST_DIR}/lib/x64-ucrt/libcurl.libs"
ls -la "${DST_DIR}/lib/x64-ucrt"

# Create build artifact
zip -r "${DST_DIR}.zip" "${DST_DIR}"

# Copy that to a file name/path that does not vary by version number so we
# can easily find it in the R package tests on CI
cp "${DST_DIR}.zip" ../libarrow.zip
popd

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

# Build the static libarrow that r/configure.win downloads for Windows users,
# using the Rtools toolchain that r-lib/actions/setup-r installs.
#
# There are two flows, chosen by RTOOLS_VERSION:
#
# * Rtools40 is MSYS2-based, so libarrow is built with makepkg-mingw from
#   ci/scripts/PKGBUILD, with its dependencies installed by pacman.
# * Rtools42 and later are MXE-based: there is no pacman/makepkg, and the
#   compiler plus a fixed set of static libraries live in
#   ${RTOOLS_HOME}/x86_64-w64-mingw32.static.posix. So we call CMake directly
#   and point it at that directory. Libraries Rtools doesn't ship (snappy,
#   utf8proc) or ships in a form we don't want to depend on (re2, which needs
#   abseil; abseil and protobuf, where Arrow bundles newer versions) are built
#   from source via Arrow's bundled dependency mechanism.
#
# The two flows produce zips with different contents; r/configure.win handles
# both.

set -ex

: "${ARROW_HOME:=$(pwd)}"
# Make sure it is absolute and exported
ARROW_HOME="$(cd "${ARROW_HOME}" && pwd)"
export ARROW_HOME

: "${RTOOLS_VERSION:=45}"

if [ "${RTOOLS_VERSION}" = "40" ]; then
  pacman --noconfirm -Syy

  : "${MINGW_ARCH:=mingw32 mingw64 ucrt64}"
  export MINGW_ARCH

  cp "${ARROW_HOME}/ci/scripts/PKGBUILD" .
  printenv
  makepkg-mingw --noconfirm --noprogressbar --skippgpcheck --nocheck --syncdeps --cleanbuild

  VERSION="$(grep Version "${ARROW_HOME}/r/DESCRIPTION" | cut -d " " -f 2)"
  DST_DIR="r-libarrow-windows-x86_64-$VERSION"

  # Collect the build artifacts and make the shape of zip file that rwinlib expects
  ls
  mkdir -p build
  mv mingw* build
  cd build

  # This may vary by system/CI provider
  MSYS_LIB_DIR="/c/rtools${RTOOLS_VERSION}"

  # Untar the builds we made
  find . -maxdepth 1 -type f -name '*.xz' -print0 | xargs -0 -n1 tar -xJf
  mkdir -p "${DST_DIR}"
  # Grab the headers from one, either one is fine
  # (if we're building twice to combine old and new toolchains, this may already exist)
  if [ ! -d "${DST_DIR}/include" ]; then
    mv "$(echo "${MINGW_ARCH}" | cut -d ' ' -f 1)/include" "${DST_DIR}"
  fi

  # mingw64 -> x64
  # mingw32 -> i386
  # ucrt64 -> x64-ucrt

  if [ -d mingw64/lib/ ]; then
    ls "${MSYS_LIB_DIR}/mingw64/lib/"
    # Make the rest of the directory structure
    mkdir -p "${DST_DIR}/lib/x64"
    # Move the 64-bit versions of libarrow into the expected location
    mv mingw64/lib/*.a "${DST_DIR}/lib/x64"
    # These are from https://dl.bintray.com/rtools/mingw{32,64}/
    cp "${MSYS_LIB_DIR}"/mingw64/lib/lib{snappy,zstd,lz4,bz2,crypto,curl,ss*,utf8proc,re2,nghttp2}.a "${DST_DIR}/lib/x64"
  fi

  # Same for the 32-bit versions
  if [ -d mingw32/lib/ ]; then
    ls "${MSYS_LIB_DIR}/mingw32/lib/"
    mkdir -p "${DST_DIR}/lib/i386"
    mv mingw32/lib/*.a "${DST_DIR}/lib/i386"
    cp "${MSYS_LIB_DIR}"/mingw32/lib/lib{snappy,zstd,lz4,bz2,crypto,curl,ss*,utf8proc,re2,nghttp2}.a "${DST_DIR}/lib/i386"
  fi

  # Do the same also for ucrt64
  if [ -d ucrt64/lib/ ]; then
    ls "${MSYS_LIB_DIR}/ucrt64/lib/"
    mkdir -p "${DST_DIR}/lib/x64-ucrt"
    mv ucrt64/lib/*.a "${DST_DIR}/lib/x64-ucrt"
    cp "${MSYS_LIB_DIR}"/ucrt64/lib/lib{snappy,zstd,lz4,bz2,crypto,curl,ss*,utf8proc,re2,nghttp2}.a "$DST_DIR"/lib/x64-ucrt
  fi

  # Create build artifact
  zip -r "${DST_DIR}.zip" "$DST_DIR"

  # Copy that to a file name/path that does not vary by version number so we
  # can easily find it in the R package tests on CI
  cp "${DST_DIR}.zip" ../libarrow.zip

  exit 0
fi

: "${RTOOLS_HOME:=/c/rtools${RTOOLS_VERSION}}"
TOOLCHAIN="${RTOOLS_HOME}/x86_64-w64-mingw32.static.posix"
# setup-r adds these to PATH already, but be explicit so this also works
# from a non-Rtools bash.
export PATH="${TOOLCHAIN}/bin:${RTOOLS_HOME}/usr/bin:${PATH}"

# CMake is a native Windows program, so give it Windows-style paths
TOOLCHAIN_WIN="$(cygpath -am "${TOOLCHAIN}")"
BUILD_DIR="$(pwd)/build"
CPP_BUILD_DIR="${BUILD_DIR}/cpp"
INSTALL_DIR="${BUILD_DIR}/install"
INSTALL_DIR_WIN="$(cygpath -am "${INSTALL_DIR}")"

# Prefer the CMake installed on the runner (as the old PKGBUILD did): the
# CMake inside older Rtools toolchains can predate Arrow's minimum (3.25).
CMAKE="cmake"
if [ -x "${PROGRAMFILES}/CMake/bin/cmake.exe" ]; then
  CMAKE="${PROGRAMFILES}/CMake/bin/cmake.exe"
fi

printenv
gcc --version
g++ --version
"${CMAKE}" --version
make --version | head -n 1
ls "${TOOLCHAIN}/lib"
# Logged for debugging; parsed below to generate libcurl.libs for r/configure.win
cat "${TOOLCHAIN}/lib/pkgconfig/libcurl.pc"

# We use static cURL in google-cloud-cpp. If we can use cURL's CMake
# package, we don't need to specify CURL_STATICLIB explicitly. But
# we don't have cURL's CMake package. We need to use CXXFLAGS
# instead of ARROW_CXXFLAGS because ARROW_CXXFLAGS aren't passed to
# ExternalProjects.
export CXXFLAGS="${CXXFLAGS:-} -DCURL_STATICLIB"

rm -rf "${CPP_BUILD_DIR}"
mkdir -p "${CPP_BUILD_DIR}" "${INSTALL_DIR}"
pushd "${CPP_BUILD_DIR}"

# CMAKE_UNITY_BUILD is set to OFF as otherwise some compute functionality
# segfaults in tests
#
# AWS SDK, Thrift and google-cloud-cpp are bundled because Rtools doesn't
# ship them. snappy and utf8proc are bundled because Rtools45 doesn't ship
# them. re2 is bundled because the Rtools45 re2 depends on abseil, which
# would need all the absl_* libraries on the link line in r/configure.win.
# abseil and protobuf are bundled explicitly because Rtools45 ships older
# versions (20250127 and 29.5) than Arrow's bundled google-cloud-cpp expects,
# and with CMAKE_PREFIX_PATH set, AUTO would otherwise pick them up.
# ARROW_USE_STD_CHRONO=0 keeps the vendored date library for time zones
# instead of C++20 std::chrono, which Arrow otherwise selects on Windows with
# gcc 14's libstdc++ (GH-48593). The R package supplies the time zone database
# through the tzdb package via the vendored library's set_install(), which
# std::chrono ignores; libstdc++ 14 also can't format zoned_time<days> and has
# a DST bug (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=116110). This is
# the same behaviour as the Rtools40 build, whose gcc 10 has no C++20 chrono
# time zone support.
#
# Brotli is bundled because the Rtools45 Brotli (1.2.0) predates the
# security fix in 1.2.1 (GH-55165). The Rtools45 cURL is built against
# Brotli too, so we deliberately don't ship or link the Rtools Brotli; cURL's
# Brotli symbols resolve from the bundled copy in arrow_bundled_dependencies.
"${CMAKE}" \
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

VERSION="$(grep Version "${ARROW_HOME}/r/DESCRIPTION" | cut -d " " -f 2)"
DST_DIR="r-libarrow-windows-x86_64-${VERSION}"

# Collect the build artifacts and make the shape of zip file that rwinlib
# expects: include/ plus lib/x64-ucrt/ (R_ARCH + CRT, see r/configure.win)
pushd "${BUILD_DIR}"
rm -rf "${DST_DIR}"
mkdir -p "${DST_DIR}/lib/x64-ucrt"
cp -r install/include "${DST_DIR}/include"
cp install/lib/*.a "${DST_DIR}/lib/x64-ucrt/"

# Rtools libraries that r/configure.win links against. Users compiling the R
# package have an Rtools with these too, but ship them so the zip is
# self-contained. Keep in sync with PKG_LIBS in r/configure.win.
for lib in z zstd lz4 bz2 crypto ssl curl; do
  cp "${TOOLCHAIN}/lib/lib${lib}.a" "${DST_DIR}/lib/x64-ucrt/"
done

# cURL's link dependencies differ between Rtools versions (e.g. Rtools43's
# cURL has no nghttp2 or libpsl, Rtools45's does), so derive them from this
# toolchain's libcurl.pc, ship the ones that are toolchain libraries, and
# record the flags in the zip for r/configure.win to use as GCS_LIBS.
# Brotli is skipped: see the Brotli comment above. Windows system and mingw
# runtime import libraries (ws2_32, crypt32, ...) aren't ours to ship, and
# they are written after all the toolchain libraries: with static linking
# every library that needs Winsock etc. must come before -lws2_32, and
# pkg-config's own ordering doesn't guarantee that (Rtools43's libcurl.pc
# lists -lcrypto after the last -lws2_32).
#
# libcurl.pc repeats libraries, and with static linking a library must come
# after everything that needs it, so when removing the duplicates keep the
# last occurrence of each flag, not the first (Rtools45's libcurl.pc has -lz
# and -lunistring both before and after the -lcrypto and -lpsl that need
# them).
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

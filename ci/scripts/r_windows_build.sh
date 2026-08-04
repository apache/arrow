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

set -ex

# SC2223: Quote the default assignment to prevent globbing
: "${ARROW_HOME:=$(pwd)}"
# Make sure it is absolute and exported
# SC2155: Declare and assign separately to avoid masking return values
ARROW_HOME="$(cd "${ARROW_HOME}" && pwd)"
export ARROW_HOME

pacman --noconfirm -Syy

# SC2034: Removed unused variable RWINLIB_LIB_DIR
# SC2223: Quote the default assignment to prevent globbing
: "${MINGW_ARCH:="mingw32 mingw64 ucrt64"}"

export MINGW_ARCH

# SC2086: Double quote to prevent globbing and word splitting
cp "$ARROW_HOME/ci/scripts/PKGBUILD" .
printenv
makepkg-mingw --noconfirm --noprogressbar --skippgpcheck --nocheck --syncdeps --cleanbuild

# SC2086: Double quote to prevent globbing and word splitting
VERSION=$(grep Version "$ARROW_HOME/r/DESCRIPTION" | cut -d " " -f 2)
DST_DIR="r-libarrow-windows-x86_64-$VERSION"

# Collect the build artifacts and make the shape of zip file that rwinlib expects
ls
mkdir -p build
mv mingw* build
cd build

# This may vary by system/CI provider
MSYS_LIB_DIR="/c/rtools${RTOOLS_VERSION}"

# Untar the builds we made
# SC2011: Use find -print0 | xargs -0 to handle non-alphanumeric filenames
# SC2035: Use ./* glob so names with dashes won't become options
find . -maxdepth 1 -name "*.xz" -print0 | xargs -0 -n 1 tar -xJf
# SC2086: Double quote to prevent globbing and word splitting
mkdir -p "$DST_DIR"
# Grab the headers from one, either one is fine
# (if we're building twice to combine old and new toolchains, this may already exist)
# SC2046: Quote command substitution; SC2086: double-quote variables
if [ ! -d "$DST_DIR/include" ]; then
  mv "$(echo "$MINGW_ARCH" | cut -d ' ' -f 1)/include" "$DST_DIR"
fi

# mingw64 -> x64
# mingw32 -> i386
# ucrt64 -> x64-ucrt

if [ -d mingw64/lib/ ]; then
  ls "$MSYS_LIB_DIR/mingw64/lib/"
  # Make the rest of the directory structure
  mkdir -p "$DST_DIR/lib/x64"
  # Move the 64-bit versions of libarrow into the expected location
  mv mingw64/lib/*.a "$DST_DIR/lib/x64"
  # These are from https://dl.bintray.com/rtools/mingw{32,64}/
  cp "$MSYS_LIB_DIR"/mingw64/lib/lib{snappy,zstd,lz4,brotli*,bz2,crypto,curl,ss*,utf8proc,re2,nghttp2}.a "$DST_DIR/lib/x64"
fi

# Same for the 32-bit versions
if [ -d mingw32/lib/ ]; then
  ls "$MSYS_LIB_DIR/mingw32/lib/"
  mkdir -p "$DST_DIR/lib/i386"
  mv mingw32/lib/*.a "$DST_DIR/lib/i386"
  cp "$MSYS_LIB_DIR"/mingw32/lib/lib{snappy,zstd,lz4,brotli*,bz2,crypto,curl,ss*,utf8proc,re2,nghttp2}.a "$DST_DIR/lib/i386"
fi

# Do the same also for ucrt64
if [ -d ucrt64/lib/ ]; then
  ls "$MSYS_LIB_DIR/ucrt64/lib/"
  mkdir -p "$DST_DIR/lib/x64-ucrt"
  mv ucrt64/lib/*.a "$DST_DIR/lib/x64-ucrt"
  cp "$MSYS_LIB_DIR"/ucrt64/lib/lib{snappy,zstd,lz4,brotli*,bz2,crypto,curl,ss*,utf8proc,re2,nghttp2}.a "$DST_DIR/lib/x64-ucrt"
fi

# Create build artifact
zip -r "${DST_DIR}.zip" "$DST_DIR"

# Copy that to a file name/path that does not vary by version number so we
# can easily find it in the R package tests on CI
cp "${DST_DIR}.zip" ../libarrow.zip

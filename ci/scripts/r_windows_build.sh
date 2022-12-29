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

: ${ARROW_HOME:=$(pwd)}
# Make sure it is absolute and exported
export ARROW_HOME="$(cd "${ARROW_HOME}" && pwd)"

# Uncomment L38-41 if you're testing a new rtools dependency that hasn't yet sync'd to CRAN
# curl https://raw.githubusercontent.com/r-windows/rtools-packages/master/pacman.conf > /etc/pacman.conf
# curl -OSsl "http://repo.msys2.org/msys/x86_64/msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz"
# pacman -U --noconfirm msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz && rm msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz
# pacman --noconfirm -Scc

pacman --noconfirm -Syy
RWINLIB_LIB_DIR="lib"
: ${MINGW_ARCH:="mingw32 mingw64 ucrt64"}

export MINGW_ARCH

cp $ARROW_HOME/ci/scripts/PKGBUILD .
printenv
makepkg-mingw --noconfirm --noprogressbar --skippgpcheck --nocheck --syncdeps --cleanbuild

VERSION=$(grep Version $ARROW_HOME/r/DESCRIPTION | cut -d " " -f 2)
DST_DIR="arrow-$VERSION"

# Collect the build artifacts and make the shape of zip file that rwinlib expects
ls
mkdir -p build
mv mingw* build
cd build

# This may vary by system/CI provider
MSYS_LIB_DIR="/c/rtools40"

# Untar the builds we made
ls *.xz | xargs -n 1 tar -xJf
mkdir -p $DST_DIR
# Grab the headers from one, either one is fine
# (if we're building twice to combine old and new toolchains, this may already exist)
if [ ! -d $DST_DIR/include ]; then
  mv $(echo $MINGW_ARCH | cut -d ' ' -f 1)/include $DST_DIR
fi

# mingw64 -> x64
# mingw32 -> i386
# ucrt64 -> x64-ucrt

if [ -d mingw64/lib/ ]; then
  ls $MSYS_LIB_DIR/mingw64/lib/
  # Make the rest of the directory structure
  mkdir -p $DST_DIR/lib/x64
  # Move the 64-bit versions of libarrow into the expected location
  mv mingw64/lib/*.a $DST_DIR/lib/x64
  # These are from https://dl.bintray.com/rtools/mingw{32,64}/
  cp $MSYS_LIB_DIR/mingw64/lib/lib{thrift,snappy,zstd,lz4,brotli*,bz2,crypto,curl,ss*,utf8proc,re2,aws*,nghttp2}.a $DST_DIR/lib/x64
fi

# Same for the 32-bit versions
if [ -d mingw32/lib/ ]; then
  ls $MSYS_LIB_DIR/mingw32/lib/
  mkdir -p $DST_DIR/lib/i386
  mv mingw32/lib/*.a $DST_DIR/lib/i386
  cp $MSYS_LIB_DIR/mingw32/lib/lib{thrift,snappy,zstd,lz4,brotli*,bz2,crypto,curl,ss*,utf8proc,re2,aws*,nghttp2}.a $DST_DIR/lib/i386
fi

# Do the same also for ucrt64
if [ -d ucrt64/lib/ ]; then
  ls $MSYS_LIB_DIR/ucrt64/lib/
  mkdir -p $DST_DIR/lib/x64-ucrt
  mv ucrt64/lib/*.a $DST_DIR/lib/x64-ucrt
  cp $MSYS_LIB_DIR/ucrt64/lib/lib{thrift,snappy,zstd,lz4,brotli*,bz2,crypto,curl,ss*,utf8proc,re2,aws*,nghttp2}.a $DST_DIR/lib/x64-ucrt
fi

# Create build artifact
zip -r ${DST_DIR}.zip $DST_DIR

# Copy that to a file name/path that does not vary by version number so we
# can easily find it in the R package tests on CI
cp ${DST_DIR}.zip ../libarrow.zip

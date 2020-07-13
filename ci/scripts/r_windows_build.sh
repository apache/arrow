#!/bin/bash

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

if [ "$RTOOLS_VERSION" = "35" ]; then
  # Use rtools-backports if building with rtools35
  curl https://raw.githubusercontent.com/r-windows/rtools-backports/master/pacman.conf > /etc/pacman.conf
  pacman --noconfirm -Scc
  pacman --noconfirm -Syy
  # lib-4.9.3 is for libraries compiled with gcc 4.9 (Rtools 3.5)
  RWINLIB_LIB_DIR="lib-4.9.3"
else
  pacman --noconfirm -Syy
  RWINLIB_LIB_DIR="lib"
fi

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

ls $MSYS_LIB_DIR/mingw64/lib/
ls $MSYS_LIB_DIR/mingw32/lib/

# Untar the two builds we made
ls *.xz | xargs -n 1 tar -xJf
mkdir -p $DST_DIR
# Grab the headers from one, either one is fine
# (if we're building twice to combine old and new toolchains, this may already exist)
if [ ! -d $DST_DIR/include ]; then
  mv mingw64/include $DST_DIR
fi

# Make the rest of the directory structure
# lib-4.9.3 is for libraries compiled with gcc 4.9 (Rtools 3.5)
mkdir -p $DST_DIR/${RWINLIB_LIB_DIR}/x64
mkdir -p $DST_DIR/${RWINLIB_LIB_DIR}/i386
# lib is for the new gcc 8 toolchain (Rtools 4.0)
mkdir -p $DST_DIR/lib/x64
mkdir -p $DST_DIR/lib/i386

# Move the 64-bit versions of libarrow into the expected location
mv mingw64/lib/*.a $DST_DIR/${RWINLIB_LIB_DIR}/x64
# Same for the 32-bit versions
mv mingw32/lib/*.a $DST_DIR/${RWINLIB_LIB_DIR}/i386

# These may be from https://dl.bintray.com/rtools/backports/
cp $MSYS_LIB_DIR/mingw64/lib/lib{thrift,snappy}.a $DST_DIR/${RWINLIB_LIB_DIR}/x64
cp $MSYS_LIB_DIR/mingw32/lib/lib{thrift,snappy}.a $DST_DIR/${RWINLIB_LIB_DIR}/i386

# These are from https://dl.bintray.com/rtools/mingw{32,64}/
cp $MSYS_LIB_DIR/mingw64/lib/lib{zstd,lz4,crypto}.a $DST_DIR/lib/x64
cp $MSYS_LIB_DIR/mingw32/lib/lib{zstd,lz4,crypto}.a $DST_DIR/lib/i386

# Create build artifact
zip -r ${DST_DIR}.zip $DST_DIR

# Copy that to a file name/path that does not vary by version number so we
# can easily find it in the R package tests on CI
cp ${DST_DIR}.zip ../libarrow.zip

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

# ccache may be broken on MinGW.
# pacman --sync --noconfirm ccache

wget https://raw.githubusercontent.com/r-windows/rtools-backports/master/pacman.conf
cp -f pacman.conf /etc/pacman.conf

pacman --noconfirm -Scc
pacman --noconfirm -Syyu
pacman --noconfirm --needed -S git base-devel binutils zip

# Install core build stuff
pacman --noconfirm --needed -S mingw-w64-{i686,x86_64}-{toolchain,crt,winpthreads,gcc,libtre,pkg-config,xz}

# Force static linking
rm -f /mingw32/lib/*.dll.a
rm -f /mingw64/lib/*.dll.a
export PKG_CONFIG="/${MINGW_PREFIX}/bin/pkg-config --static"

cp $ARROW_HOME/ci/scripts/PKGBUILD .
export PKGEXT='.pkg.tar.xz' # pacman default changed to .zst in 2020, but keep the old ext for compat
printenv
makepkg-mingw --noconfirm --noprogressbar --skippgpcheck --nocheck --syncdeps --cleanbuild

VERSION=$(grep Version $ARROW_HOME/r/DESCRIPTION | cut -d " " -f 2)
DST_DIR="arrow-$VERSION"

# Collect the build artifacts and make the shape of zip file that rwinlib expects
ls
mkdir build
cp mingw* build
cd build

# This may vary by system/CI provider
MSYS_LIB_DIR="D:/a/_temp/msys/msys64"

ls $MSYS_LIB_DIR/mingw64/lib/
ls $MSYS_LIB_DIR/mingw32/lib/

# Untar the two builds we made
ls | xargs -n 1 tar -xJf
mkdir $DST_DIR
# Grab the headers from one, either one is fine
mv mingw64/include $DST_DIR

# Make the rest of the directory structure
# lib-4.9.3 is for libraries compiled with gcc 4.9 (Rtools 3.5)
mkdir -p $DST_DIR/lib-4.9.3/x64
mkdir -p $DST_DIR/lib-4.9.3/i386
# lib is for the new gcc 8 toolchain (Rtools 4.0)
mkdir -p $DST_DIR/lib/x64
mkdir -p $DST_DIR/lib/i386

# Move the 64-bit versions of libarrow into the expected location
mv mingw64/lib/*.a $DST_DIR/lib-4.9.3/x64
# Same for the 32-bit versions
mv mingw32/lib/*.a $DST_DIR/lib-4.9.3/i386

# These are from https://dl.bintray.com/rtools/backports/
cp $MSYS_LIB_DIR/mingw64/lib/lib{thrift,snappy}.a $DST_DIR/lib-4.9.3/x64
cp $MSYS_LIB_DIR/mingw32/lib/lib{thrift,snappy}.a $DST_DIR/lib-4.9.3/i386

# These are from https://dl.bintray.com/rtools/mingw{32,64}/
cp $MSYS_LIB_DIR/mingw64/lib/lib{zstd,lz4,crypto}.a $DST_DIR/lib/x64
cp $MSYS_LIB_DIR/mingw32/lib/lib{zstd,lz4,crypto}.a $DST_DIR/lib/i386

# Create build artifact
zip -r ${DST_DIR}.zip $DST_DIR

# Copy that to a file name/path that does not vary by version number so we
# can easily find it in the R package tests on CI
cp ${DST_DIR}.zip ../libarrow.zip

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

set -eu

VERSION=$(grep Version ../r/DESCRIPTION | cut -d " " -f 2)
DST_DIR="arrow-$VERSION"

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

# Get dependencies from rtools-backports
mkdir deps35 && cd deps35
wget https://dl.bintray.com/rtools/backports/mingw-w64-i686-boost-1.67.0-8000-any.pkg.tar.xz
wget https://dl.bintray.com/rtools/backports/mingw-w64-x86_64-boost-1.67.0-8000-any.pkg.tar.xz
wget https://dl.bintray.com/rtools/backports/mingw-w64-i686-thrift-0.12.0-8000-any.pkg.tar.xz
wget https://dl.bintray.com/rtools/backports/mingw-w64-x86_64-thrift-0.12.0-8000-any.pkg.tar.xz
wget https://dl.bintray.com/rtools/backports/mingw-w64-i686-snappy-1.1.7-2-any.pkg.tar.xz
wget https://dl.bintray.com/rtools/backports/mingw-w64-x86_64-snappy-1.1.7-2-any.pkg.tar.xz

ls | xargs -n 1 tar -xJf
mv mingw64/lib/*.a ../${DST_DIR}/lib-4.9.3/x64
mv mingw32/lib/*.a ../${DST_DIR}/lib-4.9.3/i386

cd ..

mkdir deps40 && cd deps40
# double-conversion is only available in the Rtools4.0 builds, but apparently that's ok
wget https://dl.bintray.com/rtools/mingw64/mingw-w64-x86_64-double-conversion-3.1.2-1-any.pkg.tar.xz
wget https://dl.bintray.com/rtools/mingw32/mingw-w64-i686-double-conversion-3.1.2-1-any.pkg.tar.xz
# These are the other Rtools 4.0 packages, for future reference
# wget https://dl.bintray.com/rtools/mingw32/mingw-w64-i686-boost-1.67.0-9002-any.pkg.tar.xz
# wget https://dl.bintray.com/rtools/mingw64/mingw-w64-x86_64-boost-1.67.0-9002-any.pkg.tar.xz
# wget https://dl.bintray.com/rtools/mingw32/mingw-w64-i686-thrift-0.12.0-1-any.pkg.tar.xz
# wget https://dl.bintray.com/rtools/mingw64/mingw-w64-x86_64-thrift-0.12.0-1-any.pkg.tar.xz
# wget https://dl.bintray.com/rtools/mingw32/mingw-w64-i686-snappy-1.1.7-2-any.pkg.tar.xz
# wget https://dl.bintray.com/rtools/mingw64/mingw-w64-x86_64-snappy-1.1.7-2-any.pkg.tar.xz

ls | xargs -n 1 tar -xJf
mv mingw64/lib/*.a ../${DST_DIR}/lib/x64
mv mingw32/lib/*.a ../${DST_DIR}/lib/i386

cd ..

# Create build artifact
zip -r ${DST_DIR}.zip $DST_DIR

# Copy that to a file name/path that does not vary by version number so we
# can easily find it in the R package tests on Appveyor
cp ${DST_DIR}.zip ../libarrow.zip

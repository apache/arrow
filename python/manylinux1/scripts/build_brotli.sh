#!/bin/bash -ex
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

export BROTLI_VERSION="0.6.0"
wget "https://github.com/google/brotli/archive/v${BROTLI_VERSION}.tar.gz" -O brotli-${BROTLI_VERSION}.tar.gz
tar xf brotli-${BROTLI_VERSION}.tar.gz
pushd brotli-${BROTLI_VERSION}
mkdir build
pushd build
cmake -DCMAKE_BUILD_TYPE=release \
    "-DCMAKE_CXX_FLAGS=-fPIC" \
    "-DCMAKE_C_FLAGS=-fPIC" \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DBUILD_SHARED_LIBS=OFF \
    ..
make -j5
make install
popd
popd
rm -rf brotli-${BROTLI_VERSION}.tar.gz brotli-${BROTLI_VERSION}

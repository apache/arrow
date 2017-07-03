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

export LZ4_VERSION="1.7.5"
export PREFIX="/usr"
export LDFLAGS="${LDFLAGS} -Wl,-rpath,${PREFIX}/lib -L${PREFIX}/lib"
wget "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz" -O lz4-${LZ4_VERSION}.tar.gz
tar xf lz4-${LZ4_VERSION}.tar.gz
pushd lz4-${LZ4_VERSION}

make -j5 PREFIX=${PREFIX}
make install PREFIX=$PREFIX
popd
rm -rf lz4-${LZ4_VERSION}.tar.gz lz4-${LZ4_VERSION}

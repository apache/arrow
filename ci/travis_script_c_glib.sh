#!/usr/bin/env bash

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

set -e

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

pushd $ARROW_C_GLIB_DIR

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib
NO_MAKE=yes test/run-test.sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_C_GLIB_INSTALL/lib
export GI_TYPELIB_PATH=$ARROW_C_GLIB_INSTALL/lib/girepository-1.0
export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig
export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_C_GLIB_INSTALL/lib/pkgconfig

pushd example/lua
if [ $TRAVIS_OS_NAME == "osx" ]; then
  lua write-batch.lua
  lua read-batch.lua
  lua write-stream.lua
  lua read-stream.lua
else
  . ~/torch/install/bin/torch-activate
  luajit write-batch.lua
  luajit read-batch.lua
  luajit write-stream.lua
  luajit read-stream.lua
  luajit stream-to-torch-tensor.lua
fi
popd

pushd example/go
make generate
make
./write-batch
./read-batch
./write-stream
./read-stream
popd

popd

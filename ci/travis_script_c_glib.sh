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

set -e

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

pushd $ARROW_C_GLIB_DIR

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib
if [ $BUILD_SYSTEM = "autotools" ]; then
  arrow_c_glib_lib_dir=$ARROW_C_GLIB_INSTALL/lib
else
  arrow_c_glib_lib_dir=$ARROW_C_GLIB_INSTALL/lib/$(arch)-linux-gnu
fi
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$arrow_c_glib_lib_dir
export GI_TYPELIB_PATH=$arrow_c_glib_lib_dir/girepository-1.0
test/run-test.rb

if [ $BUILD_SYSTEM = "meson" ]; then
  exit
fi

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig
export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$arrow_c_glib_lib_dir/pkgconfig

pushd example/lua
if [ $TRAVIS_OS_NAME = "osx" ]; then
  lua write-batch.lua
  lua read-batch.lua
  lua write-stream.lua
  lua read-stream.lua
else
  if [ $BUILD_TORCH_EXAMPLE = "yes" ]; then
    . ~/torch/install/bin/torch-activate
    luajit write-batch.lua
    luajit read-batch.lua
    luajit write-stream.lua
    luajit read-stream.lua
    luajit stream-to-torch-tensor.lua
  else
    lua write-batch.lua
    lua read-batch.lua
    lua write-stream.lua
    lua read-stream.lua
  fi
fi
popd

popd

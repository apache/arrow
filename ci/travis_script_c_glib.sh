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

arrow_c_glib_run_test()
{
  local arrow_c_glib_lib_dir=$1

  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$arrow_c_glib_lib_dir
  export GI_TYPELIB_PATH=$arrow_c_glib_lib_dir/girepository-1.0
  test/run-test.rb

  export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$arrow_c_glib_lib_dir/pkgconfig

  pushd example/lua
  lua write-batch.lua
  lua read-batch.lua
  lua write-stream.lua
  lua read-stream.lua
  popd
}

pushd $ARROW_C_GLIB_DIR

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib
export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig

(arrow_c_glib_run_test $ARROW_C_GLIB_INSTALL_AUTOTOOLS/lib)
if [ -d $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu ]; then
  (arrow_c_glib_run_test $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu)
# else # TODO: Enable this
#   (arrow_c_glib_run_test $ARROW_C_GLIB_INSTALL_MESON/lib)
fi

popd

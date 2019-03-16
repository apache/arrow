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

arrow_ruby_run_test()
{
  local arrow_c_glib_lib_dir="$1"

  local ld_library_path_keep="$LD_LIBRARY_PATH"
  local pkg_config_path_keep="$PKG_COFNIG_PATH"
  LD_LIBRARY_PATH="${arrow_c_glib_lib_dir}:${LD_LIBRARY_PATH}"
  PKG_CONFIG_PATH="${arrow_c_glib_lib_dir}/pkgconfig:${PKG_CONFIG_PATH}"
  export GI_TYPELIB_PATH="${arrow_c_glib_lib_dir}/girepository-1.0"
  test/run-test.rb
  LD_LIBRARY_PATH="$ld_library_path_keep"
  PKG_CONFIG_PATH="$pkg_config_path_keep"
}

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib
export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig

pushd $ARROW_RUBY_DIR/red-arrow
(arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_AUTOTOOLS/lib)
if [ -d $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu ]; then
  (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu)
# else # TODO: Enable this
#   (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib)
fi
popd

# TODO: GPU required
# pushd $ARROW_RUBY_DIR/red-arrow-gpu
# (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_AUTOTOOLS/lib)
# if [ -d $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu ]; then
#   (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu)
# # else # TODO: Enable this
# #   (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib)
# fi
# popd

pushd $ARROW_RUBY_DIR/red-gandiva
(arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_AUTOTOOLS/lib)
if [ -d $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu ]; then
  (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu)
# else # TODO: Enable this
#   (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib)
fi
popd

pushd $ARROW_RUBY_DIR/red-parquet
(arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_AUTOTOOLS/lib)
if [ -d $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu ]; then
  (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu)
# else # TODO: Enable this
#   (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib)
fi
popd

pushd $ARROW_RUBY_DIR/red-plasma
(arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_AUTOTOOLS/lib)
if [ -d $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu ]; then
  (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib/$(arch)-linux-gnu)
# else # TODO: Enable this
#   (arrow_ruby_run_test $ARROW_C_GLIB_INSTALL_MESON/lib)
fi
popd

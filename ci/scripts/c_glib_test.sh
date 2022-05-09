#!/usr/bin/env bash
#
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

source_dir=${1}/c_glib
build_dir=${2}/c_glib

: ${ARROW_GLIB_VALA:=true}

export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig
export GI_TYPELIB_PATH=${ARROW_HOME}/lib/girepository-1.0

# Enable memory debug checks.
export ARROW_DEBUG_MEMORY_POOL=trap

pushd ${source_dir}

ruby test/run-test.rb

if [[ "$(uname -s)" == "Linux" ]]; then
  # TODO(kszucs): on osx it fails to load 'lgi.corelgilua51' despite that lgi
  # was installed by luarocks
  pushd example/lua
  lua write-file.lua
  lua read-file.lua
  lua write-stream.lua
  lua read-stream.lua
  popd
fi

popd

pushd ${build_dir}
example/build
example/extension-type
if [ "${ARROW_GLIB_VALA}" = "true" ]; then
  example/vala/build
fi
popd

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

export ARROW_C_GLIB_HOME=$CONDA_PREFIX

export CFLAGS="-DARROW_NO_DEPRECATED_API"
export CXXFLAGS="-DARROW_NO_DEPRECATED_API"

mkdir -p /build/c_glib

# Build with Meson
meson --prefix=$ARROW_C_GLIB_HOME \
      --libdir=lib \
      /build/c_glib \
      /arrow/c_glib

pushd /build/c_glib
  ninja
  ninja install
popd

#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex

arrow_dir=${1}
build_dir=${2}/docs

export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig:${PKG_CONFIG_PATH}
export GI_TYPELIB_PATH=${ARROW_HOME}/lib/girepository-1.0
export CFLAGS="-DARROW_NO_DEPRECATED_API"
export CXXFLAGS="-DARROW_NO_DEPRECATED_API"

# Prose and Python
sphinx-build -b html ${arrow_dir}/docs/source ${build_dir}

# C++ - original doxygen
# rsync -a ${arrow_dir}/cpp/apidoc/ ${build_dir}/cpp

# R
rsync -a ${arrow_dir}/r/docs/ ${build_dir}/r

# C GLib
rsync -a ${ARROW_HOME}/share/gtk-doc/html/ ${build_dir}/c_glib

# Java
rsync -a ${arrow_dir}/java/target/site/apidocs/ ${build_dir}/java/reference

# Javascript
rsync -a ${arrow_dir}/js/doc/ ${build_dir}/js

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

# install emscripten sdk version to match pyodide in $2 to directory $1/emsdk

set -e

target_path=$1
pyodide_path=$2

emscripten_version=$(${pyodide_path}/python -c "import sys;print(*sys._emscripten_info.emscripten_version,sep='.')")

cd ${target_path}
ls emsdk || git clone https://github.com/emscripten-core/emsdk.git
cd emsdk 
./emsdk install ${emscripten_version} 
./emsdk activate ${emscripten_version}
echo "Installed emsdk to: ${target_path}"
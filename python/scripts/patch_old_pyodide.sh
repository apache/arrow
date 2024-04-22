#! /bin/bash
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

# TEMPORARY PATCH FOR PYODIDE / EMSCRIPTEN BUG
# - n.b. this can be removed once Pyodide 0.26 is released
# if pyodide version is 0.25.1, then it needs a patched version of pyodide.js
# or else tests will fail due to this bug:
# https://github.com/emscripten-core/emscripten/pull/21759
# 
source_dir=${1}/python
pyodide_dir=${2}

npx --yes prettier --write ${pyodide_dir}/pyodide.asm.js 
patch -u -b ${pyodide_dir}/pyodide.asm.js -i slang.patch ${source_dir}/scripts/pyodide_25.1.patch

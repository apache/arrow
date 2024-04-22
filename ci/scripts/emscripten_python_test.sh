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

# run tests against 

set -ex
source_dir=${1}/python
cd ${source_dir}

# apply patch to pyodide 0.25.1
if [[ $PYODIDE_VERSION == "0.25.1" ]]; then
    bash scripts/patch_old_pyodide.sh ${1} ${2}
fi

dist_dir=${2}

# note: this assumes that there is only one wheel built into dist 
# (which is true if you build using emscripten_python_build.sh)
echo "-------------- Running emscripten tests in Chrome --------------------"
python scripts/run_emscripten_tests.py dist/pyarrow*.whl --dist-dir=${dist_dir} --runtime=chrome

echo "-------------- Running emscripten tests in Node ----------------------"
python scripts/run_emscripten_tests.py dist/pyarrow*.whl --dist-dir=${dist_dir} --runtime=node

echo "-------------- Running emscripten tests in Firefox -------------------"
python scripts/run_emscripten_tests.py dist/pyarrow*.whl --dist-dir=${dist_dir} --runtime=firefox

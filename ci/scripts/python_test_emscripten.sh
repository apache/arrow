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

# run tests against Chrome and node.js as representative
# WebAssembly platforms (i.e. one browser, one non-browser).

set -ex

build_dir=${1}/python
pyodide_dist_dir=${2}

cd ${build_dir}

# note: this uses the newest wheel in dist
pyodide_wheel=$(ls -t dist/pyarrow*.whl | head -1)

echo "-------------- Running emscripten tests in Node ----------------------"
python scripts/run_emscripten_tests.py ${pyodide_wheel} --dist-dir=${pyodide_dist_dir} --runtime=node

echo "-------------- Running emscripten tests in Chrome --------------------"
python scripts/run_emscripten_tests.py ${pyodide_wheel} --dist-dir=${pyodide_dist_dir} --runtime=chrome


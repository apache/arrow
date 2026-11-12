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
# keep the virtual environment outside of the source tree, so that
# pytest imports the installed wheel and does not pick up the
# python/setup.cfg (its faulthandler_timeout setting needs threads)
pyodide_venv_dir=${1}/pyodide-venv

cd "${build_dir}"

# note: this uses the newest wheel in dist
# shellcheck disable=SC2012
pyodide_wheel=$(ls -t dist/pyarrow*.whl | head -1)

echo "-------------- Running emscripten tests in Node ----------------------"
# A Pyodide virtual environment runs Python in Node.js, and
# its pip installs pyemscripten wheels
rm -rf "${pyodide_venv_dir}"
pyodide venv "${pyodide_venv_dir}"
"${pyodide_venv_dir}/bin/pip" install \
  "${pyodide_wheel}" \
  cffi \
  cloudpickle \
  fsspec \
  hypothesis \
  packaging \
  pandas \
  pytest \
  pytz \
  scipy \
  tzdata

pushd "${pyodide_venv_dir}"
# FIXME: Substrait expression serialization crashes the interpreter
# with a "Cannot convert a BigInt value to a number" error.
"${pyodide_venv_dir}/bin/python" -m pytest -r s -m "not substrait" --pyargs pyarrow
popd

echo "-------------- Running emscripten tests in Chrome --------------------"
python scripts/run_emscripten_tests.py "${pyodide_wheel}" --dist-dir="${pyodide_dist_dir}" --runtime=chrome


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

arrow_dir=${1}
build_dir=${2}

# We don't need to follow this external file.
# See also: https://www.shellcheck.net/wiki/SC1090
#
# shellcheck source=/dev/null
source ~/emsdk/emsdk_env.sh

source_dir=${arrow_dir}/python
python_build_dir=${build_dir}/python

rm -rf "${python_build_dir}"
cp -aL "${source_dir}" "${python_build_dir}"

# conda sets LDFLAGS / CFLAGS etc. which break
# emcmake so we unset them
unset LDFLAGS CFLAGS CXXFLAGS CPPFLAGS

pushd "${python_build_dir}"
pyodide build
# Verify PyInit_lib export has been kept
python -c "
import glob, zipfile
whl = glob.glob('dist/pyarrow-*.whl')[0]
z = zipfile.ZipFile(whl)
so = [n for n in z.namelist() if n.endswith('lib.cpython-312-wasm32-emscripten.so')][0]
print(so, 'contains PyInit_lib:', b'PyInit_lib' in z.read(so))
"
popd

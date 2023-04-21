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

source_dir=${1}/csharp

# Python and PyArrow are required for C Data Interface tests.
if [ -z "${PYTHON}" ]; then
  if type python3 > /dev/null 2>&1; then
    export PYTHON=python3
  else
    export PYTHON=python
  fi
fi
${PYTHON} -m pip install pyarrow find-libpython
export PYTHONNET_PYDLL=$(${PYTHON} -m find_libpython)

pushd ${source_dir}
dotnet test
for pdb in artifacts/Apache.Arrow/*/*/Apache.Arrow.pdb; do
  sourcelink test ${pdb}
done
popd

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

source_dir=${1}/go

if [ -n "${ARROW_PYTHON_VENV:-}" ]; then
  . "${ARROW_PYTHON_VENV}/bin/activate"
fi

export GOFLAGS="${GOFLAGS} -gcflags=all=-d=checkptr"

pushd ${source_dir}/arrow/cdata/test

case "$(uname)" in
    Linux)
        testlib="cgotest.so"
        ;;
    Darwin)
        testlib="cgotest.so"
        ;;
    MINGW*)
        testlib="cgotest.dll"
        ;;
esac

go build -tags cdata_test,assert -buildmode=c-shared -o $testlib .

python test_export_to_cgo.py

rm $testlib
rm "${testlib%.*}.h"

popd

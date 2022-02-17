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

testargs="-race"
case "$(uname)" in
    MINGW*)
        # -race doesn't work on windows currently
        testargs=""
        ;;
esac

if [[ "$(go env GOHOSTARCH)" = "s390x" ]]; then
    testargs="" # -race not supported on s390x
fi

pushd ${source_dir}/arrow

TAGS="assert,test"
if [[ -n "${ARROW_GO_TESTCGO}" ]]; then    
    if [[ "${MSYSTEM}" = "MINGW64" ]]; then
        export PATH=${MINGW_PREFIX}/bin:$PATH        
    fi
    TAGS="${TAGS},ccalloc"
fi


# the cgo implementation of the c data interface requires the "test"
# tag in order to run its tests so that the testing functions implemented
# in .c files don't get included in non-test builds.

for d in $(go list ./... | grep -v vendor); do
    go test $testargs -tags $TAGS $d
done

popd

export PARQUET_TEST_DATA=${1}/cpp/submodules/parquet-testing/data

pushd ${source_dir}/parquet

for d in $(go list ./... | grep -v vendor); do
    go test $testargs -tags assert $d
done

popd

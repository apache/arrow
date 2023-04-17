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

# simplistic semver comparison
verlte() {
    [ "$1" = "`echo -e "$1\n$2" | sort -V | head -n1`" ]
}
verlt() {
    [ "$1" = "$2" ] && return 1 || verlte $1 $2
}

ver=`go env GOVERSION`

source_dir=${1}/go

testargs="-race"
if verlte "1.18" "${ver#go}" && [ "$(go env GOOS)" != "darwin" ]; then
    # asan not supported on darwin/amd64
    testargs="-asan"
fi

case "$(uname)" in
    MINGW*)
        # -asan and -race don't work on windows currently
        testargs=""
        ;;
esac

if [[ "$(go env GOHOSTARCH)" = "s390x" ]]; then
    testargs="" # -race and -asan not supported on s390x
fi

# Go static check (skipped in MinGW)
if [[ -z "${MINGW_LINT}" ]]; then
    pushd ${source_dir}
    "$(go env GOPATH)"/bin/staticcheck ./...
    popd
fi


pushd ${source_dir}/arrow

TAGS="assert,test"
if [[ -n "${ARROW_GO_TESTCGO}" ]]; then
    if [[ "${MSYSTEM}" = "MINGW64" ]]; then
        export PATH=${MINGW_PREFIX}\\bin:${MINGW_PREFIX}\\lib:$PATH
    fi
    TAGS="${TAGS},ccalloc"
fi

# the cgo implementation of the c data interface requires the "test"
# tag in order to run its tests so that the testing functions implemented
# in .c files don't get included in non-test builds.

go test $testargs -tags $TAGS ./...

# run it again but with the noasm tag
go test $testargs -tags $TAGS,noasm ./...

popd

export PARQUET_TEST_DATA=${1}/cpp/submodules/parquet-testing/data
export ARROW_TEST_DATA=${1}/testing/data
pushd ${source_dir}/parquet

go test $testargs -tags assert ./...

# run the tests again but with the noasm tag
go test $testargs -tags assert,noasm ./...

popd

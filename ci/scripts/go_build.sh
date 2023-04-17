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

pushd ${source_dir}/arrow

if [[ -n "${ARROW_GO_TESTCGO}" ]]; then
    if [[ "${MSYSTEM}" = "MINGW64" ]]; then        
        export PATH=${MINGW_PREFIX}/bin:$PATH
        go clean -cache
        go clean -testcache        
    fi
    TAGS="-tags assert,test,ccalloc"    
fi

go install $TAGS -v ./...

popd

pushd ${source_dir}/parquet

go install -v ./...

popd

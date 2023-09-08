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

# this will output the benchmarks to STDOUT but if `-json` is passed
# as the second argument, it will create a file "bench_stats.json"
# in the directory this is called from containing a json representation

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

export PARQUET_TEST_DATA=${1}/cpp/submodules/parquet-testing/data
pushd ${source_dir}

# lots of benchmarks, they can take a while
# the timeout is for *ALL* benchmarks together,
# not per benchmark
go test -bench=. -benchmem -timeout 40m -run=^$ ./... | tee bench_stat.dat

popd

if [[ "$2" = "-json" ]]; then
    go install go.bobheadxi.dev/gobenchdata@latest
    export PATH=`go env GOPATH`/bin:$PATH
    cat ${source_dir}/bench_*.dat | gobenchdata --json bench_stats.json
fi    

rm ${source_dir}/bench_*.dat
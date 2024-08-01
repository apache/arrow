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

data_gen_dir=${1}/swift/data-generator/swift-datagen
export GOPATH=/
pushd ${data_gen_dir}
go get -d ./...
go run .
cp *.arrow ../../Arrow
popd

source_dir=${1}/swift
pushd ${source_dir}
swiftlint --strict
popd

source_dir=${1}/swift/Arrow
pushd ${source_dir}
sed 's/\/\/ build://g' Package.swift > Package.swift.build
mv Package.swift.build Package.swift
swift test
popd

source_dir=${1}/swift/ArrowFlight
pushd ${source_dir}
sed 's/\/\/ build://g' Package.swift > Package.swift.build
mv Package.swift.build Package.swift
swift test
popd

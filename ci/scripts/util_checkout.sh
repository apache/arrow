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

# this script is github actions specific to check out the submodules and tags

set -ex

# TODO(kszucs): remove it once the "submodules: recursive" feature is released
auth_header="$(git config --local --get http.https://github.com/.extraheader)"
git submodule sync --recursive
git -c "http.extraheader=$auth_header" -c protocol.version=2 submodule update --init --force --recursive --depth=1

# fetch all the tags
git fetch --depth=1 origin +refs/tags/*:refs/tags/*

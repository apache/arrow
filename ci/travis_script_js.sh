#!/usr/bin/env bash

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

set -e

JS_DIR=${TRAVIS_BUILD_DIR}/js

pushd $JS_DIR

npm run lint
npm run build
# run once to write the snapshots
npm test -- -t ts -u
# run again to test all builds against the snapshots
npm test
# run tests against source to generate coverage data
npm run test:coverage
# Uncomment to upload to coveralls
# cat ./coverage/lcov.info | ./node_modules/coveralls/bin/coveralls.js;

popd

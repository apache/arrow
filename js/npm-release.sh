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

# validate the targets pass all tests before publishing
yarn --frozen-lockfile
yarn gulp

read -p "Please enter your npm 2FA one-time password (or leave empty if you don't have 2FA enabled): " NPM_OTP </dev/tty

# publish the JS target modules to npm
yarn lerna exec --concurrency 1 --no-bail "npm publish${NPM_OTP:+ --otp=$NPM_OTP}"

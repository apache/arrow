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

set -e

# Pin azurite to 3.29.0 due to https://github.com/apache/arrow/issues/41505
case "$(uname)" in
  Darwin)
    npm install -g azurite@v3.29.0
    which azurite
    ;;
  MINGW*)
    choco install nodejs.install
    npm install -g azurite@v3.29.0
    ;;
  Linux)
    npm install -g azurite@v3.29.0
    which azurite
    ;;
esac
echo "node version = $(node --version)"
echo "azurite version = $(azurite --version)"
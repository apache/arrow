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
set -u
set -o pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <artifact>"
  exit 1
fi

artifact="$1"

if type shasum >/dev/null 2>&1; then
  sha256_generate="shasum -a 256"
  sha512_generate="shasum -a 512"
else
  sha256_generate="sha256sum"
  sha512_generate="sha512sum"
fi

${sha256_generate} "${artifact}" > "${artifact}.sha256"
${sha512_generate} "${artifact}" > "${artifact}.sha512"

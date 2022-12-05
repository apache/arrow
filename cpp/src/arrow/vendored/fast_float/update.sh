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

set -eu

source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 VERSION"
  echo " e.g.: $0 3.8.1"
  exit 1
fi

version="$1"

pushd "${source_dir}"
rm -rf fast_float
git clone \
    --branch "v${version}" \
    --depth 1 \
    https://github.com/fastfloat/fast_float.git
mv fast_float/include/fast_float/* ./
rm -rf fast_float
sed -i.bak -E -e "s/v[0-9.]+/v${version}/g" *.h
sed -i.bak -E \
    -e '/^namespace fast_float \{/ i namespace arrow_vendored {' \
    -e '/^} \/\/ namespace fast_float/ a } // namespace arrow_vendored' \
    *.h
rm *.bak
popd

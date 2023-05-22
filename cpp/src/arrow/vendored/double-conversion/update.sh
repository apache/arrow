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
  echo " e.g.: $0 3.2.1"
  exit 1
fi

version="$1"

pushd "${source_dir}"
rm -rf double-conversion
git clone \
    --branch "v${version}" \
    --depth 1 \
    https://github.com//google/double-conversion.git
rm *.cc *.h
for f in double-conversion/double-conversion/*{.h,.cc}; do
  mv "${f}" ./
done
rm -rf double-conversion
sed -i.bak -E -e "s/v[0-9.]+/v${version}/g" *.md
sed -i.bak -E \
    -e '/^namespace double_conversion \{/ i\
namespace arrow_vendored {' \
    -e '/^}  \/\/ namespace double_conversion/ a\
}  // namespace arrow_vendored' \
    *.{h,cc}
rm *.bak

# Custom changes for Arrow
patch double-to-string.cc  patches/double-to-string.cc.patch
patch double-to-string.h  patches/double-to-string.h.patch

popd

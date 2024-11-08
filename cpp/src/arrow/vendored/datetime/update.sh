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

set -eux

source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 VERSION"
  echo " e.g.: $0 3.0.3"
  exit 1
fi

version="$1"

pushd "${source_dir}"
rm -rf date
git clone \
    --branch "v${version}" \
    --depth 1 \
    https://github.com/HowardHinnant/date.git
commit_id=$(git -C date log -1 --format=format:%H)
mv date/include/date/date.h ./
mv date/include/date/ios.h ./
mv date/include/date/tz.h ./
mv date/include/date/tz_private.h ./
mv date/src/* ./
rm -rf date
sed -i.bak -E \
    -e 's/namespace date/namespace arrow_vendored::date/g' \
    -e 's,include "date/,include ",g' \
    *.{cpp,h,mm}
sed -i.bak -E \
    -e "s/changeset [0-9a-f]+/changeset ${commit_id}/g" \
    README.md
rm *.bak
popd

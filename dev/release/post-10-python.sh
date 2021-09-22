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
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: ${TEST_PYPI:=0}

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

tmp=$(mktemp -d -t "arrow-post-python.XXXXX")
${PYTHON:-python} \
  "${SOURCE_DIR}/download_rc_binaries.py" \
  ${version} \
  ${rc} \
  --dest="${tmp}" \
  --package_type=python \
  --regex=".*\.(whl|tar\.gz)$"

if [ ${TEST_PYPI} -gt 0 ]; then
  TWINE_ARGS="--repository-url https://test.pypi.org/legacy/"
fi

twine upload ${TWINE_ARGS} ${tmp}/python-rc/${version}-rc${rc}/*.{whl,tar.gz}

rm -rf "${tmp}"

echo "Success! The released PyPI packages are available here:"
echo "  https://pypi.org/project/pyarrow/${version}"

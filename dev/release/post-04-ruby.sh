#!/usr/bin/env bash
# -*- indent-tabs-mode: nil; sh-indentation: 2; sh-basic-offset: 2 -*-
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
#
set -e
set -o pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit
fi

version=$1
archive_name=apache-arrow-${version}
tar_gz=${archive_name}.tar.gz

echo "NOTE: We should release RubyGems after Homebrew and MSYS2 packages are updated!!!"

echo "Checking Homebrew package..."
homebrew_version=$(
  curl \
    --fail \
    --no-progress-meter \
    https://raw.githubusercontent.com/Homebrew/homebrew-core/HEAD/Formula/apache-arrow-glib.rb | \
    grep url | \
    grep -o "[0-9]*\.[0-9]*\.[0-9]*" | \
    head -n 1)
echo "Homebrew package version: ${homebrew_version}"
if [ "${version}" = "${homebrew_version}" ]; then
  echo "OK!"
else
  echo "Different!"
  exit 1
fi


echo "Checking MSYS2 package..."
msys2_version=$(
  curl \
    --fail \
    --no-progress-meter \
    https://packages.msys2.org/base/mingw-w64-arrow | \
    grep -A 1 ">Version:<" | \
    grep -o "[0-9]*\.[0-9]*\.[0-9]*")
echo "MSYS2 package version: ${msys2_version}"
if [ "${version}" = "${msys2_version}" ]; then
  echo "OK!"
else
  echo "Different!"
  exit 1
fi


rm -f ${tar_gz}
curl \
  --remote-name \
  --fail \
  https://downloads.apache.org/arrow/arrow-${version}/${tar_gz}
rm -rf ${archive_name}
tar xf ${tar_gz}

read -p "Please enter your RubyGems MFA one-time password (or leave empty if you don't have MFA enabled): " GEM_HOST_OTP_CODE </dev/tty
export GEM_HOST_OTP_CODE

modules=()
for module in ${archive_name}/ruby/red-*; do
  pushd ${module}
  rake release
  modules+=($(basename ${module}))
  popd
done
rm -rf ${archive_name}
rm -f ${tar_gz}

echo "Success! The released RubyGems are available here:"
for module in ${modules[@]}; do
  echo "  https://rubygems.org/gems/${module}/versions/${version}"
done

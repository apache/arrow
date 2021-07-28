#!/bin/bash
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
set -u

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

tmp_dir=tmp-apache-arrow-dist

echo "Recreate temporary directory: ${tmp_dir}"
rm -rf ${tmp_dir}
mkdir -p ${tmp_dir}

echo "Clone dev dist repository"
svn \
  co \
  https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-${version}-rc${rc} \
  ${tmp_dir}/dev

echo "Clone release dist repository"
svn co https://dist.apache.org/repos/dist/release/arrow ${tmp_dir}/release

echo "Copy ${version}-rc${rc} to release working copy"
release_version=arrow-${version}
mkdir -p ${tmp_dir}/release/${release_version}
cp -r ${tmp_dir}/dev/* ${tmp_dir}/release/${release_version}/
svn add ${tmp_dir}/release/${release_version}

echo "Keep only the three most recent versions"
old_releases=$(
  svn ls ${tmp_dir}/release/ | \
  grep -E '^arrow-[0-9\.]+' | \
  sort --version-sort --reverse | \
  tail -n +4
)
for old_release_version in $old_releases; do
  echo "Remove old release ${old_release_version}"
  svn delete ${tmp_dir}/release/${old_release_version}
done

echo "Commit release"
svn ci -m "Apache Arrow ${version}" ${tmp_dir}/release

echo "Clean up"
rm -rf ${tmp_dir}

echo "Success! The release is available here:"
echo "  https://dist.apache.org/repos/dist/release/arrow/${release_version}"

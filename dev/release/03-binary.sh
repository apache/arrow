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

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <version> <rc-num> <artifact_dir>"
  exit
fi

version=$1
rc=$2
artifact_dir=$3

if [ -d tmp/ ]; then
  echo "Cannot run: tmp/ exists"
  exit
fi

if [ -z "$artifact_dir" ]; then
  echo "artifact_dir is empty"
  exit 1
fi

if [ ! -e "$artifact_dir" ]; then
  echo "$artifact_dir does not exist"
  exit 1
fi

if [ ! -d "$artifact_dir" ]; then
  echo "$artifact_dir is not a directory"
  exit 1
fi

tag=apache-arrow-${version}
tagrc=${tag}-rc${rc}

# check out the arrow RC folder
svn co https://dist.apache.org/repos/dist/dev/arrow/${tagrc} tmp

# ensure directories for binary artifacts
mkdir -p tmp/binaries

# copy binary artifacts
cp -rf "$artifact_dir"/* tmp/binaries/

# commit to svn
for dir in "$artifact_dir"/*; do
  svn add tmp/binaries/$(basename $dir)
done
svn ci -m "Apache Arrow ${version} RC${rc} binaries" tmp/

# clean up
rm -rf tmp

echo "Success! The release candidate binaries are available here:"
echo "  https://dist.apache.org/repos/dist/dev/arrow/${tagrc}/binaries"

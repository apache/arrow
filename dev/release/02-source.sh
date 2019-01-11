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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

tag=apache-arrow-${version}
tagrc=${tag}-rc${rc}

echo "Preparing source for tag ${tag}"

release_hash=`git rev-list $tag 2> /dev/null | head -n 1 `

if [ -z "$release_hash" ]; then
  echo "Cannot continue: unknown git tag: $tag"
  exit
fi

echo "Using commit $release_hash"

tarball=$SOURCE_DIR/${tag}.tar.gz

# build source distribution in dockerfile and dump to ${tarball}
docker-compose -f $SOURCE_DIR/docker-compose.yml \
  build source-release
docker-compose -f $SOURCE_DIR/docker-compose.yml \
  run --rm source-release $version $release_hash

${SOURCE_DIR}/run-rat.sh ${tarball}

# sign the archive
gpg --armor --output ${tarball}.asc --detach-sig ${tarball}
shasum -a 256 $tarball > ${tarball}.sha256
shasum -a 512 $tarball > ${tarball}.sha512

# # check out the arrow RC folder
# svn co --depth=empty https://dist.apache.org/repos/dist/dev/arrow tmp
#
# # add the release candidate for the tag
# mkdir -p tmp/${tagrc}
#
# # copy the rc tarball into the tmp dir
# cp ${tarball}* tmp/${tagrc}
#
# # commit to svn
# svn add tmp/${tagrc}
# svn ci -m "Apache Arrow ${version} RC${rc}" tmp/${tagrc}
#
# # clean up
# rm -rf tmp

echo "Success! The release candidate is available here:"
echo "  https://dist.apache.org/repos/dist/dev/arrow/${tagrc}"
echo ""
echo "Commit SHA1: ${release_hash}"

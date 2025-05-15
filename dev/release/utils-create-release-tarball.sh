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
#

# This must generate reproducible source archive. For reproducible
# source archive, we must use the same timestamp, user, group and so
# on.
#
# See also https://reproducible-builds.org/ for Reproducible Builds.

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

tag=apache-arrow-${version}-rc${rc}
root_folder=apache-arrow-${version}
tarball=apache-arrow-${version}.tar.gz

: ${release_hash:=$(git rev-list --max-count=1 ${tag})}

rm -rf ${root_folder}

# be conservative and use the release hash, even though git produces the same
# archive (identical hashes) using the scm tag
(cd "${SOURCE_TOP_DIR}" && \
  git archive ${release_hash} --prefix ${root_folder}/) | \
  tar xf -

# Resolve symbolic and hard links
rm -rf ${root_folder}.tmp
mv ${root_folder} ${root_folder}.tmp
cp -a -L ${root_folder}.tmp ${root_folder}
rm -rf ${root_folder}.tmp

# Create a dummy .git/ directory to download the source files from
# GitHub with Source Link in C#. See the followings for more Source
# Link support and why we need a dummy .git directory:
# * https://github.com/apache/arrow/issues/21580
# * https://github.com/apache/arrow/pull/4488
#
# We need to set constant timestamp for a dummy .git/ directory for
# Reproducible Builds. We use mtime of csharp/ for it.
if stat --help > /dev/null 2>&1; then
  # GNU stat
  #
  # touch accepts YYYYMMDDhhmm.ss format but GNU stat doesn't support
  # for outputting with the format. So we use date for it.
  csharp_mtime=$(date --date="$(stat --format="%y" csharp)" +%Y%m%d%H%M.%S)
else
  # BSD stat
  csharp_mtime=$(stat -f %Sm -t %Y%m%d%H%M.%S csharp)
fi
dummy_git=${root_folder}/csharp/dummy.git
mkdir ${dummy_git}
pushd ${dummy_git}
echo ${release_hash} > HEAD
echo "[remote \"origin\"] url = https://github.com/${GITHUB_REPOSITORY:-apache/arrow}.git" >> config
mkdir objects refs
find . -exec touch -t "${csharp_mtime}" '{}' ';'
popd
touch -t "${csharp_mtime}" ${root_folder}/csharp

# Create new tarball from modified source directory.
#
# We need to strip unreproducible information. See also:
# https://reproducible-builds.org/docs/stripping-unreproducible-information/
#
# We need GNU tar for Reproducible Builds. We want to use the same
# owner, group, mode, file order for Reproducible Builds.
# See also: https://reproducible-builds.org/docs/archives/
#
# gzip --no-name is for omitting timestamp in .gz. It's also for
# Reproducible Builds.
if type gtar > /dev/null 2>&1; then
  gtar=gtar
else
  gtar=tar
fi
${gtar} \
  --group=0 \
  --mode=a=rX,u+w \
  --numeric-owner \
  --owner=0 \
  --sort=name \
  -cf \
  - \
  ${root_folder} | \
    gzip --no-name > ${tarball}
rm -rf ${root_folder}

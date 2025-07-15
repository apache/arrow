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

: ${release_hash:=$(git -C "${SOURCE_TOP_DIR}" rev-list --max-count=1 ${tag})}

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
# Reproducible Builds. We use committer date of the target commit hash
# for it. If SOURCE_DATE_EPOCH is defined, this mtime is overwritten
# when tar file is created.
csharp_mtime=$(TZ=UTC \
                 git \
                 -C "${SOURCE_TOP_DIR}" \
                 log \
                 --format=%cd \
                 --date=format:%Y%m%d%H%M.%S \
                 -n1 \
                 "${release_hash}")
dummy_git=${root_folder}/csharp/dummy.git
mkdir ${dummy_git}
pushd ${dummy_git}
echo ${release_hash} > HEAD
echo "[remote \"origin\"] url = https://github.com/${GITHUB_REPOSITORY:-apache/arrow}.git" >> config
mkdir objects refs
TZ=UTC find . -exec touch -t "${csharp_mtime}" '{}' ';'
popd
TZ=UTC touch -t "${csharp_mtime}" ${root_folder}/csharp

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
gtar_options=(
  --group=0 \
  --hard-dereference \
  --mode=a=rX,u+w \
  --numeric-owner \
  --owner=0 \
  --sort=name \
)
if [ -n "${SOURCE_DATE_EPOCH:-}" ]; then
  gtar_options+=(--mtime="$(date +%Y-%m-%dT%H:%M:%S --date=@${SOURCE_DATE_EPOCH})")
fi
${gtar} \
  "${gtar_options[@]}" \
  -cf \
  - \
  ${root_folder} | \
    gzip --no-name > ${tarball}
rm -rf ${root_folder}

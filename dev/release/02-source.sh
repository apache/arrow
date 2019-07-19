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

: ${SOURCE_DEFAULT:=1}
: ${SOURCE_GLIB:=${SOURCE_DEFAULT}}
: ${SOURCE_RAT:=${SOURCE_DEFAULT}}
: ${SOURCE_UPLOAD:=${SOURCE_DEFAULT}}
: ${SOURCE_VOTE:=${SOURCE_DEFAULT}}

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

tag=apache-arrow-${version}
tagrc=${tag}-rc${rc}
rc_url="https://dist.apache.org/repos/dist/dev/arrow/${tagrc}"

echo "Preparing source for tag ${tag}"

: ${release_hash:=$(cd "${SOURCE_TOP_DIR}" && git rev-list --max-count=1 ${tag})}

if [ ${SOURCE_UPLOAD} -gt 0 ]; then
  if [ -z "$release_hash" ]; then
    echo "Cannot continue: unknown git tag: $tag"
    exit
  fi
fi

echo "Using commit $release_hash"

tarball=${tag}.tar.gz

rm -rf ${tag}
# be conservative and use the release hash, even though git produces the same
# archive (identical hashes) using the scm tag
(cd "${SOURCE_TOP_DIR}" && \
  git archive ${release_hash} --prefix ${tag}/) | \
  tar xf -

# Replace c_glib/ after running c_glib/autogen.sh to create c_gilb/ source archive containing the configure script
if [ ${SOURCE_GLIB} -gt 0 ]; then
  archive_name=tmp-apache-arrow
  (cd "${SOURCE_TOP_DIR}" && \
    git archive ${release_hash} --prefix ${archive_name}/) \
    > "${SOURCE_TOP_DIR}/${archive_name}.tar"
  c_glib_including_configure_tar_gz=c_glib.tar.gz
  "${SOURCE_TOP_DIR}/dev/run_docker_compose.sh" \
    release-source \
    /arrow/dev/release/source/build.sh \
    ${archive_name} \
    ${c_glib_including_configure_tar_gz}
  rm -f "${SOURCE_TOP_DIR}/${archive_name}.tar"
  rm -rf ${tag}/c_glib
  tar xf "${SOURCE_TOP_DIR}/${c_glib_including_configure_tar_gz}" -C ${tag}
  rm -f "${SOURCE_TOP_DIR}/${c_glib_including_configure_tar_gz}"
fi

# Resolve all hard and symbolic links
rm -rf ${tag}.tmp
mv ${tag} ${tag}.tmp
cp -R -L ${tag}.tmp ${tag}
rm -rf ${tag}.tmp

# Create a dummy .git/ directory to download the source files from GitHub with Source Link in C#.
dummy_git=${tag}/csharp/dummy.git
mkdir ${dummy_git}
pushd ${dummy_git}
echo ${release_hash} > HEAD
echo '[remote "origin"] url = https://github.com/apache/arrow.git' >> config
mkdir objects refs
popd

# Create new tarball from modified source directory
tar czf ${tarball} ${tag}
rm -rf ${tag}

if [ ${SOURCE_RAT} -gt 0 ]; then
  "${SOURCE_DIR}/run-rat.sh" ${tarball}
fi

if [ ${SOURCE_UPLOAD} -gt 0 ]; then
  # sign the archive
  gpg --armor --output ${tarball}.asc --detach-sig ${tarball}
  shasum -a 256 $tarball > ${tarball}.sha256
  shasum -a 512 $tarball > ${tarball}.sha512

  # check out the arrow RC folder
  svn co --depth=empty https://dist.apache.org/repos/dist/dev/arrow tmp

  # add the release candidate for the tag
  mkdir -p tmp/${tagrc}

  # copy the rc tarball into the tmp dir
  cp ${tarball}* tmp/${tagrc}

  # commit to svn
  svn add tmp/${tagrc}
  svn ci -m "Apache Arrow ${version} RC${rc}" tmp/${tagrc}

  # clean up
  rm -rf tmp

  echo "Success! The release candidate is available here:"
  echo "  ${rc_url}"
  echo ""
  echo "Commit SHA1: ${release_hash}"
  echo ""
fi

if [ ${SOURCE_VOTE} -gt 0 ]; then
  echo "The following draft email has been created to send to the"
  echo "dev@arrow.apache.org mailing list"
  echo ""
  echo "---------------------------------------------------------"
  jira_url="https://issues.apache.org/jira"
  jql="project%20%3D%20ARROW%20AND%20status%20in%20%28Resolved%2C%20Closed%29%20AND%20fixVersion%20%3D%20${version}"
  n_resolved_issues=$(curl "${jira_url}/rest/api/2/search/?jql=${jql}" | jq ".total")
  cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE] Release Apache Arrow ${version} - RC${rc}

Hi,

I would like to propose the following release candidate (RC${rc}) of Apache
Arrow version ${version}. This is a release consiting of ${n_resolved_issues}
resolved JIRA issues[1].

This release candidate is based on commit:
${release_hash} [2]

The source release rc${rc} is hosted at [3].
The binary artifacts are hosted at [4][5][6][7].
The changelog is located at [8].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [9] for how to validate a release candidate.

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow ${version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow ${version} because...

[1]: ${jira_url}/issues/?jql=${jql}
[2]: https://github.com/apache/arrow/tree/${release_hash}
[3]: ${rc_url}
[4]: https://bintray.com/apache/arrow/centos-rc/${version}-rc${rc}
[5]: https://bintray.com/apache/arrow/debian-rc/${version}-rc${rc}
[6]: https://bintray.com/apache/arrow/python-rc/${version}-rc${rc}
[7]: https://bintray.com/apache/arrow/ubuntu-rc/${version}-rc${rc}
[8]: https://github.com/apache/arrow/blob/${release_hash}/CHANGELOG.md
[9]: https://cwiki.apache.org/confluence/display/ARROW/How+to+Verify+Release+Candidates
MAIL
  echo "---------------------------------------------------------"
fi

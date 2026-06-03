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

set -eu

: ${VOTE_DEFAULT:=1}
: ${VOTE_CLEANUP:=${VOTE_DEFAULT}}
: ${VOTE_DOWNLOAD:=${VOTE_DEFAULT}}
: ${VOTE_TEMPLATE:=${VOTE_DEFAULT}}

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

. "${SOURCE_DIR}/utils-env.sh"

tag=apache-arrow-${version}-rc${rc}
rc_branch="release-${version}-rc${rc}"
rc_url="https://dist.apache.org/repos/dist/dev/arrow/${tag}"

: ${release_hash:=$(cd "${SOURCE_TOP_DIR}" && git rev-list --max-count=1 ${tag})}
: ${GITHUB_REPOSITORY:=apache/arrow}

echo "Using commit $release_hash"

tarball_sha512=apache-arrow-${version}.tar.gz.sha512

if [ ${VOTE_DOWNLOAD} -gt 0 ]; then
  echo "Downloading tarball checksum for ${tag}"
  rm -rf artifacts
  gh release download ${tag} \
    --dir artifacts \
    --repo "${GITHUB_REPOSITORY}" \
    --pattern "${tarball_sha512}"
fi

if [ ${VOTE_TEMPLATE} -gt 0 ]; then
  curl_common_options=(--header "Authorization: Bearer ${GH_TOKEN}")

  curl_options=("${curl_common_options[@]}")
  curl_options+=(--data "{\"query\": \"query {search(query: \\\"repo:apache/arrow is:issue is:closed milestone:${version}\\\", type:ISSUE) {issueCount}}\"}")
  curl_options+=("https://api.github.com/graphql")
  n_resolved_issues=$(curl "${curl_options[@]}" | jq ".data.search.issueCount")

  curl_options=("${curl_common_options[@]}")
  curl_options+=(--header "Accept: application/vnd.github+json")
  curl_options+=(--get)
  curl_options+=(--data "state=open")
  curl_options+=(--data "head=apache:${rc_branch}")
  curl_options+=(https://api.github.com/repos/${GITHUB_REPOSITORY}/pulls)
  verify_pr_url=$(curl "${curl_options[@]}" | jq -r ".[0].html_url")
  # Read the checksum so we can include it in the vote thread email.
  sha512_path="artifacts/${tarball_sha512}"
  [[ -f "${sha512_path}" ]] || { echo "Error: ${sha512_path} must exist"; exit 1; }
  tarball_hash=$(cat "${sha512_path}" | awk '{print $1}')

  echo "The following draft email has been created to send to the"
  echo "dev@arrow.apache.org mailing list"
  echo ""
  echo "---------------------------------------------------------"
  cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE] Release Apache Arrow ${version} - RC${rc}

Hi,

I would like to propose the following release candidate (RC${rc}) of Apache
Arrow version ${version}. This is a release consisting of ${n_resolved_issues}
resolved GitHub issues[1].

This release candidate is based on commit:
${release_hash} [2]

The source release rc${rc} is hosted at [3].
The binary artifacts are hosted at [4][5][6][7][8][9].
The changelog is located at [10].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [11] for the SHA-512 checksum for this RC and [12]
for how to validate a release candidate.

See also a verification result on GitHub pull request [13].

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow ${version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow ${version} because...

[1]: https://github.com/apache/arrow/issues?q=is%3Aissue+milestone%3A${version}+is%3Aclosed
[2]: https://github.com/apache/arrow/tree/${release_hash}
[3]: ${rc_url}
[4]: https://packages.apache.org/artifactory/arrow/almalinux-rc/
[5]: https://packages.apache.org/artifactory/arrow/amazon-linux-rc/
[6]: https://packages.apache.org/artifactory/arrow/centos-rc/
[7]: https://packages.apache.org/artifactory/arrow/debian-rc/
[8]: https://packages.apache.org/artifactory/arrow/ubuntu-rc/
[9]: https://github.com/apache/arrow/releases/tag/apache-arrow-${version}-rc${rc}
[10]: https://github.com/apache/arrow/blob/${release_hash}/CHANGELOG.md
[11]: ${tarball_hash}
[12]: https://arrow.apache.org/docs/developers/release_verification.html
[13]: ${verify_pr_url}
MAIL
  echo "---------------------------------------------------------"
fi

if [ ${VOTE_CLEANUP} -gt 0 ]; then
  rm -rf artifacts
fi

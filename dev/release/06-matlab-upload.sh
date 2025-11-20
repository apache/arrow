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

set -e
set -u
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

. "${SOURCE_DIR}/utils-env.sh"

version=$1
rc=$2

: ${UPLOAD_DEFAULT=1}
: ${UPLOAD_FORCE_SIGN=${UPLOAD_DEFAULT}}

version_with_rc="${version}-rc${rc}"
crossbow_job_prefix="release-${version_with_rc}"
crossbow_package_dir="${SOURCE_DIR}/../../packages"

: ${CROSSBOW_JOB_NUMBER:="0"}
: ${CROSSBOW_JOB_ID:="${crossbow_job_prefix}-${CROSSBOW_JOB_NUMBER}"}
: ${ARROW_ARTIFACTS_DIR:="${crossbow_package_dir}/${CROSSBOW_JOB_ID}/matlab"}

if [ ! -e "${ARROW_ARTIFACTS_DIR}" ]; then
  echo "${ARROW_ARTIFACTS_DIR} does not exist"
  exit 1
fi

if [ ! -d "${ARROW_ARTIFACTS_DIR}" ]; then
  echo "${ARROW_ARTIFACTS_DIR} is not a directory"
  exit 1
fi

pushd "${ARROW_ARTIFACTS_DIR}"

if [ ${UPLOAD_FORCE_SIGN} -gt 0 ]; then
  # Upload the MATLAB MLTBX Release Candidate to the GitHub Releases
  # area of the Apache Arrow GitHub project.
  mltbx_file="matlab-arrow-${version}.mltbx"
  mltbx_signature_gpg_ascii_armor="${mltbx_file}.asc"
  mltbx_checksum_sha512="${mltbx_file}.sha512"

  rm -rf ${mltbx_signature_gpg_ascii_armor}
  rm -rf ${mltbx_checksum_sha512}

  # Sign the MLTBX file and create a detached (--deatch-sign) ASCII armor (--armor) GPG signature file.
  gpg --detach-sign --local-user "${GPG_KEY_ID}" --armor ${mltbx_file}

  # Compute the SHA512 checksum of the MLTBX file.
  shasum --algorithm 512 ${mltbx_file} > ${mltbx_checksum_sha512}
fi

tag="apache-arrow-${version_with_rc}"
gh release upload ${tag} \
  --clobber \
  --repo apache/arrow \
  *

popd

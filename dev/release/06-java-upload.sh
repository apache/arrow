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

version=$1
rc=$2

: ${UPLOAD_DEFAULT=1}
: ${UPLOAD_FORCE_SIGN=${UPLOAD_DEFAULT}}

if [ ${UPLOAD_FORCE_SIGN} -gt 0 ]; then
  pushd "${SOURCE_DIR}"
  if [ ! -f .env ]; then
    echo "You must create $(pwd)/.env"
    echo "You can use $(pwd)/.env.example as template"
    exit 1
  fi
  . .env
  popd
fi

version_with_rc="${version}-rc${rc}"
crossbow_job_prefix="release-${version_with_rc}"
crossbow_package_dir="${SOURCE_DIR}/../../packages"

: ${CROSSBOW_JOB_NUMBER:="0"}
: ${CROSSBOW_JOB_ID:="${crossbow_job_prefix}-${CROSSBOW_JOB_NUMBER}"}
: ${ARROW_ARTIFACTS_DIR:="${crossbow_package_dir}/${CROSSBOW_JOB_ID}/java-jars"}

if [ ! -e "${ARROW_ARTIFACTS_DIR}" ]; then
  echo "${ARROW_ARTIFACTS_DIR} does not exist"
  exit 1
fi

if [ ! -d "${ARROW_ARTIFACTS_DIR}" ]; then
  echo "${ARROW_ARTIFACTS_DIR} is not a directory"
  exit 1
fi

pushd "${ARROW_ARTIFACTS_DIR}"

files=
types=
classifiers=

sign() {
  local path="$1"
  local classifier="$2"
  local type=$(echo "${path}" | grep -o "[^.]*$")

  local asc_path="${path}.asc"
  if [ ${UPLOAD_FORCE_SIGN} -gt 0 ]; then
    rm -f "${asc_path}"
    gpg \
      --detach-sig \
      --local-user "${GPG_KEY_ID}" \
      --output "${asc_path}" \
      "${path}"
  fi
  if [ -n "${files}" ]; then
    files="${files},"
    types="${types},"
    classifiers="${classifiers},"
  fi
  files="${files}${asc_path}"
  types="${types}${type}.asc"
  classifiers="${classifiers}${classifier}"

  # .md5 and .sha1 are generated automatically on repository side.
  # local sha512_path="${path}.sha512"
  # shasum --algorithm 512 "${path}" > "${sha512_path}"
  # files="${files},${sha512_path}"
  # types="${types},${type}.sha512"
  # classifiers="${classifiers},${classifier}"
}

for pom in *.pom; do
  base=$(basename ${pom} .pom)
  files=""
  types=""
  classifiers=""
  args=()
  args+=(deploy:deploy-file)
  args+=(-Durl=https://repository.apache.org/service/local/staging/deploy/maven2)
  args+=(-DrepositoryId=apache.releases.https)
  pom="${PWD}/${pom}"
  args+=(-DpomFile="${pom}")
  if [ -f "${base}.jar" ]; then
    jar="${PWD}/${base}.jar"
    args+=(-Dfile="${jar}")
    sign "${jar}" ""
  else
    args+=(-Dfile="${pom}")
  fi
  sign "${pom}" ""
  if [ "$(echo ${base}-*)" != "${base}-*" ]; then
    for other_file in ${base}-*; do
      file="${PWD}/${other_file}"
      type=$(echo "${other_file}" | grep -o "[^.]*$")
      case "${type}" in
        asc|sha256|sha512)
          continue
          ;;
      esac
      classifier=$(basename "${other_file}" ".${type}" | sed -e "s/${base}-//g")
      files="${files},${file}"
      types="${types},${type}"
      classifiers="${classifiers},${classifier}"
      sign "${file}" "${classifier}"
    done
  fi
  args+=(-Dfiles="${files}")
  args+=(-Dtypes="${types}")
  args+=(-Dclassifiers="${classifiers}")
  pushd "${SOURCE_DIR}"
  mvn deploy:deploy-file "${args[@]}"
  popd
done

popd

echo "Success!"
echo "Press the 'Close' button manually by Web interface:"
echo "    https://repository.apache.org/#stagingRepositories"
echo "It publishes the artifacts to the staging repository:"
echo "    https://repository.apache.org/content/repositories/staging/org/apache/arrow/"

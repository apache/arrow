#!/usr/bin/env bash

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

# This script downloads all the thirdparty dependencies as a series of tarballs
# that can be used for offline builds, etc.
# This script is specific for R users, using R's download facilities rather than
# wget. The original version of this script is cpp/thirdparty/download_dependencies.sh
# Changes from the original:
#  * don't download the files, just emit R code to download
#  * require a download directory
#  * don't print env vars

set -eu

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 1 ]; then
  >&2 echo "Download directory must be specified on the command line"
  exit 1
else
  DESTDIR=$1
fi

download_dependency() {
  local url=$1
  local out=$2

  echo 'download.file("'${url}'", "'${out}'", quiet = TRUE)'
}

main() {
  mkdir -p "${DESTDIR}"

  # Load `DEPENDENCIES` variable.
  source ${SOURCE_DIR}/cpp/thirdparty/versions.txt

  for ((i = 0; i < ${#DEPENDENCIES[@]}; i++)); do
    local dep_packed=${DEPENDENCIES[$i]}

    # Unpack each entry of the form "$home_var $tar_out $dep_url"
    IFS=" " read -r dep_url_var dep_tar_name dep_url <<< "${dep_packed}"

    local out=${DESTDIR}/${dep_tar_name}
    download_dependency "${dep_url}" "${out}"
  done
}

main

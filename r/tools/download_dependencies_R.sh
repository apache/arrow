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
#  * stricter settings
#  * add `run_mode` switch and `print_tar_name` function

set -eufo pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 1 ]; then
  run_mode="print_tar_name"
  echo 'env_varname,filename'
else
  run_mode="download_dependency"
  DESTDIR=$1
  mkdir -p "${DESTDIR}"
fi

download_dependency() {
  local url=$1
  local out=$2

  echo 'download.file("'${url}'", "'${out}'", quiet = TRUE)'
}

print_tar_name() {
  local url_var=$1
  local tar_name=$2
  echo "'${url_var}','${tar_name}'"
}

main() {
  # Load `DEPENDENCIES` variable.
  source ${SOURCE_DIR}/cpp/thirdparty/versions.txt

  for ((i = 0; i < ${#DEPENDENCIES[@]}; i++)); do
    local dep_packed=${DEPENDENCIES[$i]}

    # Unpack each entry of the form "$home_var $tar_out $dep_url"
    IFS=" " read -r dep_url_var dep_tar_name dep_url <<< "${dep_packed}"

    if [[ "$run_mode" == "download_dependency" ]]; then
      local out=${DESTDIR}/${dep_tar_name}
      download_dependency "${dep_url}" "${out}"
    elif [[ "$run_mode" == "print_tar_name" ]]; then
      print_tar_name "${dep_url_var}" "${dep_tar_name}"
    else
      >&2 echo "Bad run_mode"
      exit 1
    fi
  done
}

main

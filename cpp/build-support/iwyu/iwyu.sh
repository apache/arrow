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
set -uo pipefail

ROOT=$(cd $(dirname $BASH_SOURCE)/../../..; pwd)

IWYU_LOG=$(mktemp -t arrow-cpp-iwyu.XXXXXX)
trap "rm -f $IWYU_LOG" EXIT

IWYU_MAPPINGS_PATH="$ROOT/cpp/build-support/iwyu/mappings"
IWYU_ARGS="\
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-all.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-all-private.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-extra.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/gflags.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/glog.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/gmock.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/gtest.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/arrow-misc.imp"

set -e

affected_files() {
  pushd $ROOT > /dev/null
  local commit=$($ROOT/cpp/build-support/get-upstream-commit.sh)
  git diff --name-only $commit | awk '/\.(c|cc|h)$/'
  popd > /dev/null
}

# Show the IWYU version. Also causes the script to fail if iwyu is not in your
# PATH
include-what-you-use --version

if [[ "${1:-}" == "all" ]]; then
  ${PYTHON:-python3} $ROOT/cpp/build-support/iwyu/iwyu_tool.py -p ${IWYU_COMPILATION_DATABASE_PATH:-.} \
      -- $IWYU_ARGS | awk -f $ROOT/cpp/build-support/iwyu/iwyu-filter.awk
elif [[ "${1:-}" == "match" ]]; then
  ALL_FILES=
  IWYU_FILE_LIST=
  for path in $(find $ROOT/cpp/src -type f | awk '/\.(c|cc|h)$/'); do
    if [[ $path =~ $2 ]]; then
      IWYU_FILE_LIST="$IWYU_FILE_LIST $path"
    fi
  done

  echo "Running IWYU on $IWYU_FILE_LIST"
  ${PYTHON:-python3} $ROOT/cpp/build-support/iwyu/iwyu_tool.py \
      -p ${IWYU_COMPILATION_DATABASE_PATH:-.} $IWYU_FILE_LIST  -- \
       $IWYU_ARGS | awk -f $ROOT/cpp/build-support/iwyu/iwyu-filter.awk
else
  # Build the list of updated files which are of IWYU interest.
  file_list_tmp=$(affected_files)
  if [ -z "$file_list_tmp" ]; then
    exit 0
  fi

  # Adjust the path for every element in the list. The iwyu_tool.py normalizes
  # paths (via realpath) to match the records from the compilation database.
  IWYU_FILE_LIST=
  for p in $file_list_tmp; do
    IWYU_FILE_LIST="$IWYU_FILE_LIST $ROOT/$p"
  done

  ${PYTHON:-python3} $ROOT/cpp/build-support/iwyu/iwyu_tool.py \
      -p ${IWYU_COMPILATION_DATABASE_PATH:-.} $IWYU_FILE_LIST  -- \
       $IWYU_ARGS | awk -f $ROOT/cpp/build-support/iwyu/iwyu-filter.awk > $IWYU_LOG
fi

if [ -s "$IWYU_LOG" ]; then
  # The output is not empty: the changelist needs correction.
  cat $IWYU_LOG 1>&2
  exit 1
fi

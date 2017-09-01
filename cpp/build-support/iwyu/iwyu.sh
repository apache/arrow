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
set -x

ROOT=$(cd $(dirname $BASH_SOURCE)/../../..; pwd)

IWYU_LOG=$(mktemp -t arrow-cpp-iwyu.XXXXXX)
trap "rm -f $IWYU_LOG" EXIT

echo "Logging IWYU to $IWYU_LOG"

# Build the list of updated files which are of IWYU interest.
file_list_tmp=$(git diff --name-only \
    $($ROOT/cpp/build-support/get-upstream-commit.sh) | grep -E '\.(c|cc|h)$')
if [ -z "$file_list_tmp" ]; then
  echo "IWYU verification: no updates on related files, declaring success"
  exit 0
fi

# Adjust the path for every element in the list. The iwyu_tool.py normalizes
# paths (via realpath) to match the records from the compilation database.
IWYU_FILE_LIST=
for p in $file_list_tmp; do
  IWYU_FILE_LIST="$IWYU_FILE_LIST $ROOT/$p"
done

IWYU_MAPPINGS_PATH="$ROOT/cpp/build-support/iwyu/mappings"
IWYU_ARGS="\
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-all.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-all-private.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/boost-extra.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/gflags.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/glog.imp \
    --mapping_file=$IWYU_MAPPINGS_PATH/gtest.imp"

set -e

if [ "$1" == "all" ]; then
  python $ROOT/cpp/build-support/iwyu/iwyu_tool.py -p . -- \
       $IWYU_ARGS | awk -f $ROOT/cpp/build-support/iwyu/iwyu-filter.awk | \
       tee $IWYU_LOG
else
  python $ROOT/cpp/build-support/iwyu/iwyu_tool.py -p . $IWYU_FILE_LIST  -- \
       $IWYU_ARGS | awk -f $ROOT/cpp/build-support/iwyu/iwyu-filter.awk | \
       tee $IWYU_LOG
fi

if [ -s "$IWYU_LOG" ]; then
  # The output is not empty: the changelist needs correction.
  exit 1
fi

# The output is empty: the changelist looks good.
echo "IWYU verification: the changes look good"
exit 0

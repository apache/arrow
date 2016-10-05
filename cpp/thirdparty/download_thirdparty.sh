#!/bin/bash

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

source $TP_DIR/versions.sh

download_extract_and_cleanup() {
	type curl >/dev/null 2>&1 || { echo >&2 "curl not installed.  Aborting."; exit 1; }
	filename=$TP_DIR/$(basename "$1")
	curl -#LC - "$1" -o $filename
	tar xzf $filename -C $TP_DIR
	rm $filename
}

if [ ! -d ${GTEST_BASEDIR} ]; then
  echo "Fetching gtest"
  download_extract_and_cleanup $GTEST_URL
fi

echo ${GBENCHMARK_BASEDIR}
if [ ! -d ${GBENCHMARK_BASEDIR} ]; then
  echo "Fetching google benchmark"
  download_extract_and_cleanup $GBENCHMARK_URL
fi

if [ ! -d ${FLATBUFFERS_BASEDIR} ]; then
  echo "Fetching flatbuffers"
  download_extract_and_cleanup $FLATBUFFERS_URL
fi

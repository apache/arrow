#!/usr/bash

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

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
source $SOURCE_DIR/versions.sh

if [ -z "$THIRDPARTY_DIR" ]; then
	THIRDPARTY_DIR=$SOURCE_DIR
fi

export GTEST_HOME=$THIRDPARTY_DIR/$GTEST_BASEDIR
export GBENCHMARK_HOME=$THIRDPARTY_DIR/installed
export FLATBUFFERS_HOME=$THIRDPARTY_DIR/installed

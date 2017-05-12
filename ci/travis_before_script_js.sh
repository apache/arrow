#!/usr/bin/env bash

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


set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh
source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

# Download flatbuffers
export FLATBUFFERS_HOME=$TRAVIS_BUILD_DIR/flatbuffers
conda create -y -q -p $FLATBUFFERS_HOME python=2.7 flatbuffers
export PATH="$FLATBUFFERS_HOME/bin:$PATH"

npm install -g typescript
npm install -g webpack

pushd $ARROW_JS_DIR

npm install
npm run build

popd

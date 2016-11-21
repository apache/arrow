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

set -e

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build}

# Check licenses according to Apache policy
git archive HEAD -o arrow-src.tar.gz
./dev/release/run-rat.sh arrow-src.tar.gz

pushd $CPP_BUILD_DIR

make lint

# ARROW-209: checks depending on the LLVM toolchain are disabled temporarily
# until we are able to install the full LLVM toolchain in Travis CI again

# if [ $TRAVIS_OS_NAME == "linux" ]; then
#   make check-format
#   make check-clang-tidy
# fi

ctest -VV -L unittest

popd

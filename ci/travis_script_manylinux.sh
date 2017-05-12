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

pushd python/manylinux1
git clone ../../ arrow
docker build -t arrow-base-x86_64 -f Dockerfile-x86_64 .
docker run --rm -e PYARROW_PARALLEL=3 -v $PWD:/io arrow-base-x86_64 /io/build_arrow.sh

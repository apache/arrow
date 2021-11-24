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
set -o pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit
fi

version=$1
archive_name=apache-arrow-${version}
tar_gz=${archive_name}.tar.gz

rm -f ${tar_gz}
curl \
  --remote-name \
  --fail \
  https://downloads.apache.org/arrow/arrow-${version}/${tar_gz}
rm -rf ${archive_name}
tar xf ${tar_gz}

pushd ${archive_name}

# clone the testing data to the appropiate directories
git clone https://github.com/apache/arrow-testing.git testing
git clone https://github.com/apache/parquet-testing.git cpp/submodules/parquet-testing

# build the jni bindings similarly like the 01-perform.sh does
mkdir -p cpp/java-build
pushd cpp/java-build
cmake \
  -DARROW_DATASET=ON \
  -DARROW_FILESYSTEM=ON \
  -DARROW_GANDIVA_JAVA=ON \
  -DARROW_GANDIVA=ON \
  -DARROW_JNI=ON \
  -DARROW_ORC=ON \
  -DARROW_PARQUET=ON \
  -DCMAKE_BUILD_TYPE=release \
  -G Ninja \
  ..
ninja
popd

# go in the java subfolder
pushd java
# stage the artifacts using both the apache-release and arrow-jni profiles
# Note: on ORC checkstyle failure use -Dcheckstyle.skip=true until https://issues.apache.org/jira/browse/ARROW-12552 gets resolved
mvn -Papache-release,arrow-jni -Darrow.cpp.build.dir=$(realpath ../cpp/java-build/release) deploy
popd

popd

echo "Success! The maven artifacts have been stated. Proceed with the following steps:"
echo "1. Login to the apache repository: https://repository.apache.org/#stagingRepositories"
echo "2. Select the arrow staging repository you just just created: orgapachearrow-100x"
echo "3. Click the \"close\" button"
echo "4. Once validation has passed, click the \"release\" button"
echo ""
echo "Note, that you must set up Maven to be able to publish to Apache's repositories."
echo "Read more at https://www.apache.org/dev/publishing-maven-artifacts.html."

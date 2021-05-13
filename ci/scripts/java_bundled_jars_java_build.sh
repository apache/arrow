#!/bin/bash

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

arrow_dir=${1}
cpp_build_dir=${2}
copy_jar_to_distribution_folder=${3:-true}
java_dir=${arrow_dir}/java

export ARROW_TEST_DATA=${arrow_dir}/testing/data

pushd $java_dir
  # build the entire project
  mvn clean install -DskipTests -P arrow-jni -Darrow.cpp.build.dir=$cpp_build_dir
  # test jars that have cpp dependencies
  mvn test -P arrow-jni -pl adapter/orc,gandiva,dataset -Dgandiva.cpp.build.dir=$cpp_build_dir

  if [[ $copy_jar_to_distribution_folder ]] ; then
    # copy the jars that has cpp dependencies to distribution folder
    find gandiva/target/ -name "*.jar" -not -name "*tests*" -exec cp  {} $cpp_build_dir \;
    find adapter/orc/target/ -name "*.jar" -not -name "*tests*" -exec cp  {} $cpp_build_dir \;
    find dataset/target/ -name "*.jar" -not -name "*tests*" -exec cp  {} $cpp_build_dir \;
  fi
popd

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

CPP_BUILD_DIR=$GITHUB_WORKSPACE/arrow/dist/

pushd java
  if [[ $OS_NAME == "linux" ]]; then
    SO_DEP=ldd
    GANDIVA_LIB="$CPP_BUILD_DIR"libgandiva_jni.so
    WHITELIST=(linux-vdso libz librt libdl libpthread libstdc++ libm libgcc_s libc ld-linux-x86-64)
  else
    SO_DEP="otool -L"
    GANDIVA_LIB="$CPP_BUILD_DIR"libgandiva_jni.dylib
    WHITELIST=(libgandiva_jni libz libncurses libSystem libc++)
  fi

  # print the shared library dependencies
  eval "$SO_DEP" "$GANDIVA_LIB"

  if [[ $CHECK_SHARED_DEPENDENCIES ]] ; then
    # exit if any shared library not in whitelisted set is found
    echo "Checking shared dependencies"
    while read -r line
    do
      found=false
      for item in "${WHITELIST[@]}"
      do
        if [[ "$line" == *"$item"* ]] ; then
            found=true
        fi
      done
      if [[ "$found" == false ]] ; then
        echo "Unexpected shared dependency found"
        exit 1
      fi
    done < <(eval "$SO_DEP" "$GANDIVA_LIB" | awk '{print $1}')
  fi

  # build the entire project
  mvn clean install -q -DskipTests -P arrow-jni -Darrow.cpp.build.dir=$CPP_BUILD_DIR
  # test only gandiva
  mvn test -q -P arrow-jni -pl gandiva -Dgandiva.cpp.build.dir=$CPP_BUILD_DIR

  # copy the jars to distribution folder
  find gandiva/target/ -name "*.jar" -not -name "*tests*" -exec cp  {} $CPP_BUILD_DIR \;
popd

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

if [[ $OS_NAME == "linux" ]]; then
  SO_DEP=ldd
  
  GANDIVA_LIB="$CPP_BUILD_DIR"libgandiva_jni.so
  DATASET_LIB="$CPP_BUILD_DIR"libarrow_dataset_jni.so
  ORC_LIB="$CPP_BUILD_DIR"libarrow_orc_jni.so
  
  WHITELIST=(linux-vdso libz librt libdl libpthread libstdc++ libm libgcc_s libc ld-linux-x86-64)
else
  SO_DEP="otool -L"

  GANDIVA_LIB="$CPP_BUILD_DIR"libgandiva_jni.dylib
  DATASET_LIB="$CPP_BUILD_DIR"libarrow_dataset_jni.dylib
  ORC_LIB="$CPP_BUILD_DIR"libarrow_orc_jni.dylib

  WHITELIST=(libgandiva_jni libarrow_orc_jni libarrow_dataset_jni libz libncurses libSystem libc++)
fi

LIBRARIES=($GANDIVA_LIB $ORC_LIB $DATASET_LIB)

for library in "${LIBRARIES[@]}"
do
  # print the shared library dependencies
  $SO_DEP "$library" | tee dependencies_temp_file.txt 

  if [[ $CHECK_SHARED_DEPENDENCIES ]] ; then
    # exit if any shared library not in whitelisted set is found
    echo "Checking shared dependencies"

    awk '{print $1}' dependencies_temp_file.txt | \
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
      echo "Unexpected shared dependency found in $library : $line"
      exit 1
    fi
    done
  fi

  rm dependencies_temp_file.txt
done
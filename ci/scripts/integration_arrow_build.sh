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

set -ex

arrow_dir=${1}
build_dir=${2}

: ${ARROW_INTEGRATION_CPP:=ON}
: ${ARROW_INTEGRATION_CSHARP:=ON}
: ${ARROW_INTEGRATION_GO:=ON}
: ${ARROW_INTEGRATION_JAVA:=ON}
: ${ARROW_INTEGRATION_JS:=ON}

${arrow_dir}/ci/scripts/rust_build.sh ${arrow_dir} ${build_dir}

if [ "${ARROW_INTEGRATION_CPP}" == "ON" ]; then
    ${arrow_dir}/ci/scripts/cpp_build.sh ${arrow_dir} ${build_dir}
fi

if [ "${ARROW_INTEGRATION_CSHARP}" == "ON" ]; then
    ${arrow_dir}/ci/scripts/csharp_build.sh ${arrow_dir} ${build_dir}
fi

if [ "${ARROW_INTEGRATION_GO}" == "ON" ]; then
    ${arrow_dir}/ci/scripts/go_build.sh ${arrow_dir} ${build_dir}
fi

if [ "${ARROW_INTEGRATION_JAVA}" == "ON" ]; then
    export ARROW_JAVA_CDATA="ON"
    export JAVA_JNI_CMAKE_ARGS="-DARROW_JAVA_JNI_ENABLE_DEFAULT=OFF -DARROW_JAVA_JNI_ENABLE_C=ON"

    ${arrow_dir}/ci/scripts/java_jni_build.sh ${arrow_dir} ${ARROW_HOME} ${build_dir} /tmp/dist/java
    ${arrow_dir}/ci/scripts/java_build.sh ${arrow_dir} ${build_dir} /tmp/dist/java
fi

if [ "${ARROW_INTEGRATION_JS}" == "ON" ]; then
    ${arrow_dir}/ci/scripts/js_build.sh ${arrow_dir} ${build_dir}
fi

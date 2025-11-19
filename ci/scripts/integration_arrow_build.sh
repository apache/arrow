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

arrow_dir=${1}
build_dir=${2}

: "${ARROW_INTEGRATION_CPP:=ON}"

. "${arrow_dir}/ci/scripts/util_log.sh"

github_actions_group_begin "Integration: Build: Rust"
"${arrow_dir}/ci/scripts/rust_build.sh" "${arrow_dir}" "${build_dir}"
github_actions_group_end

github_actions_group_begin "Integration: Build: nanoarrow"
"${arrow_dir}/ci/scripts/nanoarrow_build.sh" "${arrow_dir}" "${build_dir}"
github_actions_group_end

github_actions_group_begin "Integration: Build: Go"
if [ "${ARCHERY_INTEGRATION_WITH_GO}" -gt "0" ]; then
    "${arrow_dir}/go/ci/scripts/build.sh" "${arrow_dir}/go"
fi
github_actions_group_end

github_actions_group_begin "Integration: Build: C++"
if [ "${ARROW_INTEGRATION_CPP}" == "ON" ]; then
    "${arrow_dir}/ci/scripts/cpp_build.sh" "${arrow_dir}" "${build_dir}"
fi
github_actions_group_end

github_actions_group_begin "Integration: Build: .NET"
if [ "${ARCHERY_INTEGRATION_WITH_DOTNET}" -gt "0" ]; then
    "${arrow_dir}/dotnet/ci/scripts/build.sh" "${arrow_dir}/dotnet"
    cp -a "${arrow_dir}/dotnet" "${build_dir}/dotnet"
fi
github_actions_group_end

github_actions_group_begin "Integration: Build: Java"
if [ "${ARCHERY_INTEGRATION_WITH_JAVA}" -gt "0" ]; then
    export ARROW_JAVA_CDATA="ON"
    export JAVA_JNI_CMAKE_ARGS="-DARROW_JAVA_JNI_ENABLE_DEFAULT=OFF -DARROW_JAVA_JNI_ENABLE_C=ON"

    "${arrow_dir}/java/ci/scripts/jni_build.sh" "${arrow_dir}/java" "${ARROW_HOME}" "${build_dir}/java/" /tmp/dist/java
    "${arrow_dir}/java/ci/scripts/build.sh" "${arrow_dir}/java" "${build_dir}/java" /tmp/dist/java
fi
github_actions_group_end

github_actions_group_begin "Integration: Build: JavaScript"
if [ "${ARCHERY_INTEGRATION_WITH_JS}" -gt "0" ]; then
    "${arrow_dir}/js/ci/scripts/build.sh" "${arrow_dir}/js"
    cp -a "${arrow_dir}/js" "${build_dir}/js"
fi
github_actions_group_end

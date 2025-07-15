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

gold_dir=$arrow_dir/testing/data/arrow-ipc-stream/integration

: "${ARROW_INTEGRATION_CPP:=ON}"
: "${ARROW_INTEGRATION_CSHARP:=ON}"

: "${ARCHERY_INTEGRATION_TARGET_IMPLEMENTATIONS:=cpp,csharp}"
export ARCHERY_INTEGRATION_TARGET_IMPLEMENTATIONS

. "${arrow_dir}/ci/scripts/util_log.sh"

github_actions_group_begin "Integration: Prepare: Archery"
pip install -e "$arrow_dir/dev/archery[integration]"
github_actions_group_end

github_actions_group_begin "Integration: Prepare: Dependencies"
# For C Data Interface testing
if [ "${ARROW_INTEGRATION_CSHARP}" == "ON" ]; then
    pip install pythonnet
fi
if [ "${ARCHERY_INTEGRATION_WITH_JAVA}" -gt "0" ]; then
    pip install jpype1
fi
github_actions_group_end

export ARROW_BUILD_ROOT=${build_dir}
export ARROW_JS_ROOT=${build_dir}/js

# Get more detailed context on crashes
export PYTHONFAULTHANDLER=1

# Due to how Go reads environment variables, we have to set them from the calling
# process, or they would get ignored.
# (see https://forum.golangbridge.org/t/are-godebug-and-other-env-vars-ignored-when-loading-a-go-dll-from-foreign-code/33694)
export GOMEMLIMIT=200MiB
export GODEBUG=gctrace=1,clobberfree=1

ARCHERY_WITH_CPP=$([ "$ARROW_INTEGRATION_CPP" == "ON" ] && echo "1" || echo "0")
ARCHERY_WITH_CSHARP=$([ "$ARROW_INTEGRATION_CSHARP" == "ON" ] && echo "1" || echo "0")

# Rust can be enabled by exporting ARCHERY_INTEGRATION_WITH_RUST=1
time archery integration \
    --run-c-data \
    --run-ipc \
    --run-flight \
    --with-cpp="${ARCHERY_WITH_CPP}" \
    --with-csharp="${ARCHERY_WITH_CSHARP}"\
    --gold-dirs="$gold_dir/0.14.1" \
    --gold-dirs="$gold_dir/0.17.1" \
    --gold-dirs="$gold_dir/1.0.0-bigendian" \
    --gold-dirs="$gold_dir/1.0.0-littleendian" \
    --gold-dirs="$gold_dir/2.0.0-compression" \
    --gold-dirs="$gold_dir/4.0.0-shareddict" \

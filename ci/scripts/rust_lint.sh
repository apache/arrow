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

source_dir=${1}/rust

pushd ${source_dir}
    max_error_cnt=3
    max_warn_cnt=9

    error_msg=$(cargo clippy -- -A clippy::redundant_field_names 2>&1 | grep 'error: aborting due to')

    error_cnt=$(echo "${error_msg}" | awk '{print $5}')
    [[ ${error_cnt} -gt ${max_error_cnt} ]] && {
        echo "More clippy errors introduced, got: ${error_cnt}, was: ${max_error_cnt}"
        exit 1
    }

    warn_cnt=$(echo "${error_msg}" | awk '{print $8}')
    [[ ${warn_cnt} -gt ${max_warn_cnt} ]] && {
        echo "More clippy warnings introduced, got: ${warn_cnt}, was: ${max_warn_cnt}"
        exit 1
    }
popd

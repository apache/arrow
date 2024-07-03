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

set -u

export LANG=C.UTF-8
export LC_ALL=C.UTF-8

target_dir=/host/binary/tmp
original_owner=$(stat --format=%u ${target_dir})
original_group=$(stat --format=%g ${target_dir})

sudo -H chown -R ${USER}: ${target_dir}
restore_owner() {
  sudo -H chown -R ${original_owner}:${original_group} ${target_dir}
}
trap restore_owner EXIT

cd /host

"$@"

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

set -ex

eval "${MATRIX_EVAL}"

# Enable core files
ulimit -c unlimited -S

if [[ "${TRAVIS_OS_NAME}" == "linux" ]]; then
  # Remove apport's core_pattern
  sudo bash -c "echo '/tmp/core.%p.%E' > /proc/sys/kernel/core_pattern"

  echo -e 'Acquire::Retries 10; Acquire::http::Timeout \"20\";' | \
    sudo tee /etc/apt/apt.conf.d/99-travis-retry
  sudo apt-get update -qq
  ccache --show-stats
fi

eval "$(python ${TRAVIS_BUILD_DIR}/ci/detect-changes.py)"

set +ex

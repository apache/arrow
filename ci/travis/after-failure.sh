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

if [[ "${TRAVIS_OS_NAME}" = "osx" ]]; then
  COREFILE=$(find /cores -maxdepth 1 -type f -name "core.*" | head -n 1)
  if [[ -f "$COREFILE" ]]; then
    lldb -c "$COREFILE" --batch --one-line "thread backtrace all -e true"
  fi
  ls -la ~/Library/Logs/DiagnosticReports/
  cat ~/Library/Logs/DiagnosticReports/*.crash
else
  ls -fd1 /tmp/core.*
  COREFILE=$(ls -fd1 /tmp/core.* | head -n 1)
  if [[ -f "$COREFILE" ]]; then
    gdb -c "$COREFILE" $TRAVIS_BUILD_DIR/current-exe -ex "thread apply all bt" -ex "set pagination 0" -batch
  fi
fi

set +ex

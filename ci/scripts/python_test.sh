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

export ARROW_SOURCE_DIR=${arrow_dir}
export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${arrow_dir}/cpp/submodules/parquet-testing/data
export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}

# Enable some checks inside Python itself
export PYTHONDEVMODE=1

pytest -r s -v ${PYTEST_ARGS} --pyargs pyarrow

function print_coredumps() {
  # The script expects core files relative to the current working directory,
  # and the actual executable is a Python interpreter. So the corefile
  # patterns must be set with prefix `core.python*`:
  #
  # In case of macOS:
  #   sudo sysctl -w kern.corefile=core.%N.%P
  # On Linux:
  #   sudo sysctl -w kernel.core_pattern=core.%e.%p
  #
  # and the ulimit must be increased:
  #   ulimit -c unlimited

  PATTERN="^core\.python"

  COREFILES=$(ls | grep $PATTERN)
  if [ -n "$COREFILES" ]; then
    echo "Found core dump, printing backtrace:"

    for COREFILE in $COREFILES; do
      # Print backtrace
      if [ "$(uname)" == "Darwin" ]; then
        lldb -c "${COREFILE}" --batch --one-line "thread backtrace all -e true"
      else
        gdb -c "${COREFILE}" $TEST_EXECUTABLE -ex "thread apply all bt" -ex "set pagination 0" -batch
      fi
      # Remove the coredump, regenerate it via running the test case directly
      rm "${COREFILE}"
    done
  fi
}

print_coredumps

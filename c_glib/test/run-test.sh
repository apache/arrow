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

test_dir="$(cd $(dirname $0); pwd)"
build_dir="$(cd .; pwd)"

modules=(
  arrow-glib
  arrow-cuda-glib
  arrow-dataset-glib
  arrow-flight-glib
  arrow-flight-sql-glib
  gandiva-glib
  parquet-glib
)

for module in "${modules[@]}"; do
  module_build_dir="${build_dir}/${module}"
  if [ -d "${module_build_dir}" ]; then
    LD_LIBRARY_PATH="${module_build_dir}:${LD_LIBRARY_PATH}"
    DYLD_LIBRARY_PATH="${module_build_dir}:${DYLD_LIBRARY_PATH}"
  fi
done
export LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH

if [ "${BUILD}" != "no" ]; then
  if [ -f "build.ninja" ]; then
    ninja || exit $?
  fi
fi

for module in "${modules[@]}"; do
  MODULE_TYPELIB_DIR_VAR_NAME="$(echo ${module} | tr a-z- A-Z_)_TYPELIB_DIR"
  module_typelib_dir=$(eval "echo \${${MODULE_TYPELIB_DIR_VAR_NAME}}")
  if [ -z "${module_typelib_dir}" ]; then
    module_typelib_dir="${build_dir}/${module}"
  fi

  if [ -d "${module_typelib_dir}" ]; then
    GI_TYPELIB_PATH="${module_typelib_dir}:${GI_TYPELIB_PATH}"
  fi
done
export GI_TYPELIB_PATH

if type rbenv > /dev/null 2>&1; then
  RUBY="$(rbenv which ruby)"
else
  RUBY=ruby
fi
DEBUGGER_ARGS=()
case "${DEBUGGER}" in
  "gdb")
    DEBUGGER_ARGS+=(--args)
    ;;
  "lldb")
    DEBUGGER_ARGS+=(--one-line "env DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}")
    DEBUGGER_ARGS+=(--)
    ;;
esac
${DEBUGGER} "${DEBUGGER_ARGS[@]}" "${RUBY}" ${test_dir}/run-test.rb "$@"

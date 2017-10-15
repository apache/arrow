#!/bin/sh
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

arrow_glib_build_dir="${build_dir}/arrow-glib/"
libtool_dir="${arrow_glib_build_dir}/.libs"
if [ -d "${libtool_dir}" ]; then
  LD_LIBRARY_PATH="${libtool_dir}:${LD_LIBRARY_PATH}"
else
  if [ -d "${arrow_glib_build_dir}" ]; then
    LD_LIBRARY_PATH="${arrow_glib_build_dir}:${LD_LIBRARY_PATH}"
  fi
fi

if [ -f "Makefile" -a "${NO_MAKE}" != "yes" ]; then
  make -j8 > /dev/null || exit $?
fi

arrow_glib_typelib_dir="${ARROW_GLIB_TYPELIB_DIR}"
if [ -z "${arrow_glib_typelib_dir}" ]; then
  arrow_glib_typelib_dir="${build_dir}/arrow-glib"
fi

if [ -d "${arrow_glib_typelib_dir}" ]; then
  GI_TYPELIB_PATH="${arrow_glib_typelib_dir}:${GI_TYPELIB_PATH}"
fi

${GDB} ruby ${test_dir}/run-test.rb "$@"

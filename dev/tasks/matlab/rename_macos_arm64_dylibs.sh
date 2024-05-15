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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <dylib-dir>"
  exit
fi

DYLIB_DIR=${1}
ORIG_DIR=$(pwd)

cd ${DYLIB_DIR}

ORIG_LIBARROW_DYLIB="$(find . -name 'libarrow.dylib' | xargs basename)"
ORIG_LIBARROW_MAJOR_DYLIB="$(find . -name 'libarrow.*.dylib' -type l | xargs basename)"
ORIG_LIBARROW_MAJOR_MINOR_PATCH_DYLIB="$(echo libarrow.*.*.dylib)"
ORIG_LIBMEXCLASS_DYLIB="$(find . -name 'libmexclass.dylib' | xargs basename)"
ORIG_LIBARROWPROXY_DYLIB="$(find . -name 'libarrowproxy.dylib' | xargs basename)"
MEX_GATEWAY="$(find . -name 'gateway.mexmaca64' | xargs basename)"

MAJOR_MINOR_PATCH_VERSION=${ORIG_LIBARROW_MAJOR_MINOR_PATCH_DYLIB#*.}
MAJOR_MINOR_PATCH_VERSION=${MAJOR_MINOR_PATCH_VERSION%.*}

NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB="libarrow_arm64.${MAJOR_MINOR_PATCH_VERSION}.dylib"
NEW_LIBARROWPROXY_DYLIB="libarrowproxy_arm64.dylib"
NEW_LIBMEXCLASS_DYLIB="libmexclass_arm64.dylib"

rm ${ORIG_LIBARROW_MAJOR_DYLIB}
rm ${ORIG_LIBARROW_DYLIB}

mv ${ORIG_LIBARROW_MAJOR_MINOR_PATCH_DYLIB} ${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB}
mv ${ORIG_LIBARROWPROXY_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}
mv ${ORIG_LIBMEXCLASS_DYLIB} ${NEW_LIBMEXCLASS_DYLIB}

install_name_tool -id @rpath/${NEW_LIBMEXCLASS_DYLIB} ${NEW_LIBMEXCLASS_DYLIB}
install_name_tool -id @rpath/${NEW_LIBARROWPROXY_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}
install_name_tool -id @rpath/${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB} ${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB}

install_name_tool -change @rpath/${ORIG_LIBARROW_MAJOR_DYLIB} @rpath/${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}
install_name_tool -change @rpath/${ORIG_LIBMEXCLASS_DYLIB} @rpath/${NEW_LIBMEXCLASS_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}

install_name_tool -change @rpath/${ORIG_LIBMEXCLASS_DYLIB} @rpath/${NEW_LIBMEXCLASS_DYLIB} ${MEX_GATEWAY}
install_name_tool -change @rpath/${ORIG_LIBARROWPROXY_DYLIB} @rpath/${NEW_LIBARROWPROXY_DYLIB} ${MEX_GATEWAY}

cd ${ORIG_DIR}
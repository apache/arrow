#!/bin/bash
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
#

# This assumes you've just run cpp/build-support/trim-boost.sh, so the file
# to upload is at ${BOOST_FILE}/${BOOST_FILE}.tar.gz
#
# Also, you must have a bintray account on the "ursalabs" organization and
# set the BINTRAY_USER and BINTRAY_APIKEY env vars.
#
# Ensure that the boost tarball is also updated at
# https://github.com/ursa-labs/thirdparty/releases/latest
# TODO(ARROW-6407) automate uploading to github as well.

set -eu

# if version is not defined by the caller, set a default.
: ${BOOST_VERSION:=1.71.0}
: ${BOOST_FILE:=boost_${BOOST_VERSION//./_}}
: ${DST_URL:=https://api.bintray.com/content/ursalabs/arrow-boost/arrow-boost/latest}

if [ "$BINTRAY_USER" = "" ]; then
  echo "Must set BINTRAY_USER"
  exit 1
fi
if [ "$BINTRAY_APIKEY" = "" ]; then
  echo "Must set BINTRAY_APIKEY"
  exit 1
fi

upload_file() {
  if [ -f "$1" ]; then
    echo "PUT ${DST_URL}/$1?override=1&publish=1"
    curl -sS -u "${BINTRAY_USER}:${BINTRAY_APIKEY}" -X PUT "${DST_URL}/$1?override=1&publish=1" --data-binary "@$1"
  else
    echo "$1 not found"
  fi
}

pushd ${BOOST_FILE}
upload_file ${BOOST_FILE}.tar.gz
popd

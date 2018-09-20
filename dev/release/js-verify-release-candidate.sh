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

# Requirements
# - nodejs >= 6.0.0 (best way is to use nvm)

case $# in
  2) VERSION="$1"
     RC_NUMBER="$2"
     ;;

  *) echo "Usage: $0 X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

set -ex

HERE=$(cd `dirname "${BASH_SOURCE[0]:-$0}"` && pwd)

ARROW_DIST_URL='https://dist.apache.org/repos/dist/dev/arrow'

download_dist_file() {
  curl -f -O $ARROW_DIST_URL/$1
}

download_rc_file() {
  download_dist_file apache-arrow-js-${VERSION}-rc${RC_NUMBER}/$1
}

import_gpg_keys() {
  download_dist_file KEYS
  gpg --import KEYS
}

fetch_archive() {
  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha256
  download_rc_file ${dist_name}.tar.gz.sha512
  gpg --verify ${dist_name}.tar.gz.asc ${dist_name}.tar.gz
  if [ "$(uname)" == "Darwin" ]; then
    shasum -a 256 ${dist_name}.tar.gz | diff - ${dist_name}.tar.gz.sha256
    shasum -a 512 ${dist_name}.tar.gz | diff - ${dist_name}.tar.gz.sha512
  else
    sha256sum ${dist_name}.tar.gz | diff - ${dist_name}.tar.gz.sha256
    sha512sum ${dist_name}.tar.gz | diff - ${dist_name}.tar.gz.sha512
  fi
}

setup_tempdir() {
  cleanup() {
    rm -fr "$TMPDIR"
  }
  trap cleanup EXIT
  TMPDIR=$(mktemp -d -t "$1.XXXXX")
}

setup_tempdir "arrow-js-$VERSION"
echo "Working in sandbox $TMPDIR"
cd $TMPDIR

VERSION=$1
RC_NUMBER=$2

TARBALL=apache-arrow-js-$1.tar.gz

import_gpg_keys

DIST_NAME="apache-arrow-js-${VERSION}"
fetch_archive $DIST_NAME
tar xvzf ${DIST_NAME}.tar.gz
cd ${DIST_NAME}

npm install
# npx run-s clean:all lint create:testdata build
# npm run test -- -t ts -u --integration
# npm run test -- --integration
npx run-s clean:all lint build
npm run test

echo 'Release candidate looks good!'
exit 0

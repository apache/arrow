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

if [ $# -lt 1 ]; then
  echo "Usage: $0 VERSION"
  echo " e.g.: $0 6.0.2    # Verify 6.0.2 RC"
  exit 1
fi

set -e
set -x
set -o pipefail

setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${ARROW_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${ARROW_TMPDIR} for details."
    fi
  }

  if [ -z "${ARROW_TMPDIR}" ]; then
    # clean up automatically if ARROW_TMPDIR is not defined
    ARROW_TMPDIR=$(mktemp -d -t "$1.XXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${ARROW_TMPDIR}"
  fi
}

# Install NodeJS locally for running the JavaScript tests rather than using the
# system Node installation, which may be too old.
: ${INSTALL_NODE:=1}

test_js() {

  pushd js
  PROFILE=/dev/null bash [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
  if [ "${INSTALL_NODE}" -gt 0 ]; then
    export NVM_DIR="`pwd`/.nvm"
    mkdir -p $NVM_DIR
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | \
      PROFILE=/dev/null bash
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

    nvm install --lts
    npm install -g yarn
  fi

  yarn --frozen-lockfile
  yarn run-s clean:all lint build
  yarn test
  popd
}

get_source(){
  git clone https://github.com/apache/arrow
  cd arrow
  git checkout release-${VERSION}-js
}

VERSION="$1"
TEST_SUCCESS=no
setup_tempdir "arrow-${VERSION}"
echo "Working in sandbox ${ARROW_TMPDIR}"
cd ${ARROW_TMPDIR}
get_source
test_js

TEST_SUCCESS=yes
echo 'Javascript release candidate looks good!'
exit 0

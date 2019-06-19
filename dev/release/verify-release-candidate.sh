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
# - Ruby >= 2.3
# - Maven >= 3.3.9
# - JDK >=7
# - gcc >= 4.8
# - nodejs >= 6.0.0 (best way is to use nvm)
#
# If using a non-system Boost, set BOOST_ROOT and add Boost libraries to
# LD_LIBRARY_PATH.

# See release-verification-functions.sh for configuration options for
# this script (TEST_* variables)

case $# in
  3) ARTIFACT="$1"
     VERSION="$2"
     RC_NUMBER="$3"
     case $ARTIFACT in
       source|binaries) ;;
       *) echo "Invalid argument: '${ARTIFACT}', valid options are 'source' or 'binaries'"
          exit 1
          ;;
     esac
     ;;
  *) echo "Usage: $0 source|binaries X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
source $SOURCE_DIR/release-verification-functions.sh

set -ex
set -o pipefail

setup_tempdir "arrow-$VERSION"
echo "Working in sandbox $TMPDIR"
cd $TMPDIR

import_gpg_keys

if [ "$ARTIFACT" == "source" ]; then
  TARBALL=apache-arrow-$1.tar.gz
  DIST_NAME="apache-arrow-${VERSION}"

  fetch_archive $DIST_NAME
  tar xvzf ${DIST_NAME}.tar.gz
  cd ${DIST_NAME}

  test_source_distribution
else
  test_binary_distribution
fi

echo 'Release candidate looks good!'
exit 0

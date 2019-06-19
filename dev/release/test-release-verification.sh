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

# To use this script to test release verification functions, create a
# source archive:
#
#    git archive $HASH --prefix apache-arrow/ > apache-arrow.tar.gz
#
# Then invoke the script with
#
#    test-release-verification.sh /path/to/apache-arrow.tar.gz

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
source $SOURCE_DIR/release-verification-functions.sh

set -ex
set -o pipefail

ARTIFACT="$1"

NO_REMOVE_TEMPDIR=1
setup_tempdir "arrow-testing"
echo "Working in sandbox $TMPDIR"
cd $TMPDIR

TARBALL=$1
DIST_NAME="apache-arrow"

tar xvf ${TARBALL}
cd ${DIST_NAME}

test_source_distribution

echo 'Release candidate looks good!'
exit 0

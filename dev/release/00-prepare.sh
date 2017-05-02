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
set -e

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -eq 2 ]; then
  version=$1
  nextVersion=$2
  nextVersionSNAPSHOT=${nextVersion}-SNAPSHOT
  tag=apache-arrow-${version}

  echo "prepare release ${version} rc ${rcnum} on tag ${tag} then reset to version ${nextVersionSNAPSHOT}" 

  cd "${SOURCE_DIR}/../../java"

  mvn release:clean
  mvn release:prepare -Dtag=${tag} -DreleaseVersion=${version} -DautoVersionSubmodules -DdevelopmentVersion=${nextVersionSNAPSHOT}

  cd -

  echo "Finish staging binary artifacts by running: sh dev/release/01-perform.sh"

else  
  echo "Usage: $0 <version> <nextVersion>"
  exit
fi


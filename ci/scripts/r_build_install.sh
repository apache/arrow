#!/usr/bin/env bash
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

: ${R_BIN:=R}

source_dir=${1}/r

pushd ${source_dir}

printenv

if [ "$ARROW_USE_PKG_CONFIG" != "false" ]; then
  export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
  export R_LD_LIBRARY_PATH=${LD_LIBRARY_PATH}
fi
if [ "$ARROW_R_DEV" = "TRUE" ]; then
  # These are used in the Arrow C++ build and are not a problem
  export _R_CHECK_COMPILATION_FLAGS_KNOWN_="${_R_CHECK_COMPILATION_FLAGS_KNOWN_} -Wno-attributes -msse4.2"
  # Note that NOT_CRAN=true means (among other things) that optional dependencies are built
  export NOT_CRAN=true
fi


ls

${R_BIN} CMD INSTALL .

popd

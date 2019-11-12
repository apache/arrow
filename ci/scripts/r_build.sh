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

# ARROW-6171: Because lz4 is installed in the base Ubuntu image, there's an
# issue of which library is loaded at runtime. R by default will override
# LD_LIBRARY_PATH at runtime by concatenating (in that order)
# R_LD_LIBRARY_PATH, R_JAVA_LD_LIBRARY_PATH and LD_LIBRARY_PATH. If
# R_LD_LIBRARY_PATH is not set, it'll default to a list of directories which
# contains /usr/lib/x86_64-linux-gnu.
export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
export R_LD_LIBRARY_PATH=${LD_LIBRARY_PATH}

pushd ${source_dir}

make clean
${R_BIN} CMD build --keep-empty-dirs .
${R_BIN} CMD INSTALL $(ls | grep arrow_*.tar.gz)

popd

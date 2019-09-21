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

set -e

export ARROW_HOME=$CONDA_PREFIX

: ${R_BIN:=R}

# Build arrow
pushd /arrow/r

if [ "$R_CONDA" = "" ]; then
  ${R_BIN} -e "install.packages(c('remotes', 'dplyr', 'glue'))"
  ${R_BIN} -e "remotes::install_deps(dependencies = TRUE)"
  ${R_BIN} -e "remotes::install_github('romainfrancois/decor')"
fi

make clean
${R_BIN} CMD build --keep-empty-dirs .
${R_BIN} CMD INSTALL $(ls | grep arrow_*.tar.gz)

export _R_CHECK_FORCE_SUGGESTS_=false
${R_BIN} CMD check $(ls | grep arrow_*.tar.gz) --as-cran --no-manual

popd

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

# Install R package dependencies
${R_BIN} -e "install.packages('remotes'); remotes::install_cran(c('glue', 'rcmdcheck'))"
${R_BIN} -e "remotes::install_deps(dependencies = TRUE)"
# This isn't required for testing, only for if you're using this to build your dev environment
${R_BIN} -e "try(remotes::install_github('nealrichardson/decor'))"

popd

if [ "`which curl`" ]; then
  # We need this on R >= 4.0
  curl -L https://sourceforge.net/projects/checkbaskisms/files/2.0.0.2/checkbashisms/download > /usr/local/bin/checkbashisms
  chmod 755 /usr/local/bin/checkbashisms
fi

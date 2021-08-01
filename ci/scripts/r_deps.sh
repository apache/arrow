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
${R_BIN} -e "install.packages('remotes'); remotes::install_cran(c('glue', 'rcmdcheck', 'sys'))"

if [ ${R_BIN} = "RDsan" ]; then
  # To prevent the build from timing out, let's prune some optional deps
  ${R_BIN} -e 'd <- read.dcf("DESCRIPTION")
  to_prune <- c("duckdb", "DBI", "dbplyr", "decor", "knitr", "rmarkdown", "pkgload", "reticulate")
  pattern <- paste0("\\n?", to_prune, ",?", collapse = "|")
  d[,"Suggests"] <- gsub(pattern, "", d[,"Suggests"])
  write.dcf(d, "DESCRIPTION")'
fi
${R_BIN} -e "remotes::install_deps(dependencies = TRUE)"

popd

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

# Ensure parallel compilation of C/C++ code
echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Makeconf

# Special hacking to try to reproduce quirks on fedora-clang-devel on CRAN
# which uses a bespoke clang compiled to use libc++
# https://www.stats.ox.ac.uk/pub/bdr/Rconfig/r-devel-linux-x86_64-fedora-clang
if [ "$RHUB_PLATFORM" = "linux-x86_64-fedora-clang" ]; then
  dnf install -y libcxx-devel
  sed -i.bak -E -e 's/(CXX1?1? =.*)/\1 -stdlib=libc++/g' $(R RHOME)/etc/Makeconf
  rm -rf $(R RHOME)/etc/Makeconf.bak
fi

# Workaround for html help install failure; see https://github.com/r-lib/devtools/issues/2084#issuecomment-530912786
Rscript -e 'x <- file.path(R.home("doc"), "html"); if (!file.exists(x)) {dir.create(x, recursive=TRUE); file.copy(system.file("html/R.css", package="stats"), x)}'

if [ "`which curl`" ]; then
  # We need this on R >= 4.0
  curl -L https://sourceforge.net/projects/checkbaskisms/files/2.0.0.2/checkbashisms/download > /usr/local/bin/checkbashisms
  chmod 755 /usr/local/bin/checkbashisms
fi

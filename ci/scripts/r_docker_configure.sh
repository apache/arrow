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
# This is where our docker setup puts things; set this to run outside of docker
: ${ARROW_SOURCE_HOME:=/arrow}

# The Dockerfile should have put this file here
if [ -f "${ARROW_SOURCE_HOME}/ci/etc/rprofile" ]; then
  # Ensure parallel R package installation, set CRAN repo mirror,
  # and use pre-built binaries where possible
  cat ${ARROW_SOURCE_HOME}/ci/etc/rprofile >> $(${R_BIN} RHOME)/etc/Rprofile.site
fi

# Ensure parallel compilation of C/C++ code
echo "MAKEFLAGS=-j$(${R_BIN} -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Renviron.site

# Figure out what package manager we have
if [ "`which dnf`" ]; then
  PACKAGE_MANAGER=dnf
elif [ "`which yum`" ]; then
  PACKAGE_MANAGER=yum
elif [ "`which zypper`" ]; then
  PACKAGE_MANAGER=zypper
else
  PACKAGE_MANAGER=apt-get
  apt-get update
fi

# Enable ccache if requested based on http://dirk.eddelbuettel.com/blog/2017/11/27/
: ${R_CUSTOM_CCACHE:=FALSE}
R_CUSTOM_CCACHE=`echo $R_CUSTOM_CCACHE | tr '[:upper:]' '[:lower:]'`
if [ ${R_CUSTOM_CCACHE} = "true" ]; then
  # install ccache
  $PACKAGE_MANAGER install -y epel-release || true
  $PACKAGE_MANAGER install -y ccache

  mkdir -p ~/.R
  echo "VER=
CCACHE=ccache
CC=\$(CCACHE) gcc\$(VER)
CXX=\$(CCACHE) g++\$(VER)
CXX17=\$(CCACHE) g++\$(VER)" >> ~/.R/Makevars

  mkdir -p ~/.ccache/
  echo "max_size = 5.0G
# important for R CMD INSTALL *.tar.gz as tarballs are expanded freshly -> fresh ctime
sloppiness = include_file_ctime
# also important as the (temp.) directory name will differ
hash_dir = false" >> ~/.ccache/ccache.conf
fi

if [ -f "${ARROW_SOURCE_HOME}/ci/scripts/r_install_system_dependencies.sh" ]; then
  "${ARROW_SOURCE_HOME}/ci/scripts/r_install_system_dependencies.sh"
fi

# Install rsync for bundling cpp source and curl to make sure it is installed on all images,
# cmake is now a listed sys req.
$PACKAGE_MANAGER install -y rsync cmake curl

# Update clang version to latest available.
# This is only for rhub/clang20. If we change the base image from rhub/clang20,
# we need to update this part too.
if [ "$R_UPDATE_CLANG" = true ]; then
  apt update -y
  apt install -y gnupg
  curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/trusted.gpg.d/llvm.gpg
  echo "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-20 main" > /etc/apt/sources.list.d/llvm20.list
  apt update -y
  apt install -y clang-20 lld-20
fi

# Workaround for html help install failure; see https://github.com/r-lib/devtools/issues/2084#issuecomment-530912786
Rscript -e 'x <- file.path(R.home("doc"), "html"); if (!file.exists(x)) {dir.create(x, recursive=TRUE); file.copy(system.file("html/R.css", package="stats"), x)}'

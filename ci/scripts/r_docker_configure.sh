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

# The Dockerfile should have put this file here
if [ -f "/arrow/ci/etc/rprofile" ]; then
  # Ensure parallel R package installation, set CRAN repo mirror,
  # and use pre-built binaries where possible
  cat /arrow/ci/etc/rprofile >> $(${R_BIN} RHOME)/etc/Rprofile.site
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

# Special hacking to try to reproduce quirks on fedora-clang-devel on CRAN
# which uses a bespoke clang compiled to use libc++
# https://www.stats.ox.ac.uk/pub/bdr/Rconfig/r-devel-linux-x86_64-fedora-clang
if [ "$RHUB_PLATFORM" = "linux-x86_64-fedora-clang" ]; then
  dnf install -y libcxx-devel
  sed -i.bak -E -e 's/(CXX1?1? =.*)/\1 -stdlib=libc++/g' $(${R_BIN} RHOME)/etc/Makeconf
  rm -rf $(${R_BIN} RHOME)/etc/Makeconf.bak

  sed -i.bak -E -e 's/(CXXFLAGS = )(.*)/\1 -g -O3 -Wall -pedantic -frtti -fPIC/' $(${R_BIN} RHOME)/etc/Makeconf
  rm -rf $(${R_BIN} RHOME)/etc/Makeconf.bak

  sed -i.bak -E -e 's/(LDFLAGS =.*)/\1 -stdlib=libc++/g' $(${R_BIN} RHOME)/etc/Makeconf
  rm -rf $(${R_BIN} RHOME)/etc/Makeconf.bak
fi

# Special hacking to try to reproduce quirks on centos using non-default build
# tooling.
if [[ "$DEVTOOLSET_VERSION" -gt 0 ]]; then
  $PACKAGE_MANAGER install -y centos-release-scl
  $PACKAGE_MANAGER install -y "devtoolset-$DEVTOOLSET_VERSION"
fi

if [ "$ARROW_S3" == "ON" ] || [ "$ARROW_R_DEV" == "TRUE" ]; then
  # Install curl and openssl for S3 support
  if [ "$PACKAGE_MANAGER" = "apt-get" ]; then
    apt-get install -y libcurl4-openssl-dev libssl-dev
  else
    $PACKAGE_MANAGER install -y libcurl-devel openssl-devel
  fi

  # The Dockerfile should have put this file here
  if [ -f "/arrow/ci/scripts/install_minio.sh" ] && [ "`which wget`" ]; then
    /arrow/ci/scripts/install_minio.sh latest /usr/local
  fi

  if [ -f "/arrow/ci/scripts/install_gcs_testbench.sh" ] && [ "`which pip`" ]; then
    /arrow/ci/scripts/install_gcs_testbench.sh default
  fi
fi

# Install rsync for bundling cpp source
$PACKAGE_MANAGER install -y rsync

# Workaround for html help install failure; see https://github.com/r-lib/devtools/issues/2084#issuecomment-530912786
Rscript -e 'x <- file.path(R.home("doc"), "html"); if (!file.exists(x)) {dir.create(x, recursive=TRUE); file.copy(system.file("html/R.css", package="stats"), x)}'

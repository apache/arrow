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

# General purpose Dockerfile to take a Docker image containing R
# and install Arrow R package dependencies

ARG base
FROM ${base}

# Make sure R is on the path for the R-hub devel versions (where RPREFIX is set in its dockerfile)
ENV PATH "${RPREFIX}/bin:${PATH}"
# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
COPY ci/etc/rprofile /arrow/ci/etc/
RUN cat /arrow/ci/etc/rprofile >> $(R RHOME)/etc/Rprofile.site
# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Makeconf

# Special hacking to try to reproduce quirks on fedora-clang-devel on CRAN
# which uses a bespoke clang compiled to use libc++
# https://www.stats.ox.ac.uk/pub/bdr/Rconfig/r-devel-linux-x86_64-fedora-clang
RUN [ "$RHUB_PLATFORM" = "linux-x86_64-fedora-clang" ] && \
    dnf install -y libcxx-devel && \
    sed -i.bak -E -e 's/(CXX1?1? =.*)/\1 -stdlib=libc++/g' $(R RHOME)/etc/Makeconf && \
    rm -rf $(R RHOME)/etc/Makeconf.bak

# Workaround for html help install failure; see https://github.com/r-lib/devtools/issues/2084#issuecomment-530912786
RUN Rscript -e 'x <- file.path(R.home("doc"), "html"); if (!file.exists(x)) {dir.create(x, recursive=TRUE); file.copy(system.file("html/R.css", package="stats"), x)}'

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow

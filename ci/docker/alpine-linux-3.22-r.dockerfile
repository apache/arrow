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

# Minimal Alpine Linux container with R for testing Arrow R package
# on musl libc (CRAN runs checks on Alpine Linux 3.22)
# Replicates CRAN's Alpine environment as closely as possible

ARG arch=amd64
FROM ${arch}/alpine:3.22

# Install R and essential build tools
# Keep minimal to match CRAN's setup (no bash - CRAN uses BusyBox)
RUN apk add \
        R \
        R-dev \
        R-doc \
        cmake \
        curl-dev \
        g++ \
        gcc \
        git \
        make \
        musl-locales \
        openssl-dev \
        pkgconfig \
        re2-dev \
        zlib-dev && \
    rm -rf /var/cache/apk/* && \
    ln -s /usr/share/zoneinfo/Etc/UTC /etc/localtime && \
    echo "Etc/UTC" > /etc/timezone

# Set locale and timezone
ENV LANG=C.UTF-8 \
    LC_COLLATE=C \
    MUSL_LOCPATH=/usr/share/i18n/locales/musl

# Set CRAN repo
RUN echo 'options(repos = c(CRAN = "https://cran.rstudio.com"))' >> /usr/lib/R/etc/Rprofile.site

# Install pak for package management (following rhub pattern)
RUN R -q -e 'install.packages("pak", repos = sprintf("https://r-lib.github.io/p/pak/%s/%s/%s/%s", "devel", .Platform$pkgType, R.Version()$os, R.Version()$arch))'

# Enable automatic system requirements installation
ENV PKG_SYSREQS=true \
    R_PKG_SYSREQS2=true

# Set up parallel compilation
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> /usr/lib/R/etc/Renviron.site

# Verify R works and this is musl
RUN R --version && ldd --version 2>&1 | grep -q musl

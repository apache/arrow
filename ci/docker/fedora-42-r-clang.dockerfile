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

# Fedora 42 container with Clang and R-devel for testing Arrow R package
# Replicates CRAN's r-devel-linux-x86_64-fedora-clang environment
# See: https://www.stats.ox.ac.uk/pub/bdr/Rconfig/r-devel-linux-x86_64-fedora-clang

ARG arch=amd64
FROM ${arch}/fedora:42

# Install build dependencies
RUN dnf update -y && \
    dnf install -y \
        # Build tools
        autoconf \
        automake \
        bzip2 \
        bzip2-devel \
        cmake \
        curl \
        curl-devel \
        diffutils \
        gcc \
        gcc-c++ \
        gcc-gfortran \
        git \
        java-latest-openjdk-devel \
        libicu-devel \
        libtool \
        libuuid-devel \
        libxcrypt-devel \
        lld \
        make \
        ninja-build \
        openssl-devel \
        patch \
        pcre2-devel \
        perl \
        pkgconfig \
        python3 \
        python3-pip \
        readline-devel \
        rsync \
        subversion \
        tar \
        texinfo \
        texlive-collection-basic \
        texlive-collection-latex \
        texlive-collection-latexrecommended \
        texlive-collection-fontsrecommended \
        texlive-inconsolata \
        texlive-parskip \
        texlive-natbib \
        texlive-fancyvrb \
        texlive-framed \
        unzip \
        wget \
        which \
        xz \
        xz-devel \
        zlib-devel \
        # X11 libraries for R
        cairo-devel \
        libX11-devel \
        libXmu-devel \
        libXt-devel \
        libcurl-devel \
        libjpeg-turbo-devel \
        libpng-devel \
        libtiff-devel \
        pango-devel \
        tk-devel \
        # Additional R dependencies
        libxml2-devel \
        fontconfig-devel \
        freetype-devel \
        fribidi-devel \
        harfbuzz-devel && \
    dnf clean all

# Install LLVM/Clang from Fedora repos (will be the latest available in Fedora 42)
# Note: CRAN uses Clang 21, but we use whatever is available in Fedora repos
# This should be close enough for testing purposes
RUN dnf install -y \
        clang \
        clang-devel \
        clang-tools-extra \
        compiler-rt \
        flang \
        lld \
        llvm \
        llvm-devel \
        libcxx \
        libcxx-devel \
        libcxxabi \
        libcxxabi-devel \
        libomp \
        libomp-devel && \
    dnf clean all

# Install locale support
RUN dnf install -y glibc-langpack-en && dnf clean all

# Set up compiler environment to match CRAN's Fedora Clang configuration
# CRAN uses: -O3 -Wall -pedantic -Wp,-D_FORTIFY_SOURCE=3
# CRAN's clang is built to use libc++ by default; Fedora's defaults to libstdc++,
# so we must add -stdlib=libc++ explicitly
ENV CC=clang \
    CXX="clang++ -stdlib=libc++" \
    FC=flang-new \
    CFLAGS="-O3 -Wall -pedantic -Wp,-D_FORTIFY_SOURCE=3" \
    CXXFLAGS="-O3 -Wall -pedantic -frtti -stdlib=libc++ -Wp,-D_FORTIFY_SOURCE=3" \
    FFLAGS="-O2 -pedantic" \
    LDFLAGS="-fuse-ld=lld"

# Set locale (glibc-langpack-en must be installed first)
ENV LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    LC_COLLATE=C \
    TZ=UTC

# Build R-devel from source to match CRAN's R-devel
ARG r_version=devel
RUN cd /tmp && \
    if [ "$r_version" = "devel" ]; then \
        svn checkout https://svn.r-project.org/R/trunk R-devel && \
        cd R-devel/tools && \
        ./rsync-recommended; \
    else \
        wget -q https://cran.r-project.org/src/base/R-4/R-${r_version}.tar.gz && \
        tar xf R-${r_version}.tar.gz && \
        mv R-${r_version} R-devel; \
    fi && \
    cd /tmp/R-devel && \
    ./configure \
        --prefix=/usr/local \
        --enable-R-shlib \
        --enable-memory-profiling \
        --with-blas \
        --with-lapack \
        --with-x \
        --with-tcltk \
        CC="clang" \
        CXX="clang++ -stdlib=libc++" \
        FC="flang-new" \
        CFLAGS="-O3 -Wall -pedantic -Wp,-D_FORTIFY_SOURCE=3" \
        CXXFLAGS="-O3 -Wall -pedantic -frtti -stdlib=libc++ -Wp,-D_FORTIFY_SOURCE=3" \
        FFLAGS="-O2 -pedantic" \
        LDFLAGS="-fuse-ld=lld" && \
    make -j$(nproc) && \
    make install && \
    cd / && \
    rm -rf /tmp/R-devel

# Verify R installation and clang
RUN R --version && clang --version

# Set CRAN repo
RUN echo 'options(repos = c(CRAN = "https://cran.rstudio.com"))' >> $(R RHOME)/etc/Rprofile.site

# Install pak for package management
RUN R -q -e 'install.packages("pak", repos = sprintf("https://r-lib.github.io/p/pak/%s/%s/%s/%s", "devel", .Platform$pkgType, R.Version()$os, R.Version()$arch))'

# Enable automatic system requirements installation
ENV PKG_SYSREQS=true \
    R_PKG_SYSREQS2=true

# Set up parallel compilation
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Renviron.site

# Configure R to use clang for package compilation (matching CRAN's Makevars)
# Fedora's clang defaults to libstdc++, so we must specify -stdlib=libc++
RUN mkdir -p /root/.R && \
    echo "CC = clang" >> /root/.R/Makevars && \
    echo "CXX = clang++ -stdlib=libc++" >> /root/.R/Makevars && \
    echo "CXX11 = clang++ -stdlib=libc++" >> /root/.R/Makevars && \
    echo "CXX14 = clang++ -stdlib=libc++" >> /root/.R/Makevars && \
    echo "CXX17 = clang++ -stdlib=libc++" >> /root/.R/Makevars && \
    echo "CXX20 = clang++ -stdlib=libc++" >> /root/.R/Makevars && \
    echo "FC = flang-new" >> /root/.R/Makevars && \
    echo "CFLAGS = -O3 -Wall -pedantic -Wp,-D_FORTIFY_SOURCE=3" >> /root/.R/Makevars && \
    echo "CXXFLAGS = -O3 -Wall -pedantic -frtti -stdlib=libc++ -Wp,-D_FORTIFY_SOURCE=3" >> /root/.R/Makevars && \
    echo "CXX11FLAGS = -O3 -Wall -pedantic -frtti -stdlib=libc++ -Wp,-D_FORTIFY_SOURCE=3" >> /root/.R/Makevars && \
    echo "CXX14FLAGS = -O3 -Wall -pedantic -frtti -stdlib=libc++ -Wp,-D_FORTIFY_SOURCE=3" >> /root/.R/Makevars && \
    echo "CXX17FLAGS = -O3 -Wall -pedantic -frtti -stdlib=libc++ -Wp,-D_FORTIFY_SOURCE=3" >> /root/.R/Makevars && \
    echo "CXX20FLAGS = -O3 -Wall -pedantic -frtti -stdlib=libc++ -Wp,-D_FORTIFY_SOURCE=3" >> /root/.R/Makevars && \
    echo "FFLAGS = -O2 -pedantic" >> /root/.R/Makevars && \
    echo "LDFLAGS = -fuse-ld=lld" >> /root/.R/Makevars

# Configure image and install Arrow-specific tooling
COPY ci/scripts/r_docker_configure.sh /arrow/ci/scripts/
COPY ci/etc/rprofile /arrow/ci/etc/
COPY ci/scripts/r_install_system_dependencies.sh /arrow/ci/scripts/
COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/r_docker_configure.sh

# Install sccache
COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

# Install R package dependencies
COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow

# Verify setup
RUN R --version && \
    clang --version && \
    R -e "sessionInfo()"

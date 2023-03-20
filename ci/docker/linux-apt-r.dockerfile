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

ARG base
FROM ${base}

ARG tz="UTC"
ENV TZ=${tz}

ARG r_prune_deps=FALSE
ENV R_PRUNE_DEPS=${r_prune_deps}

ARG r_duckdb_dev=FALSE
ENV R_DUCKDB_DEV=${r_duckdb_dev}

# Build R
# [1] https://www.digitalocean.com/community/tutorials/how-to-install-r-on-ubuntu-18-04
# [2] https://linuxize.com/post/how-to-install-r-on-ubuntu-18-04/#installing-r-packages-from-cran
ARG r=3.6
RUN apt-get update -y && \
    apt-get install -y \
        dirmngr \
        apt-transport-https \
        software-properties-common && \
    wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | \
        tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc && \
    # NOTE: Only R >= 4.0 is available in this repo
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu '$(lsb_release -cs)'-cran40/' && \
    apt-get install -y \
        r-base=${r}* \
        r-recommended=${r}* \
        # system libs needed by core R packages
        libxml2-dev \
        libgit2-dev \
        libssl-dev \
        # install clang to mirror what was done on Travis
        clang \
        clang-format \
        clang-tidy \
        # R CMD CHECK --as-cran needs pdflatex to build the package manual
        texlive-latex-base \
        # Need locales so we can set UTF-8
        locales \
        # Need Python to check py-to-r bridge
        python3 \
        python3-pip \
        python3-dev && \
    locale-gen en_US.UTF-8 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG gcc_version=""
RUN if [ "${gcc_version}" != "" ]; then \
      update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${gcc_version} 100 && \
      update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-${gcc_version} 100 && \
      update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 30 && \
      update-alternatives --set cc /usr/bin/gcc && \
      update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++ 30 && \
      update-alternatives --set c++ /usr/bin/g++; \
    fi

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
COPY ci/etc/rprofile /arrow/ci/etc/
RUN cat /arrow/ci/etc/rprofile >> $(R RHOME)/etc/Rprofile.site
# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Renviron.site

# Set up Python 3 and its dependencies
RUN ln -s /usr/bin/python3 /usr/local/bin/python && \
    ln -s /usr/bin/pip3 /usr/local/bin/pip

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow

RUN pip install -U pip setuptools wheel

COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_minio.sh latest /usr/local

COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_gcs_testbench.sh default

COPY python/requirements-build.txt /arrow/python/
RUN pip install -r arrow/python/requirements-build.txt

ENV \
    ARROW_ACERO=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_FILESYSTEM=ON \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_HDFS=OFF \
    ARROW_JSON=ON \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_S3=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_USE_GLOG=OFF \
    LC_ALL=en_US.UTF-8

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

# Build R
# [1] https://www.digitalocean.com/community/tutorials/how-to-install-r-on-ubuntu-18-04
# [2] https://linuxize.com/post/how-to-install-r-on-ubuntu-18-04/#installing-r-packages-from-cran
ARG r=3.6
RUN apt-get update -y && \
    apt-get install -y \
        dirmngr \
        apt-transport-https \
        software-properties-common && \
    apt-key adv \
        --keyserver keyserver.ubuntu.com \
        --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    # NOTE: R 3.5 and 3.6 are available in the repos with -cran35 suffix
    # for trusty, xenial, bionic, and eoan (as of May 2020)
    # -cran40 has 4.0 versions for bionic and focal
    # R 3.2, 3.3, 3.4 are available without the suffix but only for trusty and xenial
    # TODO: make sure OS version and R version are valid together and conditionally set repo suffix
    # This is a hack to turn 3.6 into 35 and 4.0 into 40:
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu '$(lsb_release -cs)'-cran'$(echo "${r}" | tr -d . | tr 6 5)'/' && \
    apt-get install -y \
        r-base=${r}* \
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

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
COPY ci/etc/rprofile /arrow/ci/etc/
RUN cat /arrow/ci/etc/rprofile >> $(R RHOME)/etc/Rprofile.site
# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Makeconf

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow

# Set up Python 3 and its dependencies
RUN ln -s /usr/bin/python3 /usr/local/bin/python && \
    ln -s /usr/bin/pip3 /usr/local/bin/pip

COPY python/requirements-build.txt /arrow/python/
RUN pip install -r arrow/python/requirements-build.txt

ENV \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=OFF \
    ARROW_PYTHON=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_USE_GLOG=OFF \
    LC_ALL=en_US.UTF-8

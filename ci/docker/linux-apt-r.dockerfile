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
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' && \
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
        texlive-latex-base && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
RUN printf "\
    options(Ncpus = parallel::detectCores(), \
            repos = 'https://demo.rstudiopm.com/all/__linux__/bionic/latest', \
            HTTPUserAgent = sprintf(\
                'R/%%s R (%%s)', getRversion(), \
                paste(getRversion(), R.version\$platform, R.version\$arch, R.version\$os)))\n" \
    >> /etc/R/Rprofile.site

# Also ensure parallel compilation of each individual package
RUN echo "MAKEFLAGS=-j8" >> /usr/lib/R/etc/Makeconf

ENV \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=OFF \
    ARROW_USE_GLOG=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_R_DEV=TRUE

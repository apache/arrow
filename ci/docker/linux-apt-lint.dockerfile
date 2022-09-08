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
FROM hadolint/hadolint:v1.17.2 AS hadolint
FROM ${base}

ARG clang_tools
RUN apt-get update && \
    apt-get install -y -q \
        clang-${clang_tools} \
        clang-format-${clang_tools} \
        clang-tidy-${clang_tools} \
        clang-tools-${clang_tools} \
        cmake \
        curl \
        libclang-${clang_tools}-dev \
        llvm-${clang_tools}-dev \
        openjdk-11-jdk-headless \
        python3 \
        python3-dev \
        python3-pip \
        ruby \
        apt-transport-https \
        software-properties-common \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG r=4.2
RUN wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | \
        tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc && \
    # NOTE: Only R >= 4.0 is available in this repo
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu '$(lsb_release -cs)'-cran40/' && \
    apt-get install -y \
        r-base=${r}* \
        r-recommended=${r}* \
        libxml2-dev

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
COPY ci/etc/rprofile /arrow/ci/etc/
RUN cat /arrow/ci/etc/rprofile >> $(R RHOME)/etc/Rprofile.site
# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Renviron.site
# We don't need arrow's dependencies, only lintr (and its dependencies)
RUN R -e "install.packages('lintr')"

# Docker linter
COPY --from=hadolint /bin/hadolint /usr/bin/hadolint

# IWYU
COPY ci/scripts/install_iwyu.sh /arrow/ci/scripts/
RUN arrow/ci/scripts/install_iwyu.sh /tmp/iwyu /usr/local ${clang_tools}

# Use python3 by default in scripts
RUN ln -s /usr/bin/python3 /usr/local/bin/python && \
    ln -s /usr/bin/pip3 /usr/local/bin/pip

COPY dev/archery/setup.py /arrow/dev/archery/
RUN pip install -e arrow/dev/archery[lint]

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

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
    apt-get install -y -q --no-install-recommends \
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
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Docker linter
COPY --from=hadolint /bin/hadolint /usr/bin/hadolint

# IWYU
COPY ci/scripts/install_iwyu.sh /arrow/ci/scripts/
RUN arrow/ci/scripts/install_iwyu.sh /tmp/iwyu /usr/local ${clang_tools}

# Rust linter
ARG rust=nightly-2019-09-25
RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain stable -y
ENV PATH /root/.cargo/bin:$PATH
RUN rustup install ${rust} && \
    rustup default ${rust} && \
    rustup component add rustfmt

# Use python3 by default in scripts
RUN ln -s /usr/bin/python3 /usr/local/bin/python && \
    ln -s /usr/bin/pip3 /usr/local/bin/pip

COPY dev/archery/requirements.txt \
     dev/archery/requirements-lint.txt \
     /arrow/dev/archery/
RUN pip install \
      -r arrow/dev/archery/requirements.txt \
      -r arrow/dev/archery/requirements-lint.txt

ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

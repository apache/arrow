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

FROM arrowdev/arrow-cpp:latest

# Use bash
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# install apt extra stuff we need
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        curl \
        maven \
        rsync \
        tzdata \
        openjdk-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Various environment variables
ENV GOROOT=/go \
    GOPATH=/ \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    NVM_DIR=/.nvm \
    NODE_VERSION=11.12

# Install Go
RUN curl -sL --output go.tar.gz https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz && \
    tar -xf go.tar.gz && \
    rm go.tar.gz

ENV PATH $PATH:$GOROOT/bin

# Install Node with nvm
RUN mkdir $NVM_DIR && \
    curl -sL https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.0/install.sh | bash

CMD ["arrow/ci/docker_integration.sh"]
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

ARG repo
ARG arch=amd64
##################### GO #####################
FROM ${repo}:${arch}-conda-cpp AS builder-go

ARG arch=amd64
ARG go=1.15

ENV GOROOT=/opt/go \
    GOBIN=/opt/go/bin \
    GOPATH=/go \
    PATH=/opt/go/bin:$PATH
RUN wget -nv -O - https://dl.google.com/go/go${go}.linux-${arch}.tar.gz | tar -xzf - -C /opt

COPY ./go /arrow/go
COPY ./ci/scripts/go_build.sh /arrow/ci/scripts/go_build.sh

RUN /arrow/ci/scripts/go_build.sh /arrow

##################### C++ #####################
FROM ${repo}:${arch}-conda-cpp AS builder-cpp

# Install Archery and integration dependencies
COPY ci/conda_env_archery.txt /arrow/ci/
RUN mamba install -q \
        --file arrow/ci/conda_env_archery.txt \
        compilers && \
    mamba clean --all --force-pkgs-dirs

COPY ./.env /arrow/.env
COPY ./cpp /arrow/cpp
COPY ./format /arrow/format
COPY ./ci/scripts/cpp_build.sh /arrow/ci/scripts/cpp_build.sh
RUN git init && git checkout -b master && git -c "user.name=no one" -c "user.email=no_one@example.com" commit --allow-empty -m "bla"
COPY ./LICENSE.txt /arrow/LICENSE.txt
COPY ./NOTICE.txt /arrow/NOTICE.txt

ENV ARROW_BUILD_INTEGRATION=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_COMPUTE=OFF \
    ARROW_CSV=OFF \
    ARROW_DATASET=OFF \
    ARROW_FILESYSTEM=OFF \
    ARROW_FLIGHT=ON \
    ARROW_GANDIVA=OFF \
    ARROW_HDFS=OFF \
    ARROW_JEMALLOC=OFF \
    ARROW_JSON=OFF \
    ARROW_ORC=OFF \
    ARROW_PARQUET=OFF \
    ARROW_PLASMA=OFF \
    ARROW_S3=OFF \
    ARROW_USE_GLOG=OFF \
    CMAKE_UNITY_BUILD=ON

RUN /arrow/ci/scripts/cpp_build.sh /arrow /build

##################### C# #####################
FROM ${repo}:${arch}-conda-cpp AS builder-csharp

ENV DOTNET_ROOT=/opt/dotnet \
    PATH=/opt/dotnet:$PATH
RUN curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin -Channel 3.1 -InstallDir /opt/dotnet

COPY ./csharp /arrow/csharp
COPY ./format /arrow/format
COPY ./ci/scripts/csharp_build.sh /arrow/ci/scripts/csharp_build.sh

RUN /arrow/ci/scripts/csharp_build.sh /arrow /build

##################### Java #####################
FROM ${repo}:${arch}-conda-cpp AS builder-java

ARG maven=3.5
ARG jdk=8
RUN mamba install -q \
        maven=${maven} \
        openjdk=${jdk} && \
    mamba clean --all --force-pkgs-dirs

COPY ./java /arrow/java
COPY ./format /arrow/format
COPY ./ci/scripts/java_build.sh /arrow/ci/scripts/java_build.sh

RUN /arrow/ci/scripts/java_build.sh /arrow /build

##################### TESTS #####################
FROM ${repo}:${arch}-conda-cpp

# Install Archery and integration dependencies
COPY ci/conda_env_archery.txt /arrow/ci/
RUN mamba install -q \
        --file arrow/ci/conda_env_archery.txt \
        "python>=3.7" \
        numpy \
        nodejs=${node} \
        openjdk=${jdk} && \
    mamba clean --all --force-pkgs-dirs

# install cpp
COPY --from=builder-cpp /build/cpp /build/cpp

# install js
COPY ./js /arrow/js

# install java
COPY --from=builder-java /arrow/java/tools/target /arrow/java/tools/target
COPY --from=builder-java /arrow/java/flight/flight-integration-tests/target /arrow/java/flight/flight-integration-tests/target
COPY --from=builder-java /arrow/java/pom.xml /arrow/java/pom.xml

# install csharp
COPY --from=builder-csharp /arrow/csharp/artifacts/Apache.Arrow.IntegrationTest/ /arrow/csharp/artifacts/Apache.Arrow.IntegrationTest/
COPY --from=builder-csharp /opt/dotnet /opt/dotnet
ENV DOTNET_ROOT=/opt/dotnet

# install go
COPY --from=builder-go /opt/go/bin/arrow-* /root/go/bin/

COPY ./dev/archery /arrow/dev/archery
COPY ./ci/scripts/integration_arrow.sh /arrow/ci/scripts/integration_arrow.sh

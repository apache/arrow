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

RUN yum install -y git flex curl autoconf zip ccache wget

# Install CMake
ARG cmake=3.19.2
RUN wget https://github.com/Kitware/CMake/releases/download/v${cmake}/cmake-${cmake}-Linux-x86_64.tar.gz -O - | \
    tar -xzf - --directory /usr/local --strip-components=1

# Install Ninja
ARG ninja=1.10.2
RUN mkdir /tmp/ninja && \
    wget https://github.com/ninja-build/ninja/archive/v1.10.2.tar.gz -O - | \
    tar -xzf - --directory /tmp/ninja --strip-components=1 && \
    cd /tmp/ninja && \
    ./configure.py --bootstrap && \
    mv ninja /usr/local/bin && \
    rm -rf /tmp/ninja

# Install vcpkg
ARG vcpkg=a2135fd97e834e83a705b1cff0d91a0e45a0fb00
RUN git clone https://github.com/microsoft/vcpkg /opt/vcpkg && \
    git -C /opt/vcpkg checkout ${vcpkg} && \
    /opt/vcpkg/bootstrap-vcpkg.sh --useSystemBinaries --disableMetrics && \
    ln -s /opt/vcpkg/vcpkg /usr/bin/vcpkg

# Install dependencies from vcpkg
COPY ci/vcpkg arrow/ci/vcpkg
ARG build_type=release
ENV CMAKE_BUILD_TYPE=${build_type} \
    VCPKG_FORCE_SYSTEM_BINARIES=1 \
    VCPKG_OVERLAY_TRIPLETS=/arrow/ci/vcpkg \
    VCPKG_DEFAULT_TRIPLET=x64-linux-static-${build_type}

# TODO(kszucs): factor out the package enumration to a text file and reuse it
# from the windows image and potentially in a future macos wheel build
RUN vcpkg install --clean-after-build \
        abseil \
        aws-sdk-cpp[config,cognito-identity,core,identity-management,s3,sts,transfer] \
        boost-filesystem \
        boost-regex \
        boost-system \
        brotli \
        bzip2 \
        c-ares \
        curl \
        flatbuffers \
        gflags \
        glog \
        grpc \
        lz4 \
        openssl \
        orc \
        protobuf \
        rapidjson \
        re2 \
        snappy \
        thrift \
        utf8proc \
        zlib \
        zstd

ARG python=3.6
ENV PYTHON_VERSION=${python}
RUN PYTHON_ROOT=$(find /opt/python -name cp${PYTHON_VERSION/./}-*) && \
    echo "export PATH=$PYTHON_ROOT/bin:\$PATH" >> /etc/profile.d/python.sh

SHELL ["/bin/bash", "-i", "-c"]
ENTRYPOINT ["/bin/bash", "-i", "-c"]

COPY python/requirements-wheel-build.txt /arrow/python/
RUN pip install -r /arrow/python/requirements-wheel-build.txt

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

ARG arch=amd64
FROM ${arch}/alpine:3.16

RUN apk add \
        bash \
        benchmark-dev \
        boost-dev \
        brotli-dev \
        bzip2-dev \
        c-ares-dev \
        ccache \
        clang \
        cmake \
        curl-dev \
        g++ \
        gcc \
        gdb \
        gflags-dev \
        git \
        glog-dev \
        gmock \
        grpc-dev \
        gtest-dev \
        libxml2-dev \
        llvm13-dev \
        llvm13-static \
        lz4-dev \
        make \
        musl-locales \
        nlohmann-json \
        openssl-dev \
        perl \
        pkgconfig \
        protobuf-dev \
        py3-pip \
        py3-numpy-dev \
        python3-dev \
        rapidjson-dev \
        re2-dev \
        rsync \
        samurai \
        snappy-dev \
        sqlite-dev \
        thrift-dev \
        tzdata \
        utf8proc-dev \
        zlib-dev \
        zstd-dev && \
    rm -rf /var/cache/apk/* && \
    ln -s /usr/share/zoneinfo/Etc/UTC /etc/localtime && \
    echo "Etc/UTC" > /etc/timezone

COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_minio.sh latest /usr/local

COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_gcs_testbench.sh default

ENV ARROW_ACERO=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=ON \
    ARROW_FLIGHT_SQL=ON \
    ARROW_GANDIVA=ON \
    ARROW_GCS=ON \
    ARROW_HOME=/usr/local \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_S3=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_OPENTELEMETRY=OFF \
    ARROW_WITH_MUSL=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    AWSSDK_SOURCE=BUNDLED \
    google_cloud_cpp_storage_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PATH=/usr/lib/ccache/:$PATH \
    xsimd_SOURCE=BUNDLED

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

FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
      build-essential \
      cmake \
      libboost-filesystem-dev \
      libboost-regex-dev \
      libboost-system-dev \
      libbrotli-dev \
      libbz2-dev \
      libgflags-dev \
      liblz4-dev \
      libprotobuf-dev \
      libprotoc-dev \
      libre2-dev \
      libsnappy-dev \
      libthrift-dev \
      libutf8proc-dev \
      libzstd-dev \
      pkg-config \
      protobuf-compiler \
      rapidjson-dev \
      zlib1g-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists*

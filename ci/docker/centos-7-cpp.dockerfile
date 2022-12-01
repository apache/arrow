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

FROM centos:centos7

RUN yum install -y \
  centos-release-scl \
  curl \
  diffutils \
  gcc-c++ \
  libcurl-devel \
  make \
  openssl-devel \
  wget \
  which

# devtoolset is required for C++17
RUN yum install -y devtoolset-8

# yum install cmake version is too old
ARG cmake=3.23.1
RUN mkdir /opt/cmake-${cmake}
RUN wget -nv -O - https://github.com/Kitware/CMake/releases/download/v${cmake}/cmake-${cmake}-Linux-x86_64.tar.gz | \
  tar -xzf -  --strip-components=1 -C /opt/cmake-${cmake}

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN bash /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

ENV PATH=/opt/cmake-${cmake}/bin:$PATH \
  ARROW_R_DEV=TRUE 

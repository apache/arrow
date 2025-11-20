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

# Update mirrors to use vault.centos.org as CentOS 7
# is EOL since 2024-06-30
RUN sed -i \
      -e 's/^mirrorlist/#mirrorlist/' \
      -e 's/^#baseurl/baseurl/' \
      -e 's/mirror\.centos\.org/vault.centos.org/' \
      /etc/yum.repos.d/*.repo

# devtoolset is required for C++17
RUN \
  yum install -y \
    centos-release-scl \
    epel-release && \
  sed -i \
    -e 's/^mirrorlist/#mirrorlist/' \
    -e 's/^#baseurl/baseurl/' \
    -e 's/^# baseurl/baseurl/' \
    -e 's/mirror\.centos\.org/vault.centos.org/' \
    /etc/yum.repos.d/CentOS-SCLo-scl*.repo && \
  yum install -y \
    curl \
    devtoolset-8 \
    diffutils \
    gcc-c++ \
    libcurl-devel \
    make \
    openssl-devel \
    openssl11-devel \
    wget \
    which

ARG cmake
COPY ci/scripts/install_cmake.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_cmake.sh ${cmake} /usr/local/

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN bash /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

ENV \
  ARROW_R_DEV=TRUE \
  CMAKE=/usr/local/bin/cmake

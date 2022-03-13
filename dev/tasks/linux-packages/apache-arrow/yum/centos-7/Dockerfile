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

ARG FROM=centos:7
FROM ${FROM}

ARG DEBUG

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "--quiet") && \
  yum update -y ${quiet} && \
  yum install -y ${quiet} epel-release && \
  yum install -y ${quiet} \
    bison \
    boost169-devel \
    brotli-devel \
    bzip2-devel \
    ccache \
    cmake3 \
    curl-devel \
    flex \
    gcc-c++ \
    gflags-devel \
    git \
    glog-devel \
    gobject-introspection-devel \
    gtk-doc \
    json-devel \
    libzstd-devel \
    lz4-devel \
    make \
    ninja-build \
    openssl-devel \
    pkg-config \
    python36 \
    python36-devel \
    python36-numpy \
    rapidjson-devel \
    rpmdevtools \
    snappy-devel \
    tar \
    zlib-devel && \
  yum clean ${quiet} all

ENV \
  BOOST_INCLUDEDIR=/usr/include/boost169 \
  BOOST_LIBRARYDIR=/usr/lib64/boost169

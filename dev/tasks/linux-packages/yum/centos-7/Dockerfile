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

FROM centos:7

ARG DEBUG

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "--quiet") && \
  yum update -y ${quiet} && \
  yum install -y ${quiet} epel-release && \
  yum groupinstall -y ${quiet} "Development Tools" && \
  yum install -y ${quiet} \
    autoconf-archive \
    bison \
    boost-devel \
    cmake3 \
    double-conversion-devel \
    flex \
    gflags-devel \
    git \
    glog-devel \
    gobject-introspection-devel \
    gtk-doc \
    libzstd-devel \
    lz4-devel \
    pkg-config \
    python34 \
    python34-devel \
    python34-numpy \
    rapidjson-devel \
    rpm-build \
    rpmdevtools \
    snappy-devel \
    tar \
    thrift-devel \
    zlib-devel && \
  yum clean ${quiet} all

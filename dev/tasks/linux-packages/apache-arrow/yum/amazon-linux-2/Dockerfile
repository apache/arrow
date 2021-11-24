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

ARG FROM=amazonlinux:2
FROM ${FROM}

COPY qemu-* /usr/bin/

ARG DEBUG

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "--quiet") && \
  yum update -y ${quiet} && \
  amazon-linux-extras install -y epel && \
  yum install -y ${quiet} \
    bison \
    brotli-devel \
    bzip2-devel \
    ccache \
    cmake3 \
    flex \
    gcc-c++ \
    git \
    glog-devel \
    gobject-introspection-devel \
    gtk-doc \
    lz4-devel \
    make \
    openssl-devel \
    pkg-config \
    rapidjson-devel \
    rpmdevtools \
    snappy-devel \
    tar \
    utf8proc-devel \
    zlib-devel && \
  # Install ninja-build dependencies in amzn2-core
  yum install -y ${quiet} ninja-build && \
  # Install ninja-build from EPEL because ninja-build in amzn2-core is old.
  yum install -y ${quiet} --disablerepo=amzn2-core ninja-build && \
  yum clean ${quiet} all

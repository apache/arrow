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
ARG ubuntu=20.04
ARG config=arm-be.config
FROM ${repo}:${arch}-ubuntu-${ubuntu}-cpp

ARG DEBIAN_FRONTEND=noninteractive

# Crosstool can't be run as root.
RUN mkdir -p /opt && mkdir -p /home/gcc-user && useradd gcc-user && chown gcc-user /opt /home/gcc-user

RUN apt-get clean -y && apt-get check -y

RUN apt-get update -y -q && apt-get upgrade -y -q && apt-get upgrade -y -q && \
    apt-get install -y -q \
    automake \
    libtool \
    bison \
    bzip2 \
    file \
    flex \
    gawk \
    binutils-multiarch \
    gperf \
    help2man \
    libc6-dev-i386 \
    libncurses5-dev \
    libtool-bin \
    linux-libc-dev \
    device-tree-compiler \
    patch \
    s3cmd \
    sed \
    subversion \
    texinfo \
    unzip \
    autopoint \
    gettext \
    vim \
    zlib1g-dev \
    software-properties-common \
    xz-utils  && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

RUN CROSSTOOL_NG_VERSION=1.25.0 && \
    curl -sL https://github.com/crosstool-ng/crosstool-ng/archive/refs/tags/crosstool-ng-${CROSSTOOL_NG_VERSION}.tar.gz --output crosstool-ng-${CROSSTOOL_NG_VERSION}.tar.gz  && \
    tar -zxf crosstool-ng-${CROSSTOOL_NG_VERSION}.tar.gz && \
    cd crosstool-ng-crosstool-ng-${CROSSTOOL_NG_VERSION} && \
    ./bootstrap && \
    ./configure --prefix=/opt/crosstool-ng && \
    make -j$(nproc) && \
    make install

RUN chown -R gcc-user /opt
USER gcc-user

COPY ci/scripts/build_crosstool_toolchain.sh /arrow/ci/scripts/
COPY ci/crosstool/${config} /opt/crosstool-ng/.config
RUN /arrow/ci/scripts/build_crosstool_toolchain.sh

USER root

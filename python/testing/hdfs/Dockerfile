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

# TODO Replace this with a complete clean image build
FROM cpcloud86/impala:metastore

USER root

RUN apt-add-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update && \
    apt-get install -y \
            gcc-4.9 \
            g++-4.9 \
            build-essential \
            autotools-dev \
            autoconf \
            gtk-doc-tools \
            autoconf-archive \
            libgirepository1.0-dev \
            libtool \
            libjemalloc-dev \
            ccache \
            valgrind \
            gdb

RUN wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key|sudo apt-key add - && \
    apt-add-repository -y \
     "deb http://llvm.org/apt/trusty/ llvm-toolchain-trusty-4.0 main" && \
    apt-get update && \
    apt-get install -y clang-4.0 clang-format-4.0 clang-tidy-4.0

USER ubuntu

RUN wget -O /tmp/miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -b -p /home/ubuntu/miniconda && \
    rm /tmp/miniconda.sh

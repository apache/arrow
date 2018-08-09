#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Miniconda - Python 3.6, 64-bit, x86, latest
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O conda.sh \
 && /bin/bash conda.sh -b -p /opt/conda \
 && rm conda.sh

# create conda env with the required dependences
conda create -y -q -c conda-forge -n pyarrow-dev \
      python=$1 \
      ipython \
      matplotlib \
      nomkl \
      numpy \
      six \
      setuptools \
      cython \
      pandas \
      pytest \
      cmake \
      flatbuffers \
      rapidjson \
      boost-cpp \
      thrift-cpp \
      snappy \
      zlib \
      gflags \
      brotli \
      jemalloc \
      lz4-c \
      zstd \
      setuptools \
      setuptools_scm
 && conda clean --all

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

FROM gelog/hadoop

ENV CC=gcc \
    CXX=g++ \
    PATH=/opt/conda/bin:$PATH

RUN apt-get update -y \
 && apt-get install -y \
	  gcc \
	  g++ \
	  git \
	  wget \
	  pkg-config \
	  ninja-build

# Miniconda - Python 3.6, 64-bit, x86, latest
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O conda.sh \
 && /bin/bash conda.sh -b -p /opt/conda \
 && rm conda.sh

# create conda env with the required dependences
RUN conda create -y -q -c conda-forge -n pyarrow-dev \
      python=3.6 \
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
 && conda clean --all

# installing in the previous step boost=1.60 and boost-cpp=1.67 gets installed,
# cmake finds 1.60 and parquet fails to compile
# installing it in a separate step, boost=1.60 and boost-cpp=1.64 gets
# installed, cmake finds 1.64
# libhdfs3 needs to be pinned,see ARROW-1465 and ARROW-1445
RUN conda install -y -q -n pyarrow-dev -c conda-forge \
      hdfs3 \
      libhdfs3=2.2.31 \
 && conda clean --all

ADD . /apache-arrow
WORKDIR /apache-arrow

CMD arrow/dev/hdfs_integration/hdfs_integration.sh

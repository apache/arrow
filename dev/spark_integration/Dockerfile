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
FROM maven:3.5.2-jdk-8-slim

# Basic OS utilities
RUN apt-get update \
 && apt-get install -y \
      wget \
      git \
      pkg-config \
      build-essential \
      software-properties-common \
 && apt-get clean

# install conda in /home/ubuntu/miniconda
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O conda.sh \
  && /bin/bash conda.sh -b -p /opt/conda \
  && rm conda.sh

ENV PATH="/opt/conda/bin:${PATH}"

RUN conda create -y -q -c conda-forge -n pyarrow-dev \
      python=2.7 \
      ipython \
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
      lz4-c \
      zstd \
      setuptools \
      setuptools_scm \
 && conda clean --all

ADD . /apache-arrow
WORKDIR /apache-arrow

CMD arrow/dev/spark_integration/spark_integration.sh

# BUILD: $ docker build -f arrow/dev/spark_integration/Dockerfile -t spark-arrow .
# RUN:   $ docker run -v $HOME/.m2:/root/.m2 spark-arrow

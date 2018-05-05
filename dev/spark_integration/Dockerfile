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
RUN apt-get update && \
    apt-get install -y \
        wget \
        git \
        build-essential \
        software-properties-common && \
    apt-get clean && \
    # install conda in /home/ubuntu/miniconda
    wget -O /tmp/miniconda.sh \
    https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -b -p /home/ubuntu/miniconda && \
    rm /tmp/miniconda.sh && \
    # Python dependencies
    apt-get install -y \
        pkg-config && \
    # Create Conda environment
    /home/ubuntu/miniconda/bin/conda create -y -q -n pyarrow-dev \
        # Python
        python=2.7 \
        numpy \
        pandas \
        pytest \
        cython \
        ipython \
        matplotlib \
        six \
        setuptools \
        setuptools_scm \
        # C++
        boost-cpp \
        cmake \
        flatbuffers \
        rapidjson \
        thrift-cpp \
        snappy \
        zlib \
        gflags \
        brotli \
        jemalloc \
        lz4-c \
        zstd \
        -c conda-forge && \
    /home/ubuntu/miniconda/bin/conda clean --all

ENV PATH="/home/ubuntu/miniconda/bin:${PATH}"

ADD . /apache-arrow
WORKDIR /apache-arrow

CMD arrow/dev/spark_integration/spark_integration.sh

# BUILD: $ docker build -f arrow/dev/spark_integration/Dockerfile -t spark-arrow .
# RUN:   $ docker run -v $HOME/.m2:/root/.m2 spark-arrow

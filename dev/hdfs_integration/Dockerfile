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
FROM ubuntu:14.04

# Basic OS utilities
RUN apt-get update && \
    apt-get install -y \
        git \
        wget \
        ccache \
        build-essential

# install conda in /home/ubuntu/miniconda
RUN wget -O /tmp/miniconda.sh \
    https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -b -p /opt/conda && \
    rm /tmp/miniconda.sh

ENV PATH=/opt/conda/bin:$PATH CONDA_PREFIX=/opt/conda

# Create Conda environment
RUN conda update conda -y && \
    conda install -c conda-forge \
        python=3.6 numpy six setuptools cython pandas pytest \
        cmake flatbuffers rapidjson boost-cpp thrift-cpp snappy zlib \
        gflags brotli jemalloc lz4-c zstd hdfs3 libhdfs3 && \
    conda clean --all

ADD . /apache-arrow
WORKDIR /apache-arrow

CMD arrow/dev/hdfs_integration/hdfs_integration.sh

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
ADD . /apache-arrow
WORKDIR /apache-arrow
# Prerequsites for apt-add-repository
RUN apt-get update && apt-get install -y \
    software-properties-common python-software-properties
# Basic OS dependencies
RUN apt-add-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update && apt-get install -y \
        wget \
        rsync \
        git \
        gcc-4.9 \
        g++-4.9 \
        build-essential 
# This will install conda in /home/ubuntu/miniconda
RUN wget -O /tmp/miniconda.sh \
    https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -b -p /home/ubuntu/miniconda && \
    rm /tmp/miniconda.sh
# C++ dependencies
RUN /home/ubuntu/miniconda/bin/conda install -c conda-forge \
    boost-cpp \
    doxygen \
    maven \
    cmake \
    zlib \
    thrift-cpp
# C_Glib dependencies
RUN apt-get install -y \
    libgtk2.0-dev \
    gtk-doc-tools \
    gobject-introspection \
    libgirepository1.0-dev \
    autogen \
    autoconf-archive
# Python dependencies
RUN apt-get install -y \
    pkg-config
# Create Conda environment
RUN /home/ubuntu/miniconda/bin/conda create -y -q -n pyarrow-dev \
        # Python
        python=3.6 \
        numpy \
        pandas \
        pytest \
        cython \
        ipython \
        matplotlib \
        numpydoc \
        sphinx \
        sphinx_bootstrap_theme \
        six \
        setuptools \
        # C++
        cmake \
        flatbuffers \
        rapidjson \
        thrift-cpp \
        snappy \
        zlib \
        brotli \
        jemalloc \
        lz4-c \
        zstd \
        -c conda-forge
CMD arrow/dev/gen_apidocs/create_documents.sh

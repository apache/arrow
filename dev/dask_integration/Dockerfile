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
# Basic OS utilities
RUN apt-get update && apt-get install -y \
        wget \
        git \
        gcc \
        g++
# This will install conda in /home/ubuntu/miniconda
RUN wget -O /tmp/miniconda.sh \
    https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -b -p /home/ubuntu/miniconda && \
    rm /tmp/miniconda.sh
# Create Conda environment
ENV PATH="/home/ubuntu/miniconda/bin:${PATH}"
RUN conda create -y -q -n test-environment \
    python=3.6
# Install dependencies
RUN conda install -c conda-forge \
    numpy \
    pandas \
    bcolz \
    blosc \
    bokeh \
    boto3 \
    chest \
    cloudpickle \
    coverage \
    cytoolz \
    distributed \
    graphviz \
    h5py \
    ipython \
    partd \
    psutil \
    "pytest<=3.1.1" \
    scikit-image \
    scikit-learn \
    scipy \
    sqlalchemy \
    toolz
# install pytables from defaults for now
RUN conda install pytables

RUN pip install -q git+https://github.com/dask/partd --upgrade --no-deps
RUN pip install -q git+https://github.com/dask/zict --upgrade --no-deps
RUN pip install -q git+https://github.com/dask/distributed --upgrade --no-deps
RUN pip install -q git+https://github.com/mrocklin/sparse --upgrade --no-deps
RUN pip install -q git+https://github.com/dask/s3fs --upgrade --no-deps

RUN conda install -q -c conda-forge numba cython
RUN pip install -q git+https://github.com/dask/fastparquet

RUN pip install -q \
    cachey \
    graphviz \
    moto \
    pyarrow \
    --upgrade --no-deps

RUN pip install -q \
    cityhash \
    flake8 \
    mmh3 \
    pandas_datareader \
    pytest-xdist \
    xxhash \
    pycodestyle

CMD arrow/dev/dask_integration/dask_integration.sh


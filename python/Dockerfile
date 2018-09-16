FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y \
      	gcc \
  	    g++ \
  		  git \
        wget \
    	  pkg-config \
        ninja-build

ENV CC=gcc \
    CXX=g++ \
    PATH=/opt/conda/bin:$PATH \
    CONDA_PREFIX=/opt/conda

ADD ci /arrow/ci
RUN arrow/ci/docker_install_conda.sh

ARG PYTHON_VERSION=3.6
RUN conda install -c conda-forge \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_python.yml \
        python=$PYTHON_VERSION && \
    conda clean --all

RUN conda install -y -c conda-forge hdfs3 libhdfs3=2.2.31 && \
    conda clean --all

# build cpp without tests
ENV ARROW_PYTHON=ON
ADD cpp /arrow/cpp
ADD format /arrow/format
ADD java/pom.xml /arrow/java/pom.xml
RUN arrow/ci/docker_build_cpp.sh

# build python
ADD python /arrow/python
RUN arrow/ci/docker_build_python.sh

WORKDIR arrow/python
CMD pytest -v pyarrow

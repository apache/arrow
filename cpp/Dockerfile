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
RUN conda install -c conda-forge --file arrow/ci/conda_env_cpp.yml && \
    conda clean --all

# build cpp with tests
ENV ARROW_BUILD_TESTS=ON
ADD cpp /arrow/cpp
ADD format /arrow/format
ADD java/pom.xml /arrow/java/pom.xml
RUN arrow/ci/docker_build_cpp.sh

# execute the tests
WORKDIR arrow/cpp/build
ENV PARQUET_TEST_DATA=/arrow/cpp/submodules/parquet-testing/data
CMD ninja test

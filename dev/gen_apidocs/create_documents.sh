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

# Set up environment and output directory for C++ libraries
cd /apache-arrow
rm -rf dist
mkdir dist
export ARROW_BUILD_TYPE=release
export ARROW_HOME=$(pwd)/dist
export PARQUET_HOME=$(pwd)/dist
CONDA_BASE=/home/ubuntu/miniconda
export LD_LIBRARY_PATH=$(pwd)/dist/lib:${CONDA_BASE}/lib:${LD_LIBRARY_PATH}
export THRIFT_HOME=${CONDA_BASE}
export BOOST_ROOT=${CONDA_BASE}
export PATH=${CONDA_BASE}/bin:${PATH}

# Prepare the asf-site before copying api docs
pushd arrow/site
rm -rf asf-site
export GIT_COMMITTER_NAME="Nobody"
export GIT_COMMITTER_EMAIL="nobody@nowhere.com"
git clone --branch=asf-site \
    https://git-wip-us.apache.org/repos/asf/arrow-site.git asf-site
popd

# Make Python documentation (Depends on C++ )
# Build Arrow C++
source activate pyarrow-dev
rm -rf arrow/cpp/build
mkdir arrow/cpp/build
pushd arrow/cpp/build
cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_PYTHON=on \
      -DARROW_PLASMA=on \
      -DARROW_BUILD_TESTS=OFF \
      ..
make -j4
make install
popd

# Build Parquet C++
rm -rf parquet-cpp/build
mkdir parquet-cpp/build
pushd parquet-cpp/build
cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_BUILD_TESTS=off \
      ..
make -j4
make install
popd

# Now Python documentation can be built
pushd arrow/python
rm -rf build/*
rm -rf doc/_build
python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
    --with-plasma --with-parquet --inplace
python setup.py build_sphinx -s doc/source
mkdir -p ../site/asf-site/docs/python
rsync -r doc/_build/html/ ../site/asf-site/docs/python
popd

# Build c_glib documentation
pushd arrow/c_glib
rm -rf doc/reference/html/*
./autogen.sh
./configure \
    --with-arrow-cpp-build-dir=$(pwd)/../cpp/build \
    --with-arrow-cpp-build-type=$ARROW_BUILD_TYPE \
    --enable-gtk-doc
LD_LIBRARY_PATH=$(pwd)/../cpp/build/$ARROW_BUILD_TYPE make GTK_DOC_V_XREF=": "
mkdir -p ../site/asf-site/docs/c_glib
rsync -r doc/reference/html/ ../site/asf-site/docs/c_glib
popd

# Make C++ documentation
pushd arrow/cpp/apidoc
rm -rf html/*
doxygen Doxyfile
mkdir -p ../../site/asf-site/docs/cpp
rsync -r html/ ../../site/asf-site/docs/cpp
popd

# Make Java documentation
pushd arrow/java
rm -rf target/site/apidocs/*
mvn -Drat.skip=true install
mvn -Drat.skip=true site
mkdir -p ../site/asf-site/docs/java/
rsync -r target/site/apidocs ../site/asf-site/docs/java/
popd

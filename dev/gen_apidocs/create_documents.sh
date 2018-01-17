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

set -ex

# Set up environment and output directory for C++ libraries
cd /apache-arrow
rm -rf dist
mkdir dist
export ARROW_BUILD_TYPE=release
export ARROW_HOME=$(pwd)/dist
export PARQUET_HOME=$(pwd)/dist
CONDA_BASE=/home/ubuntu/miniconda
export LD_LIBRARY_PATH=$(pwd)/dist/lib:${CONDA_BASE}/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=$(pwd)/dist/lib/pkgconfig:${PKG_CONFIG_PATH}
export PATH=${CONDA_BASE}/bin:${PATH}

# Prepare the asf-site before copying api docs
pushd arrow/site
rm -rf asf-site
export GIT_COMMITTER_NAME="Nobody"
export GIT_COMMITTER_EMAIL="nobody@nowhere.com"
git clone --branch=asf-site \
    https://git-wip-us.apache.org/repos/asf/arrow-site.git asf-site
popd

# Make Java documentation
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
wget http://mirrors.gigenet.com/apache/maven/maven-3/3.5.2/binaries/apache-maven-3.5.2-bin.tar.gz
tar xvf apache-maven-3.5.2-bin.tar.gz
export PATH=$(pwd)/apache-maven-3.5.2/bin:$PATH

pushd arrow/java
rm -rf target/site/apidocs/*
mvn -Drat.skip=true install
mvn -Drat.skip=true site
mkdir -p ../site/asf-site/docs/java/
rsync -r target/site/apidocs/ ../site/asf-site/docs/java/
popd

# Make Python documentation (Depends on C++ )
# Build Arrow C++
source activate pyarrow-dev

export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
export BOOST_ROOT=$CONDA_PREFIX
export PARQUET_BUILD_TOOLCHAIN=$CONDA_PREFIX
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=$CONDA_PREFIX/lib/pkgconfig:${PKG_CONFIG_PATH}

export CC=gcc-4.9
export CXX=g++-4.9

CPP_BUILD_DIR=$(pwd)/arrow/cpp/build_docs

rm -rf $CPP_BUILD_DIR
mkdir $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR
cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_PYTHON=on \
      -DARROW_PLASMA=on \
      -DARROW_BUILD_TESTS=OFF \
      ..
make -j4
make install
popd

# Build c_glib documentation
pushd arrow/c_glib
if [ -f Makefile ]; then
    # Ensure updating to prevent auto re-configure
    touch configure **/Makefile
    make distclean
    # Work around for 'make distclean' removes doc/reference/xml/
    git checkout doc/reference/xml
fi
./autogen.sh
rm -rf build_docs
mkdir build_docs
pushd build_docs
../configure \
    --prefix=${AROW_HOME} \
    --enable-gtk-doc
make -j4 GTK_DOC_V_XREF=": "
mkdir -p ../../site/asf-site/docs/c_glib
rsync -r doc/reference/html/ ../../site/asf-site/docs/c_glib
popd
popd

# Build Parquet C++
rm -rf parquet-cpp/build_docs
mkdir parquet-cpp/build_docs
pushd parquet-cpp/build_docs
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

# Make C++ documentation
pushd arrow/cpp/apidoc
rm -rf html/*
doxygen Doxyfile
mkdir -p ../../site/asf-site/docs/cpp
rsync -r html/ ../../site/asf-site/docs/cpp
popd

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

mkdir -p apidocs-dist
export ARROW_BUILD_TYPE=release
export ARROW_HOME=$(pwd)/apidocs-dist
export PARQUET_HOME=$(pwd)/apidocs-dist
CONDA_BASE=/home/ubuntu/miniconda
export PKG_CONFIG_PATH=$(pwd)/apidocs-dist/lib/pkgconfig:${PKG_CONFIG_PATH}
export PATH=${CONDA_BASE}/bin:${PATH}
# For newer GCC per https://arrow.apache.org/docs/python/development.html#known-issues
export CXXFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0"

# Make Java documentation
# Override user.home to cache dependencies outside the Docker container
pushd arrow/java
mvn -Duser.home=`pwd`/.apidocs-m2 -Drat.skip=true -Dcheckstyle.skip=true install site
mkdir -p ../site/asf-site/docs/java/
rsync -r target/site/apidocs/ ../site/asf-site/docs/java/
popd

# Make Javascript documentation
pushd arrow/js
npm install
npm run doc
rsync -r doc/ ../site/asf-site/docs/js
popd

# Make Python documentation (Depends on C++ )
# Build Arrow C++
conda activate pyarrow-dev
export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
export PARQUET_BUILD_TOOLCHAIN=$CONDA_PREFIX
export LD_LIBRARY_PATH=$(pwd)/apidocs-dist/lib:${CONDA_PREFIX}/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=${CONDA_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH}
export PKG_CONFIG_PATH=$(pwd)/apidocs-dist/lib/pkgconfig:${PKG_CONFIG_PATH}

CPP_BUILD_DIR=$(pwd)/arrow/cpp/build_apidocs

mkdir -p $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR
cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_PYTHON=on \
      -DARROW_PLASMA=on \
      -DARROW_BUILD_TESTS=OFF \
      -GNinja \
      ..
ninja install
popd

# Build c_glib documentation
pushd arrow/c_glib
if [ -f Makefile ]; then
    # Ensure updating to prevent auto re-configure
    touch configure **/Makefile
    make distclean
fi
./autogen.sh
mkdir -p build_apidocs
pushd build_apidocs
../configure \
    --prefix=${AROW_HOME} \
    --enable-gtk-doc
make -j4 GTK_DOC_V_XREF=": "
mkdir -p ../../site/asf-site/docs/c_glib
rsync -r doc/reference/html/ ../../site/asf-site/docs/c_glib
popd
popd

# Build Parquet C++
mkdir -p parquet-cpp/build_apidocs
pushd parquet-cpp/build_apidocs
cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_BUILD_TESTS=off \
      -GNinja \
      ..
ninja install
popd

# Now Python documentation can be built
pushd arrow/python
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

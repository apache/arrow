#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

function build_wheel {
    pip install -U pip
    pip install setuptools_scm

    # Include brew installed versions of flex and bison.
    # We need them to build Thrift. The ones that come with Xcode are too old.
    export PATH="/usr/local/opt/flex/bin:/usr/local/opt/bison/bin:$PATH"

    echo `pwd`
    echo CFLAGS=${CFLAGS}
    echo CXXFLAGS=${CXXFLAGS}
    echo LDFLAGS=${LDFLAGS}

    pushd $1

    boost_version="1.65.1"
    boost_directory_name="boost_${boost_version//\./_}"
    boost_tarball_name="${boost_directory_name}.tar.gz"
    wget --no-check-certificate \
        http://downloads.sourceforge.net/project/boost/boost/"${boost_version}"/"${boost_tarball_name}" \
        -O "${boost_tarball_name}"
    tar xf "${boost_tarball_name}"

    arrow_boost="$PWD/arrow_boost"
    arrow_boost_dist="$PWD/arrow_boost_dist"
    mkdir "$arrow_boost" "$arrow_boost_dist"

    # Arrow is 64-bit-only at the moment
    export CFLAGS="-fPIC -arch x86_64 ${CFLAGS//"-arch i386"/}"
    export CXXFLAGS="-fPIC -arch x86_64 ${CXXFLAGS//"-arch i386"} -std=c++11"

    # Build Boost's bcp tool to create a custom namespaced boost build.
    # Using this build, we can dynamically link our own boost build and
    # don't need to fear any clashes with system / thirdparty provided versions
    # of Boost.
    pushd "${boost_directory_name}"
    ./bootstrap.sh
    ./b2 tools/bcp > /dev/null 2>&1
    ./dist/bin/bcp --namespace=arrow_boost --namespace-alias \
        filesystem date_time system regex build algorithm locale format \
        "$arrow_boost" > /dev/null 2>&1
    popd

    # Now build our custom namespaced Boost version.
    pushd "$arrow_boost"
    ./bootstrap.sh
    ./bjam cxxflags="${CXXFLAGS}" \
        linkflags="-std=c++11" \
        cflags="${CFLAGS}" \
        variant=release \
        link=shared \
        --prefix="$arrow_boost_dist" \
        --with-filesystem --with-date_time --with-system --with-regex \
        install > /dev/null 2>&1
    popd

    # The boost libraries don't set an explicit install name and we have not
    # yet found the correct option on `bjam` to set the install name to the
    # one we desire.
    #
    # Set it to @rpath/<binary_name> so that they are search in the same
    # directory as the library that loaded them.
    pushd "${arrow_boost_dist}"/lib
      for dylib in *.dylib; do
        install_name_tool -id @rpath/${dylib} ${dylib}
      done
      # Manually adjust libarrow_boost_filesystem.dylib which also references
      # libarrow_boost_system.dylib. It's reference should be to the
      # libarrow_boost_system.dylib with an @rpath prefix so that it also
      # searches for it in the local folder.
      install_name_tool -change libarrow_boost_system.dylib @rpath/libarrow_boost_system.dylib libarrow_boost_filesystem.dylib
    popd

    # We build a custom version of thrift instead of using the one that comes
    # with brew as we also want it to use our namespaced version of Boost.
    # TODO(PARQUET-1262): Use the external project facilities of parquet-cpp.
    export THRIFT_HOME=`pwd`/thift-dist
    export THRIFT_VERSION=0.11.0
    wget http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz
    tar xf thrift-${THRIFT_VERSION}.tar.gz
    pushd thrift-${THRIFT_VERSION}
    mkdir build-tmp
    pushd build-tmp
    cmake -DCMAKE_BUILD_TYPE=release \
        "-DCMAKE_CXX_FLAGS=-fPIC" \
        "-DCMAKE_C_FLAGS=-fPIC" \
        "-DCMAKE_INSTALL_PREFIX=${THRIFT_HOME}" \
        "-DCMAKE_INSTALL_RPATH=${THRIFT_HOME}/lib" \
        "-DBUILD_SHARED_LIBS=OFF" \
        "-DBUILD_TESTING=OFF" \
        "-DWITH_QT4=OFF" \
        "-DWITH_C_GLIB=OFF" \
        "-DWITH_JAVA=OFF" \
        "-DWITH_PYTHON=OFF" \
        "-DWITH_CPP=ON" \
        "-DWITH_STATIC_LIB=ON" \
        "-DWITH_LIBEVENT=OFF" \
        -DBoost_NAMESPACE=arrow_boost \
        -DBOOST_ROOT="$arrow_boost_dist" \
        ..
    make install -j5
    popd
    popd

    # Now we can start with the actual build of Arrow and Parquet.
    # We pin NumPy to an old version here as the NumPy version one builds
    # with is the oldest supported one. Thanks to NumPy's guarantees our Arrow
    # build will also work with newer NumPy versions.
    export ARROW_HOME=`pwd`/arrow-dist
    export PARQUET_HOME=`pwd`/arrow-dist
    pip install "cython==0.27.3" "numpy==${NP_TEST_DEP}"
    pushd cpp
    mkdir build
    pushd build
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
          -DARROW_BUILD_TESTS=OFF \
          -DARROW_BUILD_SHARED=ON \
          -DARROW_BOOST_USE_SHARED=ON \
          -DARROW_JEMALLOC=ON \
          -DARROW_PLASMA=ON \
          -DARROW_RPATH_ORIGIN=ON \
          -DARROW_PYTHON=ON \
          -DARROW_ORC=ON \
          -DBOOST_ROOT="$arrow_boost_dist" \
          -DBoost_NAMESPACE=arrow_boost \
          -DMAKE=make \
          ..
    make -j5
    make install
    popd
    popd

    git clone https://github.com/apache/parquet-cpp.git
    pushd parquet-cpp
    mkdir build
    pushd build
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
          -DPARQUET_VERBOSE_THIRDPARTY_BUILD=ON \
          -DPARQUET_BUILD_TESTS=OFF \
          -DPARQUET_BOOST_USE_SHARED=ON \
          -DBoost_NAMESPACE=arrow_boost \
          -DBOOST_ROOT="$arrow_boost_dist" \
          ..
    make -j5 VERBOSE=1
    make install
    popd
    popd

    # Unset the HOME variables and use pkg-config to discover the previously
    # built binaries. By using pkg-config, we also are able to discover the
    # ABI and SO versions of the dynamic libraries.
    export PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig:${PARQUET_HOME}/lib/pkgconfig:${PKG_CONFIG_PATH}
    unset ARROW_HOME
    unset PARQUET_HOME

    export PYARROW_WITH_PARQUET=1
    export PYARROW_WITH_ORC=1
    export PYARROW_WITH_JEMALLOC=1
    export PYARROW_WITH_PLASMA=1
    export PYARROW_BUNDLE_BOOST=1
    export PYARROW_BUNDLE_ARROW_CPP=1
    export PYARROW_BUILD_TYPE='release'
    export PYARROW_CMAKE_OPTIONS="-DBOOST_ROOT=$arrow_boost_dist"
    export SETUPTOOLS_SCM_PRETEND_VERSION=$PYARROW_VERSION
    pushd python
    python setup.py build_ext \
           --with-plasma --with-orc --with-parquet \
           --bundle-arrow-cpp --bundle-boost --boost-namespace=arrow_boost \
           bdist_wheel
    ls -l dist/

    # Do a test installation of the built wheel and change into another
    # directory to ensure our import tests later on pick up the wheel and
    # not the binaries from the build directory.
    for wheel in dist/*.whl; do
      pip install "$wheel"
    done
    mkdir -p tmp
    pushd tmp
    python -c "import pyarrow"
    python -c "import pyarrow.parquet"
    popd
    popd
    popd
}

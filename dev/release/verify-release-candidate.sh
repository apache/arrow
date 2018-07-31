#!/bin/bash
#
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
#

# Requirements
# - Ruby 2.x
#   - Plus gem dependencies, see c_glib/README
# - Maven >= 3.3.9
# - JDK >=7
# - gcc >= 4.8
# - nodejs >= 6.0.0 (best way is to use nvm)
#
# If using a non-system Boost, set BOOST_ROOT and add Boost libraries to
# LD_LIBRARY_PATH

case $# in
  2) VERSION="$1"
     RC_NUMBER="$2"
     ;;

  *) echo "Usage: $0 X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

set -ex

HERE=$(cd `dirname "${BASH_SOURCE[0]:-$0}"` && pwd)

ARROW_DIST_URL='https://dist.apache.org/repos/dist/dev/arrow'

download_dist_file() {
  curl -f -O $ARROW_DIST_URL/$1
}

download_rc_file() {
  download_dist_file apache-arrow-${VERSION}-rc${RC_NUMBER}/$1
}

import_gpg_keys() {
  download_dist_file KEYS
  gpg --import KEYS
}

fetch_archive() {
  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha1
  download_rc_file ${dist_name}.tar.gz.sha256
  gpg --verify ${dist_name}.tar.gz.asc ${dist_name}.tar.gz
  shasum -a 1 -c ${dist_name}.tar.gz.sha1
  shasum -a 256 -c ${dist_name}.tar.gz.sha256
}

verify_binary_artifacts() {
  # download the binaries folder for the current RC
  download_rc_file binaries

  # verify the signature and the checksums of each artifact
  find binaries -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd $(dirname $artifact)
    shasum -a 1 -c $artifact.sha1
    shasum -a 256 -c $artifact.sha256
    popd
  done
}

setup_tempdir() {
  cleanup() {
    rm -fr "$TMPDIR"
  }
  trap cleanup EXIT
  TMPDIR=$(mktemp -d -t "$1.XXXXX")
}


setup_miniconda() {
  # Setup short-lived miniconda for Python and integration tests
  if [ "$(uname)" == "Darwin" ]; then
    MINICONDA_URL=https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh
  else
    MINICONDA_URL=https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
  fi

  MINICONDA=`pwd`/test-miniconda

  wget -O miniconda.sh $MINICONDA_URL
  bash miniconda.sh -b -p $MINICONDA
  rm -f miniconda.sh

  export PATH=$MINICONDA/bin:$PATH

  conda create -n arrow-test -y -q python=3.6 \
        nomkl \
        numpy \
        pandas \
        six \
        cython -c conda-forge
  source activate arrow-test
}

# Build and test C++

test_and_install_cpp() {
  mkdir cpp/build
  pushd cpp/build

  cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
        -DCMAKE_INSTALL_LIBDIR=$ARROW_HOME/lib \
        -DARROW_PLASMA=on \
        -DARROW_PYTHON=on \
        -DARROW_BOOST_USE_SHARED=on \
        -DCMAKE_BUILD_TYPE=release \
        -DARROW_BUILD_BENCHMARKS=on \
        ..

  make -j$NPROC
  make install

  ctest -VV -L unittest
  popd
}

# Build and install Parquet master so we can test the Python bindings

install_parquet_cpp() {
  git clone git@github.com:apache/parquet-cpp.git

  mkdir parquet-cpp/build
  pushd parquet-cpp/build

  cmake -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
        -DCMAKE_INSTALL_LIBDIR=$PARQUET_HOME/lib \
        -DCMAKE_BUILD_TYPE=release \
        -DPARQUET_BOOST_USE_SHARED=on \
        -DPARQUET_BUILD_TESTS=off \
        ..

  make -j$NPROC
  make install

  popd
}

# Build and test Python

test_python() {
  pushd python

  pip install -r requirements.txt

  python setup.py build_ext --inplace --with-parquet --with-plasma
  py.test pyarrow -v --pdb

  popd
}


test_glib() {
  pushd c_glib

  ./configure --prefix=$ARROW_HOME
  make -j$NPROC
  make install

  GI_TYPELIB_PATH=$ARROW_HOME/lib/girepository-1.0 \
                 NO_MAKE=yes \
                 test/run-test.sh

  popd
}

test_js() {
  pushd js
  npm install
  # clean, lint, and build JS source
  npx run-s clean:all lint build
  npm run test

  # create initial integration test data
  # npm run create:testdata

  # run once to write the snapshots
  # npm test -- -t ts -u --integration

  # run again to test all builds against the snapshots
  # npm test -- --integration
  popd
}

# Build and test Java (Requires newer Maven -- I used 3.3.9)

test_package_java() {
  pushd java

  mvn test
  mvn package

  popd
}

# Run integration tests
test_integration() {
  JAVA_DIR=`pwd`/java
  CPP_BUILD_DIR=`pwd`/cpp/build

  export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
  export ARROW_CPP_EXE_PATH=$CPP_BUILD_DIR/release

  pushd integration

  python integration_test.py

  popd
}

setup_tempdir "arrow-$VERSION"
echo "Working in sandbox $TMPDIR"
cd $TMPDIR

export ARROW_HOME=$TMPDIR/install
export PARQUET_HOME=$TMPDIR/install
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig:$PKG_CONFIG_PATH

if [ "$(uname)" == "Darwin" ]; then
  NPROC=$(sysctl -n hw.ncpu)
else
  NPROC=$(nproc)
fi
VERSION=$1
RC_NUMBER=$2

TARBALL=apache-arrow-$1.tar.gz

import_gpg_keys
verify_binary_artifacts

DIST_NAME="apache-arrow-${VERSION}"
fetch_archive $DIST_NAME
tar xvzf ${DIST_NAME}.tar.gz
cd ${DIST_NAME}

test_package_java
setup_miniconda
test_and_install_cpp
test_js
test_integration
test_glib
install_parquet_cpp
test_python

echo 'Release candidate looks good!'
exit 0

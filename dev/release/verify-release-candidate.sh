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
# - Ruby >= 2.3
# - Maven >= 3.3.9
# - JDK >=7
# - gcc >= 4.8
# - Node.js >= 11.12 (best way is to use nvm)
# - Go >= 1.11
#
# If using a non-system Boost, set BOOST_ROOT and add Boost libraries to
# LD_LIBRARY_PATH.

case $# in
  3) ARTIFACT="$1"
     VERSION="$2"
     RC_NUMBER="$3"
     case $ARTIFACT in
       source|binaries) ;;
       *) echo "Invalid argument: '${ARTIFACT}', valid options are 'source' or 'binaries'"
          exit 1
          ;;
     esac
     ;;
  *) echo "Usage: $0 source|binaries X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

set -ex
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

detect_cuda() {
  if ! (which nvcc && which nvidia-smi) > /dev/null; then
    return 1
  fi

  local n_gpus=$(nvidia-smi --list-gpus | wc -l)
  return $((${n_gpus} < 1))
}

# Build options for the C++ library

if [ -z "${ARROW_CUDA:-}" ] && detect_cuda; then
  ARROW_CUDA=ON
fi
: ${ARROW_CUDA:=OFF}
: ${ARROW_FLIGHT:=ON}

ARROW_DIST_URL='https://dist.apache.org/repos/dist/dev/arrow'

download_dist_file() {
  curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --remote-name $ARROW_DIST_URL/$1
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
  download_rc_file ${dist_name}.tar.gz.sha256
  download_rc_file ${dist_name}.tar.gz.sha512
  gpg --verify ${dist_name}.tar.gz.asc ${dist_name}.tar.gz
  shasum -a 256 -c ${dist_name}.tar.gz.sha256
  shasum -a 512 -c ${dist_name}.tar.gz.sha512
}

bintray() {
  local command=$1
  shift
  local path=$1
  shift
  local url=https://bintray.com/api/v1${path}
  echo "${command} ${url}" 1>&2
  curl \
    --fail \
    --request ${command} \
    ${url} \
    "$@" | \
      jq .
}

download_bintray_files() {
  local target=$1

  local version_name=${VERSION}-rc${RC_NUMBER}

  local file
  bintray \
    GET /packages/${BINTRAY_REPOSITORY}/${target}-rc/versions/${version_name}/files | \
      jq -r ".[].path" | \
      while read file; do
    mkdir -p "$(dirname ${file})"
    curl \
      --fail \
      --location \
      --output ${file} \
      https://dl.bintray.com/${BINTRAY_REPOSITORY}/${file} &
  done
  wait
}

test_binary() {
  local download_dir=binaries
  mkdir -p ${download_dir}
  pushd ${download_dir}

  # takes longer on slow network
  for target in centos debian python ubuntu; do
    download_bintray_files ${target}
  done

  # verify the signature and the checksums of each artifact
  find . -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact || exit 1

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd $(dirname $artifact)
    base_artifact=$(basename $artifact)
    if [ -f $base_artifact.sha256 ]; then
      shasum -a 256 -c $base_artifact.sha256 || exit 1
    fi
    shasum -a 512 -c $base_artifact.sha512 || exit 1
    popd
  done

  popd
}

test_apt() {
  for target in debian-stretch \
                debian-buster \
                ubuntu-trusty \
                ubuntu-xenial \
                ubuntu-bionic \
                ubuntu-cosmic \
                ubuntu-disco; do
    if ! "${SOURCE_DIR}/../run_docker_compose.sh" \
           "${target}" \
           /arrow/dev/release/verify-apt.sh \
           "${VERSION}" \
           "yes" \
           "${BINTRAY_REPOSITORY}"; then
      echo "Failed to verify the APT repository for ${target}"
      exit 1
    fi
  done
}

test_yum() {
  for target in centos-6 \
                centos-7; do
    if ! "${SOURCE_DIR}/../run_docker_compose.sh" \
           "${target}" \
           /arrow/dev/release/verify-yum.sh \
           "${VERSION}" \
           "yes" \
           "${BINTRAY_REPOSITORY}"; then
      echo "Failed to verify the Yum repository for ${target}"
      exit 1
    fi
  done
}


setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${TMPDIR} for details."
    fi
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

  MINICONDA=$PWD/test-miniconda

  wget -O miniconda.sh $MINICONDA_URL
  bash miniconda.sh -b -p $MINICONDA
  rm -f miniconda.sh

  . $MINICONDA/etc/profile.d/conda.sh

  conda create -n arrow-test -y -q -c conda-forge \
        python=3.6 \
        nomkl \
        numpy \
        pandas \
        six \
        cython
  conda activate arrow-test
}

# Build and test Java (Requires newer Maven -- I used 3.3.9)

test_package_java() {
  pushd java

  mvn test
  mvn package

  popd
}

# Build and test C++

test_and_install_cpp() {
  mkdir cpp/build
  pushd cpp/build

  ARROW_CMAKE_OPTIONS="
${ARROW_CMAKE_OPTIONS}
-DCMAKE_INSTALL_PREFIX=$ARROW_HOME
-DCMAKE_INSTALL_LIBDIR=lib
-DARROW_FLIGHT=${ARROW_FLIGHT}
-DARROW_PLASMA=ON
-DARROW_ORC=ON
-DARROW_PYTHON=ON
-DARROW_GANDIVA=ON
-DARROW_PARQUET=ON
-DARROW_BOOST_USE_SHARED=ON
-DCMAKE_BUILD_TYPE=release
-DARROW_BUILD_TESTS=ON
-DARROW_CUDA=${ARROW_CUDA}
-DARROW_DEPENDENCY_SOURCE=AUTO
"
  cmake $ARROW_CMAKE_OPTIONS ..

  make -j$NPROC install

  # TODO: ARROW-5036: plasma-serialization_tests broken
  # TODO: ARROW-5054: libgtest.so link failure in flight-server-test
  LD_LIBRARY_PATH=$PWD/release:$LD_LIBRARY_PATH ctest \
    --exclude-regex "plasma-serialization_tests" \
    -j$NPROC \
    --output-on-failure \
    -L unittest
  popd
}

# Build and test Python

test_python() {
  pushd python

  pip install -r requirements.txt -r requirements-test.txt

  export PYARROW_WITH_GANDIVA=1
  export PYARROW_WITH_PARQUET=1
  export PYARROW_WITH_PLASMA=1
  if [ "${ARROW_CUDA}" = "ON" ]; then
    export PYARROW_WITH_CUDA=1
  fi
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    export PYARROW_WITH_FLIGHT=1
  fi

  python setup.py build_ext --inplace
  py.test pyarrow -v --pdb

  popd
}

test_glib() {
  pushd c_glib

  if brew --prefix libffi > /dev/null 2>&1; then
    PKG_CONFIG_PATH=$(brew --prefix libffi)/lib/pkgconfig:$PKG_CONFIG_PATH
  fi

  if [ -f configure ]; then
    ./configure --prefix=$ARROW_HOME
    make -j$NPROC
    make install
  else
    meson build --prefix=$ARROW_HOME --libdir=lib
    ninja -C build
    ninja -C build install
  fi

  export GI_TYPELIB_PATH=$ARROW_HOME/lib/girepository-1.0:$GI_TYPELIB_PATH

  if ! bundle --version; then
    gem install bundler
  fi

  bundle install --path vendor/bundle
  bundle exec ruby test/run-test.rb

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

test_ruby() {
  pushd ruby

  local modules="red-arrow red-plasma red-gandiva red-parquet"
  if [ "${ARROW_CUDA}" = "ON" ]; then
    modules="${modules} red-arrow-cuda"
  fi

  for module in ${modules}; do
    pushd ${module}
    bundle install --path vendor/bundle
    bundle exec ruby test/run-test.rb
    popd
  done

  popd
}

test_go() {
  pushd go/arrow

  go get -v ./...
  go test ./...

  popd
}

test_rust() {
  # install rust toolchain in a similar fashion like test-miniconda
  export RUSTUP_HOME=$PWD/test-rustup
  export CARGO_HOME=$PWD/test-rustup

  curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path

  export PATH=$RUSTUP_HOME/bin:$PATH
  source $RUSTUP_HOME/env

  # build and test rust
  pushd rust

  # raises on any formatting errors
  rustup component add rustfmt --toolchain stable
  cargo +stable fmt --all -- --check

  # we are targeting Rust nightly for releases
  rustup default nightly

  # raises on any warnings
  RUSTFLAGS="-D warnings" cargo build
  cargo test

  popd
}

# Run integration tests
test_integration() {
  JAVA_DIR=$PWD/java
  CPP_BUILD_DIR=$PWD/cpp/build

  export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
  export ARROW_CPP_EXE_PATH=$CPP_BUILD_DIR/release

  pushd integration

  INTEGRATION_TEST_ARGS=

  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    INTEGRATION_TEST_ARGS=--run_flight
  fi

  python integration_test.py $INTEGRATION_TEST_ARGS

  popd
}

test_source_distribution() {
  export ARROW_HOME=$TMPDIR/install
  export PARQUET_HOME=$TMPDIR/install
  export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH
  export PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig:$PKG_CONFIG_PATH

  if [ "$(uname)" == "Darwin" ]; then
    NPROC=$(sysctl -n hw.ncpu)
  else
    NPROC=$(nproc)
  fi

  git clone https://github.com/apache/arrow-testing.git
  export ARROW_TEST_DATA=$PWD/arrow-testing/data

  git clone https://github.com/apache/parquet-testing.git
  export PARQUET_TEST_DATA=$PWD/parquet-testing/data

  if [ ${TEST_JAVA} -gt 0 ]; then
    test_package_java
  fi
  if [ ${TEST_CPP} -gt 0 ]; then
    setup_miniconda
    test_and_install_cpp
  fi
  if [ ${TEST_PYTHON} -gt 0 ]; then
    test_python
  fi
  if [ ${TEST_GLIB} -gt 0 ]; then
    test_glib
  fi
  if [ ${TEST_RUBY} -gt 0 ]; then
    test_ruby
  fi
  if [ ${TEST_JS} -gt 0 ]; then
    test_js
  fi
  if [ ${TEST_GO} -gt 0 ]; then
    test_go
  fi
  if [ ${TEST_RUST} -gt 0 ]; then
    test_rust
  fi
  if [ ${TEST_INTEGRATION} -gt 0 ]; then
    test_integration
  fi
}

test_binary_distribution() {
  : ${BINTRAY_REPOSITORY:=apache/arrow}

  if [ ${TEST_BINARY} -gt 0 ]; then
    test_binary
  fi
  if [ ${TEST_APT} -gt 0 ]; then
    test_apt
  fi
  if [ ${TEST_YUM} -gt 0 ]; then
    test_yum
  fi
}

# By default test all functionalities.
# To deactivate one test, deactivate the test and all of its dependents
# To explicitly select one test, set TEST_DEFAULT=0 TEST_X=1
: ${TEST_DEFAULT:=1}
: ${TEST_SOURCE:=${TEST_DEFAULT}}
: ${TEST_JAVA:=${TEST_DEFAULT}}
: ${TEST_CPP:=${TEST_DEFAULT}}
: ${TEST_GLIB:=${TEST_DEFAULT}}
: ${TEST_RUBY:=${TEST_DEFAULT}}
: ${TEST_PYTHON:=${TEST_DEFAULT}}
: ${TEST_JS:=${TEST_DEFAULT}}
: ${TEST_GO:=${TEST_DEFAULT}}
: ${TEST_RUST:=${TEST_DEFAULT}}
: ${TEST_INTEGRATION:=${TEST_DEFAULT}}
: ${TEST_BINARY:=${TEST_DEFAULT}}
: ${TEST_APT:=${TEST_DEFAULT}}
: ${TEST_YUM:=${TEST_DEFAULT}}

# Automatically test if its activated by a dependent
TEST_GLIB=$((${TEST_GLIB} + ${TEST_RUBY}))
TEST_PYTHON=$((${TEST_PYTHON} + ${TEST_INTEGRATION}))
TEST_CPP=$((${TEST_CPP} + ${TEST_GLIB} + ${TEST_PYTHON}))
TEST_JAVA=$((${TEST_JAVA} + ${TEST_INTEGRATION}))
TEST_JS=$((${TEST_JS} + ${TEST_INTEGRATION}))

: ${TEST_ARCHIVE:=apache-arrow-${VERSION}.tar.gz}
case "${TEST_ARCHIVE}" in
  /*)
   ;;
  *)
   TEST_ARCHIVE=${PWD}/${TEST_ARCHIVE}
   ;;
esac

TEST_SUCCESS=no

setup_tempdir "arrow-${VERSION}"
echo "Working in sandbox ${TMPDIR}"
cd ${TMPDIR}

if [ "${ARTIFACT}" == "source" ]; then
  dist_name="apache-arrow-${VERSION}"
  if [ ${TEST_SOURCE} -gt 0 ]; then
    import_gpg_keys
    fetch_archive ${dist_name}
    tar xf ${dist_name}.tar.gz
  else
    mkdir -p ${dist_name}
    tar xf ${TEST_ARCHIVE} -C ${dist_name} --strip-components=1
  fi
  pushd ${dist_name}
  test_source_distribution
  popd
else
  import_gpg_keys
  test_binary_distribution
fi

TEST_SUCCESS=yes
echo 'Release candidate looks good!'
exit 0

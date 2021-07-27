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
# - Go >= 1.15
#
# If using a non-system Boost, set BOOST_ROOT and add Boost libraries to
# LD_LIBRARY_PATH.
#
# To reuse build artifacts between runs set ARROW_TMPDIR environment variable to
# a directory where the temporary files should be placed to, note that this
# directory is not cleaned up automatically.

case $# in
  3) ARTIFACT="$1"
     VERSION="$2"
     RC_NUMBER="$3"
     case $ARTIFACT in
       source|binaries|wheels) ;;
       *) echo "Invalid argument: '${ARTIFACT}', valid options are \
'source', 'binaries', or 'wheels'"
          exit 1
          ;;
     esac
     ;;
  *) echo "Usage: $0 source|binaries X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

set -e
set -x
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
ARROW_DIR="$(dirname $(dirname ${SOURCE_DIR}))"

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
: ${ARROW_GANDIVA:=ON}

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

verify_dir_artifact_signatures() {
  # verify the signature and the checksums of each artifact
  find $1 -name '*.asc' | while read sigfile; do
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
}

test_binary() {
  local download_dir=binaries
  mkdir -p ${download_dir}

  ${PYTHON:-python} $SOURCE_DIR/download_rc_binaries.py $VERSION $RC_NUMBER \
         --dest=${download_dir}

  verify_dir_artifact_signatures ${download_dir}
}

test_apt() {
  for target in "debian:bullseye" \
                "arm64v8/debian:bullseye" \
                "debian:buster" \
                "arm64v8/debian:buster" \
                "ubuntu:bionic" \
                "arm64v8/ubuntu:bionic" \
                "ubuntu:focal" \
                "arm64v8/ubuntu:focal" \
                "ubuntu:groovy" \
                "arm64v8/ubuntu:groovy"; do \
    case "${target}" in
      arm64v8/debian:bullseye)
        # qemu-user-static in Ubuntu 20.04 has a crash bug:
        #   https://bugs.launchpad.net/qemu/+bug/1749393
        continue
        ;;
      arm64v8/*)
        if [ "$(arch)" = "aarch64" -o -e /usr/bin/qemu-aarch64-static ]; then
          : # OK
        else
          continue
        fi
        ;;
    esac
    if ! docker run --rm -v "${SOURCE_DIR}"/../..:/arrow:delegated \
           "${target}" \
           /arrow/dev/release/verify-apt.sh \
           "${VERSION}" \
           "rc"; then
      echo "Failed to verify the APT repository for ${target}"
      exit 1
    fi
  done
}

test_yum() {
  for target in "amazonlinux:2" \
                "centos:7" \
                "centos:8" \
                "arm64v8/centos:8"; do
    case "${target}" in
      arm64v8/*)
        if [ "$(arch)" = "aarch64" -o -e /usr/bin/qemu-aarch64-static ]; then
          : # OK
        else
          continue
        fi
        ;;
    esac
    if ! docker run --rm -v "${SOURCE_DIR}"/../..:/arrow:delegated \
           "${target}" \
           /arrow/dev/release/verify-yum.sh \
           "${VERSION}" \
           "rc"; then
      echo "Failed to verify the Yum repository for ${target}"
      exit 1
    fi
  done
}


setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${ARROW_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${ARROW_TMPDIR} for details."
    fi
  }

  if [ -z "${ARROW_TMPDIR}" ]; then
    # clean up automatically if ARROW_TMPDIR is not defined
    ARROW_TMPDIR=$(mktemp -d -t "$1.XXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${ARROW_TMPDIR}"
  fi
}

setup_miniconda() {
  # Setup short-lived miniconda for Python and integration tests
  OS="$(uname)"
  if [ "${OS}" == "Darwin" ]; then
    OS=MacOSX
  fi
  ARCH="$(uname -m)"
  MINICONDA_URL="https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-${OS}-${ARCH}.sh"

  MINICONDA=$PWD/test-miniconda

  if [ ! -d "${MINICONDA}" ]; then
    # Setup miniconda only if the directory doesn't exist yet
    wget -O miniconda.sh $MINICONDA_URL
    bash miniconda.sh -b -p $MINICONDA
    rm -f miniconda.sh
  fi
  echo "Installed miniconda at ${MINICONDA}"

  . $MINICONDA/etc/profile.d/conda.sh

  conda create -n arrow-test -y -q -c conda-forge \
    python=3.8 \
    nomkl \
    numpy \
    pandas \
    cython
  conda activate arrow-test
  echo "Using conda environment ${CONDA_PREFIX}"
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
  mkdir -p cpp/build
  pushd cpp/build

  ARROW_CMAKE_OPTIONS="
${ARROW_CMAKE_OPTIONS:-}
-DCMAKE_INSTALL_PREFIX=$ARROW_HOME
-DCMAKE_INSTALL_LIBDIR=lib
-DARROW_FLIGHT=${ARROW_FLIGHT}
-DARROW_PLASMA=ON
-DARROW_ORC=ON
-DARROW_PYTHON=ON
-DARROW_GANDIVA=${ARROW_GANDIVA}
-DARROW_PARQUET=ON
-DARROW_DATASET=ON
-DPARQUET_REQUIRE_ENCRYPTION=ON
-DARROW_VERBOSE_THIRDPARTY_BUILD=ON
-DARROW_WITH_BZ2=ON
-DARROW_WITH_ZLIB=ON
-DARROW_WITH_ZSTD=ON
-DARROW_WITH_LZ4=ON
-DARROW_WITH_SNAPPY=ON
-DARROW_WITH_BROTLI=ON
-DARROW_BOOST_USE_SHARED=ON
-DCMAKE_BUILD_TYPE=release
-DARROW_BUILD_TESTS=ON
-DARROW_BUILD_INTEGRATION=ON
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

test_csharp() {
  pushd csharp

  local csharp_bin=${PWD}/bin
  mkdir -p ${csharp_bin}

  if which dotnet > /dev/null 2>&1; then
    if ! which sourcelink > /dev/null 2>&1; then
      local dotnet_tools_dir=$HOME/.dotnet/tools
      if [ -d "${dotnet_tools_dir}" ]; then
        PATH="${dotnet_tools_dir}:$PATH"
      fi
    fi
  else
    local dotnet_version=3.1.405
    local dotnet_platform=
    case "$(uname)" in
      Linux)
        dotnet_platform=linux
        ;;
      Darwin)
        dotnet_platform=macos
        ;;
    esac
    local dotnet_download_thank_you_url=https://dotnet.microsoft.com/download/thank-you/dotnet-sdk-${dotnet_version}-${dotnet_platform}-x64-binaries
    local dotnet_download_url=$( \
      curl --location ${dotnet_download_thank_you_url} | \
        grep 'window\.open' | \
        grep -E -o '[^"]+' | \
        sed -n 2p)
    curl ${dotnet_download_url} | \
      tar xzf - -C ${csharp_bin}
    PATH=${csharp_bin}:${PATH}
  fi

  dotnet test
  mv dummy.git ../.git
  dotnet pack -c Release
  mv ../.git dummy.git

  if ! which sourcelink > /dev/null 2>&1; then
    dotnet tool install --tool-path ${csharp_bin} sourcelink
    PATH=${csharp_bin}:${PATH}
    if ! sourcelink --help > /dev/null 2>&1; then
      export DOTNET_ROOT=${csharp_bin}
    fi
  fi

  sourcelink test artifacts/Apache.Arrow/Release/netstandard1.3/Apache.Arrow.pdb
  sourcelink test artifacts/Apache.Arrow/Release/netcoreapp2.1/Apache.Arrow.pdb

  popd
}

# Build and test Python

test_python() {
  pushd python

  pip install -r requirements-build.txt -r requirements-test.txt

  export PYARROW_WITH_DATASET=1
  export PYARROW_WITH_PARQUET=1
  export PYARROW_WITH_PLASMA=1
  if [ "${ARROW_CUDA}" = "ON" ]; then
    export PYARROW_WITH_CUDA=1
  fi
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    export PYARROW_WITH_FLIGHT=1
  fi
  if [ "${ARROW_GANDIVA}" = "ON" ]; then
    export PYARROW_WITH_GANDIVA=1
  fi

  python setup.py build_ext --inplace
  pytest pyarrow -v --pdb

  popd
}

test_glib() {
  pushd c_glib

  pip install meson

  meson build --prefix=$ARROW_HOME --libdir=lib
  ninja -C build
  ninja -C build install

  export GI_TYPELIB_PATH=$ARROW_HOME/lib/girepository-1.0:$GI_TYPELIB_PATH

  if ! bundle --version; then
    gem install --no-document bundler
  fi

  bundle install --path vendor/bundle
  bundle exec ruby test/run-test.rb

  popd
}

test_js() {
  pushd js

  if [ "${INSTALL_NODE}" -gt 0 ]; then
    export NVM_DIR="`pwd`/.nvm"
    mkdir -p $NVM_DIR
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | \
      PROFILE=/dev/null bash
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

    nvm install --lts
    npm install -g yarn
  fi

  yarn --frozen-lockfile
  yarn run-s clean:all lint build
  yarn test
  popd
}

test_ruby() {
  pushd ruby

  local modules="red-arrow red-arrow-dataset red-plasma red-parquet"
  if [ "${ARROW_CUDA}" = "ON" ]; then
    modules="${modules} red-arrow-cuda"
  fi
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    modules="${modules} red-arrow-flight"
  fi
  if [ "${ARROW_GANDIVA}" = "ON" ]; then
    modules="${modules} red-gandiva"
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
  local VERSION=1.15.14
  local ARCH=amd64

  if [ "$(uname)" == "Darwin" ]; then
    local OS=darwin
  else
    local OS=linux
  fi

  local GO_ARCHIVE=go$VERSION.$OS-$ARCH.tar.gz
  wget https://dl.google.com/go/$GO_ARCHIVE

  mkdir -p local-go
  tar -xzf $GO_ARCHIVE -C local-go
  rm -f $GO_ARCHIVE

  export GOROOT=`pwd`/local-go/go
  export GOPATH=`pwd`/local-go/gopath
  export PATH=$GOROOT/bin:$GOPATH/bin:$PATH

  pushd go/arrow

  go get -v ./...
  go test ./...
  go clean -modcache

  popd
}

# Run integration tests
test_integration() {
  JAVA_DIR=$PWD/java
  CPP_BUILD_DIR=$PWD/cpp/build

  export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
  export ARROW_CPP_EXE_PATH=$CPP_BUILD_DIR/release

  pip install -e dev/archery

  INTEGRATION_TEST_ARGS=""

  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    INTEGRATION_TEST_ARGS="${INTEGRATION_TEST_ARGS} --run-flight"
  fi

  # Flight integration test executable have runtime dependency on
  # release/libgtest.so
  LD_LIBRARY_PATH=$ARROW_CPP_EXE_PATH:$LD_LIBRARY_PATH \
      archery integration \
              --with-cpp=${TEST_INTEGRATION_CPP} \
              --with-java=${TEST_INTEGRATION_JAVA} \
              --with-js=${TEST_INTEGRATION_JS} \
              --with-go=${TEST_INTEGRATION_GO} \
              $INTEGRATION_TEST_ARGS
}

clone_testing_repositories() {
  # Clone testing repositories if not cloned already
  if [ ! -d "arrow-testing" ]; then
    git clone https://github.com/apache/arrow-testing.git
  fi
  if [ ! -d "parquet-testing" ]; then
    git clone https://github.com/apache/parquet-testing.git
  fi
  export ARROW_TEST_DATA=$PWD/arrow-testing/data
  export PARQUET_TEST_DATA=$PWD/parquet-testing/data
}

test_source_distribution() {
  export ARROW_HOME=$ARROW_TMPDIR/install
  export PARQUET_HOME=$ARROW_TMPDIR/install
  export LD_LIBRARY_PATH=$ARROW_HOME/lib:${LD_LIBRARY_PATH:-}
  export PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig:${PKG_CONFIG_PATH:-}

  if [ "$(uname)" == "Darwin" ]; then
    NPROC=$(sysctl -n hw.ncpu)
  else
    NPROC=$(nproc)
  fi

  clone_testing_repositories

  if [ ${TEST_JAVA} -gt 0 ]; then
    test_package_java
  fi
  if [ ${TEST_CPP} -gt 0 ]; then
    test_and_install_cpp
  fi
  if [ ${TEST_CSHARP} -gt 0 ]; then
    test_csharp
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
  if [ ${TEST_INTEGRATION} -gt 0 ]; then
    test_integration
  fi
}

test_binary_distribution() {
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

test_linux_wheels() {
  local py_arches="3.6m 3.7m 3.8 3.9"
  local manylinuxes="2010 2014"

  for py_arch in ${py_arches}; do
    local env=_verify_wheel-${py_arch}
    conda create -yq -n ${env} python=${py_arch//[mu]/}
    conda activate ${env}
    pip install -U pip

    for ml_spec in ${manylinuxes}; do
      # check the mandatory and optional imports
      pip install python-rc/${VERSION}-rc${RC_NUMBER}/pyarrow-${VERSION}-cp${py_arch//[mu.]/}-cp${py_arch//./}-manylinux${ml_spec}_x86_64.whl
      INSTALL_PYARROW=OFF ${ARROW_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_DIR}
    done

    conda deactivate
  done
}

test_macos_wheels() {
  local py_arches="3.6m 3.7m 3.8 3.9"
  local macos_version=$(sw_vers -productVersion)
  local macos_short_version=${macos_version:0:5}

  local check_s3=ON
  local check_flight=ON

  # macOS version <= 10.13
  if [ $(echo "${macos_short_version}\n10.14" | sort -V | head -n1) == "${macos_short_version}" ]; then
    local check_s3=OFF
  fi
  # apple silicon processor
  if [ "$(uname -m)" = "arm64" ]; then
    local py_arches="3.8 3.9"
    local check_flight=OFF
  fi

  # verify arch-native wheels inside an arch-native conda environment
  for py_arch in ${py_arches}; do
    local env=_verify_wheel-${py_arch}
    conda create -yq -n ${env} python=${py_arch//m/}
    conda activate ${env}
    pip install -U pip

    # check the mandatory and optional imports
    pip install --find-links python-rc/${VERSION}-rc${RC_NUMBER} pyarrow==${VERSION}
    INSTALL_PYARROW=OFF ARROW_FLIGHT=${check_flight} ARROW_S3=${check_s3} \
      ${ARROW_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_DIR}

    conda deactivate
  done

  # verify arm64 and universal2 wheels using an universal2 python binary
  # the interpreter should be installed from python.org:
  #   https://www.python.org/ftp/python/3.9.6/python-3.9.6-macosx10.9.pkg
  if [ "$(uname -m)" = "arm64" ]; then
    for py_arch in "3.9"; do
      local pyver=${py_arch//m/}
      local python="/Library/Frameworks/Python.framework/Versions/${pyver}/bin/python${pyver}"

      # create and activate a virtualenv for testing as arm64
      for arch in "arm64" "x86_64"; do
        local venv="${ARROW_TMPDIR}/test-${arch}-virtualenv"
        $python -m virtualenv $venv
        source $venv/bin/activate
        pip install -U pip

        # install pyarrow's universal2 wheel
        pip install \
            --find-links python-rc/${VERSION}-rc${RC_NUMBER} \
            --target $(python -c 'import site; print(site.getsitepackages()[0])') \
            --platform macosx_11_0_universal2 \
            --only-binary=:all: \
            pyarrow==${VERSION}
        # check the imports and execute the unittests
        INSTALL_PYARROW=OFF ARROW_FLIGHT=${check_flight} ARROW_S3=${check_s3} \
          arch -${arch} ${ARROW_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_DIR}

        deactivate
      done
    done
  fi
}

test_wheels() {
  clone_testing_repositories

  local download_dir=binaries
  mkdir -p ${download_dir}

  if [ "$(uname)" == "Darwin" ]; then
    local filter_regex=.*macosx.*
  else
    local filter_regex=.*manylinux.*
  fi

  python $SOURCE_DIR/download_rc_binaries.py $VERSION $RC_NUMBER \
         --package_type python \
         --regex=${filter_regex} \
         --dest=${download_dir}

  verify_dir_artifact_signatures ${download_dir}

  pushd ${download_dir}

  if [ "$(uname)" == "Darwin" ]; then
    test_macos_wheels
  else
    test_linux_wheels
  fi

  popd
}

# By default test all functionalities.
# To deactivate one test, deactivate the test and all of its dependents
# To explicitly select one test, set TEST_DEFAULT=0 TEST_X=1

# Install NodeJS locally for running the JavaScript tests rather than using the
# system Node installation, which may be too old.
: ${INSTALL_NODE:=1}

if [ "${ARTIFACT}" == "source" ]; then
  : ${TEST_SOURCE:=1}
elif [ "${ARTIFACT}" == "wheels" ]; then
  TEST_WHEELS=1
else
  TEST_BINARY_DISTRIBUTIONS=1
fi
: ${TEST_SOURCE:=0}
: ${TEST_WHEELS:=0}
: ${TEST_BINARY_DISTRIBUTIONS:=0}

: ${TEST_DEFAULT:=1}
: ${TEST_JAVA:=${TEST_DEFAULT}}
: ${TEST_CPP:=${TEST_DEFAULT}}
: ${TEST_CSHARP:=${TEST_DEFAULT}}
: ${TEST_GLIB:=${TEST_DEFAULT}}
: ${TEST_RUBY:=${TEST_DEFAULT}}
: ${TEST_PYTHON:=${TEST_DEFAULT}}
: ${TEST_JS:=${TEST_DEFAULT}}
: ${TEST_GO:=${TEST_DEFAULT}}
: ${TEST_INTEGRATION:=${TEST_DEFAULT}}
if [ ${TEST_BINARY_DISTRIBUTIONS} -gt 0 ]; then
  TEST_BINARY_DISTRIBUTIONS_DEFAULT=${TEST_DEFAULT}
else
  TEST_BINARY_DISTRIBUTIONS_DEFAULT=0
fi
: ${TEST_BINARY:=${TEST_BINARY_DISTRIBUTIONS_DEFAULT}}
: ${TEST_APT:=${TEST_BINARY_DISTRIBUTIONS_DEFAULT}}
: ${TEST_YUM:=${TEST_BINARY_DISTRIBUTIONS_DEFAULT}}

# For selective Integration testing, set TEST_DEFAULT=0 TEST_INTEGRATION_X=1 TEST_INTEGRATION_Y=1
: ${TEST_INTEGRATION_CPP:=${TEST_INTEGRATION}}
: ${TEST_INTEGRATION_JAVA:=${TEST_INTEGRATION}}
: ${TEST_INTEGRATION_JS:=${TEST_INTEGRATION}}
: ${TEST_INTEGRATION_GO:=${TEST_INTEGRATION}}

# Automatically test if its activated by a dependent
TEST_GLIB=$((${TEST_GLIB} + ${TEST_RUBY}))
TEST_CPP=$((${TEST_CPP} + ${TEST_GLIB} + ${TEST_PYTHON} + ${TEST_INTEGRATION_CPP}))
TEST_JAVA=$((${TEST_JAVA} + ${TEST_INTEGRATION_JAVA}))
TEST_JS=$((${TEST_JS} + ${TEST_INTEGRATION_JS}))
TEST_GO=$((${TEST_GO} + ${TEST_INTEGRATION_GO}))
TEST_INTEGRATION=$((${TEST_INTEGRATION} + ${TEST_INTEGRATION_CPP} + ${TEST_INTEGRATION_JAVA} + ${TEST_INTEGRATION_JS} + ${TEST_INTEGRATION_GO}))

if [ "${ARTIFACT}" == "source" ]; then
  NEED_MINICONDA=$((${TEST_CPP} + ${TEST_INTEGRATION}))
elif [ "${ARTIFACT}" == "wheels" ]; then
  NEED_MINICONDA=$((${TEST_WHEELS}))
else
  if [ -z "${PYTHON:-}" ]; then
    NEED_MINICONDA=$((${TEST_BINARY}))
  else
    NEED_MINICONDA=0
  fi
fi

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
echo "Working in sandbox ${ARROW_TMPDIR}"
cd ${ARROW_TMPDIR}

if [ ${NEED_MINICONDA} -gt 0 ]; then
  setup_miniconda
fi

if [ "${ARTIFACT}" == "source" ]; then
  dist_name="apache-arrow-${VERSION}"
  if [ ${TEST_SOURCE} -gt 0 ]; then
    import_gpg_keys
    if [ ! -d "${dist_name}" ]; then
      fetch_archive ${dist_name}
      tar xf ${dist_name}.tar.gz
    fi
  else
    mkdir -p ${dist_name}
    if [ ! -f ${TEST_ARCHIVE} ]; then
      echo "${TEST_ARCHIVE} not found"
      exit 1
    fi
    tar xf ${TEST_ARCHIVE} -C ${dist_name} --strip-components=1
  fi
  pushd ${dist_name}
  test_source_distribution
  popd
elif [ "${ARTIFACT}" == "wheels" ]; then
  import_gpg_keys
  test_wheels
else
  import_gpg_keys
  test_binary_distribution
fi

TEST_SUCCESS=yes
echo 'Release candidate looks good!'
exit 0

#!/usr/bin/env bash
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
# - Docker
#
# If using a non-system Boost, set BOOST_ROOT and add Boost libraries to
# LD_LIBRARY_PATH.
#
# To reuse build artifacts between runs set ARROW_TMPDIR environment variable to
# a directory where the temporary files should be placed to, note that this
# directory is not cleaned up automatically.

set -e
set -o pipefail

if [ ${VERBOSE:-0} -gt 0 ]; then
  set -x
fi

case $# in
  1) ARTIFACT="$1"
     VERSION="HEAD"
     SOURCE_KIND="local"
     case $ARTIFACT in
       source) ;;
       *) echo "Invalid argument: '${ARTIFACT}', only valid option is 'source'"
          exit 1
          ;;
     esac
     ;;
  2) ARTIFACT="$1"
     VERSION="$2"
     SOURCE_KIND="git"
     case $ARTIFACT in
       source) ;;
       *) echo "Invalid argument: '${ARTIFACT}', only valid option is 'source'"
          exit 1
          ;;
     esac
     ;;
  3) ARTIFACT="$1"
     VERSION="$2"
     RC_NUMBER="$3"
     SOURCE_KIND="tarball"
     case $ARTIFACT in
       source|binaries|wheels|jars) ;;
       *) echo "Invalid argument: '${ARTIFACT}', valid options are \
'source', 'binaries', 'wheels', or 'jars'"
          exit 1
          ;;
     esac
     ;;
  *) echo "Usage: $0 source|binaries|wheels|jars X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

detect_cuda() {
  if ! (which nvcc && which nvidia-smi) > /dev/null; then
    return 1
  fi

  local n_gpus=$(nvidia-smi --list-gpus | wc -l)
  return $((${n_gpus} < 1))
}

# Execute tests in a conda enviroment
: ${USE_CONDA:=0}

# Build options for the C++ library
if [ -z "${ARROW_CUDA:-}" ] && detect_cuda; then
  ARROW_CUDA=ON
fi
: ${ARROW_S3:=OFF}
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

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha256
  download_rc_file ${dist_name}.tar.gz.sha512
  gpg --verify ${dist_name}.tar.gz.asc ${dist_name}.tar.gz
  ${sha256_verify} ${dist_name}.tar.gz.sha256
  ${sha512_verify} ${dist_name}.tar.gz.sha512
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
      ${sha256_verify} $base_artifact.sha256 || exit 1
    fi
    if [ -f $base_artifact.sha512 ]; then
      ${sha512_verify} $base_artifact.sha512 || exit 1
    fi
    popd
  done
}

test_binary() {
  local download_dir=binaries
  mkdir -p ${download_dir}

  ${PYTHON:-python3} $SOURCE_DIR/download_rc_binaries.py $VERSION $RC_NUMBER \
         --dest=${download_dir}

  verify_dir_artifact_signatures ${download_dir}
}

test_apt() {
  for target in "debian:buster" \
                "arm64v8/debian:buster" \
                "debian:bullseye" \
                "arm64v8/debian:bullseye" \
                "debian:bookworm" \
                "arm64v8/debian:bookworm" \
                "ubuntu:bionic" \
                "arm64v8/ubuntu:bionic" \
                "ubuntu:focal" \
                "arm64v8/ubuntu:focal" \
                "ubuntu:hirsute" \
                "arm64v8/ubuntu:hirsute" \
                "ubuntu:impish" \
                "arm64v8/ubuntu:impish"; do \
    case "${target}" in
      arm64v8/*)
        if [ "$(arch)" = "aarch64" -o -e /usr/bin/qemu-aarch64-static ]; then
          case "${target}" in
          arm64v8/debian:buster|arm64v8/ubuntu:bionic|arm64v8/ubuntu:focal)
            ;; # OK
          *)
            # qemu-user-static in Ubuntu 20.04 has a crash bug:
            #   https://bugs.launchpad.net/qemu/+bug/1749393
            continue
            ;;
          esac
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
  for target in "almalinux:8" \
                "arm64v8/almalinux:8" \
                "amazonlinux:2" \
                "centos:7"; do
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
    ARROW_TMPDIR=$(mktemp -d -t "arrow-${VERSION}.XXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${ARROW_TMPDIR}"
  fi

  echo "Working in sandbox ${ARROW_TMPDIR}"
}

install_nodejs() {
  # Install NodeJS locally for running the JavaScript tests rather than using the
  # system Node installation, which may be too old.
  if [ "${NODEJS_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    return 0
  fi

  required_node_major_version=16
  node_major_version=$(node --version 2>&1 | \grep -o '^v[0-9]*' | sed -e 's/^v//g' || :)

  if [ -n "${node_major_version}" -a
      "${node_major_version}" -ge ${required_node_major_version} ]; then
    echo "Found NodeJS installation with major version ${node_major_version}"
  else
    export NVM_DIR="`pwd`/.nvm"
    mkdir -p $NVM_DIR
    curl -sL https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | \
      PROFILE=/dev/null bash
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

    nvm install --lts
    npm install -g yarn
  fi

  NODEJS_ALREADY_INSTALLED=1
}

install_csharp() {
  # Install C# if doesn't already exist
  if [ "${CSHARP_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    return 0
  fi

  if which dotnet > /dev/null 2>&1; then
    local csharp_bin=$(dirname $(which dotnet))
    if ! which sourcelink > /dev/null 2>&1; then
      local dotnet_tools_dir=$HOME/.dotnet/tools
      if [ -d "${dotnet_tools_dir}" ]; then
        PATH="${dotnet_tools_dir}:$PATH"
      fi
    fi
    echo "Found C# at ${csharp_bin}"
  else
    local csharp_bin=${ARROW_TMPDIR}/csharp/bin
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
      curl -sL ${dotnet_download_thank_you_url} | \
        grep 'window\.open' | \
        grep -E -o '[^"]+' | \
        sed -n 2p)
    mkdir -p ${csharp_bin}
    curl -sL ${dotnet_download_url} | \
      tar xzf - -C ${csharp_bin}
    PATH=${csharp_bin}:${PATH}
    echo "Installed C# to ${csharp_bin}"
  fi

  # Ensure to have sourcelink installed
  if ! which sourcelink > /dev/null 2>&1; then
    dotnet tool install --tool-path ${csharp_bin} sourcelink
    PATH=${csharp_bin}:${PATH}
    if ! sourcelink --help > /dev/null 2>&1; then
      export DOTNET_ROOT=${csharp_bin}
    fi
  fi

  CSHARP_ALREADY_INSTALLED=1
}

install_go() {
  # Install go
  if [ "${GO_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    return 0
  fi

  local version=1.16.12

  local arch="$(uname -m)"
  if [ "$arch" == "x86_64" ]; then
    arch=amd64
  elif [ "$arch" == "aarch64" ]; then
    arch=arm64
  fi

  if [ "$(uname)" == "Darwin" ]; then
    local os=darwin
  else
    local os=linux
  fi

  local archive="go${version}.${os}-${arch}.tar.gz"
  curl -sLO https://dl.google.com/go/$archive

  local prefix=${ARROW_TMPDIR}/go
  mkdir -p $prefix
  tar -xzf $archive -C $prefix
  rm -f $archive

  export GOROOT=${prefix}/go
  export GOPATH=${prefix}/gopath
  export PATH=$GOROOT/bin:$GOPATH/bin:$PATH

  GO_ALREADY_INSTALLED=1
}

mamba() {
  # Helper to access mamba directly instead after activating base conda environment
  $ARROW_TMPDIR/mambaforge/bin/mamba "$@"
}

install_conda() {
  # Setup short-lived miniconda for Python and integration tests
  if [ "${CONDA_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    return 0
  fi

  local arch=$(uname -m)
  local platform=$(uname)
  local url="https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-${platform}-${arch}.sh"
  local prefix=$ARROW_TMPDIR/mambaforge

  # Setup miniconda only if the directory doesn't exist yet
  if [ ! -d "${prefix}" ]; then
    curl -sL -o miniconda.sh $url
    bash miniconda.sh -b -p $prefix
    rm -f miniconda.sh
    echo "Installed miniconda at ${MINICONDA}"
  else
    echo "Miniconda already installed at ${MINICONDA}"
  fi

  # Creating a separate conda environment
  . $prefix/etc/profile.d/conda.sh
  conda activate base

  CONDA_ALREADY_INSTALLED=1
}

setup_conda() {
  # Optionally setup conda environment with the passed dependencies
  local env="conda-${ENV:-source}"
  local pyver=${PYTHON_VERSION:-3}

  if [ "${USE_CONDA}" -gt 0 ]; then
    # Deactivate previous env
    conda deactivate || :
    # Ensure that conda is installed
    install_conda
    # Create environment
    if ! conda env list | grep $env; then
      mamba create -y -n $env python=${pyver}
      echo "Created conda environment ${CONDA_PREFIX}"
    fi
    conda activate $env
    # Install dependencies
    if [ $# -gt 0 ]; then
      mamba install -y $@
    fi
  elif [ ! -z ${CONDA_PREFIX} ]; then
    echo "Conda environment is active despite that USE_CONDA is set to 0."
    echo "Deactivate the environment before running the verification script."
    exit 1
  fi
}

setup_virtualenv() {
  # Optionally setup pip virtualenv with the passed dependencies
  local env="venv-${ENV:-source}"
  local pyver=${PYTHON_VERSION:-3}
  local python=${PYTHON:-"python${pyver}"}
  local virtualenv="${ARROW_TMPDIR}/${env}"

  if [ "${USE_CONDA}" -eq 0 ]; then
    if [ ! -z ${CONDA_PREFIX} ]; then
      echo "Conda environment is active despite that USE_CONDA is set to 0."
      echo "Deactivate the environment before running the verification script."
      exit 1
    fi
    # Deactivate previous env
    deactivate || :
    # Create environment
    if [ ! -d "${virtualenv}" ]; then
      $python -m pip install virtualenv
      $python -m virtualenv ${virtualenv}
      source "${virtualenv}/bin/activate"
    fi
    # Install dependencies
    if [ $# -gt 0 ]; then
      pip install $@
    fi
  fi
}

test_package_java() {
  # Build and test Java (Requires newer Maven -- I used 3.3.9)
  setup_conda maven

  pushd java
  mvn test
  mvn package
  popd
}

test_and_install_cpp() {
  # Build and test C++
  setup_virtualenv numpy
  setup_conda \
    --file ci/conda_env_unix.txt \
    --file ci/conda_env_cpp.txt \
    --file ci/conda_env_gandiva.txt \
    ncurses \
    numpy \
    sqlite \
    compilers

  if [ "${USE_CONDA}" -gt 0 ]; then
    DEFAULT_DEPENDENCY_SOURCE="CONDA"
    # TODO(kszucs): remove
    mamba remove -y gtest
  else
    DEFAULT_DEPENDENCY_SOURCE="AUTO"
  fi

  rm -rf cpp/verification-build
  mkdir -p cpp/verification-build
  pushd cpp/verification-build

  if [ ! -z "$CMAKE_GENERATOR" ]; then
    ARROW_CMAKE_OPTIONS="${ARROW_CMAKE_OPTIONS:-} -G Ninja"
  fi

  cmake \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BUILD_INTEGRATION=ON \
    -DARROW_BUILD_TESTS=ON \
    -DARROW_CUDA=${ARROW_CUDA} \
    -DARROW_DATASET=ON \
    -DARROW_DEPENDENCY_SOURCE=${ARROW_DEPENDENCY_SOURCE:-$DEFAULT_DEPENDENCY_SOURCE} \
    -DARROW_FLIGHT=${ARROW_FLIGHT} \
    -DARROW_GANDIVA=${ARROW_GANDIVA} \
    -DARROW_HDFS=ON \
    -DARROW_ORC=ON \
    -DARROW_PARQUET=ON \
    -DARROW_PLASMA=ON \
    -DARROW_PYTHON=ON \
    -DARROW_S3=${ARROW_S3} \
    -DARROW_USE_CCACHE=${ARROW_USE_CCACHE:-OFF} \
    -DARROW_VERBOSE_THIRDPARTY_BUILD=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
    -DGTest_SOURCE=BUNDLED \
    -DPARQUET_REQUIRE_ENCRYPTION=ON \
    ${ARROW_CMAKE_OPTIONS:-} \
    ..
  cmake --build . --target install

  # TODO: ARROW-5036: plasma-serialization_tests broken
  # TODO: ARROW-5054: libgtest.so link failure in flight-server-test
  ctest \
    --exclude-regex "plasma-serialization_tests" \
    -j$NPROC \
    --output-on-failure \
    -L unittest

  popd
}

test_python() {
  # Build and test Python
  setup_virtualenv cython numpy setuptools_scm setuptools
  setup_conda --file ci/conda_env_python.txt

  export PYARROW_PARALLEL=$NPROC
  export PYARROW_WITH_DATASET=1
  export PYARROW_WITH_HDFS=1
  export PYARROW_WITH_ORC=1
  export PYARROW_WITH_PARQUET=1
  export PYARROW_WITH_PARQUET_ENCRYPTION=1
  export PYARROW_WITH_PLASMA=1
  if [ "${ARROW_S3}" = "ON" ]; then
    export PYARROW_WITH_S3=1
  fi
  if [ "${ARROW_CUDA}" = "ON" ]; then
    export PYARROW_WITH_CUDA=1
  fi
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    export PYARROW_WITH_FLIGHT=1
  fi
  if [ "${ARROW_GANDIVA}" = "ON" ]; then
    export PYARROW_WITH_GANDIVA=1
  fi

  pushd python

  # Build pyarrow
  python setup.py build_ext --inplace

  # Check mandatory and optional imports
  python -c "
import pyarrow
import pyarrow._hdfs
import pyarrow.csv
import pyarrow.dataset
import pyarrow.fs
import pyarrow.json
import pyarrow.orc
import pyarrow.parquet
import pyarrow.plasma
"
  if [ "${PYARROW_WITH_S3}" == "ON" ]; then
    python -c "import pyarrow._s3fs"
  fi
  if [ "${PYARROW_WITH_CUDA}" == "ON" ]; then
    python -c "import pyarrow.cuda"
  fi
  if [ "${PYARROW_WITH_FLIGHT}" == "ON" ]; then
    python -c "import pyarrow.flight"
  fi
  if [ "${ARROW_WITH_GANDIVA}" == "ON" ]; then
    python -c "import pyarrow.gandiva"
  fi

  # Install test dependencies
  pip install -r requirements-test.txt

  # Execute pyarrow unittests
  pytest pyarrow -v

  popd
}

test_glib() {
  # Build and test C GLib
  setup_virtualenv meson
  setup_conda meson

  # Install bundler if doesn't exist
  if ! bundle --version; then
    gem install --no-document bundler
  fi

  pushd c_glib

  # Build the C GLib bindings
  meson build --prefix=$ARROW_HOME --libdir=lib
  ninja -C build
  ninja -C build install

  # Test the C GLib bindings
  export GI_TYPELIB_PATH=$ARROW_HOME/lib/girepository-1.0:$GI_TYPELIB_PATH
  bundle config set --local path 'vendor/bundle'
  bundle install
  bundle exec ruby test/run-test.rb

  popd
}

test_ruby() {
  setup_conda
  setup_virtualenv

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
    bundle config set --local path 'vendor/bundle'
    bundle install
    bundle exec ruby test/run-test.rb
    popd
  done

  popd
}

test_csharp() {
  install_csharp

  pushd csharp

  dotnet test

  if [ "${SOURCE_KIND}" = "local" -o "${SOURCE_KIND}" = "git" ]; then
    dotnet pack -c Release
  else
    mv dummy.git ../.git
    dotnet pack -c Release
    mv ../.git dummy.git
  fi

  sourcelink test artifacts/Apache.Arrow/Release/netstandard1.3/Apache.Arrow.pdb
  sourcelink test artifacts/Apache.Arrow/Release/netcoreapp2.1/Apache.Arrow.pdb

  popd
}

test_js() {
  install_nodejs

  pushd js
  yarn --frozen-lockfile
  yarn clean:all
  yarn lint
  yarn build
  yarn test
  yarn test:bundle
  popd
}

test_go() {
  install_go

  pushd go/arrow
  go get -v ./...
  go test ./...
  go clean -modcache
  popd
}

# Run integration tests
test_integration() {
  setup_conda
  setup_virtualenv

  pip install -e dev/archery

  JAVA_DIR=$PWD/java
  CPP_BUILD_DIR=$PWD/cpp/build

  # export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
  # export ARROW_CPP_EXE_PATH=$CPP_BUILD_DIR/release

  INTEGRATION_TEST_ARGS=""
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    INTEGRATION_TEST_ARGS="${INTEGRATION_TEST_ARGS} --run-flight"
  fi

  # Flight integration test executable have runtime dependency on
  # release/libgtest.so
  #LD_LIBRARY_PATH=$ARROW_CPP_EXE_PATH:$LD_LIBRARY_PATH \
      archery integration \
              --with-cpp=${TEST_INTEGRATION_CPP} \
              --with-java=${TEST_INTEGRATION_JAVA} \
              --with-js=${TEST_INTEGRATION_JS} \
              --with-go=${TEST_INTEGRATION_GO} \
              $INTEGRATION_TEST_ARGS
}

ensure_source_directory() {
  dist_name="apache-arrow-${VERSION}"

  if [ "${SOURCE_KIND}" = "local" ]; then
    # Local arrow repository, testing repositories should be already present
    if [ -z "$ARROW_SOURCE_DIR" ]; then
      export ARROW_SOURCE_DIR="$(cd ${SOURCE_DIR}/../.. && pwd)"
    fi
  elif [ "${SOURCE_KIND}" = "git" ]; then
    # Remote arrow repository, testing repositories must be cloned
    export ARROW_SOURCE_DIR="${ARROW_TMPDIR}/arrow"
    if [ ! -d "${ARROW_SOURCE_DIR}" ]; then
      : ${SOURCE_REPOSITORY:="https://github.com/apache/arrow"}
      git clone --recurse-submodules $SOURCE_REPOSITORY $ARROW_SOURCE_DIR
      git -C $ARROW_SOURCE_DIR checkout $VERSION
    fi
  else
    # Release tarball, testing repositories must be cloned separately
    export ARROW_SOURCE_DIR="${ARROW_TMPDIR}/${dist_name}"
    if [ $((${TEST_SOURCE} + ${TEST_WHEELS})) -gt 0 ]; then
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
  fi

  # Ensure that the testing repositories are cloned
  pushd $ARROW_SOURCE_DIR
  if [ ! -d "testing/data" ]; then
    git clone https://github.com/apache/arrow-testing.git testing
  fi
  if [ ! -d "cpp/submodules/parquet-testing/data" ]; then
    git clone https://github.com/apache/parquet-testing.git cpp/submodules/parquet-testing
  fi
  popd

  export ARROW_TEST_DATA=$PWD/testing/data
  export PARQUET_TEST_DATA=$PWD/cpp/submodules/parquet-testing/data
  export ARROW_GDB_SCRIPT=$PWD/cpp/gdb_arrow.py
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

  pushd $ARROW_SOURCE_DIR

  if [ ${TEST_GO} -gt 0 ]; then
    test_go
  fi
  if [ ${TEST_CSHARP} -gt 0 ]; then
    test_csharp
  fi
  if [ ${TEST_JS} -gt 0 ]; then
    test_js
  fi
  if [ ${TEST_CPP} -gt 0 ]; then
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
  if [ ${TEST_JAVA} -gt 0 ]; then
    test_package_java
  fi
  if [ ${TEST_INTEGRATION} -gt 0 ]; then
    test_integration
  fi

  popd
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
  if [ "$(uname -m)" = "aarch64" ]; then
    local arch="aarch64"
  else
    local arch="x86_64"
  fi

  local python_versions="3.7 3.8 3.9 3.10"
  local platform_tags="manylinux_2_12_${arch}.manylinux2010_${arch} manylinux_2_17_${arch}.manylinux2014_${arch}"

  for pyver in ${python_versions}; do
    if [ "${USE_CONDA}" -eq 0 -a ! command -v "python${py_ver}"]; then
      echo "Couldn't locate python interpreter with version ${py_arch}"
      echo "Call the script with USE_CONDA=1 to test all of the python versions."
      continue
    fi
    ENV=wheel-${pyver} PYTHON_VERSION=${pyver} setup_conda
    ENV=wheel-${pyver} PYTHON_VERSION=${pyver} setup_virtualenv

    for platform in ${platform_tags}; do
      # check the mandatory and optional imports
      # TODO(kszucs): may need to define --target $(python -c 'import site; print(site.getsitepackages()[0])') --only-binary=:all:
      pip install --force-reinstall --platform ${platform} --find-links python-rc/${VERSION}-rc${RC_NUMBER} pyarrow==${VERSION}
      INSTALL_PYARROW=OFF ${ARROW_SOURCE_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_SOURCE_DIR}
    done
  done
}

test_macos_wheels() {
  local python_versions="3.7m 3.8 3.9 3.10"
  local macos_version=$(sw_vers -productVersion)
  local macos_short_version=${macos_version:0:5}

  local check_s3=ON
  local check_flight=ON

  # macOS version <= 10.13
  if [ $(echo "${macos_short_version}\n10.14" | sort | head -n1) == "${macos_short_version}" ]; then
    local check_s3=OFF
  fi
  # apple silicon processor
  if [ "$(uname -m)" = "arm64" ]; then
    local python_versions="3.8 3.9 3.10"
    local check_flight=OFF
  fi

  # verify arch-native wheels inside an arch-native conda environment
  for pyver in ${python_versions}; do
    if [ "${USE_CONDA}" -eq 0 -a ! command -v "python${py_ver}"]; then
      echo "Couldn't locate python interpreter with version ${py_arch}"
      echo "Call the script with USE_CONDA=1 to test all of the python versions."
      continue
    fi
    ENV=wheel-${pyver} PYTHON_VERSION=${pyver} setup_conda
    ENV=wheel-${pyver} PYTHON_VERSION=${pyver} setup_virtualenv

    # check the mandatory and optional imports
    # TODO(kszucs): may need to define --target $(python -c 'import site; print(site.getsitepackages()[0])') --only-binary=:all:
    pip install --force-reinstall --find-links python-rc/${VERSION}-rc${RC_NUMBER} pyarrow==${VERSION}
    INSTALL_PYARROW=OFF ARROW_FLIGHT=${check_flight} ARROW_S3=${check_s3} \
      ${ARROW_SOURCE_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_SOURCE_DIR}
  done

  # verify arm64 and universal2 wheels using an universal2 python binary
  # the interpreter should be installed from python.org:
  #   https://www.python.org/ftp/python/3.9.6/python-3.9.6-macosx10.9.pkg
  if [ "$(uname -m)" = "arm64" ]; then
    for pyver in "3.9 3.10"; do
      local python="/Library/Frameworks/Python.framework/Versions/${pyver}/bin/python${pyver}"
      if [ ! command -v "python${py_ver}" ]; then
        echo "Couldn't locate python interpreter with version ${py_arch}"
        continue
      fi
      # create and activate a virtualenv for testing as arm64
      for arch in "arm64" "x86_64"; do
        ENV=wheel-${pyver}-${arch} PYTHON=${python} setup_virtualenv
        # install pyarrow's universal2 wheel
        pip install \
            --find-links python-rc/${VERSION}-rc${RC_NUMBER} \
            --target $(python -c 'import site; print(site.getsitepackages()[0])') \
            --platform macosx_11_0_universal2 \
            --only-binary=:all: \
            pyarrow==${VERSION}
        # check the imports and execute the unittests
        INSTALL_PYARROW=OFF ARROW_FLIGHT=${check_flight} ARROW_S3=${check_s3} \
          arch -${arch} ${ARROW_SOURCE_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_SOURCE_DIR}
      done
    done
  fi
}

test_wheels() {
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

test_jars() {
  local download_dir=jars
  mkdir -p ${download_dir}

  ${PYTHON:-python3} $SOURCE_DIR/download_rc_binaries.py $VERSION $RC_NUMBER \
         --dest=${download_dir} \
         --package_type=jars

  verify_dir_artifact_signatures ${download_dir}
}

# By default test all functionalities.
# To deactivate one test, deactivate the test and all of its dependents
# To explicitly select one test, set TEST_DEFAULT=0 TEST_X=1

case "${ARTIFACT}" in
  source)
    : ${TEST_SOURCE:=1}
    ;;
  binaries)
    TEST_BINARY_DISTRIBUTIONS=1
    ;;
  wheels)
    TEST_WHEELS=1
    USE_CONDA=1
    ;;
  jars)
    TEST_JARS=1
    ;;
esac
: ${TEST_SOURCE:=0}
: ${TEST_BINARY_DISTRIBUTIONS:=0}
: ${TEST_WHEELS:=0}
: ${TEST_JARS:=0}

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

: ${TEST_ARCHIVE:=apache-arrow-${VERSION}.tar.gz}
case "${TEST_ARCHIVE}" in
  /*)
   ;;
  *)
   TEST_ARCHIVE=${PWD}/${TEST_ARCHIVE}
   ;;
esac

TEST_SUCCESS=no
setup_tempdir

case "${ARTIFACT}" in
  source)
    ensure_source_directory
    pushd ${ARROW_SOURCE_DIR}
    test_source_distribution
    popd
    ;;
  binaries)
    import_gpg_keys
    test_binary_distribution
    ;;
  wheels)
    ensure_source_directory
    test_wheels
    ;;
  jars)
    import_gpg_keys
    test_jars
    ;;
esac

TEST_SUCCESS=yes
echo 'Release candidate looks good!'
exit 0

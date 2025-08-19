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
# - gcc >= 4.8
# - Go >= 1.22
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
  0) VERSION="HEAD"
     SOURCE_KIND="local"
     TEST_BINARIES=0
     ;;
  1) VERSION="$1"
     SOURCE_KIND="git"
     TEST_BINARIES=0
     ;;
  2) VERSION="$1"
     RC_NUMBER="$2"
     SOURCE_KIND="tarball"
     ;;
  *) echo "Usage:"
     echo "  Verify release candidate:"
     echo "    $0 X.Y.Z RC_NUMBER"
     echo "  Verify only the source distribution:"
     echo "    TEST_DEFAULT=0 TEST_SOURCE=1 $0 X.Y.Z RC_NUMBER"
     echo "  Verify only the binary distributions:"
     echo "    TEST_DEFAULT=0 TEST_BINARIES=1 $0 X.Y.Z RC_NUMBER"
     echo "  Verify only the wheels:"
     echo "    TEST_DEFAULT=0 TEST_WHEELS=1 $0 X.Y.Z RC_NUMBER"
     echo ""
     echo "  Run the source verification tasks on a remote git revision:"
     echo "    $0 GIT-REF"
     echo "  Run the source verification tasks on this arrow checkout:"
     echo "    $0"
     exit 1
     ;;
esac

# Note that these point to the current verify-release-candidate.sh directories
# which is different from the ARROW_SOURCE_DIR set in ensure_source_directory()
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
ARROW_DIR="$(cd "${SOURCE_DIR}/../.." && pwd)"

: ${GITHUB_REPOSITORY:=apache/arrow}

show_header() {
  echo ""
  printf '=%.0s' $(seq ${#1}); printf '\n'
  echo "${1}"
  printf '=%.0s' $(seq ${#1}); printf '\n'
}

show_info() {
  echo "└ ${1}"
}

detect_cuda() {
  show_header "Detect CUDA"

  if ! (which nvcc && which nvidia-smi) > /dev/null; then
    echo "No devices found."
    return 1
  fi

  local n_gpus=$(nvidia-smi --list-gpus | wc -l)
  echo "Found ${n_gpus} GPU."
  return $((${n_gpus} < 1))
}

ARROW_RC_URL="https://dist.apache.org/repos/dist/dev/arrow"
ARROW_KEYS_URL="https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/KEYS"

download_file() {
  curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --output "$2" \
    "$1"
}

download_rc_file() {
  download_file \
    "${ARROW_RC_URL}/apache-arrow-${VERSION}-rc${RC_NUMBER}/$1" \
    "$1"
}

import_gpg_keys() {
  if [ "${GPGKEYS_ALREADY_IMPORTED:-0}" -gt 0 ]; then
    return 0
  fi
  download_file "${ARROW_KEYS_URL}" KEYS
  gpg --import KEYS

  GPGKEYS_ALREADY_IMPORTED=1
}

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  import_gpg_keys

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
  import_gpg_keys

  # verify the signature and the checksums of each artifact
  find $1 -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd $(dirname $artifact)
    base_artifact=$(basename $artifact)
    if [ -f $base_artifact.sha256 ]; then
      ${sha256_verify} $base_artifact.sha256
    fi
    if [ -f $base_artifact.sha512 ]; then
      ${sha512_verify} $base_artifact.sha512
    fi
    popd
  done
}

test_binary() {
  # this downloads all artifacts and verifies their checksums and signatures
  show_header "Testing binary artifacts"
  maybe_setup_conda

  local download_dir=${ARROW_TMPDIR}/binaries
  mkdir -p ${download_dir}

  ${PYTHON:-python3} $SOURCE_DIR/download_rc_binaries.py $VERSION $RC_NUMBER \
         --dest=${download_dir} \
         --repository=${GITHUB_REPOSITORY} \
         --tag="apache-arrow-$VERSION-rc$RC_NUMBER"

  verify_dir_artifact_signatures ${download_dir}
}

check_verification_result_on_github() {
  pushd ${ARROW_TMPDIR}
  curl \
    --get \
    --data "branch=apache-arrow-${VERSION}-rc${RC_NUMBER}" \
    "https://api.github.com/repos/apache/arrow/actions/workflows/verify_rc.yml/runs" | \
    jq '.workflow_runs[0]' > latest_verify_rc.json
  conclusion="$(jq -r '.conclusion' latest_verify_rc.json)"
  if [ "${conclusion}" != "success" ]; then
    html_url="$(jq -r '.html_url' latest_verify_rc.json)"
    echo "Verification on GitHub wasn't successful: ${conclusion}: ${html_url}"
    exit 1
  fi
  popd
}

test_apt() {
  show_header "Testing APT packages"

  if [ "${GITHUB_ACTIONS}" != "true" ]; then
    check_verification_result_on_github
    return 0
  fi

  case "$(arch)" in
    "x86_64")
      for target in "debian:bookworm" \
                    "debian:trixie" \
                    "debian:forky" \
                    "ubuntu:jammy" \
                    "ubuntu:noble"; do \
        if ! docker run \
               --platform=linux/x86_64 \
               --rm \
               --security-opt="seccomp=unconfined" \
               --volume "${ARROW_DIR}":/arrow:delegated \
               "${target}" \
               /arrow/dev/release/verify-apt.sh \
               "${VERSION}" \
               "rc"; then
          echo "Failed to verify the APT repository for ${target} on x86_64"
          exit 1
        fi
      done
      ;;
    "aarch64")
      for target in "arm64v8/debian:bookworm" \
                    "arm64v8/debian:trixie" \
                    "arm64v8/debian:forky" \
                    "arm64v8/ubuntu:jammy" \
                    "arm64v8/ubuntu:noble"; do \
        if ! docker run \
               --platform=linux/arm64 \
               --rm \
               --security-opt="seccomp=unconfined" \
               --volume "${ARROW_DIR}":/arrow:delegated \
               "${target}" \
               /arrow/dev/release/verify-apt.sh \
               "${VERSION}" \
               "rc"; then
          echo "Failed to verify the APT repository for ${target} on arm64"
          exit 1
        fi
      done
      ;;
  esac
}

test_yum() {
  show_header "Testing Yum packages"

  if [ "${GITHUB_ACTIONS}" != "true" ]; then
    check_verification_result_on_github
    return 0
  fi

  case "$(arch)" in
    "x86_64")
      for target in "almalinux:10" \
                    "almalinux:9" \
                    "almalinux:8" \
                    "amazonlinux:2023" \
                    "quay.io/centos/centos:stream9" \
                    "centos:7"; do
        if ! docker run \
               --platform linux/x86_64 \
               --rm \
               --security-opt="seccomp=unconfined" \
               --volume "${ARROW_DIR}":/arrow:delegated \
               "${target}" \
               /arrow/dev/release/verify-yum.sh \
               "${VERSION}" \
               "rc"; then
          echo "Failed to verify the Yum repository for ${target} on x86_64"
          exit 1
        fi
      done
      ;;
    "aarch64")
      for target in "arm64v8/almalinux:10" \
                    "arm64v8/almalinux:9" \
                    "arm64v8/almalinux:8" \
                    "arm64v8/amazonlinux:2023" \
                    "quay.io/centos/centos:stream9"; do
        if ! docker run \
               --platform linux/arm64 \
               --rm \
               --security-opt="seccomp=unconfined" \
               --volume "${ARROW_DIR}":/arrow:delegated \
               "${target}" \
               /arrow/dev/release/verify-yum.sh \
               "${VERSION}" \
               "rc"; then
          echo "Failed to verify the Yum repository for ${target} on arm64"
          exit 1
        fi
      done
      ;;
  esac
}

setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${ARROW_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${ARROW_TMPDIR} for details."
    fi
  }

  show_header "Creating temporary directory"

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

install_csharp() {
  # Install C# if doesn't already exist
  if [ "${CSHARP_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    show_info "C# already installed $(which csharp) (.NET $(dotnet --version))"
    return 0
  fi

  show_info "Ensuring that C# is installed..."

  if dotnet --version | grep 8\.0 > /dev/null 2>&1; then
    local csharp_bin=$(dirname $(which dotnet))
    show_info "Found C# at $(which csharp) (.NET $(dotnet --version))"
  else
    if which dotnet > /dev/null 2>&1; then
      show_info "dotnet found but it is the wrong version and will be ignored."
    fi
    local csharp_bin=${ARROW_TMPDIR}/csharp/bin
    local dotnet_version=8.0.204
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
        grep 'directLink' | \
        grep -E -o 'https://builds.dotnet[^"]+' | \
        sed -n 2p)
    mkdir -p ${csharp_bin}
    curl -sL ${dotnet_download_url} | \
      tar xzf - -C ${csharp_bin}
    PATH=${csharp_bin}:${PATH}
    show_info "Installed C# at $(which csharp) (.NET $(dotnet --version))"
  fi

  CSHARP_ALREADY_INSTALLED=1
}

install_conda() {
  # Setup short-lived miniconda for Python and integration tests
  show_info "Ensuring that Conda is installed..."
  local prefix=$ARROW_TMPDIR/miniforge

  # Setup miniconda only if the directory doesn't exist yet
  if [ "${CONDA_ALREADY_INSTALLED:-0}" -eq 0 ]; then
    if [ ! -d "${prefix}" ]; then
      show_info "Installing miniconda at ${prefix}..."
      local arch=$(uname -m)
      local platform=$(uname)
      local url="https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-${platform}-${arch}.sh"
      curl -sL -o miniconda.sh $url
      bash miniconda.sh -b -p $prefix
      rm -f miniconda.sh
    else
      show_info "Miniconda already installed at ${prefix}"
    fi
  else
    show_info "Conda installed at ${prefix}"
  fi
  CONDA_ALREADY_INSTALLED=1

  # Creating a separate conda environment
  . $prefix/etc/profile.d/conda.sh
  conda activate base
}

maybe_setup_conda() {
  # Optionally setup conda environment with the passed dependencies
  local env="conda-${CONDA_ENV:-source}"
  local pyver=${PYTHON_VERSION:-3}

  if [ "${USE_CONDA}" -gt 0 ]; then
    show_info "Configuring Conda environment..."

    # Deactivate previous env
    if [ ! -z ${CONDA_PREFIX} ]; then
      conda deactivate || :
    fi
    # Ensure that conda is installed
    install_conda
    # Create environment
    if ! conda env list | cut -d" " -f 1 | grep $env; then
      mamba create -y -n $env python=${pyver}
    fi
    # Install dependencies
    if [ $# -gt 0 ]; then
      mamba install -y -n $env $@
    fi
    # Activate the environment
    conda activate $env
  elif [ ! -z ${CONDA_PREFIX} ]; then
    echo "Conda environment is active despite that USE_CONDA is set to 0."
    echo "Deactivate the environment using \`conda deactivate\` before running the verification script."
    return 1
  fi
}

maybe_setup_virtualenv() {
  # Optionally setup pip virtualenv with the passed dependencies
  local env="venv-${VENV_ENV:-source}"
  local pyver=${PYTHON_VERSION:-3}
  local python=${PYTHON:-"python${pyver}"}
  local virtualenv="${ARROW_TMPDIR}/${env}"
  local skip_missing_python=${SKIP_MISSING_PYTHON:-0}

  if [ "${USE_CONDA}" -eq 0 ]; then
    show_info "Configuring Python ${pyver} virtualenv..."

    if [ ! -z ${CONDA_PREFIX} ]; then
      echo "Conda environment is active despite that USE_CONDA is set to 0."
      echo "Deactivate the environment before running the verification script."
      return 1
    fi
    # Deactivate previous env
    if command -v deactivate &> /dev/null; then
      deactivate
    fi
    # Check that python interpreter exists
    if ! command -v "${python}" &> /dev/null; then
      echo "Couldn't locate python interpreter with version ${pyver}"
      echo "Call the script with USE_CONDA=1 to test all of the python versions."
      return 1
    else
      show_info "Found interpreter $($python --version): $(which $python)"
    fi
    # Create environment
    if [ ! -d "${virtualenv}" ]; then
      show_info "Creating python virtualenv at ${virtualenv}..."
      $python -m venv ${virtualenv}
      # Activate the environment
      source "${virtualenv}/bin/activate"
      # Upgrade pip and setuptools
      pip install -U pip setuptools
    else
      show_info "Using already created virtualenv at ${virtualenv}"
      # Activate the environment
      source "${virtualenv}/bin/activate"
    fi
    # Install dependencies
    if [ $# -gt 0 ]; then
      show_info "Installed pip packages $@..."
      pip install "$@"
    fi
  fi
}

test_and_install_cpp() {
  show_header "Build, install and test C++ libraries"

  # Build and test C++
  maybe_setup_virtualenv numpy
  maybe_setup_conda \
    --file ci/conda_env_unix.txt \
    --file ci/conda_env_cpp.txt \
    --file ci/conda_env_gandiva.txt \
    ncurses \
    numpy \
    sqlite \
    compilers

  if [ "${USE_CONDA}" -gt 0 ]; then
    DEFAULT_DEPENDENCY_SOURCE="CONDA"
    CMAKE_PREFIX_PATH="${CONDA_BACKUP_CMAKE_PREFIX_PATH}:${CMAKE_PREFIX_PATH}"
  else
    DEFAULT_DEPENDENCY_SOURCE="AUTO"
  fi

  mkdir -p $ARROW_TMPDIR/cpp-build
  pushd $ARROW_TMPDIR/cpp-build

  if [ ! -z "$CMAKE_GENERATOR" ]; then
    ARROW_CMAKE_OPTIONS="${ARROW_CMAKE_OPTIONS:-} -G ${CMAKE_GENERATOR}"
  fi

  local ARROW_BUILD_INTEGRATION=OFF
  local ARROW_BUILD_TESTS=OFF
  if [ ${TEST_INTEGRATION_CPP} -gt 0 ]; then
    ARROW_BUILD_INTEGRATION=ON
  fi
  if [ ${TEST_CPP} -gt 0 ]; then
    ARROW_BUILD_TESTS=ON
  fi

  cmake \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BUILD_EXAMPLES=OFF \
    -DARROW_BUILD_INTEGRATION=${ARROW_BUILD_INTEGRATION} \
    -DARROW_BUILD_TESTS=${ARROW_BUILD_TESTS} \
    -DARROW_BUILD_UTILITIES=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_CUDA=${ARROW_CUDA} \
    -DARROW_DATASET=ON \
    -DARROW_DEPENDENCY_SOURCE=${ARROW_DEPENDENCY_SOURCE:-$DEFAULT_DEPENDENCY_SOURCE} \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=${ARROW_FLIGHT} \
    -DARROW_FLIGHT_SQL=${ARROW_FLIGHT_SQL} \
    -DARROW_GANDIVA=${ARROW_GANDIVA} \
    -DARROW_GCS=${ARROW_GCS} \
    -DARROW_HDFS=ON \
    -DARROW_JSON=ON \
    -DARROW_ORC=ON \
    -DARROW_PARQUET=ON \
    -DARROW_SUBSTRAIT=ON \
    -DARROW_S3=${ARROW_S3} \
    -DARROW_USE_CCACHE=${ARROW_USE_CCACHE:-ON} \
    -DARROW_VERBOSE_THIRDPARTY_BUILD=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_RE2=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_UTF8PROC=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-release} \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
    -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD:-OFF} \
    -DGTest_SOURCE=${GTest_SOURCE:-BUNDLED} \
    -DPARQUET_BUILD_EXAMPLES=ON \
    -DPARQUET_BUILD_EXECUTABLES=ON \
    -DPARQUET_REQUIRE_ENCRYPTION=ON \
    ${ARROW_CMAKE_OPTIONS:-} \
    ${ARROW_SOURCE_DIR}/cpp
  export CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL:-${NPROC}}
  cmake --build . --target install

  if [ ${TEST_CPP} -gt 0 ]; then
    LD_LIBRARY_PATH=$PWD/release:$LD_LIBRARY_PATH ctest \
      --label-regex unittest \
      --output-on-failure \
      --parallel $NPROC \
      --timeout 300
  fi

  popd
}

test_python() {
  show_header "Build and test Python libraries"

  # Build and test Python
  maybe_setup_virtualenv
  maybe_setup_conda --file ci/conda_env_python.txt

  if [ "${USE_CONDA}" -gt 0 ]; then
    CMAKE_PREFIX_PATH="${CONDA_BACKUP_CMAKE_PREFIX_PATH}:${CMAKE_PREFIX_PATH}"
  fi

  export PYARROW_PARALLEL=$NPROC
  export PYARROW_WITH_DATASET=1
  export PYARROW_WITH_HDFS=1
  export PYARROW_WITH_ORC=1
  export PYARROW_WITH_PARQUET=1
  export PYARROW_WITH_PARQUET_ENCRYPTION=1
  if [ "${ARROW_CUDA}" = "ON" ]; then
    export PYARROW_WITH_CUDA=1
  fi
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    export PYARROW_WITH_FLIGHT=1
  fi
  if [ "${ARROW_GANDIVA}" = "ON" ]; then
    export PYARROW_WITH_GANDIVA=1
  fi
  if [ "${ARROW_GCS}" = "ON" ]; then
    export PYARROW_WITH_GCS=1
  fi
  if [ "${ARROW_S3}" = "ON" ]; then
    export PYARROW_WITH_S3=1
  fi

  pushd python

  # Build pyarrow
  python -m pip install -e .

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
"
  if [ "${ARROW_CUDA}" == "ON" ]; then
    python -c "import pyarrow.cuda"
  fi
  if [ "${ARROW_FLIGHT}" == "ON" ]; then
    python -c "import pyarrow.flight"
  fi
  if [ "${ARROW_GANDIVA}" == "ON" ]; then
    python -c "import pyarrow.gandiva"
  fi
  if [ "${ARROW_GCS}" == "ON" ]; then
    python -c "import pyarrow._gcsfs"
  fi
  if [ "${ARROW_S3}" == "ON" ]; then
    python -c "import pyarrow._s3fs"
  fi


  # Install test dependencies
  pip install -r requirements-test.txt

  # Execute pyarrow unittests
  pytest pyarrow -v

  popd
}

test_glib() {
  show_header "Build and test C GLib libraries"

  # Build and test C GLib
  maybe_setup_conda glib gobject-introspection meson ninja ruby
  maybe_setup_virtualenv meson

  # Install bundler if doesn't exist
  if ! bundle --version; then
    gem install --no-document bundler
  fi

  local build_dir=$ARROW_TMPDIR/c-glib-build
  mkdir -p $build_dir

  pushd c_glib

  # Build the C GLib bindings
  meson \
    --buildtype=${CMAKE_BUILD_TYPE:-release} \
    --libdir=lib \
    --prefix=$ARROW_HOME \
    $build_dir
  ninja -C $build_dir
  ninja -C $build_dir install

  # Test the C GLib bindings
  export GI_TYPELIB_PATH=$ARROW_HOME/lib/girepository-1.0:$GI_TYPELIB_PATH
  bundle config set --local path 'vendor/bundle'
  bundle install
  bundle exec ruby test/run-test.rb

  popd
}

test_ruby() {
  show_header "Build and test Ruby libraries"

  # required dependencies are installed by test_glib
  maybe_setup_conda
  maybe_setup_virtualenv

  which ruby
  which bundle

  pushd ruby

  local modules="red-arrow red-arrow-dataset red-parquet"
  if [ "${ARROW_CUDA}" = "ON" ]; then
    modules="${modules} red-arrow-cuda"
  fi
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    modules="${modules} red-arrow-flight"
  fi
  if [ "${ARROW_FLIGHT_SQL}" = "ON" ]; then
    modules="${modules} red-arrow-flight-sql"
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
  show_header "Build and test C# libraries"

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

  popd
}

# Run integration tests
test_integration() {
  show_header "Build and execute integration tests"

  maybe_setup_conda
  maybe_setup_virtualenv

  pip install -e dev/archery[integration]

  CPP_BUILD_DIR=$ARROW_TMPDIR/cpp-build

  export ARROW_CPP_EXE_PATH=$CPP_BUILD_DIR/release

  INTEGRATION_TEST_ARGS=""
  if [ "${ARROW_FLIGHT}" = "ON" ]; then
    INTEGRATION_TEST_ARGS="${INTEGRATION_TEST_ARGS} --run-flight"
  fi

  # Flight integration test executable have runtime dependency on release/libgtest.so
  LD_LIBRARY_PATH=$ARROW_CPP_EXE_PATH:$LD_LIBRARY_PATH archery integration \
    --run-ipc --run-flight --run-c-data \
    --with-cpp=${TEST_INTEGRATION_CPP} \
    $INTEGRATION_TEST_ARGS
}

ensure_source_directory() {
  show_header "Ensuring source directory"

  dist_name="apache-arrow-${VERSION}"

  if [ "${SOURCE_KIND}" = "local" ]; then
    # Local arrow repository, testing repositories should be already present
    if [ -z "$ARROW_SOURCE_DIR" ]; then
      export ARROW_SOURCE_DIR="${ARROW_DIR}"
    fi
    echo "Verifying local Arrow checkout at ${ARROW_SOURCE_DIR}"
  elif [ "${SOURCE_KIND}" = "git" ]; then
    # Remote arrow repository, testing repositories must be cloned
    : ${SOURCE_REPOSITORY:="https://github.com/apache/arrow"}
    case "${VERSION}" in
      *.*.*)
        revision="apache-arrow-${VERSION}"
        ;;
      *)
        revision="${VERSION}"
        ;;
    esac
    echo "Verifying Arrow repository ${SOURCE_REPOSITORY} with revision checkout ${revision}"
    export ARROW_SOURCE_DIR="${ARROW_TMPDIR}/arrow"
    if [ ! -d "${ARROW_SOURCE_DIR}" ]; then
      git clone --recurse-submodules $SOURCE_REPOSITORY $ARROW_SOURCE_DIR
      git -C $ARROW_SOURCE_DIR checkout "${revision}"
    fi
  else
    # Release tarball, testing repositories must be cloned separately
    echo "Verifying official Arrow release candidate ${VERSION}-rc${RC_NUMBER}"
    export ARROW_SOURCE_DIR="${ARROW_TMPDIR}/${dist_name}"
    if [ ! -d "${ARROW_SOURCE_DIR}" ]; then
      pushd $ARROW_TMPDIR
      fetch_archive ${dist_name}
      tar xf ${dist_name}.tar.gz
      popd
    fi
  fi

  # Ensure that the testing repositories are prepared
  if [ ! -d ${ARROW_SOURCE_DIR}/testing/data ]; then
    if [ -d ${SOURCE_DIR}/../../testing/data ]; then
      cp -a ${SOURCE_DIR}/../../testing ${ARROW_SOURCE_DIR}/
    else
      git clone \
        https://github.com/apache/arrow-testing.git \
        ${ARROW_SOURCE_DIR}/testing
    fi
  fi
  if [ ! -d ${ARROW_SOURCE_DIR}/cpp/submodules/parquet-testing/data ]; then
    if [ -d ${SOURCE_DIR}/../../cpp/submodules/parquet-testing/data ]; then
      cp -a \
         ${SOURCE_DIR}/../../cpp/submodules/parquet-testing \
         ${ARROW_SOURCE_DIR}/cpp/submodules/
    else
      git clone \
        https://github.com/apache/parquet-testing.git \
        ${ARROW_SOURCE_DIR}/cpp/submodules/parquet-testing
    fi
  fi

  export ARROW_TEST_DATA=$ARROW_SOURCE_DIR/testing/data
  export PARQUET_TEST_DATA=$ARROW_SOURCE_DIR/cpp/submodules/parquet-testing/data
  export ARROW_GDB_SCRIPT=$ARROW_SOURCE_DIR/cpp/gdb_arrow.py
}

test_source_distribution() {
  export ARROW_HOME=$ARROW_TMPDIR/install
  export CMAKE_PREFIX_PATH=$ARROW_HOME${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}
  export PARQUET_HOME=$ARROW_TMPDIR/install
  export PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}

  if [ "$(uname)" == "Darwin" ]; then
    NPROC=$(sysctl -n hw.ncpu)
    export DYLD_LIBRARY_PATH=$ARROW_HOME/lib:${DYLD_LIBRARY_PATH:-}
  else
    NPROC=$(nproc)
    export LD_LIBRARY_PATH=$ARROW_HOME/lib:${LD_LIBRARY_PATH:-}
  fi

  pushd $ARROW_SOURCE_DIR

  if [ "${SOURCE_KIND}" = "tarball" ] && [ "${TEST_SOURCE_REPRODUCIBLE}" -gt 0 ]; then
    pushd ..
    git clone "https://github.com/${GITHUB_REPOSITORY}.git" arrow
    pushd arrow
    dev/release/utils-create-release-tarball.sh "${VERSION}" "${RC_NUMBER}"
    tarball="apache-arrow-${VERSION}.tar.gz"
    if ! cmp "${tarball}" "../${tarball}"; then
      echo "Source archive isn't reproducible"
      if ! tar --version | grep --quiet --fixed GNU && \
          ! gtar --version | grep --quiet --fixed GNU; then
        echo "We need GNU tar to verify reproducible build"
      fi
      if ! gzip --version | grep --quiet --fixed GNU; then
        echo "We need GNU gzip to verify reproducible build"
      fi
      return 1
    fi
    popd
    popd
  fi

  if [ ${TEST_CSHARP} -gt 0 ]; then
    test_csharp
  fi
  if [ ${BUILD_CPP} -gt 0 ]; then
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
  if [ ${TEST_WHEELS} -gt 0 ]; then
    test_wheels
  fi
}

test_linux_wheels() {
  local check_gcs=OFF

  if [ "$(uname -m)" = "aarch64" ]; then
    local arch="aarch64"
  else
    local arch="x86_64"
  fi

  local python_versions="${TEST_PYTHON_VERSIONS:-3.9 3.10 3.11 3.12 3.13}"
  local platform_tags="${TEST_WHEEL_PLATFORM_TAGS:-manylinux_2_28_${arch}}"

  if [ "${SOURCE_KIND}" != "local" ]; then
    local wheel_content="OFF"
  fi

  if [ "${SOURCE_KIND}" = "tarball" ]; then
    local check_version="ON"
  else
    local check_version="OFF"
  fi

  for python in ${python_versions}; do
    local pyver=${python/m}
    for platform in ${platform_tags}; do
      show_header "Testing Python ${pyver} wheel for platform ${platform}"
      CONDA_ENV=wheel-${pyver}-${platform} PYTHON_VERSION=${pyver} maybe_setup_conda tzdata
      if ! VENV_ENV=wheel-${pyver}-${platform} PYTHON_VERSION=${pyver} maybe_setup_virtualenv; then
        continue
      fi
      pip install pyarrow-${TEST_PYARROW_VERSION:-${VERSION}}-cp${pyver/.}-cp${python/.}-${platform}.whl
      ARROW_GCS=${check_gcs} \
        ARROW_VERSION=${VERSION} \
        CHECK_VERSION=${check_version} \
        CHECK_WHEEL_CONTENT=${wheel_content:-"ON"} \
        INSTALL_PYARROW=OFF \
        ${ARROW_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_SOURCE_DIR}
    done
  done
}

test_macos_wheels() {
  local check_gcs=OFF
  local check_s3=ON
  local check_flight=ON

  # apple silicon processor
  if [ "$(uname -m)" = "arm64" ]; then
    local python_versions="3.9 3.10 3.11 3.12 3.13"
    local platform_tags="macosx_12_0_arm64"
    local check_flight=OFF
  else
    local python_versions="3.9 3.10 3.11 3.12 3.13"
    local platform_tags="macosx_12_0_x86_64"
  fi

  if [ "${SOURCE_KIND}" != "local" ]; then
    local wheel_content="OFF"
  fi

  if [ "${SOURCE_KIND}" = "tarball" ]; then
    local check_version="ON"
  else
    local check_version="OFF"
  fi

  # verify arch-native wheels inside an arch-native conda environment
  for python in ${python_versions}; do
    local pyver=${python/m}
    for platform in ${platform_tags}; do
      show_header "Testing Python ${pyver} wheel for platform ${platform}"
      if [[ "$platform" == *"10_9"* ]]; then
        check_gcs=OFF
        check_s3=OFF
      fi

      CONDA_ENV=wheel-${pyver}-${platform} PYTHON_VERSION=${pyver} maybe_setup_conda
      if ! VENV_ENV=wheel-${pyver}-${platform} PYTHON_VERSION=${pyver} maybe_setup_virtualenv; then
        continue
      fi

      pip install pyarrow-${VERSION}-cp${pyver/.}-cp${python/.}-${platform}.whl
      ARROW_FLIGHT=${check_flight} \
        ARROW_GCS=${check_gcs} \
        ARROW_S3=${check_s3} \
        ARROW_VERSION=${VERSION} \
        CHECK_WHEEL_CONTENT=${wheel_content:-"ON"} \
        CHECK_VERSION=${check_version} \
        INSTALL_PYARROW=OFF \
        ${ARROW_DIR}/ci/scripts/python_wheel_unix_test.sh ${ARROW_SOURCE_DIR}
    done
  done
}

test_wheels() {
  show_header "Downloading Python wheels"
  maybe_setup_conda python

  local wheels_dir=
  if [ "${SOURCE_KIND}" = "local" ]; then
    wheels_dir="${ARROW_SOURCE_DIR}/python/repaired_wheels"
  else
    local download_dir=${ARROW_TMPDIR}/binaries
    mkdir -p ${download_dir}

    if [ "$(uname)" == "Darwin" ]; then
      local filter_regex=.*macosx.*
    else
      local filter_regex=.*manylinux.*
    fi

    ${PYTHON:-python3} \
      $SOURCE_DIR/download_rc_binaries.py $VERSION $RC_NUMBER \
      --package_type python \
      --regex=${filter_regex} \
      --dest=${download_dir} \
      --repository=${GITHUB_REPOSITORY} \
      --tag="apache-arrow-$VERSION-rc$RC_NUMBER"

    verify_dir_artifact_signatures ${download_dir}

    wheels_dir=${download_dir}
  fi

  pushd ${wheels_dir}

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
: ${TEST_DEFAULT:=1}

# Verification groups
: ${TEST_SOURCE:=${TEST_DEFAULT}}
: ${TEST_BINARIES:=${TEST_DEFAULT}}

# Binary verification tasks
: ${TEST_APT:=${TEST_BINARIES}}
: ${TEST_BINARY:=${TEST_BINARIES}}
: ${TEST_WHEELS:=${TEST_BINARIES}}
: ${TEST_YUM:=${TEST_BINARIES}}

# Source verification tasks
: ${TEST_SOURCE_REPRODUCIBLE:=0}
: ${TEST_CPP:=${TEST_SOURCE}}
: ${TEST_CSHARP:=${TEST_SOURCE}}
: ${TEST_GLIB:=${TEST_SOURCE}}
: ${TEST_RUBY:=${TEST_SOURCE}}
: ${TEST_PYTHON:=${TEST_SOURCE}}
: ${TEST_INTEGRATION:=${TEST_SOURCE}}

# For selective Integration testing, set TEST_DEFAULT=0 TEST_INTEGRATION_X=1 TEST_INTEGRATION_Y=1
: ${TEST_INTEGRATION_CPP:=${TEST_INTEGRATION}}

# Automatically build/test if its activated by a dependent
TEST_GLIB=$((${TEST_GLIB} + ${TEST_RUBY}))
BUILD_CPP=$((${TEST_CPP} + ${TEST_GLIB} + ${TEST_PYTHON} + ${TEST_INTEGRATION_CPP}))
TEST_INTEGRATION=$((${TEST_INTEGRATION} + ${TEST_INTEGRATION_CPP}))

# Execute tests in a conda environment
: ${USE_CONDA:=0}

# Build options for the C++ library
if [ -z "${ARROW_CUDA:-}" ] && detect_cuda; then
  ARROW_CUDA=ON
fi
: ${ARROW_CUDA:=OFF}
: ${ARROW_FLIGHT_SQL:=ON}
: ${ARROW_FLIGHT:=ON}
: ${ARROW_GANDIVA:=ON}
: ${ARROW_GCS:=OFF}
: ${ARROW_S3:=OFF}

TEST_SUCCESS=no

setup_tempdir
ensure_source_directory
test_source_distribution
test_binary_distribution

TEST_SUCCESS=yes

echo "Release candidate ${VERSION}-RC${RC_NUMBER} looks good!"
exit 0

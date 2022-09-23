#!/bin/sh

set -e
set -x

# Build dependencies
export ARROW_HOME=$PREFIX
export PARQUET_HOME=$PREFIX
export SETUPTOOLS_SCM_PRETEND_VERSION=$PKG_VERSION
export PYARROW_BUILD_TYPE=release
export PYARROW_BUNDLE_ARROW_CPP_HEADERS=0
export PYARROW_WITH_DATASET=1
export PYARROW_WITH_FLIGHT=1
if [[ "${target_platform}" == "osx-arm64" ]]; then
    # We need llvm 11+ support in Arrow for this
    export PYARROW_WITH_GANDIVA=0
else
    export PYARROW_WITH_GANDIVA=1
fi
export PYARROW_WITH_GCS=0
export PYARROW_WITH_HDFS=1
export PYARROW_WITH_ORC=1
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_PARQUET_ENCRYPTION=1
export PYARROW_WITH_PLASMA=1
export PYARROW_WITH_S3=1
export PYARROW_CMAKE_GENERATOR=Ninja
export PYARROW_CMAKE_OPTIONS="-DARROW_SIMD_LEVEL=NONE"
BUILD_EXT_FLAGS=""

if [ "$(uname)" == "Linux" ]; then
    export PYARROW_WITH_GCS=1
fi

# Enable CUDA support
if [[ ! -z "${cuda_compiler_version+x}" && "${cuda_compiler_version}" != "None" ]]; then
    export PYARROW_WITH_CUDA=1
else
    export PYARROW_WITH_CUDA=0
fi

# Resolve: Make Error at cmake_modules/SetupCxxFlags.cmake:338 (message): Unsupported arch flag: -march=.
if [[ "${target_platform}" == "linux-aarch64" ]]; then
    export PYARROW_CMAKE_OPTIONS="-DARROW_ARMV8_ARCH=armv8-a ${PYARROW_CMAKE_OPTIONS}"
fi

# See https://conda-forge.org/docs/maintainer/knowledge_base.html#newer-c-features-with-old-sdk
export CXXFLAGS="${CXXFLAGS} -D_LIBCPP_DISABLE_AVAILABILITY"

cd python

$PYTHON setup.py \
        build_ext \
        install --single-version-externally-managed \
                --record=record.txt

if [[ "$PKG_NAME" == "pyarrow" ]]; then
    rm -r ${SP_DIR}/pyarrow/tests
fi

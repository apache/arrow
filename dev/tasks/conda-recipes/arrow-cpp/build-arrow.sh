#!/usr/bin/env bash

set -e
set -x

mkdir cpp/build
pushd cpp/build

EXTRA_CMAKE_ARGS=""
ARROW_GCS="OFF"

if [ "$(uname)" == "Linux" ]; then
  # Include g++'s system headers
  SYSTEM_INCLUDES=$(echo | ${CXX} -E -Wp,-v -xc++ - 2>&1 | grep '^ ' | awk '{print "-isystem;" substr($1, 1)}' | tr '\n' ';')
  EXTRA_CMAKE_ARGS=" -DARROW_GANDIVA_PC_CXX_FLAGS=${SYSTEM_INCLUDES}"
  # GCS doesn't produce any abseil-induced linker error on Linux, enable it
  ARROW_GCS="ON"
fi

# Enable CUDA support
if [[ ! -z "${cuda_compiler_version+x}" && "${cuda_compiler_version}" != "None" ]]
then
    if [[ -z "${CUDA_HOME+x}" ]]
    then
        echo "cuda_compiler_version=${cuda_compiler_version} CUDA_HOME=$CUDA_HOME"
        CUDA_GDB_EXECUTABLE=$(which cuda-gdb || exit 0)
        if [[ -n "$CUDA_GDB_EXECUTABLE" ]]
        then
            CUDA_HOME=$(dirname $(dirname $CUDA_GDB_EXECUTABLE))
        else
            echo "Cannot determine CUDA_HOME: cuda-gdb not in PATH"
            return 1
        fi
    fi
    EXTRA_CMAKE_ARGS=" ${EXTRA_CMAKE_ARGS} -DARROW_CUDA=ON -DCUDA_TOOLKIT_ROOT_DIR=${CUDA_HOME} -DCMAKE_LIBRARY_PATH=${CUDA_HOME}/lib64/stubs"
else
    EXTRA_CMAKE_ARGS=" ${EXTRA_CMAKE_ARGS} -DARROW_CUDA=OFF"
fi

if [[ "${target_platform}" == "osx-arm64" ]]; then
    # We need llvm 11+ support in Arrow for this
    # Tell jemalloc to support 16K page size on apple arm64 silicon
    EXTRA_CMAKE_ARGS=" ${EXTRA_CMAKE_ARGS} -DARROW_GANDIVA=OFF -DARROW_JEMALLOC_LG_PAGE=14"
    sed -ie "s;protoc-gen-grpc.*$;protoc-gen-grpc=${BUILD_PREFIX}/bin/grpc_cpp_plugin\";g" ../src/arrow/flight/CMakeLists.txt
elif [[ "${target_platform}" == "linux-aarch64" ]]; then
    # Tell jemalloc to support both 4k and 64k page arm64 systems
    # See https://github.com/apache/arrow/pull/10940
    EXTRA_CMAKE_ARGS=" ${EXTRA_CMAKE_ARGS} -DARROW_GANDIVA=ON -DARROW_JEMALLOC_LG_PAGE=16"
else
    EXTRA_CMAKE_ARGS=" ${EXTRA_CMAKE_ARGS} -DARROW_GANDIVA=ON"
fi

# See https://conda-forge.org/docs/maintainer/knowledge_base.html#newer-c-features-with-old-sdk
CXXFLAGS="${CXXFLAGS} -D_LIBCPP_DISABLE_AVAILABILITY"
ARROW_GANDIVA_PC_CXX_FLAGS="-D_LIBCPP_DISABLE_AVAILABILITY"

cmake \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_STATIC=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DARROW_CXXFLAGS="${CXXFLAGS}" \
    -DARROW_GANDIVA_PC_CXX_FLAGS="${ARROW_GANDIVA_PC_CXX_FLAGS}" \
    -DARROW_DATASET=ON \
    -DARROW_DEPENDENCY_SOURCE=SYSTEM \
    -DARROW_FLIGHT=ON \
    -DARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS=ON \
    -DARROW_GCS=${ARROW_GCS} \
    -DARROW_HDFS=ON \
    -DARROW_JEMALLOC=ON \
    -DARROW_MIMALLOC=ON \
    -DARROW_ORC=ON \
    -DARROW_PACKAGE_PREFIX=$PREFIX \
    -DARROW_PARQUET=ON \
    -DPARQUET_REQUIRE_ENCRYPTION=ON \
    -DARROW_PLASMA=ON \
    -DARROW_PYTHON=ON \
    -DARROW_S3=ON \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_USE_LD_GOLD=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DLLVM_TOOLS_BINARY_DIR=$PREFIX/bin \
    -DPython3_EXECUTABLE=${PYTHON} \
    -DProtobuf_PROTOC_EXECUTABLE=$BUILD_PREFIX/bin/protoc \
    -GNinja \
    ${EXTRA_CMAKE_ARGS} \
    ..

# Commented out until jemalloc and mimalloc are fixed upstream
if [[ "${target_platform}" == "osx-arm64" ]]; then
     ninja jemalloc_ep-prefix/src/jemalloc_ep-stamp/jemalloc_ep-patch mimalloc_ep-prefix/src/mimalloc_ep-stamp/mimalloc_ep-patch
     cp $BUILD_PREFIX/share/gnuconfig/config.* jemalloc_ep-prefix/src/jemalloc_ep/build-aux/
     sed -ie 's/list(APPEND mi_cflags -march=native)//g' mimalloc_ep-prefix/src/mimalloc_ep/CMakeLists.txt
     # Use the correct register for thread-local storage
     sed -ie 's/tpidr_el0/tpidrro_el0/g' mimalloc_ep-prefix/src/mimalloc_ep/include/mimalloc-internal.h
fi

ninja install

popd

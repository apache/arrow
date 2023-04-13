#!/bin/bash
set -ex

# Copy the [de]activate scripts to $PREFIX/etc/conda/[de]activate.d, see
# https://conda-forge.org/docs/maintainer/adding_pkgs.html#activate-scripts
for CHANGE in "activate"
do
    mkdir -p "${PREFIX}/etc/conda/${CHANGE}.d"
    cp "${RECIPE_DIR}/${CHANGE}.sh" "${PREFIX}/etc/conda/${CHANGE}.d/${PKG_NAME}_${CHANGE}.sh"
done

mkdir cpp/build
pushd cpp/build

EXTRA_CMAKE_ARGS=""

# Include g++'s system headers
if [ "$(uname)" == "Linux" ]; then
  SYSTEM_INCLUDES=$(echo | ${CXX} -E -Wp,-v -xc++ - 2>&1 | grep '^ ' | awk '{print "-isystem;" substr($1, 1)}' | tr '\n' ';')
  ARROW_GANDIVA_PC_CXX_FLAGS="${SYSTEM_INCLUDES}"
  # only available on linux
  ARROW_WITH_UCX=ON
else
  # See https://conda-forge.org/docs/maintainer/knowledge_base.html#newer-c-features-with-old-sdk
  CXXFLAGS="${CXXFLAGS} -D_LIBCPP_DISABLE_AVAILABILITY"
  ARROW_GANDIVA_PC_CXX_FLAGS="-D_LIBCPP_DISABLE_AVAILABILITY"
  ARROW_WITH_UCX=OFF
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
    EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS} -DCLANG_EXECUTABLE=${BUILD_PREFIX}/bin/clang -DLLVM_LINK_EXECUTABLE=${BUILD_PREFIX}/bin/llvm-link"
    sed -ie "s;protoc-gen-grpc.*$;protoc-gen-grpc=${BUILD_PREFIX}/bin/grpc_cpp_plugin\";g" ../src/arrow/flight/CMakeLists.txt
    sed -ie 's;"--with-jemalloc-prefix\=je_arrow_";"--with-jemalloc-prefix\=je_arrow_" "--with-lg-page\=14";g' ../cmake_modules/ThirdpartyToolchain.cmake
fi

# disable -fno-plt, which causes problems with GCC on PPC
if [[ "$target_platform" == "linux-ppc64le" ]]; then
  CFLAGS="$(echo $CFLAGS | sed 's/-fno-plt //g')"
  CXXFLAGS="$(echo $CXXFLAGS | sed 's/-fno-plt //g')"
fi

# Limit number of threads used to avoid hardware oversubscription
if [[ "${target_platform}" == "linux-aarch64" ]] || [[ "${target_platform}" == "linux-ppc64le" ]]; then
     export CMAKE_BUILD_PARALLEL_LEVEL=3
fi

# point to a usable protoc if we're running on a different architecture than the target
if [[ "${build_platform}" != "${target_platform}" ]]; then
    EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS} -DProtobuf_PROTOC_EXECUTABLE=$BUILD_PREFIX/bin/protoc"
fi

# reusable variable for dependencies we cannot yet unvendor
export READ_RECIPE_META_YAML_WHY_NOT=OFF

# for available switches see
# https://github.com/apache/arrow/blame/apache-arrow-11.0.0/cpp/cmake_modules/DefineOptions.cmake
# placeholder in ARROW_GDB_INSTALL_DIR must match what's used for replacement in activate.sh
cmake -GNinja \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_STATIC=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_CXXFLAGS="${CXXFLAGS}" \
    -DARROW_DATASET=ON \
    -DARROW_DEPENDENCY_SOURCE=SYSTEM \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=ON \
    -DARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS=ON \
    -DARROW_FLIGHT_SQL=ON \
    -DARROW_GANDIVA=ON \
    -DARROW_GANDIVA_PC_CXX_FLAGS="${ARROW_GANDIVA_PC_CXX_FLAGS}" \
    -DARROW_GCS=ON \
    -DARROW_GDB_INSTALL_DIR=replace_this_section_with_absolute_slashed_path_to_CONDA_PREFIX/lib \
    -DARROW_HDFS=ON \
    -DARROW_JEMALLOC=ON \
    -DARROW_JSON=ON \
    -DARROW_MIMALLOC=ON \
    -DARROW_ORC=ON \
    -DARROW_PACKAGE_PREFIX=$PREFIX \
    -DARROW_PARQUET=ON \
    -DARROW_S3=ON \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_SUBSTRAIT=ON \
    -DARROW_USE_GLOG=ON \
    -DARROW_USE_LD_GOLD=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_NLOHMANN_JSON=ON \
    -DARROW_WITH_OPENTELEMETRY=${READ_RECIPE_META_YAML_WHY_NOT} \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_UCX=${ARROW_WITH_UCX} \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DLLVM_TOOLS_BINARY_DIR=$PREFIX/bin \
    -DPARQUET_REQUIRE_ENCRYPTION=ON \
    -DPython3_EXECUTABLE=${PYTHON} \
    ${EXTRA_CMAKE_ARGS} \
    ..

cmake --build . --target install --config Release

popd

# -DARROW_USE_ASAN=OFF \
# -DARROW_USE_UBSAN=OFF \
# -DARROW_USE_TSAN=OFF \

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
ARROW_DIR=${SCRIPT_DIR}/..
export ARROW_BUILD_TYPE=debug
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_TEST_DATA=${ARROW_DIR}/cpp/submodules/parquet-testing/data
export ARROW_TEST_DATA=${ARROW_DIR}/testing/data

export ARROW_HDFS_TEST_HOST=impala
export ARROW_HDFS_TEST_PORT=8020
export ARROW_HDFS_TEST_USER=hdfs

mkdir -p ${ARROW_DIR}/cpp/build
pushd ${ARROW_DIR}/cpp/build

cmake -GNinja \
      -DARROW_BUILD_BENCHMARKS=OFF \
      -DARROW_BUILD_STATIC=OFF \
      -DARROW_BUILD_TESTS=ON \
      -DARROW_DATASET=ON \
      -DARROW_EXTRA_ERROR_CONTEXT=ON \
      -DARROW_BUILD_INTEGRATION=ON \
      -DARROW_DEPENDENCY_SOURCE=CONDA \
      -DARROW_FLIGHT=ON \
      -DARROW_GANDIVA=OFF \
      -DARROW_JEMALLOC=ON \
      -DARROW_MIMALLOC=ON \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_COMPUTE=ON \
      -DARROW_PARQUET=ON \
      -DARROW_CSV=ON \
      -DARROW_ORC=OFF \
      -DARROW_USE_CCACHE=ON \
      -DARROW_PLASMA=OFF \
      -DARROW_S3=ON \
      -DARROW_TEST_MEMCHECK=OFF \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=YES \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      ..

ninja
ninja install

popd

export PYARROW_CMAKE_GENERATOR=Ninja
export PYARROW_BUILD_TYPE=debug
export PYARROW_WITH_PARQUET=1
# export PYARROW_WITH_PLASMA=1
# export PYARROW_WITH_HDFS=1
# export PYARROW_WITH_GANDIVA=0
export PYARROW_WITH_DATASET=1
# export PYARROW_WITH_FLIGHT=1
export PYARROW_WITH_S3=1
export PYARROW_PARALLEL=8
# export PYARROW_WITH_ORC=1

# # export DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/12.0.0/lib/darwin/libclang_rt.asan_osx_dynamic.dylib
# # export DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/12.0.0/lib/darwin/libclang_rt.tsan_osx_dynamic.dylib

pushd ${ARROW_DIR}/python
python setup.py build_ext --inplace
# python setup.py develop
popd
# pytest -sv "$@"

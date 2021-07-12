export ARROW_BUILD_TYPE=debug
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_TEST_DATA=`pwd`/../cpp/submodules/parquet-testing/data
export ARROW_TEST_DATA=`pwd`/../testing/data

export ARROW_HDFS_TEST_HOST=impala
export ARROW_HDFS_TEST_PORT=8020
export ARROW_HDFS_TEST_USER=hdfs

mkdir -p ../cpp/build
pushd ../cpp/build

# CXXFLAGS: Shrink test runtime by enabling minimal optimizations
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DARROW_BUILD_BENCHMARKS=OFF \
      -DARROW_S3=OFF \
      -DARROW_HDFS=OFF \
      -DARROW_PYTHON=ON \
      -DARROW_PLASMA=OFF \
      -DARROW_PARQUET=ON \
      -DARROW_DATASET=OFF \
      -DARROW_USE_ASAN=OFF \
      -DARROW_USE_UBSAN=OFF \
      -DARROW_USE_TSAN=OFF \
      -DARROW_GANDIVA=OFF \
      -DARROW_JEMALLOC=ON \
      -DARROW_BUILD_STATIC=OFF \
      -DARROW_ORC=OFF \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_WITH_ZLIB=ON \
      -DARROW_WITH_BROTLI=ON \
      -DGTest_SOURCE=BUNDLED \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_WITH_BZ2=ON \
      -DARROW_FLIGHT=OFF \
      -DARROW_EXTRA_ERROR_CONTEXT=ON \
      -DARROW_BUILD_TESTS=ON \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=YES \
      -DARROW_TEST_MEMCHECK=OFF \
      ..

ninja
# ninja test
ninja install

popd

export PYARROW_CMAKE_GENERATOR=Ninja
export PYARROW_BUILD_TYPE=debug
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_PLASMA=0
export PYARROW_WITH_HDFS=0
export PYARROW_WITH_GANDIVA=0
export PYARROW_WITH_DATASET=0
export PYARROW_WITH_FLIGHT=0
export PYARROW_WITH_S3=0
export PYARROW_WITH_ORC=0

# export DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/12.0.0/lib/darwin/libclang_rt.asan_osx_dynamic.dylib
# export DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/12.0.0/lib/darwin/libclang_rt.tsan_osx_dynamic.dylib

python setup.py develop

pytest -sv "$@"

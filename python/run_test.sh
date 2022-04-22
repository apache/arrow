# -DARROW_USE_ASAN=OFF \
# -DARROW_USE_UBSAN=OFF \
# -DARROW_USE_TSAN=OFF \

export ARROW_BUILD_TYPE=debug
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_TEST_DATA=`pwd`/../cpp/submodules/parquet-testing/data
export ARROW_TEST_DATA=`pwd`/../testing/data

export ARROW_HDFS_TEST_HOST=impala
export ARROW_HDFS_TEST_PORT=8020
export ARROW_HDFS_TEST_USER=hdfs

mkdir -p ../cpp/build
pushd ../cpp/build

cmake -GNinja \
      -DARROW_BUILD_BENCHMARKS=OFF \
      -DARROW_BUILD_STATIC=ON \
      -DARROW_BUILD_TESTS=ON \
      -DARROW_DATASET=OFF \
      -DARROW_DEPENDENCY_SOURCE=AUTO \
      -DARROW_EXTRA_ERROR_CONTEXT=ON \
      -DARROW_BUILD_INTEGRATION=ON \
      -DARROW_DEPENDENCY_SOURCE=CONDA \
      -DARROW_FLIGHT=ON \
      -DARROW_GANDIVA=OFF \
      -DARROW_JEMALLOC=ON \
      -DARROW_MIMALLOC=ON \
      -DARROW_PARQUET=OFF \
      -DARROW_ORC=OFF \
      -DARROW_PARQUET=ON \
      -DARROW_USE_CCACHE=ON \
      -DARROW_PLASMA=OFF \
      -DARROW_PYTHON=OFF \
      -DARROW_S3=OFF \
      -DARROW_TEST_MEMCHECK=OFF \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=YES \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      ..

ninja
ninja install

popd

# export PYARROW_CMAKE_GENERATOR=Ninja
# export PYARROW_BUILD_TYPE=debug
# export PYARROW_WITH_PARQUET=1
# export PYARROW_WITH_PLASMA=1
# export PYARROW_WITH_HDFS=1
# export PYARROW_WITH_GANDIVA=0
# export PYARROW_WITH_DATASET=1
# export PYARROW_WITH_FLIGHT=1
# export PYARROW_WITH_S3=1
# export PYARROW_WITH_ORC=1

# # export DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/12.0.0/lib/darwin/libclang_rt.asan_osx_dynamic.dylib
# # export DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/12.0.0/lib/darwin/libclang_rt.tsan_osx_dynamic.dylib

# python setup.py develop

# pytest -sv "$@"

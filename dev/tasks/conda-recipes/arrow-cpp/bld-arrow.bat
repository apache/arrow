@echo on

mkdir "%SRC_DIR%"\cpp\build
pushd "%SRC_DIR%"\cpp\build

:: Enable CUDA support
if "%cuda_compiler_version%"=="None" (
    set "EXTRA_CMAKE_ARGS=-DARROW_CUDA=OFF"
) else (
    REM this should move to nvcc-feedstock
    set "CUDA_PATH=%CUDA_PATH:\=/%"
    set "CUDA_HOME=%CUDA_HOME:\=/%"

    set "EXTRA_CMAKE_ARGS=-DARROW_CUDA=ON"
)

cmake -G "Ninja" ^
      -DBUILD_SHARED_LIBS=ON ^
      -DCMAKE_INSTALL_PREFIX="%LIBRARY_PREFIX%" ^
      -DARROW_DEPENDENCY_SOURCE=SYSTEM ^
      -DARROW_PACKAGE_PREFIX="%LIBRARY_PREFIX%" ^
      -DLLVM_TOOLS_BINARY_DIR="%LIBRARY_BIN%" ^
      -DPython3_EXECUTABLE="%PYTHON%" ^
      -DARROW_WITH_BZ2:BOOL=ON ^
      -DARROW_WITH_ZLIB:BOOL=ON ^
      -DARROW_WITH_ZSTD:BOOL=ON ^
      -DARROW_WITH_LZ4:BOOL=ON ^
      -DARROW_WITH_SNAPPY:BOOL=ON ^
      -DARROW_WITH_BROTLI:BOOL=ON ^
      -DARROW_BOOST_USE_SHARED:BOOL=ON ^
      -DARROW_BUILD_TESTS:BOOL=OFF ^
      -DARROW_BUILD_UTILITIES:BOOL=OFF ^
      -DARROW_BUILD_STATIC:BOOL=OFF ^
      -DCMAKE_BUILD_TYPE=release ^
      -DARROW_SIMD_LEVEL=NONE ^
      -DARROW_PYTHON:BOOL=ON ^
      -DARROW_MIMALLOC:BOOL=ON ^
      -DARROW_DATASET:BOOL=ON ^
      -DARROW_FLIGHT:BOOL=ON ^
      -DARROW_FLIGHT_REQUIRE_TLSCREDENTIALSOPTIONS:BOOL=OFF ^
      -DARROW_HDFS:BOOL=ON ^
      -DARROW_GCS:BOOL=OFF ^
      -DARROW_PARQUET:BOOL=ON ^
      -DPARQUET_REQUIRE_ENCRYPTION:BOOL=ON ^
      -DARROW_GANDIVA:BOOL=ON ^
      -DARROW_ORC:BOOL=ON ^
      -DARROW_S3:BOOL=ON ^
      -DBoost_NO_BOOST_CMAKE=ON ^
      -DCMAKE_UNITY_BUILD=ON ^
      %EXTRA_CMAKE_ARGS% ^
      ..
if errorlevel 1 exit 1

cmake --build . --target install --config Release
if errorlevel 1 exit 1

popd

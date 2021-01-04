echo "Building windows wheel..."

call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvars64.bat"

echo "=== (%PYTHON_VERSION%) Clear output directories and leftovers ==="
del /s /q C:\arrow-build
del /s /q C:\arrow-dist
del /s /q C:\arrow\python\dist
del /s /q C:\arrow\python\build
del /s /q C:\arrow\python\pyarrow\*.so
del /s /q C:\arrow\python\pyarrow\*.so.*

echo "=== (%PYTHON_VERSION%) Building Arrow C++ libraries ==="
set ARROW_DATASET=ON
set ARROW_FLIGHT=ON
set ARROW_GANDIVA=OFF
set ARROW_HDFS=ON
set ARROW_ORC=OFF
set ARROW_PARQUET=ON
set ARROW_MIMALLOC=ON
set ARROW_S3=ON
set ARROW_TENSORFLOW=ON
set ARROW_WITH_BROTLI=ON
set ARROW_WITH_BZ2=ON
set ARROW_WITH_LZ4=ON
set ARROW_WITH_SNAPPY=ON
set ARROW_WITH_ZLIB=ON
set ARROW_WITH_ZSTD=ON
set CMAKE_BUILD_TYPE=Release
set CMAKE_GENERATOR=Visual Studio 15 2017 Win64

mkdir C:\arrow-build
pushd C:\arrow-build
cmake ^
    -DARROW_BUILD_SHARED=ON ^
    -DARROW_BUILD_STATIC=OFF ^
    -DARROW_BUILD_TESTS=OFF ^
    -DARROW_CXXFLAGS="/MP" ^
    -DARROW_DATASET=%ARROW_DATASET% ^
    -DARROW_DEPENDENCY_SOURCE=SYSTEM ^
    -DARROW_DEPENDENCY_USE_SHARED=OFF ^
    -DARROW_FLIGHT=%ARROW_FLIGHT% ^
    -DARROW_GANDIVA=%ARROW_GANDIVA% ^
    -DARROW_HDFS=%ARROW_HDFS% ^
    -DARROW_MIMALLOC=%ARROW_MIMALLOC% ^
    -DARROW_ORC=%ARROW_ORC% ^
    -DARROW_PACKAGE_KIND="wheel-windows" ^
    -DARROW_PARQUET=%ARROW_PARQUET% ^
    -DARROW_PYTHON=ON ^
    -DARROW_S3=%ARROW_S3% ^
    -DARROW_TENSORFLOW=%ARROW_TENSORFLOW% ^
    -DARROW_WITH_BROTLI=%ARROW_WITH_BROTLI% ^
    -DARROW_WITH_BZ2=%ARROW_WITH_BZ2% ^
    -DARROW_WITH_LZ4=%ARROW_WITH_LZ4% ^
    -DARROW_WITH_SNAPPY=%ARROW_WITH_SNAPPY% ^
    -DARROW_WITH_ZLIB=%ARROW_WITH_ZLIB% ^
    -DARROW_WITH_ZSTD=%ARROW_WITH_ZSTD% ^
    -DCMAKE_BUILD_TYPE=%CMAKE_BUILD_TYPE% ^
    -DLZ4_MSVC_LIB_PREFIX="" ^
    -DLZ4_MSVC_STATIC_LIB_SUFFIX="" ^
    -DZSTD_MSVC_LIB_PREFIX="" ^
    -DCMAKE_CXX_COMPILER=clcache ^
    -DCMAKE_INSTALL_PREFIX=C:\arrow-dist ^
    -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake ^
    -DCMAKE_UNITY_BUILD=OFF ^
    -DMSVC_LINK_VERBOSE=ON ^
    -DVCPKG_TARGET_TRIPLET=x64-windows-static-md ^
    -G "%CMAKE_GENERATOR%" ^
    C:\arrow\cpp || exit /B
cmake --build . --config Release --target install || exit /B
popd

echo "=== (%PYTHON_VERSION%) Building wheel ==="
set PYARROW_BUILD_TYPE=%CMAKE_BUILD_TYPE%
set PYARROW_BUNDLE_ARROW_CPP=ON
set PYARROW_BUNDLE_BOOST=OFF
set PYARROW_CMAKE_GENERATOR=%CMAKE_GENERATOR%
set PYARROW_INSTALL_TESTS=ON
set PYARROW_WITH_DATASET=%ARROW_DATASET%
set PYARROW_WITH_FLIGHT=%ARROW_FLIGHT%
set PYARROW_WITH_GANDIVA=%ARROW_GANDIVA%
set PYARROW_WITH_HDFS=%ARROW_HDFS%
set PYARROW_WITH_ORC=%ARROW_ORC%
set PYARROW_WITH_PARQUET=%ARROW_PARQUET%
set PYARROW_WITH_S3=%ARROW_S3%
set ARROW_HOME=C:\arrow-dist

pushd C:\arrow\python
python setup.py bdist_wheel || exit /B
popd

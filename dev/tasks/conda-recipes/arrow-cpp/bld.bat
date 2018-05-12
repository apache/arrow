mkdir "%SRC_DIR%"\cpp\build
pushd "%SRC_DIR%"\cpp\build

set ARROW_BUILD_TOOLCHAIN=%LIBRARY_PREFIX%

cmake -G "%CMAKE_GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX="%LIBRARY_PREFIX%" ^
      -DARROW_BOOST_USE_SHARED:BOOL=ON ^
      -DARROW_BUILD_TESTS:BOOL=OFF ^
      -DARROW_BUILD_UTILITIES:BOOL=OFF ^
      -DCMAKE_BUILD_TYPE=release ^
      -DARROW_PYTHON:BOOL=ON ^
      -DARROW_ORC:BOOL=ON ^
      ..

cmake --build . --target INSTALL --config Release

popd

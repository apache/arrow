pushd "%SRC_DIR%"\python

@rem the symlinks for cmake modules don't work here
del cmake_modules\BuildUtils.cmake
del cmake_modules\SetupCxxFlags.cmake
del cmake_modules\CompilerInfo.cmake
del cmake_modules\FindNumPy.cmake
del cmake_modules\FindPythonLibsNew.cmake
copy /Y "%SRC_DIR%\cpp\cmake_modules\BuildUtils.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\SetupCxxFlags.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\CompilerInfo.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\FindNumPy.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\FindPythonLibsNew.cmake" cmake_modules\

SET ARROW_HOME=%LIBRARY_PREFIX%
SET SETUPTOOLS_SCM_PRETEND_VERSION=%PKG_VERSION%
"%PYTHON%" setup.py ^
           build_ext --build-type=release ^
                     --with-parquet ^
                     --with-gandiva ^
           install --single-version-externally-managed ^
                   --record=record.txt
if errorlevel 1 exit 1
popd

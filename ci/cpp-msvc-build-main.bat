@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem   http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing,
@rem software distributed under the License is distributed on an
@rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem KIND, either express or implied.  See the License for the
@rem specific language governing permissions and limitations
@rem under the License.

@rem The "main" C++ build script for Windows CI
@rem (i.e. for usual configurations)

set ARROW_HOME=%CONDA_PREFIX%\Library
set CMAKE_ARGS=-DARROW_VERBOSE_THIRDPARTY_BUILD=OFF

if "%JOB%" == "Toolchain" (
  @rem Toolchain gtest does not currently work with Visual Studio 2015
  set CMAKE_ARGS=^
      %CMAKE_ARGS% ^
      -DARROW_WITH_BZ2=ON ^
      -DARROW_DEPENDENCY_SOURCE=CONDA ^
      -DGTest_SOURCE=BUNDLED
) else (
  @rem We're in a conda enviroment but don't want to use it for the dependencies
  set CMAKE_ARGS=%CMAKE_ARGS% -DARROW_DEPENDENCY_SOURCE=AUTO
)

@rem Retrieve git submodules, configure env var for Parquet unit tests
git submodule update --init || exit /B
set ARROW_TEST_DATA=%CD%\testing\data
set PARQUET_TEST_DATA=%CD%\cpp\submodules\parquet-testing\data

@rem Enable warnings-as-errors
set ARROW_CXXFLAGS=/WX /MP

@rem In release mode, disable optimizations (/Od) for faster compiling
@rem and enable runtime assertions
set CMAKE_CXX_FLAGS_RELEASE=/Od /UNDEBUG

@rem
@rem Build and test Arrow C++ libraries (including Parquet)
@rem

mkdir cpp\build
pushd cpp\build

cmake -G "%GENERATOR%" %CMAKE_ARGS% ^
      -DCMAKE_VERBOSE_MAKEFILE=OFF ^
      -DCMAKE_INSTALL_PREFIX=%CONDA_PREFIX%\Library ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_BUILD_STATIC=OFF ^
      -DARROW_BUILD_TESTS=ON ^
      -DARROW_BUILD_EXAMPLES=ON ^
      -DARROW_BUILD_EXAMPLES=ON ^
      -DARROW_VERBOSE_THIRDPARTY_BUILD=ON ^
      -DARROW_CXXFLAGS="%ARROW_CXXFLAGS%" ^
      -DCMAKE_CXX_FLAGS_RELEASE="/MD %CMAKE_CXX_FLAGS_RELEASE%" ^
      -DARROW_GANDIVA=%ARROW_BUILD_GANDIVA% ^
      -DARROW_PARQUET=ON ^
      -DPARQUET_BUILD_EXECUTABLES=ON ^
      -DARROW_PYTHON=ON ^
      ..  || exit /B
cmake --build . --target install --config %CONFIGURATION%  || exit /B

@rem Needed so arrow-python-test.exe works
set OLD_PYTHONHOME=%PYTHONHOME%
set PYTHONHOME=%CONDA_PREFIX%

ctest --output-on-failure -j2 || exit /B

set PYTHONHOME=%OLD_PYTHONHOME%
popd

@rem
@rem Build and install pyarrow
@rem

pushd python

pip install -r requirements.txt pickle5

set PYARROW_CXXFLAGS=%ARROW_CXXFLAGS%
set PYARROW_CMAKE_GENERATOR=%GENERATOR%
set PYARROW_BUNDLE_ARROW_CPP=ON
set PYARROW_BUNDLE_BOOST=OFF
set PYARROW_WITH_STATIC_BOOST=ON
set PYARROW_WITH_PARQUET=ON
set PYARROW_WITH_GANDIVA=%ARROW_BUILD_GANDIVA%
set PYARROW_PARALLEL=2

@rem ARROW-3075; pkgconfig is broken for Parquet for now
set PARQUET_HOME=%CONDA_PREFIX%\Library

python setup.py build_ext ^
    install -q --single-version-externally-managed --record=record.text ^
    bdist_wheel -q || exit /B

for /F %%i in ('dir /B /S dist\*.whl') do set WHEEL_PATH=%%i

@rem Test directly from installed location
@rem (needed for test_cython)

set PYARROW_PATH=%CONDA_PREFIX%\Lib\site-packages\pyarrow
py.test -r sxX --durations=15 %PYARROW_PATH% || exit /B

popd

@rem
@rem Test pyarrow wheel from pristine environment
@rem

call deactivate

conda create -n wheel_test -q -y python=%PYTHON% || exit /B

call activate wheel_test

pip install %WHEEL_PATH% || exit /B

python -c "import pyarrow" || exit /B
python -c "import pyarrow.parquet" || exit /B

pip install pandas pickle5 pytest pytest-faulthandler hypothesis || exit /B

py.test -r sxX --durations=15 --pyargs pyarrow.tests || exit /B

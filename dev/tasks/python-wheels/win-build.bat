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

@echo on

@rem Building Gandiva in the wheels is disabled for now to make the wheels
@rem smaller.

@rem --file=%ARROW_SRC%\ci\conda_env_gandiva.yml ^

@rem create conda environment for compiling
call conda create -n wheel-build -q -y -c conda-forge ^
    --file=%ARROW_SRC%\ci\conda_env_cpp.yml ^
    "vs2015_runtime<14.16" ^
    python=%PYTHON_VERSION% || exit /B

call conda.bat activate wheel-build

@rem Cannot use conda_env_python.yml here because conda-forge has
@rem ceased providing up-to-date packages for Python 3.5
pip install -r %ARROW_SRC%\python\requirements-wheel-build.txt

set ARROW_HOME=%CONDA_PREFIX%\Library
set PARQUET_HOME=%CONDA_PREFIX%\Library
echo %ARROW_HOME%

@rem Build Arrow C++ libraries
mkdir %ARROW_SRC%\cpp\build
pushd %ARROW_SRC%\cpp\build

@rem ARROW-6938(wesm): bz2 is disabled on Windows because the build
@rem currently selects the shared lib for linking. Using the zstd lib from
@rem conda-forge also results in a broken build so we use the BUNDLED
@rem dependency resolution strategy for now

cmake -G "%GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DARROW_BUILD_STATIC=OFF ^
      -DARROW_BUILD_TESTS=OFF ^
      -DCMAKE_BUILD_TYPE=Release ^
      -DARROW_DEPENDENCY_SOURCE=CONDA ^
      -DOPENSSL_ROOT_DIR=%CONDA_PREFIX%/Library ^
      -DARROW_CXXFLAGS="/MP" ^
      -DARROW_WITH_BZ2=OFF ^
      -DARROW_WITH_ZLIB=ON ^
      -DARROW_WITH_ZSTD=ON ^
      -DARROW_WITH_LZ4=ON ^
      -DARROW_WITH_SNAPPY=ON ^
      -DARROW_WITH_BROTLI=ON ^
      -DARROW_DATASET=ON ^
      -DARROW_FLIGHT=ON ^
      -DARROW_GANDIVA=OFF ^
      -DARROW_MIMALLOC=ON ^
      -DARROW_PARQUET=ON ^
      -DARROW_PYTHON=ON ^
      -DARROW_VERBOSE_THIRDPARTY_BUILD=ON ^
      -DBrotli_SOURCE=BUNDLED ^
      -DZSTD_SOURCE=BUNDLED ^
      -Dutf8proc_SOURCE=BUNDLED ^
      .. || exit /B
cmake --build . --target install --config Release || exit /B
popd

set PYARROW_BUILD_TYPE=Release
set PYARROW_PARALLEL=8
set PYARROW_INSTALL_TESTS=1
set PYARROW_WITH_DATASET=1
set PYARROW_WITH_FLIGHT=1
set PYARROW_WITH_GANDIVA=0
set PYARROW_WITH_PARQUET=1
set PYARROW_WITH_STATIC_BOOST=1
set PYARROW_BUNDLE_ARROW_CPP=1
set SETUPTOOLS_SCM_PRETEND_VERSION=%PYARROW_VERSION%

pushd %ARROW_SRC%\python
python setup.py bdist_wheel || exit /B
popd

call conda.bat deactivate

set ARROW_TEST_DATA=%ARROW_SRC%\testing\data

@rem install the test dependencies
%PYTHON_INTERPRETER% -m pip install -r %ARROW_SRC%\python\requirements-wheel-test.txt || exit /B

@rem install the produced wheel in a non-conda environment
%PYTHON_INTERPRETER% -m pip install --no-index --find-links=%ARROW_SRC%\python\dist\ pyarrow || exit /B

@rem test the imports
%PYTHON_INTERPRETER% -c "import pyarrow" || exit /B
%PYTHON_INTERPRETER% -c "import pyarrow.parquet" || exit /B
%PYTHON_INTERPRETER% -c "import pyarrow.flight" || exit /B
%PYTHON_INTERPRETER% -c "import pyarrow.dataset" || exit /B

@rem run the python tests, but disable the cython because there is a linking
@rem issue on python 3.8
set PYARROW_TEST_CYTHON=OFF
%PYTHON_INTERPRETER% -m pytest -rs --pyargs pyarrow || exit /B

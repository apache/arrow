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

@rem create conda environment for compiling
call conda update --yes --quiet conda

call conda create -n wheel-build -q -y -c conda-forge ^
    "boost-cpp>=1.68.0" ^
    "python=3.5" ^
    zlib || exit /B

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

cmake -G "%GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=Release ^
      -DARROW_DEPENDENCY_SOURCE=BUNDLED ^
      -DZLIB_SOURCE=SYSTEM ^
      -DBOOST_SOURCE=SYSTEM ^
      -DZLIB_ROOT=%CONDA_PREFIX%\Library ^
      -DARROW_CXXFLAGS="/MP" ^
      -DARROW_WITH_ZLIB=ON ^
      -DARROW_WITH_ZSTD=ON ^
      -DARROW_WITH_LZ4=ON ^
      -DARROW_WITH_SNAPPY=ON ^
      -DARROW_WITH_BROTLI=ON ^
      -DARROW_PYTHON=ON ^
      -DARROW_PARQUET=ON ^
      .. || exit /B
cmake --build . --target install --config Release || exit /B
popd

set PYARROW_BUILD_TYPE=Release
set PYARROW_PARALLEL=8
set PYARROW_WITH_PARQUET=1
set PYARROW_WITH_STATIC_BOOST=1
set PYARROW_BUNDLE_ARROW_CPP=1
set SETUPTOOLS_SCM_PRETEND_VERSION=%PYARROW_VERSION%

pushd %ARROW_SRC%\python
python setup.py build_ext --extra-cmake-args="-DZLIB_ROOT=%CONDA_PREFIX%\Library" bdist_wheel || exit /B
popd

call conda.bat deactivate

set ARROW_TEST_DATA=%ARROW_SRC%\testing\data

@rem test the wheel
@rem TODO For maximum reliability, we should test in a plain virtualenv instead.
call conda create -n wheel-test -c conda-forge -q -y python=3.5 || exit /B
call conda.bat activate wheel-test

@rem install the built wheel
pip install %ARROW_SRC%\python\dist\pyarrow-%PYARROW_VERSION%-cp35-cp35m-win_amd64.whl || exit /B
pip install -r %ARROW_SRC%\python\requirements-wheel-test.txt || exit /B

@rem test the imports
python -c "import pyarrow; import pyarrow.parquet" || exit /B

@rem run the python tests
pytest -rs --pyargs pyarrow || exit /B

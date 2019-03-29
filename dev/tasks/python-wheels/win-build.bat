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
conda update --yes --quiet conda

conda create -n arrow -q -y python=%PYTHON_VERSION% ^
      six pytest setuptools numpy=%NUMPY_VERSION% pandas

conda install -n arrow -q -y -c conda-forge ^
      git flatbuffers rapidjson ^
      cmake ^
      boost-cpp thrift-cpp ^
      gflags snappy zlib zstd lz4-c double-conversion ^
      llvmdev libprotobuf

call activate arrow

set ARROW_HOME=%CONDA_PREFIX%\Library
set PARQUET_HOME=%CONDA_PREFIX%\Library
echo %ARROW_HOME%

@rem Build and test Arrow C++ libraries
mkdir %ARROW_SRC%\cpp\build
pushd %ARROW_SRC%\cpp\build

cmake -G "%GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DARROW_BUILD_TESTS=OFF ^
      -DCMAKE_BUILD_TYPE=Release ^
      -DBrotli_SOURCE=BUNDLED ^
      -DRE2_SOURCE=BUNDLED ^
      -DARROW_CXXFLAGS="/MP" ^
      -DARROW_PYTHON=ON ^
      -DARROW_PARQUET=ON ^
      -DARROW_GANDIVA=ON ^
      ..  || exit /B
cmake --build . --target INSTALL --config Release  || exit /B
popd

pushd %ARROW_SRC%\python
set PYARROW_BUILD_TYPE=Release
@rem Gandiva is not supported on Python 2.7, but We don't build 2.7 wheel for windows
set PYARROW_WITH_GANDIVA=1
set PYARROW_WITH_PARQUET=1
set PYARROW_WITH_STATIC_BOOST=1
set PYARROW_BUNDLE_ARROW_CPP=1
set SETUPTOOLS_SCM_PRETEND_VERSION=%PYARROW_VERSION%

@rem Newer Cython versions are not available on conda-forge
pip install -U pip
pip install "Cython>=0.29"

python setup.py build_ext bdist_wheel || exit /B
popd

call deactivate

@rem test the wheel
conda create -n wheel-test -q -y python=%PYTHON_VERSION% ^
      numpy=%NUMPY_VERSION% pandas pytest hypothesis
call activate wheel-test

@rem install the built wheel
pip install -vv --no-index --find-links=%ARROW_SRC%\python\dist\ pyarrow

@rem test the imports
python -c "import pyarrow; import pyarrow.parquet; import pyarrow.gandiva;" || exit /B

@rem run the python tests
pytest --pyargs pyarrow || exit /B

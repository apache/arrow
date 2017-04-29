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

set CONDA_ENV=C:\arrow-conda-env
set ARROW_HOME=C:\arrow-install

conda create -p %CONDA_ENV% -q -y python=%PYTHON% ^
      six pytest setuptools numpy pandas cython
call activate %CONDA_ENV%

@rem Build and test Arrow C++ libraries

cd cpp
mkdir build
cd build
cmake -G "%GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=Release ^
      -DARROW_CXXFLAGS="/WX" ^
      -DARROW_PYTHON=on ^
      ..  || exit /B
cmake --build . --target INSTALL --config Release  || exit /B

@rem Needed so python-test.exe works
set PYTHONPATH=%CONDA_ENV%\Lib;%CONDA_ENV%\Lib\site-packages;%CONDA_ENV%\python35.zip;%CONDA_ENV%\DLLs;%CONDA_ENV%

ctest -VV  || exit /B

set PYTHONPATH=

@rem Build and import pyarrow

set PATH=%ARROW_HOME%\bin;%PATH%

cd ..\..\python
python setup.py build_ext --inplace  || exit /B
python -c "import pyarrow"  || exit /B

py.test pyarrow -v -s || exit /B

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

set PYARROW_TEST_ACERO=ON
set PYARROW_TEST_CYTHON=ON
set PYARROW_TEST_DATASET=ON
set PYARROW_TEST_FLIGHT=ON
set PYARROW_TEST_GANDIVA=OFF
set PYARROW_TEST_GCS=ON
set PYARROW_TEST_HDFS=ON
set PYARROW_TEST_ORC=ON
set PYARROW_TEST_PARQUET=ON
set PYARROW_TEST_PARQUET_ENCRYPTION=ON
set PYARROW_TEST_SUBSTRAIT=ON
set PYARROW_TEST_S3=OFF
set PYARROW_TEST_TENSORFLOW=ON

@REM Enable again once https://github.com/scipy/oldest-supported-numpy/pull/27 gets merged
@REM set PYARROW_TEST_PANDAS=ON

set ARROW_TEST_DATA=C:\arrow\testing\data
set PARQUET_TEST_DATA=C:\arrow\cpp\submodules\parquet-testing\data

@REM List installed Pythons
py -0p

set PYTHON_CMD=py -%PYTHON%

%PYTHON_CMD% -m pip install -U pip setuptools || exit /B 1

@REM Install testing dependencies
%PYTHON_CMD% -m pip install -r C:\arrow\python\requirements-wheel-test.txt || exit /B 1

@REM Install the built wheels
%PYTHON_CMD% -m pip install --no-index --find-links=C:\arrow\python\dist\ pyarrow || exit /B 1

@REM Test that the modules are importable
%PYTHON_CMD% -c "import pyarrow" || exit /B 1
%PYTHON_CMD% -c "import pyarrow._gcsfs" || exit /B 1
%PYTHON_CMD% -c "import pyarrow._hdfs" || exit /B 1
%PYTHON_CMD% -c "import pyarrow._s3fs" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.csv" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.dataset" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.flight" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.fs" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.json" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.orc" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.parquet" || exit /B 1
%PYTHON_CMD% -c "import pyarrow.substrait" || exit /B 1

@rem Download IANA Timezone Database for ORC C++
curl https://cygwin.osuosl.org/noarch/release/tzdata/tzdata-2024a-1.tar.xz --output tzdata.tar.xz || exit /B
mkdir %USERPROFILE%\Downloads\test\tzdata
arc unarchive tzdata.tar.xz %USERPROFILE%\Downloads\test\tzdata
set TZDIR=%USERPROFILE%\Downloads\test\tzdata\usr\share\zoneinfo

@REM Execute unittest
%PYTHON_CMD% -m pytest -r s --pyargs pyarrow || exit /B 1

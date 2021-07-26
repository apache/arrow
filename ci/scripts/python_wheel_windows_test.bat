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

set PYARROW_TEST_CYTHON=OFF
set PYARROW_TEST_DATASET=ON
set PYARROW_TEST_FLIGHT=ON
set PYARROW_TEST_GANDIVA=OFF
set PYARROW_TEST_HDFS=ON
set PYARROW_TEST_ORC=OFF
set PYARROW_TEST_PANDAS=ON
set PYARROW_TEST_PARQUET=ON
set PYARROW_TEST_PLASMA=OFF
set PYARROW_TEST_S3=OFF
set PYARROW_TEST_TENSORFLOW=ON

set ARROW_TEST_DATA=C:\arrow\testing\data
set PARQUET_TEST_DATA=C:\arrow\submodules\parquet-testing\data

@REM Install the built wheels
python -m pip install numpy
python -m pip install --no-index --find-links=C:\arrow\python\dist\ pyarrow || exit /B

@REM Test that the modules are importable
python -c "import pyarrow"
python -c "import pyarrow._hdfs"
python -c "import pyarrow._s3fs"
python -c "import pyarrow.csv"
python -c "import pyarrow.dataset"
python -c "import pyarrow.flight"
python -c "import pyarrow.fs"
python -c "import pyarrow.json"
python -c "import pyarrow.parquet"

@REM Install testing dependencies
pip install -r C:\arrow\python\requirements-wheel-test.txt

@REM Execute unittest
pytest -r s --pyargs pyarrow

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

set SOURCE_DIR=%1

set ARROW_TEST_DATA=%SOURCE_DIR%\testing\data
set PARQUET_TEST_DATA=%SOURCE_DIR%\cpp\submodules\parquet-testing\data

echo Testing on Windows ...

@REM List installed Pythons
py -0p

%PYTHON_CMD% -m sysconfig || exit /B 1

pushd %SOURCE_DIR%\python

@REM Install Python test dependencies
%PYTHON_CMD% -m pip install -r requirements-test.txt || exit /B 1

popd

@REM Run Python tests
%PYTHON_CMD% -c "import pyarrow" || exit /B 1
%PYTHON_CMD% -m pytest -r s --pyargs pyarrow || exit /B 1

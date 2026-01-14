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

set PYARROW_DIR=%1

if "%PYARROW_TEST_ANNOTATIONS%"=="ON" (
    echo Annotation testing on Windows ...

    @REM Install library stubs. Note some libraries contain their own type hints so they need to be installed.
    %PYTHON_CMD% -m pip install fsspec pandas-stubs scipy-stubs types-cffi types-psutil types-requests types-python-dateutil || exit /B 1

    @REM Install type checkers
    %PYTHON_CMD% -m pip install mypy pyright ty || exit /B 1

    @REM Run type checkers
    pushd %PYARROW_DIR%

    mypy || exit /B 1
    pyright || exit /B 1
    ty check || exit /B 1
    popd
) else (
    echo Annotation testing skipped on Windows ...
)

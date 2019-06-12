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

if "%JOB%" == "Rust" (
    if "%ARROW_CI_RUST_AFFECTED%" == "0" (
        echo ===
        echo === No Rust changes, exiting job
        echo ===
        appveyor exit
    )
) else if "%JOB%" == "MinGW" (
    if "%ARROW_CI_GLIB_AFFECTED%" == "0" (
        echo ===
        echo === No C++, or GLib changes, exiting job
        echo ===
        appveyor exit
    )
) else if "%JOB%" == "C#" (
    if "%ARROW_CI_CSHARP_AFFECTED%" == "0" (
        echo ===
        echo === No C# changes, exiting job
        echo ===
        appveyor exit
    )
) else if "%JOB%" == "Go" (
    if "%ARROW_CI_GO_AFFECTED%" == "0" (
        echo ===
        echo === No Go changes, exiting job
        echo ===
        appveyor exit
    )
) else if "%JOB%" == "R" (
    if "%ARROW_CI_R_AFFECTED%" == "0" (
        echo ===
        echo === No C++ or R changes, exiting job
        echo ===
        appveyor exit
    )
) else (
    if "%ARROW_CI_PYTHON_AFFECTED%" == "0" (
        echo ===
        echo === No C++ or Python changes, exiting job
        echo ===
        appveyor exit
    )
)

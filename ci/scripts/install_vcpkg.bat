@rem # Licensed to the Apache Software Foundation (ASF) under one
@rem # or more contributor license agreements.  See the NOTICE file
@rem # distributed with this work for additional information
@rem # regarding copyright ownership.  The ASF licenses this file
@rem # to you under the Apache License, Version 2.0 (the
@rem # "License"); you may not use this file except in compliance
@rem # with the License.  You may obtain a copy of the License at
@rem #
@rem #   http://www.apache.org/licenses/LICENSE-2.0
@rem #
@rem # Unless required by applicable law or agreed to in writing,
@rem # software distributed under the License is distributed on an
@rem # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem # KIND, either express or implied.  See the License for the
@rem # specific language governing permissions and limitations
@rem # under the License.

@echo off

SET orig_dir=%CD%
@REM TODO: In PR review, decide whether this needs to be fancier
SET arrow_dir=arrow
SET default_vcpkg_ports_patch=%arrow_dir%/ci/vcpkg/ports.patch
SET vcpkg_destination=%1
SET env_file=.env

set USAGE_STRING=Usage: .\%~n0.bat target-directory [vcpkg-version [vcpkg-ports-patch]]
if "%1"=="" (
  echo %USAGE_STRING%
  exit \b 1
)

@rem Extract value of VCPKG from .env file if it exists
set VCPKG=
if exist .env (
  @REM Iterate through each line, split on '=' into %%i and %%j halves
  for /f "usebackq tokens=1,* delims==" %%i in (%env_file%) do (
      @REM %%i is storing the first token on each line
      if "%%i"=="VCPKG" (
          @REM Iterate over the second half of line (%%j), first token from each
          for /f "tokens=1 delims= " %%A in ("%%j") do (
              @REM ~ removes surrounding quotes from %%A
              set VCPKG=%%~A
          )
      )
  )
)

@REM Handle vcpkg_version argument or fall back to environment variable
@REM
@REM The "else if" here works around behavior I couldn't find a workaround for
@REM with how Windows Docker handles the empty `ARG vcpkg` in associated
@REM Dockerfiles.
if "%2"=="" (
  SET vcpkg_version=%VCPKG%
) else if "%2"=="%%vcpkg%%" (
  SET vcpkg_version=%VCPKG%
) else (
  SET vcpkg_version=%2
)

@REM Handle vcpkg_ports_patch path
if "%3"=="" (
  SET vcpkg_ports_patch=%default_vcpkg_ports_patch%
) else (
  SET vcpkg_ports_patch=%3
)

git clone --shallow-since=2021-04-01 https://github.com/microsoft/vcpkg %vcpkg_destination%
cd %vcpkg_destination%
if not "%vcpkg_version%"=="" (
  git checkout %vcpkg_version%
)
./bootstrap-vcpkg.bat -disableMetrics
if exist "%default_vcpkg_ports_patch%" (
  git apply --verbose --ignore-whitespace ${vcpkg_ports_patch}
)

cd %orig_dir%

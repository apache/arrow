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

set "PATH=C:\Miniconda37-x64;C:\Miniconda37-x64\Scripts;C:\Miniconda37-x64\Library\bin;%PATH%"
set BOOST_ROOT=C:\Libraries\boost_1_67_0
set BOOST_LIBRARYDIR=C:\Libraries\boost_1_67_0\lib64-msvc-14.0

@rem
@rem Avoid picking up AppVeyor-installed OpenSSL (linker errors with gRPC)
@rem XXX Perhaps there is a smarter way of solving this issue?
@rem
rd /s /q C:\OpenSSL-Win32
rd /s /q C:\OpenSSL-Win64
rd /s /q C:\OpenSSL-v11-Win32
rd /s /q C:\OpenSSL-v11-Win64
rd /s /q C:\OpenSSL-v111-Win32
rd /s /q C:\OpenSSL-v111-Win64

@rem
@rem Configure miniconda
@rem
conda config --set auto_update_conda false
conda config --set show_channel_urls True
@rem Help with SSL timeouts to S3
conda config --set remote_connect_timeout_secs 12
@rem Workaround for ARROW-13636
conda config --append disallowed_packages pypy3
conda info -a

@rem
@rem Install mamba to the base environment
@rem
conda install -q -y -c conda-forge mamba

@rem
@rem Create conda environment for Build and Toolchain jobs
@rem
@rem Avoid Boost 1.70 because of https://github.com/boostorg/process/issues/85

set CONDA_PACKAGES=

if "%ARROW_BUILD_GANDIVA%" == "ON" (
  @rem Install llvmdev in the toolchain if building gandiva.dll
  set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_gandiva_win.txt
)
if "%JOB%" == "Toolchain" (
  @rem Install pre-built "toolchain" packages for faster builds
  set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_cpp.txt
)
if "%JOB%" NEQ "Build_Debug" (
  @rem Arrow conda environment is only required for the Build and Toolchain jobs
  mamba create -n arrow -q -y -c conda-forge ^
    --file=ci\conda_env_python.txt ^
    %CONDA_PACKAGES%  ^
    "cmake=3.17" ^
    "ninja" ^
    "nomkl" ^
    "pandas" ^
    "fsspec" ^
    "python=%PYTHON%" ^
    || exit /B

  @rem On Windows, GTest is always bundled from source instead of using
  @rem conda binaries, avoid any interference between the two versions.
  if "%JOB%" == "Toolchain" (
    mamba uninstall -n arrow -q -y -c conda-forge gtest
  )
)

@rem
@rem Configure compiler
@rem
if "%GENERATOR%"=="Ninja" set need_vcvarsall=1
if defined need_vcvarsall (
    if "%APPVEYOR_BUILD_WORKER_IMAGE%" NEQ "Visual Studio 2017" (
        @rem ARROW-14070 Visual Studio 2015 no longer supported
        exit /B
    )
    call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvarsall.bat" amd64
)

@rem
@rem Use clcache for faster builds
@rem

pip install -q git+https://github.com/Nuitka/clcache.git || exit /B
@rem Limit cache size to 500 MB
clcache -M 500000000
clcache -c
clcache -s
powershell.exe -Command "Start-Process clcache-server" || exit /B

@rem
@rem Download Minio somewhere on PATH, for unit tests
@rem
if "%ARROW_S3%" == "ON" (
    appveyor DownloadFile https://dl.min.io/server/minio/release/windows-amd64/minio.exe -FileName C:\Windows\Minio.exe || exit /B
)

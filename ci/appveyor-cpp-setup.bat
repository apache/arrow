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
conda info -a

@rem
@rem Create conda environment for Build and Toolchain jobs
@rem
@rem Avoid Boost 1.70 because of https://github.com/boostorg/process/issues/85

set CONDA_PACKAGES=

if "%ARROW_BUILD_GANDIVA%" == "ON" (
  @rem Install llvmdev in the toolchain if building gandiva.dll
  set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_gandiva.yml
)
if "%JOB%" == "Toolchain" (
  @rem Install pre-built "toolchain" packages for faster builds
  set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_cpp.yml
)
if "%JOB%" NEQ "Build_Debug" (
  @rem Arrow conda environment is only required for the Build and Toolchain jobs
  conda create -n arrow -q -y -c conda-forge ^
    --file=ci\conda_env_python.yml ^
    %CONDA_PACKAGES%  ^
    "boost-cpp<1.70" ^
    "ninja" ^
    "nomkl" ^
    "pandas" ^
    "python=%PYTHON%" ^
    || exit /B
)

@rem
@rem Configure compiler
@rem
if "%GENERATOR%"=="Ninja" set need_vcvarsall=1
if defined need_vcvarsall (
    @rem Select desired compiler version
    if "%APPVEYOR_BUILD_WORKER_IMAGE%" == "Visual Studio 2017" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvarsall.bat" amd64
    ) else (
        call "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64
    )
)

@rem
@rem Use clcache for faster builds
@rem
pip install -q git+https://github.com/frerich/clcache.git
@rem Limit cache size to 500 MB
clcache -M 500000000
clcache -c
clcache -s
powershell.exe -Command "Start-Process clcache-server"

@rem
@rem Download Minio somewhere on PATH, for unit tests
@rem
if "%ARROW_S3%" == "ON" (
    appveyor DownloadFile https://dl.min.io/server/minio/release/windows-amd64/minio.exe -FileName C:\Windows\Minio.exe || exit /B
)

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

@rem
@rem The miniconda install on AppVeyor is very outdated, use Mambaforge instead
@rem

appveyor DownloadFile https://github.com/conda-forge/miniforge/releases/download/24.9.2-0/Mambaforge-Windows-x86_64.exe || exit /B
start /wait "" Mambaforge-Windows-x86_64.exe /InstallationType=JustMe /RegisterPython=0 /S /D=C:\Mambaforge
set "PATH=C:\Mambaforge\scripts;C:\Mambaforge\condabin;%PATH%"

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
rd /s /q C:\OpenSSL-v30-Win32
rd /s /q C:\OpenSSL-v30-Win64

@rem
@rem Configure conda
@rem
conda config --set auto_update_conda false
conda config --set show_channel_urls true
conda config --set always_yes true
@rem Help with SSL timeouts to S3
conda config --set remote_connect_timeout_secs 12

conda info -a || exit /B

@rem
@rem Create conda environment
@rem

set CONDA_PACKAGES=

if "%ARROW_BUILD_GANDIVA%" == "ON" (
  @rem Install llvmdev in the toolchain if building gandiva.dll
  set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_gandiva_win.txt
)
@rem Install pre-built "toolchain" packages for faster builds
set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_cpp.txt
@rem Arrow conda environment
conda create -n arrow ^
  --file=ci\conda_env_python.txt ^
  %CONDA_PACKAGES%  ^
  "ccache" ^
  "cmake" ^
  "ninja" ^
  "nomkl" ^
  "pandas" ^
  "python=%PYTHON%" ^
  || exit /B
conda list -n arrow

@rem
@rem Configure compiler
@rem
call "C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" amd64
set CC=cl.exe
set CXX=cl.exe

@rem
@rem Download Minio somewhere on PATH, for unit tests
@rem
if "%ARROW_S3%" == "ON" (
  appveyor DownloadFile https://dl.min.io/server/minio/release/windows-amd64/archive/minio.RELEASE.2025-01-20T14-49-07Z -FileName C:\Windows\Minio.exe || exit /B
)

@rem
@rem Download IANA Timezone Database for unit tests
@rem
@rem (Doc section: Download timezone database)
curl https://data.iana.org/time-zones/releases/tzdata2021e.tar.gz --output tzdata.tar.gz
mkdir tzdata
tar --extract --file tzdata.tar.gz --directory tzdata
move tzdata %USERPROFILE%\Downloads\tzdata
@rem Also need Windows timezone mapping
curl https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml ^
  --output %USERPROFILE%\Downloads\tzdata\windowsZones.xml
@rem (Doc section: Download timezone database)

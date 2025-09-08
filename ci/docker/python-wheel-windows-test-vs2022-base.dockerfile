# escape=`

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE: You must update PYTHON_WHEEL_WINDOWS_TEST_IMAGE_REVISION in .env
# when you update this file.

FROM mcr.microsoft.com/windows/servercore:ltsc2022

# Ensure we in a command shell and not Powershell
SHELL ["cmd", "/S", "/C"]

# Install MSVC BuildTools
#
# The set of components below (lines starting with --add) is the most minimal
# set we could find that would still compile Arrow C++.
RUN `
    curl -SL --output vs_buildtools.exe https://aka.ms/vs/17/release/vs_buildtools.exe `
    && (start /w vs_buildtools.exe --quiet --wait --norestart --nocache `
    --installPath "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\BuildTools" `
    --add Microsoft.VisualStudio.Component.VC.CoreBuildTools `
    --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
    --add Microsoft.VisualStudio.Component.Windows11SDK.26100 `
    --add Microsoft.VisualStudio.Component.VC.CMake.Project `
    || IF "%ERRORLEVEL%"=="3010" EXIT 0) `
    && del /q vs_buildtools.exe

# Install choco CLI
#
# We switch into Powershell just for this command and switch back to cmd
# See https://chocolatey.org/install#completely-offline-install
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
RUN `
    Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
SHELL ["cmd", "/S", "/C"]

# Install git, wget, minio
RUN choco install --no-progress -r -y git wget
RUN curl https://dl.min.io/server/minio/release/windows-amd64/archive/minio.RELEASE.2024-09-13T20-26-02Z `
    --output "C:\Windows\Minio.exe"

# Install the GCS testbench using a well-known Python version.
# NOTE: cannot use pipx's `--fetch-missing-python` because of
# https://github.com/pypa/pipx/issues/1521, therefore download Python ourselves.
RUN choco install -r -y --pre --no-progress python --version=3.11.9
ENV PIPX_BIN_DIR=C:\\Windows\\
ENV PIPX_PYTHON="C:\Python311\python.exe"
COPY ci/scripts/install_gcs_testbench.bat C:/arrow/ci/scripts/
RUN call "C:\arrow\ci\scripts\install_gcs_testbench.bat" && `
    storage-testbench -h

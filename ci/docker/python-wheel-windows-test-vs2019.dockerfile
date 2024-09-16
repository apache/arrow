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

# NOTE: You must update PYTHON_WHEEL_WINDOWS_IMAGE_REVISION in .env
# when you update this file.

# based on mcr.microsoft.com/windows/servercore:ltsc2019
# contains choco and vs2019 preinstalled
FROM abrarov/msvc-2019:2.11.0

# hadolint shell=cmd.exe

# Add unix tools to path
RUN setx path "%path%;C:\Program Files\Git\usr\bin"

# 1. Remove previous installations of python from the base image
# NOTE: a more recent base image (tried with 2.12.1) comes with python 3.9.7
# and the msi installers are failing to remove pip and tcl/tk "products" making
# the subsequent choco python installation step failing for installing python
# version 3.9.* due to existing python version
# 2. Install Minio for S3 testing.
RUN wmic product where "name like 'python%%'" call uninstall /nointeractive && \
    rm -rf Python* && \
    curl https://dl.min.io/server/minio/release/windows-amd64/archive/minio.RELEASE.2022-05-26T05-48-41Z \
        --output "C:\Windows\Minio.exe"

# Install the GCS testbench using a well-known Python version.
# NOTE: cannot use pipx's `--fetch-missing-python` because of
# https://github.com/pypa/pipx/issues/1521, therefore download Python ourselves.
RUN choco install -r -y --pre --no-progress python --version=3.11.9
ENV PIPX_BIN_DIR=C:\\Windows\\
ENV PIPX_PYTHON="C:\Python311\python.exe"
COPY ci/scripts/install_gcs_testbench.bat C:/arrow/ci/scripts/
RUN call "C:\arrow\ci\scripts\install_gcs_testbench.bat" && \
    storage-testbench -h

# Define the full version number otherwise choco falls back to patch number 0 (3.9 => 3.9.0)
ARG python=3.9
RUN (if "%python%"=="3.9" setx PYTHON_VERSION "3.9.13") & \
    (if "%python%"=="3.10" setx PYTHON_VERSION "3.10.11") & \
    (if "%python%"=="3.11" setx PYTHON_VERSION "3.11.9") & \
    (if "%python%"=="3.12" setx PYTHON_VERSION "3.12.5") & \
    (if "%python%"=="3.13" setx PYTHON_VERSION "3.13.0-rc1")

# Install archiver to extract xz archives
RUN choco install -r -y --pre --no-progress --force python --version=%PYTHON_VERSION% && \
    choco install --no-progress -r -y archiver

ENV PYTHON=$python

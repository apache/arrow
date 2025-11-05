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

ARG base
# https://github.com/hadolint/hadolint/wiki/DL3006
# (Hadolint does not expand variables and thinks '${base}' is an untagged image)
# hadolint ignore=DL3006
FROM ${base}

# hadolint shell=cmd.exe

# Define the full version number otherwise choco falls back to patch number 0 (3.10 => 3.10.0)
ARG python=3.10
RUN (if "%python%"=="3.10" setx PYTHON_VERSION "3.10.11" && setx PYTHON_CMD "py -3.10") & \
    (if "%python%"=="3.11" setx PYTHON_VERSION "3.11.9" && setx PYTHON_CMD "py -3.11") & \
    (if "%python%"=="3.12" setx PYTHON_VERSION "3.12.10" && setx PYTHON_CMD "py -3.12") & \
    (if "%python%"=="3.13" setx PYTHON_VERSION "3.13.9" && setx PYTHON_CMD "py -3.13") & \
    (if "%python%"=="3.14" setx PYTHON_VERSION "3.14.0" && setx PYTHON_CMD "py -3.14")

SHELL ["powershell", "-NoProfile", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
# Install Python install manager (MSIX)
RUN $msix_url = 'https://www.python.org/ftp/python/installer/python.msix'; \
    Invoke-WebRequest -Uri $msix_url -OutFile 'python.msix'; \
    Add-AppxPackage .\python.msix

# Use python_abi_tag env var to select regular or free-threaded Python
ARG freethreaded=0
ENV PYTHON_MODE=${freethreaded}
RUN if ($env:PYTHON_MODE -eq '1') { \
        pymanager install --version $env:PYTHON_VERSION --variant freethreaded \
    } else { \
        pymanager install --version $env:PYTHON_VERSION \
    }

SHELL ["cmd", "/S", "/C"]
# hadolint ignore=DL3059
RUN %PYTHON_CMD% -m pip install -U pip setuptools & \
    if "%python%"=="3.13" ( \
        setx REQUIREMENTS_FILE "requirements-wheel-test-3.13t.txt" \
    ) else ( \
        setx REQUIREMENTS_FILE "requirements-wheel-test.txt" \
    )

COPY python/requirements-wheel-test-3.13t.txt python/requirements-wheel-test.txt C:/arrow/python/
RUN %PYTHON_CMD% -m pip install -r C:/arrow/python/%REQUIREMENTS_FILE%

ENV PYTHON=$python

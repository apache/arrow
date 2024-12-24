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
ARG base
FROM ${base}

# Define the full version number otherwise choco falls back to patch number 0 (3.9 => 3.9.0)
ARG python=3.9
RUN (if "%python%"=="3.9" setx PYTHON_VERSION "3.9.13" && setx PATH "%PATH%;C:\Python39;C:\Python39\Scripts") & \
    (if "%python%"=="3.10" setx PYTHON_VERSION "3.10.11" && setx PATH "%PATH%;C:\Python310;C:\Python310\Scripts") & \
    (if "%python%"=="3.11" setx PYTHON_VERSION "3.11.9" && setx PATH "%PATH%;C:\Python311;C:\Python311\Scripts") & \
    (if "%python%"=="3.12" setx PYTHON_VERSION "3.12.5" && setx PATH "%PATH%;C:\Python312;C:\Python312\Scripts") & \
    (if "%python%"=="3.13" setx PYTHON_VERSION "3.13.0-rc1" && setx PATH "%PATH%;C:\Python313;C:\Python313\Scripts")
RUN choco install -r -y --pre --no-progress python --version=%PYTHON_VERSION%
RUN python -m pip install -U pip setuptools

COPY python/requirements-wheel-build.txt arrow/python/
RUN python -m pip install -r arrow/python/requirements-wheel-build.txt

ENV PYTHON=${python}

# For debugging purposes
# RUN wget --no-check-certificate https://github.com/lucasg/Dependencies/releases/download/v1.10/Dependencies_x64_Release.zip
# RUN unzip Dependencies_x64_Release.zip -d Dependencies && setx path "%path%;C:\Dependencies"

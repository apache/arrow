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

ARG base
FROM ${base}

# Define the full version number otherwise choco falls back to patch number 0 (3.10 => 3.10.0)
ARG python=3.10
RUN (if "%python%"=="3.10" setx PYTHON_VERSION "3.10.11" && setx PYTHON_CMD "py -3.10") & \
    (if "%python%"=="3.11" setx PYTHON_VERSION "3.11.9" && setx PYTHON_CMD "py -3.11") & \
    (if "%python%"=="3.12" setx PYTHON_VERSION "3.12.8" && setx PYTHON_CMD "py -3.12") & \
    (if "%python%"=="3.13" setx PYTHON_VERSION "3.13.1" && setx PYTHON_CMD "py -3.13")

RUN choco install -r -y --pre --no-progress python --version=%PYTHON_VERSION%
RUN %PYTHON_CMD% -m pip install -U pip setuptools

COPY python/requirements-wheel-build.txt C:/arrow/python/
RUN %PYTHON_CMD% -m pip install -r C:/arrow/python/requirements-wheel-build.txt

ENV PYTHON=${python}

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
# contains choco and vs2017 preinstalled
FROM abrarov/msvc-2017:2.11.0

# Add unix tools to path
RUN setx path "%path%;C:\Program Files\Git\usr\bin"

# Remove previous installations of python from the base image
# NOTE: a more recent base image (tried with 2.12.1) comes with python 3.9.7
# and the msi installers are failing to remove pip and tcl/tk "products" making
# the subsequent choco python installation step failing for installing python
# version 3.9.* due to existing python version
RUN wmic product where "name like 'python%%'" call uninstall /nointeractive && \
    rm -rf Python*

# Define the full version number otherwise choco falls back to patch number 0 (3.7 => 3.7.0)
ARG python=3.8
RUN (if "%python%"=="3.7" setx PYTHON_VERSION "3.7.9" && setx PATH "%PATH%;C:\Python37;C:\Python37\Scripts") & \
    (if "%python%"=="3.8" setx PYTHON_VERSION "3.8.10" && setx PATH "%PATH%;C:\Python38;C:\Python38\Scripts") & \
    (if "%python%"=="3.9" setx PYTHON_VERSION "3.9.13" && setx PATH "%PATH%;C:\Python39;C:\Python39\Scripts") & \
    (if "%python%"=="3.10" setx PYTHON_VERSION "3.10.8" && setx PATH "%PATH%;C:\Python310;C:\Python310\Scripts") & \
    (if "%python%"=="3.11" setx PYTHON_VERSION "3.11.0" && setx PATH "%PATH%;C:\Python311;C:\Python311\Scripts")
RUN choco install -r -y --no-progress python --version=%PYTHON_VERSION%
RUN python -m pip install -U pip setuptools

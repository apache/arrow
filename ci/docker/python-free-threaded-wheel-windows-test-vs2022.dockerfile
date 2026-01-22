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

ARG python=3.13

SHELL ["powershell", "-NoProfile", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
RUN $filename = 'python-3.13.1-amd64.exe'; \
    $url = 'https://www.python.org/ftp/python/3.13.1/' + $filename; \
    Invoke-WebRequest -Uri $url -OutFile $filename; \
    Start-Process -FilePath $filename -ArgumentList '/quiet', 'Include_freethreaded=1' -Wait

ENV PYTHON_CMD="py -${python}t"

SHELL ["cmd", "/S", "/C"]
RUN %PYTHON_CMD% -m pip install -U pip setuptools

COPY python/requirements-wheel-test-3.13t.txt C:/arrow/python/
# Cython and Pandas wheels for 3.13 free-threaded are not released yet
RUN %PYTHON_CMD% -m pip install \
    --extra-index-url https://pypi.anaconda.org/scientific-python-nightly-wheels/simple \
    --pre \
    --prefer-binary \
    -r C:/arrow/python/requirements-wheel-test-3.13t.txt

ENV PYTHON="${python}t"
ENV PYTHON_GIL=0

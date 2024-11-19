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

SHELL ["powershell", "-NoProfile", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
RUN $filename = 'python-3.13.0-amd64.exe'; \
    $url = 'https://www.python.org/ftp/python/3.13.0/' + $filename; \
    Invoke-WebRequest -Uri $url -OutFile $filename; \
    Start-Process -FilePath $filename -ArgumentList '/quiet', 'Include_freethreaded=1' -Wait

RUN py -3.13t -m pip install -U pip setuptools

COPY python/requirements-wheel-build.txt arrow/python/
RUN py -3.13t -m pip install --pre --prefer-binary --extra-index-url "https://pypi.anaconda.org/scientific-python-nightly-wheels/simple" cython pandas
RUN py -3.13t -m pip install -r arrow/python/requirements-wheel-build.txt

ENV PYTHON="3.13t"

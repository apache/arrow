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

FROM amd64/ubuntu:20.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN echo "debconf debconf/frontend select Noninteractive" | \
        debconf-set-selections

RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        git \
        python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

COPY python/requirements-build.txt \
     /arrow/python/requirements-build.txt
RUN pip3 install --requirement /arrow/python/requirements-build.txt

ENV PYTHON=/usr/bin/python3

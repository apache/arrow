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

ARG base
FROM ${base}

RUN apt update
RUN apt install -y -q --no-install-recommends software-properties-common gpg-agent
RUN add-apt-repository -y ppa:deadsnakes/ppa && apt update
RUN apt install -y -q --no-install-recommends python3.13-dev python3.13-nogil python3.13-venv

ENV ARROW_PYTHON_VENV /arrow-dev
RUN python3.13t -m venv ${ARROW_PYTHON_VENV}

ENV PYTHON /arrow-dev/bin/python

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

ENV DEBIAN_FRONTEND noninteractive

# install libarrow-dev to link against with CGO
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends ca-certificates lsb-release wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get install -y -q --no-install-recommends ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        cmake \
        libarrow-dev && \
    apt-get clean

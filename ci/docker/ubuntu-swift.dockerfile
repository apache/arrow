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

FROM swift:5.9.0

# Go is needed for generating test data
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        golang-go \
        unzip \
        wget && \
    apt-get clean

ARG swift_lint=0.53.0
RUN wget https://github.com/realm/SwiftLint/releases/download/${swift_lint}/swiftlint_linux.zip && \
    unzip swiftlint_linux.zip && \
    mv swiftlint /usr/local/bin/ && \
    mkdir -p /usr/local/share/doc/swiftlint/ && \
    mv LICENSE /usr/local/share/doc/swiftlint/ && \
    rm -rf swiftlint_linux.zip

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

# Install the libaries required by the Gandiva to run
RUN vcpkg install --clean-after-build \
        boost-algorithm \
        boost-crc \
        boost-date-time \
        boost-format \
        boost-locale \
        boost-multiprecision \
        boost-predef \
        boost-regex \
        boost-system \
        boost-variant \
        # Use enable rtti to avoid link problems in Gandiva
        llvm[clang,default-options,default-targets,lld,tools,enable-rtti]

# Install Java
ARG java=1.8.0
RUN yum install -y java-$java-openjdk-devel && yum clean all
ENV JAVA_HOME=/usr/lib/jvm/java-$java-openjdk/

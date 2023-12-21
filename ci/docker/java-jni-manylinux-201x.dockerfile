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

# Install the libraries required by the Gandiva to run
# Use enable llvm[enable-rtti] in the vcpkg.json to avoid link problems in Gandiva
RUN vcpkg install \
        --clean-after-build \
        --x-install-root=${VCPKG_ROOT}/installed \
        --x-manifest-root=/arrow/ci/vcpkg \
        --x-feature=dev \
        --x-feature=flight \
        --x-feature=gcs \
        --x-feature=json \
        --x-feature=parquet \
        --x-feature=gandiva \
        --x-feature=s3

# Install Java
ARG java=1.8.0
ARG maven=3.9.3
RUN yum install -y java-$java-openjdk-devel && \
      yum clean all && \
      curl \
        --fail \
        --location \
        "https://www.apache.org/dyn/closer.lua?action=download&filename=maven/maven-3/${maven}/binaries/apache-maven-${maven}-bin.tar.gz" | \
        tar xfz - -C /usr/local && \
      ln -s /usr/local/apache-maven-${maven}/bin/mvn /usr/local/bin

# Install the gcs testbench
COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN PYTHON=python /arrow/ci/scripts/install_gcs_testbench.sh default

# For ci/scripts/{cpp,java}_*.sh
ENV ARROW_HOME=/tmp/local \
    ARROW_JAVA_CDATA=ON \
    ARROW_JAVA_JNI=ON \
    ARROW_USE_CCACHE=ON

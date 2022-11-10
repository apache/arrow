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

ARG repo
ARG arch
FROM ${repo}:${arch}-conda

COPY ci/scripts/install_minio.sh /arrow/ci/scripts
RUN /arrow/ci/scripts/install_minio.sh latest /opt/conda

# Unless overriden use Python 3.10
# Google GCS fails building with Python 3.11 at the moment.
ARG python=3.10

# install the required conda packages into the test environment
COPY ci/conda_env_cpp.txt \
     ci/conda_env_gandiva.txt \
     /arrow/ci/
RUN mamba install -q -y \
        --file arrow/ci/conda_env_cpp.txt \
        --file arrow/ci/conda_env_gandiva.txt \
        compilers \
        doxygen \
        python=${python} \
        valgrind && \
    mamba clean --all

# We want to install the GCS testbench using the same Python binary that the Conda code will use.
COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts
RUN /arrow/ci/scripts/install_gcs_testbench.sh default

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

ENV ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=CONDA \
    ARROW_FLIGHT=ON \
    ARROW_FLIGHT_SQL=ON \
    ARROW_GANDIVA=ON \
    ARROW_GCS=ON \
    ARROW_HOME=$CONDA_PREFIX \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=ON \
    ARROW_S3=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    # Blocked on https://issues.apache.org/jira/browse/ARROW-15066
    ARROW_WITH_OPENTELEMETRY=OFF \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    GTest_SOURCE=BUNDLED \
    PARQUET_BUILD_EXAMPLES=ON \
    PARQUET_BUILD_EXECUTABLES=ON \
    PARQUET_HOME=$CONDA_PREFIX

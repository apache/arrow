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

ARG org
ARG arch
ARG conda
FROM ${org}/${arch}-conda-${conda}-cpp:latest

# install R specific packages
ARG r=3.6.1
COPY ci/conda_env_r.yml /arrow/ci/
RUN conda install -q \
        --file arrow/ci/conda_env_r.yml \
        r-base=$r \
        nomkl && \
    conda clean --all

# Ensure parallel compilation of each individual package
RUN printf "\nMAKEFLAGS=-j8\n" >> /opt/conda/lib/R/etc/Makeconf

ENV \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_GANDIVA=OFF \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=OFF \
    ARROW_USE_GLOG=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_R_DEV=TRUE

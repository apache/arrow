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

# General purpose Dockerfile to take a Docker image containing R
# and install Arrow R package dependencies

ARG base
FROM ${base}

ARG r_bin=R
ENV R_BIN=${r_bin}

ARG r_dev=FALSE
ENV ARROW_R_DEV=${r_dev}

ARG devtoolset_version=-1
ENV DEVTOOLSET_VERSION=${devtoolset_version}

ARG r_prune_deps=FALSE
ENV R_PRUNE_DEPS=${r_prune_deps}

ARG tz="UTC"
ENV TZ=${tz}

# Make sure R is on the path for the R-hub devel versions (where RPREFIX is set in its dockerfile)
ENV PATH "${RPREFIX}/bin:${PATH}"

# Patch up some of the docker images
COPY ci/scripts/r_docker_configure.sh /arrow/ci/scripts/
COPY ci/etc/rprofile /arrow/ci/etc/
COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/r_docker_configure.sh

# Set up Python 3 and its dependencies
RUN ln -s /usr/bin/python3 /usr/local/bin/python && \
    ln -s /usr/bin/pip3 /usr/local/bin/pip

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow

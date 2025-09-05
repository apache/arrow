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

ARG arch
ARG arch_short
ARG musllinux

ENV LINUX_WHEEL_KIND='musllinux'
ENV LINUX_WHEEL_VERSION=${musllinux}

RUN apk update
RUN apk add --no-cache \
    build-base \
    ccache \
    cmake \
    curl \
    flex \
    git \
    ninja \
    unzip \
    wget \
    zip
# Add mono from community repo because it's not in the main repo.
# We will be able to use the main repo once we move to alpine 3.22 or later.
RUN apk add --no-cache --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community mono

# A system Python is required for ninja and vcpkg in this Dockerfile.
# On musllinux_1_2 a system python is installed (3.12) but pip is not
# We therefore override the PATH with Python 3.10 in /opt/python
# so that we have a consistent Python version across base images
# as well as pip.
ENV CPYTHON_VERSION=cp310
ENV PATH=/opt/python/${CPYTHON_VERSION}-${CPYTHON_VERSION}/bin:${PATH}

# Install vcpkg
ARG vcpkg
COPY ci/vcpkg/*.patch \
     ci/vcpkg/*linux*.cmake \
     ci/vcpkg/vcpkg.json \
     arrow/ci/vcpkg/
COPY ci/scripts/install_vcpkg.sh \
     arrow/ci/scripts/
ENV VCPKG_ROOT=/opt/vcpkg
ARG build_type=release
ENV CMAKE_BUILD_TYPE=${build_type} \
    PATH="${PATH}:${VCPKG_ROOT}" \
    VCPKG_DEFAULT_TRIPLET=${arch_short}-linux-static-${build_type} \
    VCPKG_FEATURE_FLAGS="manifests" \
    VCPKG_FORCE_SYSTEM_BINARIES=1 \
    VCPKG_OVERLAY_TRIPLETS=/arrow/ci/vcpkg
# For --mount=type=secret: The GITHUB_TOKEN is the only real secret but we use
# --mount=type=secret for GITHUB_REPOSITORY_OWNER and
# VCPKG_BINARY_SOURCES too because we don't want to store them
# into the built image in order to easily reuse the built image cache.
#
# For vcpkg install: cannot use the S3 feature here because while
# aws-sdk-cpp=1.9.160 contains ssl related fixes as well as we can
# patch the vcpkg portfile to support arm machines it hits ARROW-15141
# where we would need to fall back to 1.8.186 but we cannot patch
# those portfiles since vcpkg-tool handles the checkout of previous
# versions => use bundled S3 build
RUN --mount=type=secret,id=github_repository_owner \
    --mount=type=secret,id=github_token \
    --mount=type=secret,id=vcpkg_binary_sources \
      export GITHUB_REPOSITORY_OWNER=$(cat /run/secrets/github_repository_owner); \
      export GITHUB_TOKEN=$(cat /run/secrets/github_token); \
      export VCPKG_BINARY_SOURCES=$(cat /run/secrets/vcpkg_binary_sources); \
      arrow/ci/scripts/install_vcpkg.sh ${VCPKG_ROOT} ${vcpkg} && \
      vcpkg install \
        --clean-after-build \
        --x-install-root=${VCPKG_ROOT}/installed \
        --x-manifest-root=/arrow/ci/vcpkg \
        --x-feature=azure \
        --x-feature=flight \
        --x-feature=gcs \
        --x-feature=json \
        --x-feature=orc \
        --x-feature=parquet \
        --x-feature=s3 && \
      rm -rf ~/.config/NuGet/

# Make sure auditwheel is up-to-date
RUN pipx upgrade auditwheel

# Configure Python for applications running in the bash shell of this Dockerfile
ARG python=3.10
ARG python_abi_tag=cp310
ENV PYTHON_VERSION=${python}
ENV PYTHON_ABI_TAG=${python_abi_tag}
RUN PYTHON_ROOT=$(find /opt/python -name cp${PYTHON_VERSION/./}-${PYTHON_ABI_TAG}) && \
    echo "export PATH=$PYTHON_ROOT/bin:\$PATH" >> /etc/profile.d/python.sh

SHELL ["/bin/bash", "-i", "-c", "-l"]
ENTRYPOINT ["/bin/bash", "-i", "-c", "-l"]

# Remove once there are released Cython wheels for 3.13 free-threaded available
RUN if [ "${python_abi_tag}" = "cp313t" ]; then \
      pip install cython --pre --extra-index-url "https://pypi.anaconda.org/scientific-python-nightly-wheels/simple" --prefer-binary ; \
    fi

COPY python/requirements-wheel-build.txt /arrow/python/
RUN pip install -r /arrow/python/requirements-wheel-build.txt

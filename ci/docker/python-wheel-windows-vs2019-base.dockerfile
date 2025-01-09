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

# NOTE: You must update PYTHON_WHEEL_WINDOWS_IMAGE_REVISION in .env
# when you update this file.

# based on mcr.microsoft.com/windows/servercore:ltsc2019
# contains choco and vs2019 preinstalled
FROM abrarov/msvc-2019:2.11.0

# Install CMake and Ninja
ARG cmake=3.31.2
RUN choco install --no-progress -r -y cmake --version=%cmake% --installargs 'ADD_CMAKE_TO_PATH=System' && \
    choco install --no-progress -r -y gzip wget ninja

# Add unix tools to path
RUN setx path "%path%;C:\Program Files\Git\usr\bin"

# Install vcpkg
#
# Compiling vcpkg itself from a git tag doesn't work anymore since vcpkg has
# started to ship precompiled binaries for the vcpkg-tool.
ARG vcpkg
COPY ci/vcpkg/*.patch \
     ci/vcpkg/*windows*.cmake \
     arrow/ci/vcpkg/
COPY ci/scripts/install_vcpkg.sh arrow/ci/scripts/
ENV VCPKG_ROOT=C:\\vcpkg
RUN bash arrow/ci/scripts/install_vcpkg.sh /c/vcpkg %vcpkg% && \
    setx PATH "%PATH%;%VCPKG_ROOT%"

# Configure vcpkg and install dependencies
# NOTE: use windows batch environment notation for build arguments in RUN
# statements but bash notation in ENV statements
# VCPKG_FORCE_SYSTEM_BINARIES=1 spare around ~750MB of image size if the system
# cmake's and ninja's versions are recent enough
ARG build_type=release
ENV CMAKE_BUILD_TYPE=${build_type} \
    VCPKG_OVERLAY_TRIPLETS=C:\\arrow\\ci\\vcpkg \
    VCPKG_DEFAULT_TRIPLET=amd64-windows-static-md-${build_type} \
    VCPKG_FEATURE_FLAGS="manifests"
COPY ci/vcpkg/vcpkg.json arrow/ci/vcpkg/
# cannot use the S3 feature here because while aws-sdk-cpp=1.9.160 contains
# ssl related fixes as well as we can patch the vcpkg portfile to support
# arm machines it hits ARROW-15141 where we would need to fall back to 1.8.186
# but we cannot patch those portfiles since vcpkg-tool handles the checkout of
# previous versions => use bundled S3 build
RUN vcpkg install \
        --clean-after-build \
        --x-install-root=%VCPKG_ROOT%\installed \
        --x-manifest-root=arrow/ci/vcpkg \
        --x-feature=flight \
        --x-feature=gcs \
        --x-feature=json \
        --x-feature=orc \
        --x-feature=parquet \
        --x-feature=s3

# Remove previous installations of Python from the base image
# NOTE: a more recent base image (tried with 2.12.1) comes with Python 3.9.7
# and the MSI installers are failing to remove pip and tcl/tk "products" making
# the subsequent choco python installation step failing for installing Python
# version 3.9.* due to existing Python version
RUN wmic product where "name like 'python%%'" call uninstall /nointeractive && \
    rm -rf Python*

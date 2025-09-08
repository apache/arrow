# escape=`

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


# NOTE: To build this Dockerfile, you probably need to do the following two
# things:
#
# 1. Increase your container image size to a higher value.
#
#  e.g.,
#
#    Set a custom 'storage-opts' value in your Windows Docker config and restart
#    Docker:
#
#        "storage-opts": [
#             "size=50GB"
#        ]
#
#    See
#
#       https://learn.microsoft.com/en-us/virtualization/windowscontainers/manage-containers/container-storage#example
#
#    for details on this step and
#
#       https://learn.microsoft.com/en-us/visualstudio/install/build-tools-container?view=vs-2022#troubleshoot-build-tools-containers
#
#    for more information.
#
# 2. Increase the memory limit for the build container to at least 4GB.
#
#  e.g.,
#
#     docker build -t sometag -m 4GB --file `
#       .\ci\docker\python-wheel-windows-vs2022-base.dockerfile .

# NOTE: You must update PYTHON_WHEEL_WINDOWS_IMAGE_REVISION in .env
# when you update this file.

FROM mcr.microsoft.com/windows/servercore:ltsc2022

# Ensure we in a command shell and not Powershell
SHELL ["cmd", "/S", "/C"]

# Install MSVC BuildTools
#
# The set of components below (lines starting with --add) is the most minimal
# set we could find that would still compile Arrow C++.
RUN `
  curl -SL --output vs_buildtools.exe https://aka.ms/vs/17/release/vs_buildtools.exe `
  && (start /w vs_buildtools.exe --quiet --wait --norestart --nocache `
  --installPath "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\BuildTools" `
  --add Microsoft.VisualStudio.Component.VC.CoreBuildTools `
  --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
  --add Microsoft.VisualStudio.Component.Windows11SDK.26100 `
  --add Microsoft.VisualStudio.Component.VC.CMake.Project `
  || IF "%ERRORLEVEL%"=="3010" EXIT 0) `
  && del /q vs_buildtools.exe

# Install choco CLI
#
# Switch into Powershell just for this command because choco only provides a
# Powershell installation script. After, we switch back to cmd.
#
# See https://chocolatey.org/install#completely-offline-install
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]
RUN `
  Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
SHELL ["cmd", "/S", "/C"]

# Install CMake and other tools
ARG cmake=3.31.2
RUN choco install --no-progress -r -y cmake --version=%cmake% --installargs 'ADD_CMAKE_TO_PATH=System'
RUN choco install --no-progress -r -y git gzip ninja wget

# Add UNIX tools to PATH
RUN setx path "%path%;C:\Program Files\Git\usr\bin"

# Install vcpkg
#
# Compiling vcpkg itself from a git tag doesn't work anymore since vcpkg has
# started to ship precompiled binaries for the vcpkg-tool.
ARG vcpkg
COPY ci/vcpkg/*.patch `
  ci/vcpkg/*windows*.cmake `
  arrow/ci/vcpkg/
COPY ci/scripts/install_vcpkg.sh arrow/ci/scripts/
ENV VCPKG_ROOT=C:\\vcpkg
RUN bash arrow/ci/scripts/install_vcpkg.sh /c/vcpkg %vcpkg% && `
  setx PATH "%PATH%;%VCPKG_ROOT%"

# Configure vcpkg and install dependencies
# NOTE: use windows batch environment notation for build arguments in RUN
# statements but bash notation in ENV statements
# VCPKG_FORCE_SYSTEM_BINARIES=1 spare around ~750MB of image size if the system
# cmake's and ninja's versions are recent enough
ARG build_type=release
ENV CMAKE_BUILD_TYPE=${build_type} `
  VCPKG_OVERLAY_TRIPLETS=C:\\arrow\\ci\\vcpkg `
  VCPKG_DEFAULT_TRIPLET=amd64-windows-static-md-${build_type} `
  VCPKG_FEATURE_FLAGS="manifests"
COPY ci/vcpkg/vcpkg.json arrow/ci/vcpkg/
# cannot use the S3 feature here because while aws-sdk-cpp=1.9.160 contains
# ssl related fixes as well as we can patch the vcpkg portfile to support
# arm machines it hits ARROW-15141 where we would need to fall back to 1.8.186
# but we cannot patch those portfiles since vcpkg-tool handles the checkout of
# previous versions => use bundled S3 build
RUN vcpkg install `
  --clean-after-build `
  --x-install-root=%VCPKG_ROOT%\installed `
  --x-manifest-root=arrow/ci/vcpkg `
  --x-feature=flight`
  --x-feature=gcs`
  --x-feature=json`
  --x-feature=orc`
  --x-feature=parquet`
  --x-feature=s3

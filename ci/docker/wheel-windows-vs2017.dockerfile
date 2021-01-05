# based on mcr.microsoft.com/windows/servercore:ltsc2019
# contains choco and vs2017 preinstalled
# TODO(kszucs): pin version
FROM abrarov/msvc-2017

# Install CMake and Ninja
RUN choco install --no-progress -r -y cmake --installargs 'ADD_CMAKE_TO_PATH=System' && \
    choco install --no-progress -r -y wget gzip ninja

# Install vcpkg
# ARG vcpkg=a2135fd97e834e83a705b1cff0d91a0e45a0fb00
ARG vcpkg=50ea8c0ab7aca3bb9245bba7fc877ad2f2a4464c
RUN git clone https://github.com/Microsoft/vcpkg && \
    git -C vcpkg checkout %vcpkg% && \
    vcpkg\bootstrap-vcpkg.bat -disableMetrics -win64 && \
    vcpkg\vcpkg.exe integrate install && \
    setx path "%path%;C:\vcpkg"

# Configure vcpkg and install dependencies
ARG build_type=release
ENV VCPKG_DEFAULT_TRIPLET=x64-windows-static-md \
    VCPKG_PLATFORM_TOOLSET=v141 \
    VCPKG_BUILD_TYPE=%build_type%

# could spare ~750MB with VCPKG_FORCE_SYSTEM_BINARIES=1
RUN vcpkg install --clean-after-build \
        abseil \
        aws-sdk-cpp[config,cognito-identity,core,identity-management,s3,sts,transfer] \
        boost-filesystem \
        boost-multiprecision \
        boost-regex \
        boost-system \
        brotli \
        bzip2 \
        c-ares \
        curl \
        flatbuffers \
        gflags \
        glog \
        grpc \
        lz4 \
        openssl \
        orc \
        protobuf \
        rapidjson \
        re2 \
        snappy \
        thrift \
        utf8proc \
        zlib \
        zstd

# Add unix tool to path
RUN setx path "%path%;C:\Program Files\Git\usr\bin"

# Define the full version number otherwise choco falls back to patch number 0 (3.7 => 3.7.0)
ARG python=3.6
RUN (if "%python%"=="3.6" setx PYTHON_VERSION 3.6.8) & \
    (if "%python%"=="3.7" setx PYTHON_VERSION 3.7.4) & \
    (if "%python%"=="3.8" setx PYTHON_VERSION 3.8.6) & \
    (if "%python%"=="3.9" setx PYTHON_VERSION 3.9.1)
# Remove preinstalled Python27 and Python37 then reinstall specific version using choco
RUN rm -rf Python* && \
    choco install -r -y --no-progress python --version=%PYTHON_VERSION%
RUN python -m pip install -U pip

COPY python/requirements-wheel-build.txt arrow/python/
RUN pip install -r arrow/python/requirements-wheel-build.txt

# TODO(kszucs): set clcache as the compiler
ENV CLCACHE_DIR="C:\clcache"
RUN pip install clcache

# For debugging purposes
# RUN wget --no-check-certificate https://github.com/lucasg/Dependencies/releases/download/v1.10/Dependencies_x64_Release.zip
# RUN unzip Dependencies_x64_Release.zip -d Dependencies && setx path "%path%;C:\Depencencies"

CMD arrow/ci/scripts/wheel_windows_build.bat

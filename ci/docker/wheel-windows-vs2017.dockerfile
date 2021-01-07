# based on mcr.microsoft.com/windows/servercore:ltsc2019
# contains choco and vs2017 preinstalled
# TODO(kszucs): pin version
FROM abrarov/msvc-2017

# Install CMake and Ninja
RUN choco install --no-progress -r -y cmake --installargs 'ADD_CMAKE_TO_PATH=System' && \
    choco install --no-progress -r -y gzip wget ninja

# Install vcpkg
ARG vcpkg=50ea8c0ab7aca3bb9245bba7fc877ad2f2a4464c
RUN git clone https://github.com/Microsoft/vcpkg && \
    git -C vcpkg checkout %vcpkg% && \
    vcpkg\bootstrap-vcpkg.bat -disableMetrics -win64 && \
    setx PATH "%PATH%;C:\vcpkg"

# Configure vcpkg and install dependencies
# NOTE: use windows batch environment notation for build arguments in RUN
# statements but bash notation in ENV statements
# VCPKG_FORCE_SYSTEM_BINARIES=1 spare around ~750MB of image size if the system
# cmake's and ninja's versions are recent enough
COPY ci/vcpkg arrow/ci/vcpkg
ARG build_type=release
ENV CMAKE_BUILD_TYPE=${build_type} \
    VCPKG_OVERLAY_TRIPLETS=C:\\arrow\\ci\\vcpkg \
    VCPKG_DEFAULT_TRIPLET=x64-windows-static-md-${build_type}
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

# Add unix tools to path
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

FROM mcr.microsoft.com/windows/servercore:ltsc2019

# Install Chocolatey
SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))
SHELL ["cmd", "/S", "/C"]

# Install VS 2017
ADD https://aka.ms/vs/15/release/vs_community.exe /
RUN vs_community.exe --quiet --norestart --wait --nocache \
        --includeRecommended \
        --add Microsoft.VisualStudio.Workload.NativeDesktop

# Install Git, unix tools and CMake
RUN choco install -y git --params "/GitAndUnixToolsOnPath"
RUN choco install -y cmake --installargs 'ADD_CMAKE_TO_PATH=System'
# RUN choco install -y gzip wget

# Install vcpkg
RUN git clone --branch 2020.04 https://github.com/Microsoft/vcpkg && \
    vcpkg\bootstrap-vcpkg.bat && \
    vcpkg\vcpkg.exe integrate install && \
    setx path "%path%;C:\vcpkg"

# Configure vcpkg and install dependencies
ENV VCPKG_DEFAULT_TRIPLET=x64-windows \
    VCPKG_PLATFORM_TOOLSET=v141

# Additional settings to consider
# VCPKG_LIBRARY_LINKAGE=dynamic
# VCPKG_CRT_LINKAGE=dynamic

# Install C++ dependencies
RUN vcpkg install --clean-after-build \
        boost-filesystem \
        boost-multiprecision \
        brotli \
        bzip2 \
        c-ares \
        flatbuffers \
        gflags \
        lz4 \
        openssl \
        rapidjson \
        re2 \
        snappy \
        thrift \
        zstd

ENV \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=ON \
    ARROW_BUILD_TYPE=debug \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_HOME=/usr \
    ARROW_VERBOSE_THIRDPARTY_BUILD=ON \
    CMAKE_GENERATOR="Visual Studio 15 2017 Win64" \
    CMAKE_TOOLCHAIN_FILE="C:\vcpkg\scripts\buildsystems\vcpkg.cmake" \
    ARROW_WITH_ZSTD=OFF \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_PARQUET=ON \
    ARROW_DATASET=ON \
    ARROW_HDFS=ON

# RUN ["bash", "-c", "/c/arrow/ci/scripts/cpp_build.sh /c/arrow /c/build && /c/arrow/ci/scripts/cpp_test.sh /c/arrow /c/build"]

# arrow/ci/scripts/cpp_build.sh /c/arrow /c/build2

# ENV ARROW_BUILD_TESTS=ON \
#     ARROW_CXXFLAGS="//WX //MP" \
#     ARROW_DATASET=OFF \
#     ARROW_FLIGHT=OFF \
#     ARROW_GANDIVA=OFF \
#     ARROW_ORC=ON \
#     ARROW_PARQUET=OFF \
#     ARROW_S3=OFF \
#     ARROW_JEMALLOC=OFF \
#     ARROW_WITH_BROTLI=OFF \
#     ARROW_WITH_SNAPPY=OFF \
#     ARROW_BUILD_STATIC=OFF \
#     PARQUET_BUILD_EXECUTABLES=ON \
#     PARQUET_BUILD_EXAMPLES=ON \
#     CMAKE_BUILD_TYPE="Release" \
#     CMAKE_GENERATOR="Visual Studio 15 2017 Win64" \
#     CMAKE_TOOLCHAIN_FILE="C:\vcpkg\scripts\buildsystems\vcpkg.cmake" \
#     CXXFLAGS="//MD"

# refreshenv
# RUN C:\"Program Files (x86)"\"Microsoft Visual Studio"\2017\Community\VC\Auxiliary\Build\vcvarsall.bat x64
# C:\"Program Files (x86)"\"Microsoft Visual Studio"\2017\BuildTools\VC\Auxiliary\Build\vcvarsall.bat x64
# TODO: have both debug and release builds
# TODO: have static build

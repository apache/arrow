FROM mcr.microsoft.com/windows/servercore:ltsc2019

# Install Chocolatey
SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))
SHELL ["cmd", "/S", "/C"]

# Install Git, unix tools and CMake
RUN choco install -y git --params "/GitAndUnixToolsOnPath" && \
    choco install -y cmake --installargs 'ADD_CMAKE_TO_PATH=System' && \
    choco install -y wget gzip

# Install VS 2019
RUN wget https://aka.ms/vs/16/release/vs_community.exe
RUN vs_community.exe --quiet --norestart --wait --nocache \
        --includeRecommended \
        --add Microsoft.VisualStudio.Workload.NativeDesktop

# Install vcpkg
RUN git clone --branch 2020.04 https://github.com/Microsoft/vcpkg && \
    vcpkg\bootstrap-vcpkg.bat && \
    vcpkg\vcpkg.exe integrate install && \
    setx path "%path%;C:\vcpkg"

# Configure vcpkg and install dependencies
ENV VCPKG_DEFAULT_TRIPLET=x64-windows \
    VCPKG_PLATFORM_TOOLSET=v142

# Install C++ dependencies
RUN vcpkg install --clean-after-build \
        boost-filesystem \
        boost-multiprecision \
        boost-process \
        brotli \
        bzip2 \
        c-ares \
        flatbuffers \
        gflags \
        grpc \
        lz4 \
        openssl \
        rapidjson \
        re2 \
        snappy \
        thrift \
        zstd

# RUN wget https://github.com/lucasg/Dependencies/releases/download/v1.10/Dependencies_x64_Release.zip
# RUN unzip Dependencies_x64_Release.zip -d Dependencies

# TODO(kszucs): enable dataset
ENV ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=ON \
    ARROW_BUILD_TYPE=debug \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FILESYSTEM=ON \
    ARROW_FLIGHT=ON \
    ARROW_HDFS=ON \
    ARROW_HOME=/usr \
    ARROW_PARQUET=ON \
    ARROW_VERBOSE_THIRDPARTY_BUILD=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_LZ4=OFF \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZSTD=ON \
    CMAKE_GENERATOR="Visual Studio 16 2019" \
    CMAKE_TOOLCHAIN_FILE="C:\vcpkg\scripts\buildsystems\vcpkg.cmake"

# OK
# VCPKG_LIBRARY_LINKAGE=dynamic \
# VCPKG_CRT_LINKAGE=dynamic

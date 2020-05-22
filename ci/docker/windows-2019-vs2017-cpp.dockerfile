FROM mcr.microsoft.com/windows/servercore:ltsc2019

# Install Chocolatey
SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))
SHELL ["cmd", "/S", "/C"]

# Install Git, unix tools and CMake
RUN choco install -y git --params "/GitAndUnixToolsOnPath" && \
    choco install -y cmake --installargs 'ADD_CMAKE_TO_PATH=System' && \
    choco install -y wget gzip

# Install VS 2017 using the 2019 installer
RUN wget --no-check-certificate https://aka.ms/vs/16/release/vs_community.exe
RUN vs_community.exe --quiet --norestart --wait --nocache \
        --includeRecommended \
        --add Microsoft.VisualStudio.Workload.NativeDesktop \
        --add Microsoft.VisualStudio.Component.VC.v141.x86.x64

# Install vcpkg
RUN git clone --branch 2020.04 https://github.com/Microsoft/vcpkg && \
    vcpkg\bootstrap-vcpkg.bat && \
    vcpkg\vcpkg.exe integrate install && \
    setx path "%path%;C:\vcpkg"

# Configure vcpkg and install dependencies
ENV VCPKG_DEFAULT_TRIPLET=x64-windows \
    VCPKG_PLATFORM_TOOLSET=v141

# Install C++ dependencies
RUN vcpkg install --clean-after-build \
        boost-filesystem \
        boost-multiprecision \
        boost-process \
        grpc \
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

# RUN wget https://github.com/lucasg/Dependencies/releases/download/v1.10/Dependencies_x64_Release.zip
# RUN unzip Dependencies_x64_Release.zip -d Dependencies

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
    CMAKE_GENERATOR="Visual Studio 15 2017 Win64" \
    CMAKE_TOOLCHAIN_FILE="C:\vcpkg\scripts\buildsystems\vcpkg.cmake" \
    CMAKE_UNITY_BUILD=ON

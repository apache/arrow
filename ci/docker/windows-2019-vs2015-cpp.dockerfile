FROM mcr.microsoft.com/windows/servercore:ltsc2019

# Install Chocolatey
SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))
SHELL ["cmd", "/S", "/C"]

# Install VS 2015
ADD https://aka.ms/vs/15/release/vs_community.exe /
RUN vs_community.exe --quiet --norestart --wait --nocache \
        --includeRecommended \
        --add Microsoft.VisualStudio.Workload.NativeDesktop \
        --add Microsoft.VisualStudio.Component.VC.140

# ADD https://go.microsoft.com/fwlink/?LinkId=532606&clcid=0x409 /vs_community.exe
# RUN vs_community.exe /quiet /norestart /SuppressRefreshPrompt /NoCacheOnlyMode /AdminFile

# Install Git, unix tools and CMake
RUN choco install -y git --params "/GitAndUnixToolsOnPath"
RUN choco install -y cmake --installargs 'ADD_CMAKE_TO_PATH=System'

# Install vcpkg
RUN git clone --branch 2020.04 https://github.com/Microsoft/vcpkg && \
    vcpkg\bootstrap-vcpkg.bat && \
    vcpkg\vcpkg.exe integrate install && \
    setx path "%path%;C:\vcpkg"

# # Configure vcpkg and install dependencies
# ENV VCPKG_DEFAULT_TRIPLET=x64-windows \
#     VCPKG_PLATFORM_TOOLSET=v140

# # Install C++ dependencies
# RUN vcpkg install --clean-after-build \
#         boost-filesystem \
#         brotli \
#         bzip2 \
#         c-ares \
#         flatbuffers \
#         gflags \
#         lz4 \
#         openssl \
#         rapidjson \
#         re2 \
#         snappy \
#         thrift \
#         zstd

# ENV ARROW_BUILD_TESTS=ON \
#     ARROW_DATASET=ON \
#     ARROW_FLIGHT=OFF \
#     ARROW_GANDIVA=OFF \
#     ARROW_ORC=ON \
#     ARROW_PARQUET=ON \
#     ARROW_S3=OFF \
#     ARROW_JEMALLOC=OFF \
#     ARROW_WITH_BROTLI=OFF \
#     ARROW_WITH_BZ2=ON \
#     ARROW_WITH_LZ4=ON \
#     ARROW_WITH_SNAPPY=OFF \
#     ARROW_WITH_ZLIB=ON \
#     ARROW_WITH_ZSTD=ON \
#     ARROW_BUILD_STATIC=OFF \
#     PARQUET_BUILD_EXECUTABLES=ON \
#     PARQUET_BUILD_EXAMPLES=ON \
#     CMAKE_GENERATOR="Visual Studio 15 2017 Win64" \
#     CMAKE_TOOLCHAIN_FILE="C:\vcpkg\scripts\buildsystems\vcpkg.cmake"

# refreshenv
# RUN C:\"Program Files (x86)"\"Microsoft Visual Studio"\2017\Community\VC\Auxiliary\Build\vcvarsall.bat x64
# C:\"Program Files (x86)"\"Microsoft Visual Studio"\2017\BuildTools\VC\Auxiliary\Build\vcvarsall.bat x64

# # Install Chocolatey
# SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
# RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))
# SHELL ["cmd", "/S", "/C"]

# # Install VS2015
# RUN choco install -y visualcppbuildtools
# RUN C:\"Program Files (x86)"\"Microsoft Visual Studio 14.0"\VC\vcvarsall.bat amd64

# # Install Git and CMake
# RUN choco install -y git --params "/GitAndUnixToolsOnPath"
# RUN choco install -y cmake --installargs 'ADD_CMAKE_TO_PATH=System'

# # Install vcpkg
# RUN git clone https://github.com/Microsoft/vcpkg && \
#     cd vcpkg && \
#     git checkout 2020.04 && \
#     .\bootstrap-vcpkg.bat && \
#     .\vcpkg.exe integrate install
# RUN setx path "%path%;C:\vcpkg"

# # Configure vcpkg and install dependencies
# ENV VCPKG_DEFAULT_TRIPLET=x64-windows \
#     VCPKG_PLATFORM_TOOLSET=v140

# # Install C++ dependencies
# RUN vcpkg install openssl
# RUN vcpkg install thrift
# RUN vcpkg install flatbuffers
# RUN vcpkg install re2
# RUN vcpkg install c-ares
# RUN vcpkg install brotli
# RUN vcpkg install bzip2
# RUN vcpkg install snappy
# RUN vcpkg install lz4
# RUN vcpkg install zstd
# RUN vcpkg install rapidjson
# RUN vcpkg install gflags
# RUN vcpkg install boost-filesystem

# RUN choco install -y python
# RUN choco install -y wget

# ENV ARROW_BUILD_TESTS=ON \
#     ARROW_DATASET=ON \
#     ARROW_FLIGHT=OFF \
#     ARROW_GANDIVA=OFF \
#     ARROW_ORC=ON \
#     ARROW_PARQUET=ON \
#     ARROW_S3=OFF \
#     ARROW_JEMALLOC=OFF \
#     ARROW_WITH_BROTLI=OFF \
#     ARROW_WITH_BZ2=ON \
#     ARROW_WITH_LZ4=ON \
#     ARROW_WITH_SNAPPY=OFF \
#     ARROW_WITH_ZLIB=ON \
#     ARROW_WITH_ZSTD=ON \
#     ARROW_BUILD_STATIC=OFF \
#     PARQUET_BUILD_EXECUTABLES=ON \
#     PARQUET_BUILD_EXAMPLES=ON \
#     CMAKE_GENERATOR="Visual Studio 14 2015 Win64" \
#     CMAKE_ARGS="-DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake"

# # ENTRYPOINT [ "bash", "-c" ]
# # CMD "/c/arrow/ci/scripts/cpp_build.sh /c/arrow /c/build && /c/arrow/ci/scripts/cpp_test.sh /c/arrow /c/build"
# # cmake -DCMAKE_TOOLCHAIN_FILE=/c/vcpkg/scripts/buildsystems/vcpkg.cmake -G "Visual Studio 14 2015 Win64" -DARROW_BUILD_TESTS=ON ../arrow/cpp

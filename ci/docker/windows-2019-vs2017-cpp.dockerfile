FROM mcr.microsoft.com/windows/servercore:ltsc2019

# Install Chocolatey
SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))

# Download VS 2019 Build Tools and install the 14.16 Version (VS 2017)
RUN Invoke-WebRequest 'https://aka.ms/vs/16/release/vs_buildtools.exe' -OutFile 'vs_buildtools.exe'

SHELL ["cmd", "/S", "/C"]
RUN vs_buildtools.exe --quiet --wait --norestart --nocache \
    --installPath C:\BuildTools \
    --add "Microsoft.VisualStudio.Workload.VCTools;includeRecommended" \
    --add Microsoft.VisualStudio.Component.VC.v141 \
    --add Microsoft.VisualStudio.Component.VC.v141.x86.x64 \
    --add Microsoft.VisualStudio.Component.Windows10SDK.17763 \
    --remove Microsoft.VisualStudio.Component.Windows10SDK.10240 \
    --remove Microsoft.VisualStudio.Component.Windows10SDK.10586 \
    --remove Microsoft.VisualStudio.Component.Windows10SDK.14393 \
    --remove Microsoft.VisualStudio.Component.Windows81SDK

# Install Git and CMake
RUN choco install -y git --params "/GitAndUnixToolsOnPath"
RUN choco install -y cmake --installargs 'ADD_CMAKE_TO_PATH=System'

# Install vcpkg
RUN git clone https://github.com/Microsoft/vcpkg && \
    cd vcpkg && \
    git checkout 2020.04 && \
    .\bootstrap-vcpkg.bat && \
    .\vcpkg.exe integrate install
RUN setx path "%path%;C:\vcpkg"

# Configure vcpkg and install dependencies
ENV VCPKG_DEFAULT_TRIPLET=x64-windows \
    VCPKG_PLATFORM_TOOLSET=v141

# Install C++ dependencies
RUN vcpkg install boost
RUN vcpkg install thrift
RUN vcpkg install flatbuffers
RUN vcpkg install re2
RUN vcpkg install c-ares
RUN vcpkg install brotli
RUN vcpkg install bzip2
RUN vcpkg install snappy
RUN vcpkg install lz4
RUN vcpkg install zstd
RUN vcpkg install rapidjson
RUN vcpkg install gflags
RUN vcpkg install gtest

ENV ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_S3=OFF \
    ARROW_WITH_BROTLI=OFF \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_SNAPPY=OFF \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    ARROW_BUILD_STATIC=OFF \
    PARQUET_BUILD_EXECUTABLES=ON \
    PARQUET_BUILD_EXAMPLES=ON \
    CMAKE_GENERATOR="Visual Studio 14 2015 Win64" \
    CMAKE_ARGS="-DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake"

CMD ["bash", "-c", "/c/arrow/ci/scripts/cpp_build.sh /c/arrow /c/build"]

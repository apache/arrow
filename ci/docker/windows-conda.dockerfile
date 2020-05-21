FROM mcr.microsoft.com/dotnet/framework/sdk:4.8-windowsservercore-ltsc2019

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

  # Install Chocolatey
 SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
 RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))

  # Install Git and Bash
 RUN choco install -y git.install --params "/GitAndUnixToolsOnPath /NoGitLfs /SChannel /NoAutoCrlf"

  # Install Conda
 RUN choco install -y miniconda3 --params="'/AddToPath:1'"
 RUN conda config --add channels conda-forge; \
     conda config --set channel_priority strict; \
     conda init bash

  # "C:\BuildTools\VC\Auxiliary\Build\vcvars64.bat", "-vcvars_ver=14.16", "10.0.17763.0"

  COPY ci/conda_env_cpp.yml \
      ci/conda_env_gandiva.yml \
      /arrow/ci/
 RUN conda create -n arrow -q -c conda-forge \
         --file arrow/ci/conda_env_cpp.yml \
         --file arrow/ci/conda_env_gandiva.yml; \
     conda clean --all

  ENV ARROW_BUILD_TESTS=ON \
     ARROW_DEPENDENCY_SOURCE=CONDA \
     ARROW_DATASET=ON \
     ARROW_FLIGHT=OFF \
     ARROW_GANDIVA=OFF \
     ARROW_ORC=ON \
     ARROW_PARQUET=ON \
     ARROW_PLASMA=ON \
     ARROW_S3=ON \
     ARROW_WITH_BROTLI=OFF \
     ARROW_WITH_BZ2=ON \
     ARROW_WITH_LZ4=ON \
     ARROW_WITH_SNAPPY=OFF \
     ARROW_WITH_ZLIB=ON \
     ARROW_WITH_ZSTD=ON \
     ARROW_BUILD_STATIC=OFF \
     PARQUET_BUILD_EXECUTABLES=ON \
     PARQUET_BUILD_EXAMPLES=ON \
     CMAKE_GENERATOR="Visual Studio 15 2017" \
     CMAKE_ARGS="-A x64"

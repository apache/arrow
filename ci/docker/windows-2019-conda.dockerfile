FROM mcr.microsoft.com/windows/servercore:ltsc2019

# Install Chocolatey
SHELL ["powershell.exe", "-NoLogo", "-ExecutionPolicy", "Bypass", "-Command"]
RUN (iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1')))
SHELL ["cmd", "/S", "/C"]

# Install Git and Bash
RUN choco install -y git.install --params "/GitAndUnixToolsOnPath /NoGitLfs /SChannel /NoAutoCrlf"

# Install Conda
RUN choco install -y miniconda3 --params="'/AddToPath:1'"

RUN conda init && \
    conda config --add channels conda-forge && \
    conda config --set channel_priority strict

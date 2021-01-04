# This base image has choco installed
# TODO(kszucs): pin version
# FROM abrarov/windows-dev
FROM abrarov/msvc-2017

SHELL ["cmd", "/S", "/C"]

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

# Utility for inspecting linking errors
# ADD https://github.com/lucasg/Dependencies/releases/download/v1.10/Dependencies_x64_Release.zip Dependencies.zip
# RUN python -c "import zipfile; zf = zipfile.ZipFile('Dependencies.zip', 'r'); zf.extractall('Dependencies')"
# RUN setx path "%path%;C:\Depencencies"

CMD arrow/ci/scripts/wheel_windows_test.bat

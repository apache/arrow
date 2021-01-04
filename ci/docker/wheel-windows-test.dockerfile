# This base image has choco installed
# TODO(kszucs): pin version
# FROM abrarov/windows-dev
FROM abrarov/msvc-2017

SHELL ["cmd", "/S", "/C"]

ARG python=3.6.8
RUN setx PYTHON_VERSION %python% && \
    choco install -r -y --no-progress --force python --version=%python%
RUN python -m pip install -U pip

# Utility for inspecting linking errors
# ADD https://github.com/lucasg/Dependencies/releases/download/v1.10/Dependencies_x64_Release.zip Dependencies.zip
# RUN python -c "import zipfile; zf = zipfile.ZipFile('Dependencies.zip', 'r'); zf.extractall('Dependencies')"
# RUN setx path "%path%;C:\Depencencies"

CMD arrow/ci/scripts/wheel_windows_test.bat

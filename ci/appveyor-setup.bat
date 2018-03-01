@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem   http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing,
@rem software distributed under the License is distributed on an
@rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem KIND, either express or implied.  See the License for the
@rem specific language governing permissions and limitations
@rem under the License.

@echo on

conda update -y -q conda
conda config --set auto_update_conda false
conda info -a

conda config --set show_channel_urls True

@rem Help with SSL timeouts to S3
conda config --set remote_connect_timeout_secs 12

conda config --add channels https://repo.continuum.io/pkgs/free
conda config --add channels conda-forge
conda info -a

if "%GENERATOR%"=="NMake Makefiles" set need_vcvarsall=1
if "%GENERATOR%"=="Ninja" set need_vcvarsall=1

if defined need_vcvarsall (
    @rem Select desired compiler version
    if "%APPVEYOR_BUILD_WORKER_IMAGE%" == "Visual Studio 2017" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvarsall.bat" amd64
    ) else (
        call "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64
    )
)

if "%GENERATOR%"=="Ninja" conda install -y -q -c conda-forge ninja

if "%USE_CLCACHE%" == "true" (
    @rem Use clcache for faster builds
    pip install -q git+https://github.com/frerich/clcache.git
    clcache -s
    set CLCACHE_SERVER=1
    set CLCACHE_HARDLINK=1
    powershell.exe -Command "Start-Process clcache-server"
)

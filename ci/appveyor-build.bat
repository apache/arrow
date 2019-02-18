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

IF /i "%JOB%" == "C#" goto csharp
IF /i "%JOB%" == "rust" goto rust

@rem All other builds are C++ builds
goto cpp

:cpp
git config core.symlinks true
git reset --hard
if "%JOB:~,5%" == "MinGW" (
    call ci\appveyor-cpp-build-mingw.bat
) else (
    call ci\appveyor-cpp-build.bat
)
goto scriptexit

:csharp
call ci\appveyor-csharp-build.bat
goto scriptexit

:rust
call ci\rust-build-main.bat
goto scriptexit

:scriptexit

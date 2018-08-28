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

if "%JOB%" == "Rust" (
    pushd rust

    rustup default stable
    rustup show
    cargo build --target %TARGET% || exit /B
    cargo build --target %TARGET% --release || exit /B
    cargo test --target %TARGET% || exit /B
    cargo test --target %TARGET% --release || exit /B

    rustup default nightly
    rustup show
    cargo build --target %TARGET%
    cargo build --target %TARGET% --release
    cargo test --target %TARGET%
    cargo test --target %TARGET% --release

    rustup default stable

    popd
) else (
    git config core.symlinks true
    git reset --hard
    if "%JOB%"=="Cmake_Script_Tests" (
        call ci\test-cmake-build-script.bat
    ) else (
        call ci\cpp-python-msvc-build.bat
    )
)

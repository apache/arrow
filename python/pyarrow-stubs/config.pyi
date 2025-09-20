# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import NamedTuple


class VersionInfo(NamedTuple):
    major: int
    minor: int
    patch: int


class BuildInfo(NamedTuple):
    version: str
    version_info: VersionInfo
    so_version: str
    full_so_version: str
    compiler_id: str
    compiler_version: str
    compiler_flags: str
    git_id: str
    git_description: str
    package_kind: str
    build_type: str


class RuntimeInfo(NamedTuple):
    simd_level: str
    detected_simd_level: str


cpp_build_info: BuildInfo
cpp_version: str
cpp_version_info: VersionInfo


def runtime_info() -> RuntimeInfo: ...
def set_timezone_db_path(path: str) -> None: ...


__all__ = [
    "VersionInfo",
    "BuildInfo",
    "RuntimeInfo",
    "cpp_build_info",
    "cpp_version",
    "cpp_version_info",
    "runtime_info",
    "set_timezone_db_path",
]

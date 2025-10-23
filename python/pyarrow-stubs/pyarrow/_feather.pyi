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

from typing import IO, Sequence

from _typeshed import StrPath

from .lib import Buffer, NativeFile, Table, _Weakrefable


class FeatherError(Exception):
    ...


def write_feather(
    table: Table,
    dest: StrPath | IO | NativeFile,
    compression: str | None = None,
    compression_level: int | None = None,
    chunksize: int | None = None,
    version: int = 2,
): ...


class FeatherReader(_Weakrefable):
    def __init__(
        self,
        source: StrPath | IO | NativeFile | Buffer,
        use_memory_map: bool,
        use_threads: bool,
    ) -> None: ...
    @property
    def version(self) -> str: ...
    def read(self) -> Table: ...
    def read_indices(self, indices: Sequence[int]) -> Table: ...
    def read_names(self, names: Sequence[str]) -> Table: ...

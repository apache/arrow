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

from typing import IO, Any, Literal

from _typeshed import StrPath

from .lib import MemoryPool, RecordBatchReader, Schema, Table, _Weakrefable


class ReadOptions(_Weakrefable):

    use_threads: bool

    block_size: int

    def __init__(self, use_threads: bool | None = None,
                 block_size: int | None = None): ...

    def equals(self, other: ReadOptions) -> bool: ...


class ParseOptions(_Weakrefable):

    explicit_schema: Schema

    newlines_in_values: bool

    unexpected_field_behavior: Literal["ignore", "error", "infer"]

    def __init__(
        self,
        explicit_schema: Schema | None = None,
        newlines_in_values: bool | None = None,
        unexpected_field_behavior: Literal["ignore", "error", "infer"] = "infer",
    ): ...
    def equals(self, other: ParseOptions) -> bool: ...


class JSONStreamingReader(RecordBatchReader):
    ...


def read_json(
    input_file: StrPath | IO[Any],
    read_options: ReadOptions | None = None,
    parse_options: ParseOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Table: ...


def open_json(
    input_file: StrPath | IO[Any],
    read_options: ReadOptions | None = None,
    parse_options: ParseOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> JSONStreamingReader: ...

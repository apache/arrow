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

from dataclasses import dataclass, field
from typing import IO, Any, Callable, Literal

from _typeshed import StrPath

from . import lib


@dataclass(kw_only=True)
class ReadOptions(lib._Weakrefable):
    use_threads: bool = field(default=True, kw_only=False)
    block_size: int | None = None
    skip_rows: int = 0
    skip_rows_after_names: int = 0
    column_names: list[str] | None = None
    autogenerate_column_names: bool = False
    encoding: str = "utf8"
    def validate(self) -> None: ...


@dataclass(kw_only=True)
class ParseOptions(lib._Weakrefable):

    delimiter: str = field(default=",", kw_only=False)
    quote_char: str | Literal[False] = '"'
    double_quote: bool = True
    escape_char: str | Literal[False] = False
    newlines_in_values: bool = False
    ignore_empty_lines: bool = True
    invalid_row_handler: Callable[[InvalidRow], Literal["skip", "error"]] | None = None

    def validate(self) -> None: ...


@dataclass(kw_only=True)
class ConvertOptions(lib._Weakrefable):

    check_utf8: bool = field(default=True, kw_only=False)
    column_types: lib.Schema | dict | None = None
    null_values: list[str] | None = None
    true_values: list[str] | None = None
    false_values: list[str] | None = None
    decimal_point: str = "."
    strings_can_be_null: bool = False
    quoted_strings_can_be_null: bool = True
    include_columns: list[str] | None = None
    include_missing_columns: bool = False
    auto_dict_encode: bool = False
    auto_dict_max_cardinality: int | None = None
    timestamp_parsers: list[str] | None = None

    def validate(self) -> None: ...


@dataclass(kw_only=True)
class WriteOptions(lib._Weakrefable):

    include_header: bool = field(default=True, kw_only=False)
    batch_size: int = 1024
    delimiter: str = ","
    quoting_style: Literal["needed", "all_valid", "none"] = "needed"

    def validate(self) -> None: ...


@dataclass
class InvalidRow(lib._Weakrefable):

    expected_columns: int
    actual_columns: int
    number: int | None
    text: str


class CSVWriter(lib._CRecordBatchWriter):

    def __init__(
        self,
        # TODO: OutputStream
        sink: StrPath | IO[Any],
        schema: lib.Schema,
        write_options: WriteOptions | None = None,
        *,
        memory_pool: lib.MemoryPool | None = None,
    ) -> None: ...


class CSVStreamingReader(lib.RecordBatchReader):
    ...


ISO8601: lib._Weakrefable


def open_csv(
    input_file: StrPath | IO[Any],
    read_options: ReadOptions | None = None,
    parse_options: ParseOptions | None = None,
    convert_options: ConvertOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> CSVStreamingReader: ...


def read_csv(
    input_file: StrPath | IO[Any],
    read_options: ReadOptions | None = None,
    parse_options: ParseOptions | None = None,
    convert_options: ConvertOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Table: ...


def write_csv(
    data: lib.RecordBatch | lib.Table,
    output_file: StrPath | lib.NativeFile | IO[Any],
    write_options: WriteOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> None: ...

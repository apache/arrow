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

import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias
from typing import Literal

from . import lib
from .compute import Expression, FunctionOptions
from .dataset import InMemoryDataset, Dataset

_StrOrExpr: TypeAlias = str | Expression


class Declaration(lib._Weakrefable):
    def __init__(
        self,
        factory_name: str,
        options: ExecNodeOptions,
        inputs: list[Declaration] | None = None,
    ) -> None: ...
    @classmethod
    def from_sequence(cls, decls: Iterable[Declaration]) -> Self: ...
    def to_reader(self, use_threads: bool = True) -> lib.RecordBatchReader: ...
    def to_table(self, use_threads: bool = True) -> lib.Table: ...


class ExecNodeOptions(lib._Weakrefable):
    ...


class TableSourceNodeOptions(ExecNodeOptions):
    def __init__(self, table: lib.Table) -> None: ...


class FilterNodeOptions(ExecNodeOptions):
    def __init__(self, filter_expression: Expression) -> None: ...


class ProjectNodeOptions(ExecNodeOptions):
    def __init__(self, expressions: Collection[Expression],
                 names: Collection[str] | None = None) -> None: ...


class AggregateNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        aggregates: list[tuple[list[str], str, FunctionOptions, str]],
        keys: Iterable[_StrOrExpr] | None = None,
    ) -> None: ...


class OrderByNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        sort_keys: Iterable[tuple[str, Literal["ascending", "descending"]], ...] = (),
        *,
        null_placement: Literal["at_start", "at_end"] = "at_end",
    ) -> None: ...


class HashJoinNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        join_type: Literal[
            "left semi",
            "right semi",
            "left anti",
            "right anti",
            "inner",
            "left outer",
            "right outer",
            "full outer",
        ],
        left_keys: _StrOrExpr | list[_StrOrExpr],
        right_keys: _StrOrExpr | list[_StrOrExpr],
        left_output: list[_StrOrExpr] | None = None,
        right_output: list[_StrOrExpr] | None = None,
        output_suffix_for_left: str = "",
        output_suffix_for_right: str = "",
    ) -> None: ...


class AsofJoinNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        left_on: _StrOrExpr,
        left_by: _StrOrExpr | list[_StrOrExpr],
        right_on: _StrOrExpr,
        right_by: _StrOrExpr | list[_StrOrExpr],
        tolerance: int,
    ) -> None: ...


def _perform_join(
    join_type: str,
    left_operand: lib.Table | Dataset,
    left_keys: str | list[str],
    right_operand: lib.Table | Dataset,
    right_keys: str | list[str],
    left_suffix: str,
    right_suffix: str,
    use_threads: bool,
    coalesce_keys: bool,
    output_type: type[lib.Table | InMemoryDataset] = lib.Table,
    filter_expression: Expression | None = None,
) -> lib.Table | InMemoryDataset: ...


def _filter_table(
    table: lib.Table | lib.RecordBatch, filter_expression: Expression,
    use_threads: bool = True) -> lib.Table | lib.RecordBatch: ...

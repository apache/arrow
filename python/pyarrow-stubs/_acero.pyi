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

from collections.abc import Collection, Iterable, Sequence
from typing import Literal, TypeAlias

# Placeholder (Any) until ipc.pyi is annotated; mypy rejects it as a type
# when imported through pyarrow.lib.
from pyarrow.__lib.ipc import RecordBatchReader
from pyarrow.__lib.table import _AggregateOptions, _AggregationName
from pyarrow._stubs_typing import NullPlacement, Order
from pyarrow.lib import Table, _Weakrefable

from pyarrow.compute import Expression

_StrOrExpr: TypeAlias = str | Expression
_IntoField: TypeAlias = str | int | Expression
_Target: TypeAlias = _IntoField | tuple[_IntoField, ...] | Sequence[_IntoField]
_Aggregate: TypeAlias = tuple[_Target, _AggregationName, _AggregateOptions | None, str]
_SortKey: TypeAlias = (
    tuple[str | Expression | int, Order]
    | tuple[str | Expression | int, Order, NullPlacement]
)
_JoinType: TypeAlias = Literal[
    "left semi",
    "right semi",
    "left anti",
    "right anti",
    "inner",
    "left outer",
    "right outer",
    "full outer",
]

class Declaration(_Weakrefable):
    def __init__(
        self,
        factory_name: str,
        options: ExecNodeOptions,
        inputs: list[Declaration] | None = None,
    ) -> None: ...
    @staticmethod
    def from_sequence(decls: Iterable[Declaration]) -> Declaration: ...
    def to_reader(self, use_threads: bool = True) -> RecordBatchReader: ...
    def to_table(self, use_threads: bool = True) -> Table: ...

class ExecNodeOptions(_Weakrefable): ...

class TableSourceNodeOptions(ExecNodeOptions):
    def __init__(self, table: Table) -> None: ...

class RecordBatchReaderSourceNodeOptions(ExecNodeOptions):
    def __init__(self, reader: RecordBatchReader) -> None: ...

class FilterNodeOptions(ExecNodeOptions):
    def __init__(self, filter_expression: Expression) -> None: ...

class ProjectNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        expressions: Collection[Expression],
        names: Collection[str] | None = None,
    ) -> None: ...

class AggregateNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        aggregates: Iterable[_Aggregate],
        keys: Iterable[str | Expression] | None = None,
    ) -> None: ...

class OrderByNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        sort_keys: Iterable[_SortKey] = (),
        *,
        # Deprecated: specify the null placement per sort key instead.
        null_placement: NullPlacement | None = None,
    ) -> None: ...

class HashJoinNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        join_type: _JoinType,
        left_keys: _StrOrExpr | Sequence[_StrOrExpr],
        right_keys: _StrOrExpr | Sequence[_StrOrExpr],
        left_output: Sequence[_StrOrExpr] | None = None,
        right_output: Sequence[_StrOrExpr] | None = None,
        output_suffix_for_left: str = "",
        output_suffix_for_right: str = "",
        filter_expression: Expression | None = None,
    ) -> None: ...

class AsofJoinNodeOptions(ExecNodeOptions):
    def __init__(
        self,
        left_on: _StrOrExpr,
        left_by: _StrOrExpr | Sequence[_StrOrExpr],
        right_on: _StrOrExpr,
        right_by: _StrOrExpr | Sequence[_StrOrExpr],
        tolerance: int,
    ) -> None: ...

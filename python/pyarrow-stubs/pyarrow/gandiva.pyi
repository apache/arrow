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

from collections.abc import Iterable
from typing import Literal

from .lib import Array, DataType, Field, MemoryPool, RecordBatch, Schema, _Weakrefable


class Node(_Weakrefable):
    def return_type(self) -> DataType: ...


class Expression(_Weakrefable):
    def root(self) -> Node: ...
    def result(self) -> Field: ...


class Condition(_Weakrefable):
    def root(self) -> Node: ...
    def result(self) -> Field: ...


class SelectionVector(_Weakrefable):
    def to_array(self) -> Array: ...


class Projector(_Weakrefable):
    @property
    def llvm_ir(self): ...

    def evaluate(
        self, batch: RecordBatch, selection: SelectionVector | None = None
    ) -> list[Array]: ...


class Filter(_Weakrefable):
    @property
    def llvm_ir(self): ...

    def evaluate(
        self, batch: RecordBatch, pool: MemoryPool, dtype: DataType | str = "int32"
    ) -> SelectionVector: ...


class TreeExprBuilder(_Weakrefable):
    def make_literal(self, value: float | str | bytes |
                     bool, dtype: DataType | str | None) -> Node: ...

    def make_expression(self, root_node: Node | None, return_field: Field) -> Expression: ...
    def make_function(
        self, name: str, children: list[Node | None], return_type: DataType) -> Node: ...

    def make_field(self, field: Field | None) -> Node: ...

    def make_if(
        self, condition: Node, this_node: Node | None, else_node: Node | None, return_type: DataType | None
    ) -> Node: ...
    def make_and(self, children: list[Node | None]) -> Node: ...
    def make_or(self, children: list[Node | None]) -> Node: ...
    def make_in_expression(self, node: Node | None, values: Iterable,
                           dtype: DataType) -> Node: ...

    def make_condition(self, condition: Node | None) -> Condition: ...


class Configuration(_Weakrefable):
    def __init__(self, optimize: bool = True, dump_ir: bool = False) -> None: ...


def make_projector(
    schema: Schema,
    children: list[Expression | None],
    pool: MemoryPool | None = None,
    selection_mode: Literal["NONE", "UINT16", "UINT32", "UINT64"] = "NONE",
    configuration: Configuration | None = None,
) -> Projector: ...


def make_filter(
    schema: Schema, condition: Condition | None, configuration: Configuration | None = None
) -> Filter: ...


class FunctionSignature(_Weakrefable):
    def return_type(self) -> DataType: ...
    def param_types(self) -> list[DataType]: ...
    def name(self) -> str: ...


def get_registered_function_signatures() -> list[FunctionSignature]: ...

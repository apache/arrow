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

from collections.abc import Callable
from typing import Any

from ._compute import Expression
from .lib import Buffer, RecordBatchReader, Schema, Table, _Weakrefable


def run_query(
    plan: Buffer | int,
    *,
    table_provider: Callable[[list[str], Schema], Table] | None = None,
    use_threads: bool = True,
) -> RecordBatchReader: ...
def _parse_json_plan(plan: bytes) -> Buffer: ...


class SubstraitSchema:
    schema: Schema
    expression: Expression
    def __init__(self, schema: Schema, expression: Expression) -> None: ...
    def to_pysubstrait(self) -> Any: ...


def serialize_schema(schema: Schema) -> SubstraitSchema: ...
def deserialize_schema(buf: Buffer | bytes) -> Schema: ...


def serialize_expressions(
    exprs: list[Expression],
    names: list[str],
    schema: Schema,
    *,
    allow_arrow_extensions: bool = False,
) -> Buffer: ...


class BoundExpressions(_Weakrefable):
    @property
    def schema(self) -> Schema: ...
    @property
    def expressions(self) -> dict[str, Expression]: ...
    @classmethod
    def from_substrait(cls, message: Buffer | bytes) -> BoundExpressions: ...


def deserialize_expressions(buf: Buffer | bytes) -> BoundExpressions: ...
def get_supported_functions() -> list[str]: ...

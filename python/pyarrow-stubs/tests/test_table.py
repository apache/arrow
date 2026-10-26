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
# mypy: warn-unused-ignores

import pyarrow.lib as lib

from _check import TYPE_CHECKING_INVALID_USAGE, assert_type, check


def test_table() -> None:
    table = lib.table({"a": [1, 2], "b": ["x", "y"]})
    check(assert_type(table, lib.Table), lib.Table)
    check(assert_type(table.num_rows, int), int)
    check(assert_type(table.num_columns, int), int)
    check(assert_type(table.shape, tuple[int, int]), tuple)
    check(assert_type(table.column_names, list[str]), list)
    check(assert_type(table.schema, lib.Schema), lib.Schema)
    check(assert_type(table.field("a"), lib.Field), lib.Field)

    # Columns aren't typed per tabular class, so Table and RecordBatch both
    # return the union here.
    check(assert_type(table.column("a"), lib.Array | lib.ChunkedArray),
          lib.ChunkedArray)

    check(assert_type(table.slice(0, 1), lib.Table), lib.Table)
    check(assert_type(table.select(["a"]), lib.Table), lib.Table)
    check(assert_type(table.combine_chunks(), lib.Table), lib.Table)
    check(assert_type(table.to_batches(), list[lib.RecordBatch]), list)
    check(assert_type(table.to_pydict(), dict[str, list]), dict)


def test_record_batch() -> None:
    batch = lib.record_batch({"a": [1, 2]})
    check(assert_type(batch, lib.RecordBatch), lib.RecordBatch)
    check(assert_type(batch.num_rows, int), int)
    check(assert_type(batch.column(0), lib.Array | lib.ChunkedArray),
          lib.Array)
    check(assert_type(batch.slice(1), lib.RecordBatch), lib.RecordBatch)
    check(assert_type(lib.Table.from_batches([batch]), lib.Table), lib.Table)
    check(assert_type(lib.concat_batches([batch, batch]), lib.RecordBatch),
          lib.RecordBatch)


def test_invalid_usage() -> None:
    table = lib.table({"a": [1, 2]})
    if TYPE_CHECKING_INVALID_USAGE:
        table.column(1.5)  # type: ignore[arg-type]
        table.slice("0")  # type: ignore[arg-type]

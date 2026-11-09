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

from types import MappingProxyType
from typing import Any

import pytest
from typing_extensions import CapsuleType

import pyarrow.lib as lib

from _check import TYPE_CHECKING_INVALID_USAGE, assert_type, check


class _PyArrowArrayProvider:
    def __arrow_array__(self, type: lib.DataType | None = None) -> lib.Array:
        return lib.array([1, 2], type=type)


class _ArrowArrayProvider:
    def __arrow_c_array__(
        self, requested_schema: CapsuleType | None = None
    ) -> tuple[CapsuleType, CapsuleType]:
        return lib.array([1, 2]).__arrow_c_array__(requested_schema)


class _ArrowDeviceArrayProvider:
    def __arrow_c_device_array__(
        self, requested_schema: CapsuleType | None = None, **kwargs: Any
    ) -> tuple[CapsuleType, CapsuleType]:
        return lib.array([1, 2]).__arrow_c_device_array__(requested_schema, **kwargs)


class _ArrowStreamProvider:
    def __arrow_c_stream__(
        self, requested_schema: CapsuleType | None = None
    ) -> CapsuleType:
        return lib.chunked_array([[1, 2]]).__arrow_c_stream__(requested_schema)


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
    check(assert_type(table["a"], lib.Array | lib.ChunkedArray | lib.Table),
          lib.ChunkedArray)
    check(assert_type(table[0], lib.Array | lib.ChunkedArray | lib.Table),
          lib.ChunkedArray)
    check(assert_type(table[:1], lib.Array | lib.ChunkedArray | lib.Table),
          lib.Table)

    check(assert_type(table.set_column(0, "a", [_ArrowDeviceArrayProvider()]),
                      lib.Table),
          lib.Table)
    check(assert_type(table.append_column("c", ([3], [4])), lib.Table), lib.Table)

    check(assert_type(table.slice(0, 1), lib.Table), lib.Table)
    check(assert_type(table.select(["a"]), lib.Table), lib.Table)
    check(assert_type(table.select(["a", 1]), lib.Table), lib.Table)
    check(assert_type(table.equals(None), bool), bool)
    check(assert_type(table.combine_chunks(), lib.Table), lib.Table)
    check(assert_type(table.to_batches(), list[lib.RecordBatch]), list)
    check(assert_type(table.to_pydict(), dict[str, list]), dict)
    check(assert_type(lib.Table.from_arrays([(1, 2)], names=["a"]), lib.Table),
          lib.Table)

    chunked = lib.chunked_array([[1, 2]])
    check(assert_type(table.add_column(0, "c", chunked), lib.Table), lib.Table)
    check(assert_type(table.add_column(0, "c", lib.array([1, 2])), lib.Table),
          lib.Table)
    check(assert_type(table.add_column(0, "c", [[1], [2]]), lib.Table),
          lib.Table)
    check(assert_type(table.add_column(0, "c", [lib.array([1, 2])]), lib.Table),
          lib.Table)

    table = lib.table(([1],), names=["a"])
    check(assert_type(table, lib.Table), lib.Table)

    chunked_struct = lib.chunked_array([[{"x": 1}]])
    check(assert_type(lib.Table.from_struct_array(chunked_struct), lib.Table),
          lib.Table)

    # The reader isn't annotated yet, but must not reject valid usage.
    check(table.to_reader().read_all(), lib.Table)
    for batch in table.to_reader():
        check(batch, lib.RecordBatch)


def test_table_from_arrays_protocols() -> None:
    check(assert_type(lib.Table.from_arrays([_PyArrowArrayProvider()], names=["a"]),
                      lib.Table),
          lib.Table)
    check(assert_type(lib.Table.from_arrays([_ArrowArrayProvider()], names=["a"]),
                      lib.Table),
          lib.Table)
    check(assert_type(lib.Table.from_arrays([_ArrowDeviceArrayProvider()], names=["a"]),
                      lib.Table),
          lib.Table)


def test_table_from_arrays_numpy() -> None:
    pytest.importorskip("numpy")
    import numpy as np

    columns = np.array([[1, 2], [3, 4]])
    check(assert_type(lib.Table.from_arrays(columns, names=["a", "b"]), lib.Table),
          lib.Table)


def test_record_batch() -> None:
    batch = lib.record_batch({"a": [1, 2]})  # from a dict
    check(assert_type(batch, lib.RecordBatch), lib.RecordBatch)
    check(assert_type(batch.num_rows, int), int)
    check(assert_type(batch.column(0), lib.Array | lib.ChunkedArray),
          lib.Array)
    check(assert_type(batch.slice(1), lib.RecordBatch), lib.RecordBatch)
    check(assert_type(lib.Table.from_batches([batch]), lib.Table), lib.Table)
    check(assert_type(lib.concat_batches([batch, batch]), lib.RecordBatch),
          lib.RecordBatch)
    check(assert_type(batch.add_column(0, "b", lib.array([3, 4])),
                      lib.RecordBatch),
          lib.RecordBatch)
    check(assert_type(batch.add_column(0, "b", [3, 4]), lib.RecordBatch),
          lib.RecordBatch)
    check(assert_type(batch.append_column("b", [3, 4]), lib.RecordBatch),
          lib.RecordBatch)
    check(assert_type(batch.equals(batch), bool), bool)
    # Any object can be compared, a non-batch returns False
    check(assert_type(batch.equals(42), bool), bool)

    batch = lib.record_batch([lib.array([1])], names=["a"])  # from a list
    check(assert_type(batch, lib.RecordBatch), lib.RecordBatch)

    struct_array = batch.to_struct_array()
    check(assert_type(struct_array, lib.StructArray), lib.StructArray)
    check(assert_type(lib.RecordBatch.from_struct_array(struct_array),
                      lib.RecordBatch),
          lib.RecordBatch)


def test_record_batch_from_pandas() -> None:
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"a": [1], "b": [2]})
    # Columns only need to be iterable, e.g. a generator.
    batch = lib.RecordBatch.from_pandas(df, columns=(c for c in ["a"]))
    check(assert_type(batch, lib.RecordBatch), lib.RecordBatch)


def test_chunked_array() -> None:
    chunked_array = lib.chunked_array([[1, 2], [None]])
    check(assert_type(chunked_array, lib.ChunkedArray), lib.ChunkedArray)
    check(assert_type(chunked_array.cast(lib.int32(), safe=None), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(chunked_array.fill_null(0), lib.ChunkedArray), lib.ChunkedArray)
    check(assert_type(chunked_array.sort(null_placement="at_start"), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(chunked_array.equals(None), bool), bool)

    arr = lib.array([1, 2])
    check(assert_type(lib.chunked_array(arr), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(lib.chunked_array([arr, [3]]), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(lib.chunked_array([_PyArrowArrayProvider()]), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(lib.chunked_array([_ArrowDeviceArrayProvider()]),
                      lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(lib.chunked_array(_ArrowArrayProvider()), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(lib.chunked_array([_ArrowArrayProvider()]), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(lib.chunked_array(_ArrowStreamProvider()), lib.ChunkedArray),
          lib.ChunkedArray)


def test_invalid_usage() -> None:
    table = lib.table({"a": [1, 2]})
    streams = [_ArrowStreamProvider()]
    columns = {"a": lib.array([1])}.values()  # dict as non-sequence example

    if TYPE_CHECKING_INVALID_USAGE:
        table.column(1.5)  # type: ignore[arg-type]
        table.slice("0")  # type: ignore[arg-type]
        lib.Table.from_arrays(columns, names=["a"])  # type: ignore[arg-type]
        # Stream providers are accepted as the whole input, not individual chunks
        lib.chunked_array(streams)  # type: ignore[arg-type]
        table.add_column(0, "b", streams)  # type: ignore[arg-type]


def test_record_batch_invalid_usage() -> None:
    arr = lib.array([1])
    chunked = lib.chunked_array([[{"a": 1}]])
    if TYPE_CHECKING_INVALID_USAGE:
        # Tuples are failing at runtime. Recheck after the fix of GH-55418
        lib.RecordBatch.from_arrays((arr,), names=["a"])  # type: ignore[arg-type]
        lib.RecordBatch.from_struct_array(chunked)  # type: ignore[arg-type]
        lib.record_batch((arr,), names=["a"])  # type: ignore[arg-type]
        # Only a `dict` is passed on to `from_pydict`, other mappings raise
        lib.record_batch(MappingProxyType({"a": [1]}))  # type: ignore[arg-type]

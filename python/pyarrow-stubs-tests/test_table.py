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

from collections.abc import Hashable
from types import MappingProxyType
from typing import Any, TYPE_CHECKING

import pytest
from typing_extensions import CapsuleType

import pyarrow as pa

from _check import assert_type, check


class _PyArrowArrayProvider:
    def __arrow_array__(self, type: pa.DataType | None = None) -> pa.Array:
        return pa.array([1, 2], type=type)


class _ArrowArrayProvider:
    def __arrow_c_array__(
        self, requested_schema: CapsuleType | None = None
    ) -> tuple[CapsuleType, CapsuleType]:
        return pa.array([1, 2]).__arrow_c_array__(requested_schema)


class _ArrowDeviceArrayProvider:
    def __arrow_c_device_array__(
        self, requested_schema: CapsuleType | None = None, **kwargs: Any
    ) -> tuple[CapsuleType, CapsuleType]:
        return pa.array([1, 2]).__arrow_c_device_array__(requested_schema, **kwargs)


class _ArrowStreamProvider:
    def __arrow_c_stream__(
        self, requested_schema: CapsuleType | None = None
    ) -> CapsuleType:
        return pa.chunked_array([[1, 2]]).__arrow_c_stream__(requested_schema)


class _ObjectStreamProvider:
    # Guarding return type
    # Polars annotates public __arrow_c_stream__ return with -> object
    def __arrow_c_stream__(self, requested_schema: CapsuleType | None = None) -> object:
        return pa.table({"a": [1, 2]}).__arrow_c_stream__(requested_schema)


def test_table() -> None:
    table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    check(assert_type(table, pa.Table), pa.Table)
    check(assert_type(table.num_rows, int), int)
    check(assert_type(table.num_columns, int), int)
    check(assert_type(table.shape, tuple[int, int]), tuple)
    check(assert_type(table.column_names, list[str]), list)
    check(assert_type(table.schema, pa.Schema), pa.Schema)
    check(assert_type(table.field("a"), pa.Field), pa.Field)

    # Columns aren't typed per tabular class, so Table and RecordBatch both
    # return the union here.
    check(assert_type(table.column("a"), pa.Array | pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(table["a"], pa.Array | pa.ChunkedArray | pa.Table),
          pa.ChunkedArray)
    check(assert_type(table[0], pa.Array | pa.ChunkedArray | pa.Table),
          pa.ChunkedArray)
    check(assert_type(table[:1], pa.Array | pa.ChunkedArray | pa.Table),
          pa.Table)

    check(assert_type(table.set_column(0, "a", [_ArrowDeviceArrayProvider()]),
                      pa.Table),
          pa.Table)
    check(assert_type(table.append_column("c", ([3], [4])), pa.Table), pa.Table)

    check(assert_type(table.slice(0, 1), pa.Table), pa.Table)
    check(assert_type(table.select(["a"]), pa.Table), pa.Table)
    check(assert_type(table.select(["a", 1]), pa.Table), pa.Table)
    check(assert_type(table.equals(None), bool), bool)
    check(assert_type(table.combine_chunks(), pa.Table), pa.Table)
    check(assert_type(table.to_batches(), list[pa.RecordBatch]), list)
    check(assert_type(table.to_pydict(), dict[str, list]), dict)
    check(assert_type(pa.Table.from_arrays([(1, 2)], names=["a"]), pa.Table),
          pa.Table)

    chunked = pa.chunked_array([[1, 2]])
    check(assert_type(table.add_column(0, "c", chunked), pa.Table), pa.Table)
    check(assert_type(table.add_column(0, "c", pa.array([1, 2])), pa.Table),
          pa.Table)
    check(assert_type(table.add_column(0, "c", [[1], [2]]), pa.Table),
          pa.Table)
    check(assert_type(table.add_column(0, "c", [pa.array([1, 2])]), pa.Table),
          pa.Table)

    table = pa.table(([1],), names=["a"])
    check(assert_type(table, pa.Table), pa.Table)

    chunked_struct = pa.chunked_array([[{"x": 1}]])
    check(assert_type(pa.Table.from_struct_array(chunked_struct), pa.Table),
          pa.Table)

    # The reader isn't annotated yet, but must not reject valid usage.
    check(table.to_reader().read_all(), pa.Table)
    for batch in table.to_reader():
        check(batch, pa.RecordBatch)


def test_table_from_arrays_protocols() -> None:
    check(assert_type(pa.Table.from_arrays([_PyArrowArrayProvider()], names=["a"]),
                      pa.Table),
          pa.Table)
    check(assert_type(pa.Table.from_arrays([_ArrowArrayProvider()], names=["a"]),
                      pa.Table),
          pa.Table)
    check(assert_type(pa.Table.from_arrays([_ArrowDeviceArrayProvider()], names=["a"]),
                      pa.Table),
          pa.Table)


def test_table_accepts_object_return_in_protocols() -> None:
    # The Supports* Protocols in _stubs_typing.pyi must stay wide (Any returns).
    check(assert_type(pa.table(_ObjectStreamProvider()), pa.Table), pa.Table)
    check(assert_type(pa.chunked_array(_ObjectStreamProvider()),
          pa.ChunkedArray), pa.ChunkedArray)


def test_table_from_arrays_numpy() -> None:
    pytest.importorskip("numpy")
    import numpy as np

    columns = np.array([[1, 2], [3, 4]])
    check(assert_type(pa.Table.from_arrays(columns, names=["a", "b"]), pa.Table),
          pa.Table)


def test_record_batch() -> None:
    batch = pa.record_batch({"a": [1, 2]})  # from a dict
    check(assert_type(batch, pa.RecordBatch), pa.RecordBatch)
    check(assert_type(batch.num_rows, int), int)
    check(assert_type(batch.column(0), pa.Array | pa.ChunkedArray),
          pa.Array)
    check(assert_type(batch.slice(1), pa.RecordBatch), pa.RecordBatch)
    check(assert_type(pa.Table.from_batches([batch]), pa.Table), pa.Table)
    check(assert_type(pa.concat_batches([batch, batch]), pa.RecordBatch),
          pa.RecordBatch)
    check(assert_type(batch.add_column(0, "b", pa.array([3, 4])),
                      pa.RecordBatch),
          pa.RecordBatch)
    check(assert_type(batch.add_column(0, "b", [3, 4]), pa.RecordBatch),
          pa.RecordBatch)
    check(assert_type(batch.append_column("b", [3, 4]), pa.RecordBatch),
          pa.RecordBatch)
    check(assert_type(batch.equals(batch), bool), bool)
    # Any object can be compared, a non-batch returns False
    check(assert_type(batch.equals(42), bool), bool)

    batch = pa.record_batch([pa.array([1])], names=["a"])  # from a list
    check(assert_type(batch, pa.RecordBatch), pa.RecordBatch)

    struct_array = batch.to_struct_array()
    check(assert_type(struct_array, pa.StructArray), pa.StructArray)
    check(assert_type(pa.RecordBatch.from_struct_array(struct_array),
                      pa.RecordBatch),
          pa.RecordBatch)


def test_record_batch_from_pandas() -> None:
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"a": [1], "b": [2]})
    # Columns only need to be iterable, e.g. a generator.
    batch = pa.RecordBatch.from_pandas(df, columns=(c for c in ["a"]))
    check(assert_type(batch, pa.RecordBatch), pa.RecordBatch)


def test_chunked_array() -> None:
    chunked_array = pa.chunked_array([[1, 2], [None]])
    check(assert_type(chunked_array, pa.ChunkedArray), pa.ChunkedArray)
    check(assert_type(chunked_array.cast(pa.int32(), safe=None), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(chunked_array.fill_null(0), pa.ChunkedArray), pa.ChunkedArray)
    check(assert_type(chunked_array.sort(null_placement="at_start"), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(chunked_array.equals(None), bool), bool)

    arr = pa.array([1, 2])
    check(assert_type(pa.chunked_array(arr), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pa.chunked_array([arr, [3]]), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pa.chunked_array([_PyArrowArrayProvider()]), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pa.chunked_array([_ArrowDeviceArrayProvider()]),
                      pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pa.chunked_array(_ArrowArrayProvider()), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pa.chunked_array([_ArrowArrayProvider()]), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pa.chunked_array(_ArrowStreamProvider()), pa.ChunkedArray),
          pa.ChunkedArray)


def test_invalid_usage() -> None:
    table = pa.table({"a": [1, 2]})
    streams = [_ArrowStreamProvider()]
    columns = {"a": pa.array([1])}.values()  # dict as non-sequence example

    if TYPE_CHECKING:
        table.column(1.5)  # type: ignore[arg-type]
        table.slice("0")  # type: ignore[arg-type]
        pa.Table.from_arrays(columns, names=["a"])  # type: ignore[arg-type]
        # Stream providers are accepted as the whole input, not individual chunks
        pa.chunked_array(streams)  # type: ignore[arg-type]
        table.add_column(0, "b", streams)  # type: ignore[arg-type]
        # Tables define __eq__ but not __hash__, hence unhashable at runtime
        _: Hashable = table  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501


def test_record_batch_invalid_usage() -> None:
    arr = pa.array([1])
    chunked = pa.chunked_array([[{"a": 1}]])
    if TYPE_CHECKING:
        # Tuples are failing at runtime. Recheck after the fix of GH-55418
        pa.RecordBatch.from_arrays((arr,), names=["a"])  # type: ignore[arg-type]
        pa.RecordBatch.from_struct_array(chunked)  # type: ignore[arg-type]
        pa.record_batch((arr,), names=["a"])  # type: ignore[arg-type]
        # Only a `dict` is passed on to `from_pydict`, other mappings raise
        pa.record_batch(MappingProxyType({"a": [1]}))  # type: ignore[arg-type]

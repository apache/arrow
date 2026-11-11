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
from typing import Any, cast, TYPE_CHECKING

import pytest
from typing_extensions import CapsuleType

import pyarrow as pa
from pyarrow._compute import CastOptions
from pyarrow.lib import asarray

from _check import assert_type, check


def test_array() -> None:
    # pa.array() return type is Array. Tell the type checker which subclass to expect.
    arr = check(cast(pa.Int64Array, pa.array([1, 2, None])), pa.Int64Array)

    check(assert_type(arr.type, pa.DataType), pa.DataType)
    check(assert_type(arr.null_count, int), int)
    check(assert_type(len(arr), int), int)
    check(assert_type(arr.to_pylist(), list[Any]), list)
    # Scalars aren't annotated yet, so indexing returns Any | Int64Array.
    check(assert_type(arr[0], Any | pa.Int64Array), pa.Int64Scalar)
    check(assert_type(arr[1:], Any | pa.Int64Array), pa.Int64Array)

    # Methods returning the same kind of array keep the subclass.
    check(assert_type(arr.slice(1), pa.Int64Array), pa.Int64Array)
    check(assert_type(arr.fill_null(0), pa.Int64Array), pa.Int64Array)
    check(assert_type(arr.drop_null(), pa.Int64Array), pa.Int64Array)
    indices: pa.Array = pa.array([1, 0])
    check(assert_type(arr.take(indices), pa.Int64Array), pa.Int64Array)

    check(assert_type(arr.is_null(), pa.BooleanArray), pa.BooleanArray)
    check(assert_type(arr.cast(pa.float64()), pa.Array), pa.DoubleArray)
    check(assert_type(arr.cast("float64"), pa.Array), pa.DoubleArray)
    check(assert_type(arr.cast(pa.int32(), safe=None), pa.Array), pa.Int32Array)
    # without target_type argument, as CastOptions already specifies it
    check(assert_type(arr.cast(options=CastOptions.safe(pa.int32())), pa.Array),
          pa.Int32Array)
    check(assert_type(arr.view("uint64"), pa.Array), pa.UInt64Array)
    check(assert_type(arr.to_string(element_size_limit=5), str), str)
    check(assert_type(arr.to_tensor(allow_nulls=True), pa.Tensor), pa.Tensor)
    check(assert_type(pa.concat_arrays([arr, arr]), pa.Array),
          pa.Int64Array)
    check(assert_type(pa.array([1, 2]), pa.Array), pa.Int64Array)
    check(assert_type(asarray([1, 2]), pa.Array), pa.Int64Array)
    check(assert_type(asarray(pa.chunked_array([arr])), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pa.nulls(2, pa.int8()), pa.Array), pa.Int8Array)
    check(assert_type(pa.repeat("a", 2), pa.Array), pa.StringArray)
    check(assert_type(pa.Array.from_pandas([1, 2], None, "int64"),
                      pa.Array),
          pa.Int64Array)

    other = check(cast(pa.StringArray, pa.array(["a", "b", None])), pa.StringArray)
    check(assert_type(arr.equals(arr), bool), bool)
    check(assert_type(arr.equals(other), bool), bool)
    # Arrays of any type can diff()
    check(assert_type(arr.diff(other), str), str)

    bool_mask = cast(pa.BooleanArray, pa.array([True, False, True]))
    check(assert_type(arr.filter(bool_mask), pa.Int64Array), pa.Int64Array)

    if TYPE_CHECKING:
        pa.array([1, 2], mask=[True, None])  # type: ignore[list-item]
        # TODO: when scalars get stubs
        # pa.array([1, 2], mask=pa.chunked_array([[True, False]]))


def test_array_numpy() -> None:
    pytest.importorskip("numpy")
    import numpy as np

    arr = cast(pa.Int64Array, pa.array([1, 2]))
    check(assert_type(arr[np.int64(0)], Any | pa.Int64Array), pa.Int64Scalar)
    check(assert_type(pa.infer_type(np.array([1])), pa.DataType), pa.DataType)
    offsets = np.array([0, 1, 2], dtype="int32")
    check(assert_type(pa.ListArray.from_arrays(offsets, [1, 2]), pa.ListArray),
          pa.ListArray)

    np_mask = np.array([True, False])
    check(assert_type(arr.filter(np_mask), pa.Int64Array), pa.Int64Array)

    if TYPE_CHECKING:
        dtype = pa.dictionary(pa.int8(), pa.string())
        strs = np.array(["a"])
        pa.DictionaryArray.from_buffers(dtype, 1, [], strs)  # type: ignore[arg-type]


def test_array_dlpack() -> None:
    arr = cast(pa.Int64Array, pa.array([1, 2]))
    capsule = arr.__dlpack__(
        max_version=(1, 0),
        dl_device=arr.__dlpack_device__(),
        copy=False,
    )
    check(assert_type(capsule, CapsuleType), CapsuleType)


def test_import_from_c_capsule() -> None:
    arr = cast(pa.Int64Array, pa.array([1, 2]))
    capsules = arr.__arrow_c_array__()
    check(assert_type(capsules, tuple[CapsuleType, CapsuleType]), tuple)

    # Import methods are static and return whatever type the C schema
    # holds, not the class they are called on
    imported = pa.Array._import_from_c_capsule(*capsules)
    check(assert_type(imported, pa.Array), pa.Int64Array)

    check(assert_type(arr.__arrow_c_device_array__(),
                      tuple[CapsuleType, CapsuleType]),
          tuple)


def test_from_buffers() -> None:
    values = cast(pa.Int64Array, pa.array([1, 2]))
    offsets = pa.array([0, 2], pa.int32()).buffers()[1]
    # Buffers and children can be any collection, like dict values or a tuple
    buffers = {"validity": None, "offsets": offsets}
    list_arr = pa.Array.from_buffers(pa.list_(pa.int64()), 1, buffers.values(),
                                     children=(values,))
    check(assert_type(list_arr, pa.Array), pa.ListArray)


def test_list_arrays() -> None:
    lst = pa.ListArray.from_arrays([0, 1, 3], [1, 2, 3])
    check(assert_type(lst.value_lengths(), pa.Int32Array), pa.Int32Array)

    large = pa.LargeListArray.from_arrays([0, 1, 3], pa.array([1, 2, 3]))
    check(assert_type(large.value_lengths(), pa.Int64Array), pa.Int64Array)

    list_type = pa.list_(pa.int64())
    check(assert_type(pa.ListArray.from_arrays([0, 2], [1, 2], list_type),
                      pa.ListArray),
          pa.ListArray)
    check(assert_type(pa.LargeListArray.from_arrays([0, 2], [1, 2]),
                      pa.LargeListArray),
          pa.LargeListArray)
    check(assert_type(pa.ListViewArray.from_arrays([0], [2], [1, 2]),
                      pa.ListViewArray),
          pa.ListViewArray)
    check(assert_type(pa.LargeListViewArray.from_arrays([0], [2], [1, 2]),
                      pa.LargeListViewArray),
          pa.LargeListViewArray)
    check(assert_type(pa.FixedSizeListArray.from_arrays([1, 2], 2),
                      pa.FixedSizeListArray),
          pa.FixedSizeListArray)

    # MapArray is a ListArray (of key-item structs) at runtime.
    map_arr = pa.MapArray.from_arrays([0, 2], [1, 2], ["a", "b"])
    check(assert_type(map_arr, pa.MapArray), pa.ListArray)
    check(assert_type(map_arr.offsets, pa.Int32Array), pa.Int32Array)
    check(assert_type(map_arr.values, pa.Array), pa.StructArray)
    check(assert_type(map_arr.keys, pa.Array), pa.Int64Array)
    map_type = pa.map_(pa.int64(), pa.string())
    check(assert_type(pa.MapArray.from_arrays([0, 2], [1, 2], ["a", "b"], map_type),
                      pa.MapArray),
          pa.ListArray)


def test_dictionary_array() -> None:
    arr = check(cast(pa.StringArray, pa.array(["a", "b", "a"])),
                pa.StringArray)
    dict_arr = arr.dictionary_encode()
    check(assert_type(dict_arr, pa.DictionaryArray), pa.DictionaryArray)
    check(assert_type(dict_arr.indices, pa.IntegerArray), pa.Int32Array)
    check(assert_type(dict_arr.dictionary, pa.Array), pa.StringArray)
    check(assert_type(dict_arr.dictionary_decode(), pa.Array),
          pa.StringArray)

    rebuilt = pa.DictionaryArray.from_buffers(
        dict_arr.type, len(dict_arr), dict_arr.indices.buffers(), dict_arr.dictionary
    )
    check(assert_type(rebuilt, pa.DictionaryArray), pa.DictionaryArray)


def test_run_end_encoded_array() -> None:
    ree = pa.RunEndEncodedArray.from_arrays([2, 3], ["a", "b"])
    check(assert_type(ree, pa.RunEndEncodedArray), pa.RunEndEncodedArray)
    check(
        assert_type(ree.run_ends,
                    pa.Int16Array | pa.Int32Array | pa.Int64Array),
        pa.Int64Array,
    )
    check(assert_type(ree.values, pa.Array), pa.StringArray)

    rebuilt = pa.RunEndEncodedArray.from_buffers(
        ree.type, len(ree), [None], children=(ree.run_ends, ree.values)
    )
    check(assert_type(rebuilt, pa.RunEndEncodedArray), pa.RunEndEncodedArray)

    rebuilt = pa.RunEndEncodedArray.from_buffers(
        ree.type, len(ree), [], children=[ree.run_ends, ree.values]
    )
    check(assert_type(rebuilt, pa.RunEndEncodedArray), pa.RunEndEncodedArray)


def test_chunked_array() -> None:
    chunked = pa.chunked_array([[1, 2], [3]])
    check(assert_type(chunked, pa.ChunkedArray), pa.ChunkedArray)
    check(assert_type(chunked.type, pa.DataType), pa.DataType)
    check(assert_type(chunked.num_chunks, int), int)
    check(assert_type(chunked.chunk(0), pa.Array), pa.Int64Array)
    # Scalars aren't annotated yet, so indexing returns Any | ChunkedArray.
    check(assert_type(chunked[0], Any | pa.ChunkedArray), pa.Int64Scalar)
    check(assert_type(chunked[1:], Any | pa.ChunkedArray), pa.ChunkedArray)
    check(assert_type(chunked.chunks, list[pa.Array]), list)
    check(assert_type(chunked.combine_chunks(), pa.Array), pa.Int64Array)
    check(assert_type(chunked.unique(), pa.Array), pa.Int64Array)
    check(assert_type(chunked.slice(1), pa.ChunkedArray), pa.ChunkedArray)
    check(assert_type(chunked.cast(pa.float64()), pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(chunked.is_null(), pa.ChunkedArray), pa.ChunkedArray)


def test_invalid_usage() -> None:
    arr = cast(pa.Int64Array, pa.array([1, 2]))
    if TYPE_CHECKING:
        arr.cast(1)  # type: ignore[arg-type]
        arr.slice("1")  # type: ignore[arg-type]
        arr.equals(object())  # type: ignore[arg-type]
        arr.equals(None)  # type: ignore[arg-type]
        arr.format()  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]  # noqa: E501
        # Arrays define __eq__ but not __hash__, hence unhashable at runtime
        _: Hashable = arr  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        arr.sort(null_placement="first")  # type: ignore[arg-type]
        arr.filter([True], null_selection_behavior="keep")  # type: ignore[arg-type]
        arr.dictionary_encode("drop")  # type: ignore[arg-type]
        arr.to_pylist(maps_as_pydicts="loose")  # type: ignore[arg-type]
        # Generators aren't accepted, only Sequences
        pa.infer_type(x for x in [1])  # type: ignore[arg-type]


def test_buffer_constructors_invalid_usage() -> None:
    types = cast(pa.Int8Array, pa.array([0], pa.int8()))
    children: list[pa.Array] = [pa.array([1])]
    strings = cast(pa.StringArray, pa.array(["a"]))
    off, data = cast(list[pa.Buffer], strings.buffers()[1:])
    ree = pa.RunEndEncodedArray.from_arrays([2, 3], ["a", "b"])
    if TYPE_CHECKING:
        pa.RunEndEncodedArray.from_buffers(
            ree.type, len(ree), strings.buffers()[1:2],  # type: ignore[arg-type]
            children=[ree.run_ends, ree.values])
        # Only lists are accepted for children and type codes
        pa.UnionArray.from_sparse(types, tuple(children))  # type: ignore[arg-type]
        pa.UnionArray.from_sparse(types, children, None,
                                  types)  # type: ignore[arg-type]
        pa.StringArray.from_buffers(1, off, data, None, None)  # type: ignore[arg-type]
        # Fields are passed as `fields`, `names` only takes strings
        pa.StructArray.from_arrays(
            children, names=[pa.field("a", pa.int64())])

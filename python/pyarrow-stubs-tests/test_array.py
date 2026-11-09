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
from typing import Any, cast

import pytest
from typing_extensions import CapsuleType

import pyarrow.lib as lib
from pyarrow._compute import CastOptions

from _check import TYPE_CHECKING_INVALID_USAGE, assert_type, check


def test_array() -> None:
    # pa.array() isn't annotated with a specific return type yet, so tell
    # the type checker which class to expect.
    arr = check(cast(lib.Int64Array, lib.array([1, 2, None])), lib.Int64Array)

    check(assert_type(arr.type, lib.DataType), lib.DataType)
    check(assert_type(arr.null_count, int), int)
    check(assert_type(len(arr), int), int)
    check(assert_type(arr.to_pylist(), list[Any]), list)
    # Scalars aren't annotated yet, so indexing returns Any | Int64Array.
    check(assert_type(arr[0], Any | lib.Int64Array), lib.Int64Scalar)
    check(assert_type(arr[1:], Any | lib.Int64Array), lib.Int64Array)

    # Methods returning the same kind of array keep the subclass.
    check(assert_type(arr.slice(1), lib.Int64Array), lib.Int64Array)
    check(assert_type(arr.fill_null(0), lib.Int64Array), lib.Int64Array)
    check(assert_type(arr.drop_null(), lib.Int64Array), lib.Int64Array)

    check(assert_type(arr.is_null(), lib.BooleanArray), lib.BooleanArray)
    check(assert_type(arr.cast(lib.float64()), lib.Array), lib.DoubleArray)
    check(assert_type(arr.cast("float64"), lib.Array), lib.DoubleArray)
    check(assert_type(arr.cast(lib.int32(), safe=None), lib.Array), lib.Int32Array)
    # without target_type argument, as CastOptions already specifies it
    check(assert_type(arr.cast(options=CastOptions.safe(lib.int32())), lib.Array),
          lib.Int32Array)
    check(assert_type(arr.view("uint64"), lib.Array), lib.UInt64Array)
    check(assert_type(arr.to_string(element_size_limit=5), str), str)
    check(assert_type(arr.to_tensor(allow_nulls=True), lib.Tensor), lib.Tensor)
    check(assert_type(lib.concat_arrays([arr, arr]), lib.Array),
          lib.Int64Array)
    check(assert_type(lib.Array.from_pandas([1, 2], None, lib.int64()),
                      lib.Array),
          lib.Int64Array)

    other = check(cast(lib.StringArray, lib.array(["a", "b", None])), lib.StringArray)
    check(assert_type(arr.equals(arr), bool), bool)
    check(assert_type(arr.equals(other), bool), bool)
    # Arrays of any type can diff()
    check(assert_type(arr.diff(other), str), str)


def test_array_numpy() -> None:
    pytest.importorskip("numpy")
    import numpy as np

    arr = cast(lib.Int64Array, lib.array([1, 2]))
    check(assert_type(arr[np.int64(0)], Any | lib.Int64Array), lib.Int64Scalar)
    check(assert_type(lib.infer_type(np.array([1])), lib.DataType), lib.DataType)
    offsets = np.array([0, 1, 2], dtype="int32")
    check(assert_type(lib.ListArray.from_arrays(offsets, [1, 2]), lib.ListArray),
          lib.ListArray)
    if TYPE_CHECKING_INVALID_USAGE:
        dtype = lib.dictionary(lib.int8(), lib.string())
        strs = np.array(["a"])
        lib.DictionaryArray.from_buffers(dtype, 1, [], strs)  # type: ignore[arg-type]


def test_array_dlpack() -> None:
    arr = cast(lib.Int64Array, lib.array([1, 2]))
    capsule = arr.__dlpack__(
        max_version=(1, 0),
        dl_device=arr.__dlpack_device__(),
        copy=False,
    )
    check(assert_type(capsule, CapsuleType), CapsuleType)


def test_import_from_c_capsule() -> None:
    arr = cast(lib.Int64Array, lib.array([1, 2]))
    capsules = arr.__arrow_c_array__()
    check(assert_type(capsules, tuple[CapsuleType, CapsuleType]), tuple)
    imported = lib.BooleanArray._import_from_c_capsule(*capsules)
    check(assert_type(imported, lib.Array), lib.Int64Array)

    check(assert_type(arr.__arrow_c_device_array__(),
                      tuple[CapsuleType, CapsuleType]),
          tuple)


def test_from_buffers() -> None:
    values = cast(lib.Int64Array, lib.array([1, 2]))
    offsets = lib.array([0, 2], lib.int32()).buffers()[1]
    # Children can be any collection of arrays, like a tuple
    list_arr = lib.Array.from_buffers(lib.list_(lib.int64()), 1, [None, offsets],
                                      children=(values,))
    check(assert_type(list_arr, lib.Array), lib.ListArray)


def test_list_arrays() -> None:
    lst = lib.ListArray.from_arrays([0, 1, 3], [1, 2, 3])
    check(assert_type(lst.value_lengths(), lib.Int32Array), lib.Int32Array)

    large = lib.LargeListArray.from_arrays([0, 1, 3], lib.array([1, 2, 3]))
    check(assert_type(large.value_lengths(), lib.Int64Array), lib.Int64Array)

    list_type = lib.list_(lib.int64())
    check(assert_type(lib.ListArray.from_arrays([0, 2], [1, 2], list_type),
                      lib.ListArray),
          lib.ListArray)
    check(assert_type(lib.LargeListArray.from_arrays([0, 2], [1, 2]),
                      lib.LargeListArray),
          lib.LargeListArray)
    check(assert_type(lib.ListViewArray.from_arrays([0], [2], [1, 2]),
                      lib.ListViewArray),
          lib.ListViewArray)
    check(assert_type(lib.LargeListViewArray.from_arrays([0], [2], [1, 2]),
                      lib.LargeListViewArray),
          lib.LargeListViewArray)
    check(assert_type(lib.FixedSizeListArray.from_arrays([1, 2], 2),
                      lib.FixedSizeListArray),
          lib.FixedSizeListArray)

    # MapArray is a ListArray (of key-item structs) at runtime.
    map_arr = lib.MapArray.from_arrays([0, 2], [1, 2], ["a", "b"])
    check(assert_type(map_arr, lib.MapArray), lib.ListArray)
    check(assert_type(map_arr.offsets, lib.Int32Array), lib.Int32Array)
    check(assert_type(map_arr.values, lib.Array), lib.StructArray)
    check(assert_type(map_arr.keys, lib.Array), lib.Int64Array)
    map_type = lib.map_(lib.int64(), lib.string())
    check(assert_type(lib.MapArray.from_arrays([0, 2], [1, 2], ["a", "b"], map_type),
                      lib.MapArray),
          lib.ListArray)


def test_dictionary_array() -> None:
    arr = check(cast(lib.StringArray, lib.array(["a", "b", "a"])),
                lib.StringArray)
    dict_arr = arr.dictionary_encode()
    check(assert_type(dict_arr, lib.DictionaryArray), lib.DictionaryArray)
    check(assert_type(dict_arr.indices, lib.IntegerArray), lib.Int32Array)
    check(assert_type(dict_arr.dictionary, lib.Array), lib.StringArray)
    check(assert_type(dict_arr.dictionary_decode(), lib.Array),
          lib.StringArray)

    rebuilt = lib.DictionaryArray.from_buffers(
        dict_arr.type, len(dict_arr), dict_arr.indices.buffers(), dict_arr.dictionary
    )
    check(assert_type(rebuilt, lib.DictionaryArray), lib.DictionaryArray)


def test_run_end_encoded_array() -> None:
    ree = lib.RunEndEncodedArray.from_arrays([2, 3], ["a", "b"])
    check(assert_type(ree, lib.RunEndEncodedArray), lib.RunEndEncodedArray)
    check(
        assert_type(ree.run_ends,
                    lib.Int16Array | lib.Int32Array | lib.Int64Array),
        lib.Int64Array,
    )
    check(assert_type(ree.values, lib.Array), lib.StringArray)


def test_chunked_array() -> None:
    chunked = lib.chunked_array([[1, 2], [3]])
    check(assert_type(chunked, lib.ChunkedArray), lib.ChunkedArray)
    check(assert_type(chunked.type, lib.DataType), lib.DataType)
    check(assert_type(chunked.num_chunks, int), int)
    check(assert_type(chunked.chunk(0), lib.Array), lib.Int64Array)
    # Scalars aren't annotated yet, so indexing returns Any | ChunkedArray.
    check(assert_type(chunked[0], Any | lib.ChunkedArray), lib.Int64Scalar)
    check(assert_type(chunked[1:], Any | lib.ChunkedArray), lib.ChunkedArray)
    check(assert_type(chunked.chunks, list[lib.Array]), list)
    check(assert_type(chunked.combine_chunks(), lib.Array), lib.Int64Array)
    check(assert_type(chunked.unique(), lib.Array), lib.Int64Array)
    check(assert_type(chunked.slice(1), lib.ChunkedArray), lib.ChunkedArray)
    check(assert_type(chunked.cast(lib.float64()), lib.ChunkedArray),
          lib.ChunkedArray)
    check(assert_type(chunked.is_null(), lib.ChunkedArray), lib.ChunkedArray)


def test_invalid_usage() -> None:
    arr = cast(lib.Int64Array, lib.array([1, 2]))
    if TYPE_CHECKING_INVALID_USAGE:
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
        lib.infer_type(x for x in [1])  # type: ignore[arg-type]


def test_buffer_constructors_invalid_usage() -> None:
    types = cast(lib.Int8Array, lib.array([0], lib.int8()))
    children: list[lib.Array] = [lib.array([1])]
    strings = cast(lib.StringArray, lib.array(["a"]))
    off, data = cast(list[lib.Buffer], strings.buffers()[1:])
    if TYPE_CHECKING_INVALID_USAGE:
        # Only lists are accepted for children and type codes
        lib.UnionArray.from_sparse(types, tuple(children))  # type: ignore[arg-type]
        lib.UnionArray.from_sparse(types, children, None,
                                   types)  # type: ignore[arg-type]
        lib.StringArray.from_buffers(1, off, data, None, None)  # type: ignore[arg-type]

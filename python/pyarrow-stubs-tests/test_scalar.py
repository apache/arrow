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

import datetime as dt
from collections.abc import Hashable, Iterator, Mapping, Sequence
from decimal import Decimal
from typing import Any, cast, TYPE_CHECKING
from uuid import UUID

import pytest
from typing_extensions import CapsuleType

import pyarrow as pa
# Not exported from pyarrow until io.pyi is annotated
from pyarrow.lib import Buffer

from _check import assert_type, check


def test_scalar() -> None:
    # pa.scalar() return type is Scalar. Tell the type checker which subclass
    # to expect.
    s = check(assert_type(pa.scalar(1), pa.Scalar), pa.Int64Scalar)
    check(assert_type(s.type, pa.DataType), pa.DataType)
    check(assert_type(s.is_valid, bool), bool)
    check(assert_type(s.as_py(), Any), int)
    check(assert_type(s.as_py(maps_as_pydicts="strict"), Any), int)
    check(assert_type(s.equals(pa.scalar(1)), bool), bool)
    check(assert_type(s.cast(pa.int8()), pa.Scalar), pa.Int8Scalar)
    check(assert_type(s.cast("int8", safe=None), pa.Scalar), pa.Int8Scalar)
    check(assert_type(pa.scalar(1, "int8"), pa.Scalar), pa.Int8Scalar)
    check(assert_type(pa.scalar(1, pa.int8(), from_pandas=True), pa.Scalar),
          pa.Int8Scalar)
    assert_type(s.validate(full=True), None)
    # Scalars, unlike arrays, are hashable
    _: Hashable = s

    check(assert_type(pa.NA, pa.NullScalar), pa.NullScalar)
    check(assert_type(pa.NULL, pa.NullScalar), pa.NullScalar)
    assert_type(pa.NA.as_py(), None)


def test_scalar_operators() -> None:
    s = pa.scalar(6)
    check(assert_type(-s, pa.Scalar), pa.Int64Scalar)
    check(assert_type(abs(s), pa.Scalar), pa.Int64Scalar)
    # The result is a scalar, or an array if the other operand is one
    check(assert_type(s + 1, Any), pa.Int64Scalar)
    check(assert_type(s + pa.scalar(1), Any), pa.Int64Scalar)
    check(assert_type(s - pa.array([1]), Any), pa.Int64Array)
    check(assert_type(s * pa.chunked_array([[1]]), Any), pa.ChunkedArray)
    check(assert_type(s / 4, Any), pa.Int64Scalar)
    check(assert_type(s ** 2, Any), pa.Int64Scalar)
    check(assert_type(s & 3, Any), pa.Int64Scalar)
    check(assert_type(s | 3, Any), pa.Int64Scalar)
    check(assert_type(s ^ 3, Any), pa.Int64Scalar)
    check(assert_type(s << 1, Any), pa.Int64Scalar)
    check(assert_type(s >> 1, Any), pa.Int64Scalar)

    if TYPE_CHECKING:
        # Scalars don't implement the reflected operators
        1 + s  # type: ignore[operator]  # ty: ignore[unsupported-operator]
        # nor floor division and modulo
        s // 2  # type: ignore[operator]  # ty: ignore[unsupported-operator]
        s % 2  # type: ignore[operator]  # ty: ignore[unsupported-operator]


def test_array_items() -> None:
    arr = cast(pa.Int64Array, pa.array([1, 2]))
    check(assert_type(arr[0], pa.Scalar), pa.Int64Scalar)
    check(assert_type(next(iter(arr)), pa.Scalar), pa.Int64Scalar)
    check(assert_type(arr.sum(), pa.Scalar), pa.Int64Scalar)
    check(assert_type(arr.index(2), pa.Int64Scalar), pa.Int64Scalar)
    check(assert_type(pa.chunked_array([arr]).index(2), pa.Int64Scalar),
          pa.Int64Scalar)


def test_primitive_scalars() -> None:
    boolean = cast(pa.BooleanScalar, pa.scalar(True))
    check(assert_type(boolean.as_py(), bool | None), bool)
    check(assert_type(bool(boolean), bool), bool)

    integer = cast(pa.Int8Scalar, pa.scalar(1, pa.int8()))
    check(assert_type(integer.as_py(), int | None), int)
    # Integer scalars can be used as indices
    check(assert_type([1, 2][integer], int), int)

    double = cast(pa.DoubleScalar, pa.scalar(1.5))
    check(assert_type(double.as_py(), float | None), float)
    check(float(double), float)
    check(assert_type(int(double), int), int)

    decimal = cast(pa.Decimal128Scalar, pa.scalar(Decimal("1.5")))
    check(assert_type(decimal.as_py(), Decimal | None), Decimal)

    null_int = cast(pa.Int64Scalar, pa.scalar(None, pa.int64()))
    assert_type(null_int.as_py(), int | None)
    assert null_int.as_py() is None


def test_temporal_scalars() -> None:
    date = cast(pa.Date32Scalar, pa.scalar(dt.date(2020, 1, 1)))
    check(assert_type(date.value, int | None), int)
    check(assert_type(date.as_py(), dt.date | None), dt.date)

    date64 = cast(pa.Date64Scalar, pa.scalar(dt.date(2020, 1, 1), pa.date64()))
    check(assert_type(date64.value, int | None), int)
    check(assert_type(date64.as_py(), dt.date | None), dt.date)

    time = cast(pa.Time64Scalar, pa.scalar(dt.time(1, 2)))
    check(assert_type(time.value, int | None), int)
    check(assert_type(time.as_py(), dt.time | None), dt.time)

    time32 = cast(pa.Time32Scalar, pa.scalar(dt.time(1, 2), pa.time32("s")))
    check(assert_type(time32.as_py(), dt.time | None), dt.time)

    timestamp = cast(pa.TimestampScalar, pa.scalar(dt.datetime(2020, 1, 1)))
    check(assert_type(timestamp.value, int | None), int)
    check(assert_type(timestamp.as_py(), dt.datetime | None), dt.datetime)

    duration = cast(pa.DurationScalar, pa.scalar(dt.timedelta(seconds=1)))
    check(assert_type(duration.value, int | None), int)
    check(assert_type(duration.as_py(), dt.timedelta | None), dt.timedelta)

    interval = cast(pa.MonthDayNanoIntervalScalar,
                    pa.scalar(pa.MonthDayNano([1, 2, 3])))
    check(assert_type(interval.value, pa.MonthDayNano | None), pa.MonthDayNano)
    check(assert_type(interval.as_py(), pa.MonthDayNano | None),
          pa.MonthDayNano)


def test_binary_scalars() -> None:
    binary = cast(pa.BinaryScalar, pa.scalar(b"abc"))
    check(assert_type(binary.as_py(), bytes | None), bytes)
    check(assert_type(binary.as_buffer(), Buffer | None), pa.Buffer)
    check(assert_type(bytes(binary), bytes), bytes)
    check(assert_type(memoryview(binary), memoryview), memoryview)

    # String scalars are binary scalars at runtime
    string = cast(pa.StringScalar, pa.scalar("abc"))
    check(assert_type(string.as_py(), str | None), str)
    check(assert_type(string.as_buffer(), Buffer | None), pa.Buffer)
    check(string, pa.BinaryScalar)

    large = cast(pa.LargeStringScalar, pa.scalar("abc", pa.large_string()))
    check(assert_type(large.as_py(), str | None), str)
    view = cast(pa.BinaryViewScalar, pa.scalar(b"abc", pa.binary_view()))
    check(assert_type(view.as_py(), bytes | None), bytes)


def test_list_scalar() -> None:
    lst = cast(pa.ListScalar, pa.scalar([1, 2]))
    check(assert_type(lst.values, pa.Array | None), pa.Int64Array)
    check(assert_type(len(lst), int), int)
    check(assert_type(lst[0], pa.Scalar), pa.Int64Scalar)
    check(assert_type(next(iter(lst)), pa.Scalar), pa.Int64Scalar)
    check(assert_type(lst.as_py(), list[Any] | None), list)
    # List scalars are sequences at runtime
    seq: Sequence[pa.Scalar] = check(lst, Sequence)
    check(assert_type(seq.index(lst[1]), int), int)

    fixed = cast(pa.FixedSizeListScalar,
                 pa.scalar([1, 2], pa.list_(pa.int64(), 2)))
    check(assert_type(fixed[0], pa.Scalar), pa.Int64Scalar)

    null_list = cast(pa.ListScalar, pa.scalar(None, pa.list_(pa.int64())))
    assert_type(null_list.values, pa.Array | None)
    assert null_list.values is None
    assert null_list.as_py() is None

    if TYPE_CHECKING:
        # Only integer indices are supported, not slices
        lst[1:]  # type: ignore[index]


def test_struct_scalar() -> None:
    struct = cast(pa.StructScalar, pa.scalar({"a": 1, "b": "x"}))
    check(assert_type(struct["a"], pa.Scalar), pa.Int64Scalar)
    check(assert_type(struct[1], pa.Scalar), pa.StringScalar)
    check(assert_type(len(struct), int), int)
    check(assert_type("a" in struct, bool), bool)
    check(assert_type(list(struct), list[str]), list)
    check(assert_type(struct.items(), Iterator[tuple[str, pa.Scalar]]), Iterator)
    check(assert_type(struct.as_py(), dict[str, Any] | None), dict)
    # Struct scalars are mappings at runtime
    mapping: Mapping[str, pa.Scalar] = check(struct, Mapping)
    check(assert_type(mapping.get("a"), pa.Scalar | None), pa.Int64Scalar)


def test_map_scalar() -> None:
    map_type = pa.map_(pa.string(), pa.int64())
    map_scalar = cast(pa.MapScalar, pa.scalar([("a", 1)], map_type))
    check(map_scalar, pa.ListScalar)
    check(assert_type(map_scalar.values, pa.Array | None), pa.StructArray)
    check(assert_type(map_scalar[0], tuple[pa.Scalar, pa.Scalar]), tuple)
    check(assert_type(map_scalar["a"], pa.Scalar), pa.Int64Scalar)
    check(assert_type(map_scalar.keys(), list[Any]), list)
    # Iterating yields the (key, item) pairs as Python objects
    check(assert_type(next(iter(map_scalar)), tuple[Any, Any]), tuple)
    # as_py() returns a list of pairs, or a dict if maps_as_pydicts is given
    check(assert_type(map_scalar.as_py(), list[tuple[Any, Any]] | None), list)
    check(assert_type(map_scalar.as_py(maps_as_pydicts="lossy"),
                      dict[Any, Any] | None),
          dict)

    null_map = cast(pa.MapScalar, pa.scalar(None, map_type))
    assert null_map.as_py() is None
    assert null_map.as_py(maps_as_pydicts="strict") is None
    check(assert_type(null_map.keys(), list[Any]), list)


def test_dictionary_scalar() -> None:
    dict_arr = pa.array(["a", "b"]).dictionary_encode()
    dict_scalar = cast(pa.DictionaryScalar, dict_arr[1])
    index = check(dict_scalar.index, pa.Int32Scalar)
    # The index type is one of the integer types
    check(assert_type(index.as_py(), int | None), int)
    check(assert_type(dict_scalar.value, pa.Scalar), pa.StringScalar)
    check(assert_type(dict_scalar.dictionary, pa.Array), pa.StringArray)


def test_run_end_encoded_scalar() -> None:
    ree = pa.RunEndEncodedArray.from_arrays([2], ["a"])
    ree_scalar = cast(pa.RunEndEncodedScalar, ree[0])
    check(assert_type(ree_scalar.value, pa.Scalar), pa.StringScalar)


def test_union_scalar() -> None:
    union = pa.UnionArray.from_sparse(
        cast(pa.Int8Array, pa.array([0], pa.int8())), [pa.array([1])]
    )
    union_scalar = cast(pa.UnionScalar, union[0])
    check(assert_type(union_scalar.value, pa.Scalar | None), pa.Int64Scalar)
    check(assert_type(union_scalar.type_code, int), int)

    null_union = pa.UnionArray.from_sparse(
        cast(pa.Int8Array, pa.array([0], pa.int8())),
        [pa.array([None], pa.int64())],
    )
    null_scalar = cast(pa.UnionScalar, null_union[0])
    assert_type(null_scalar.value, pa.Scalar | None)
    assert null_scalar.value is None


def test_extension_scalars() -> None:
    ext = pa.ExtensionScalar.from_storage(pa.uuid(), b"0" * 16)
    uuid = check(assert_type(ext, pa.ExtensionScalar), pa.UuidScalar)
    check(assert_type(uuid.value, pa.Scalar | None), pa.FixedSizeBinaryScalar)

    uuid_scalar = cast(pa.UuidScalar, uuid)
    check(assert_type(uuid_scalar.as_py(), UUID | None), UUID)

    null_uuid = cast(pa.UuidScalar,
                     pa.ExtensionScalar.from_storage(pa.uuid(), None))
    assert null_uuid.value is None
    assert null_uuid.as_py() is None

    bool8 = cast(pa.Bool8Scalar, pa.scalar(1, pa.bool8()))
    check(assert_type(bool8.as_py(), bool | None), bool)


def test_scalar_numpy_index() -> None:
    pytest.importorskip("numpy")
    import numpy as np

    # Anything with __index__ can index a list or map scalar, e.g. NumPy ints
    lst = cast(pa.ListScalar, pa.scalar([1, 2]))
    check(assert_type(lst[np.int64(0)], pa.Scalar), pa.Int64Scalar)
    map_scalar = cast(pa.MapScalar,
                      pa.scalar([("a", 1)], pa.map_(pa.string(), pa.int64())))
    check(assert_type(map_scalar[np.int64(0)], tuple[pa.Scalar, pa.Scalar]),
          tuple)
    if TYPE_CHECKING:
        # Struct scalars only take int positions
        struct = cast(pa.StructScalar, pa.scalar({"a": 1}))
        struct[np.int64(0)]  # type: ignore[index]


def test_fixed_shape_tensor_scalar() -> None:
    pytest.importorskip("numpy")
    import numpy as np

    tensor_type = pa.fixed_shape_tensor(pa.int32(), [2])
    storage = pa.array([[1, 2]], pa.list_(pa.int32(), 2))
    tensors = cast(pa.FixedShapeTensorArray, tensor_type.wrap_array(storage))
    tensor = cast(pa.FixedShapeTensorScalar, tensors[0])
    check(assert_type(tensor.to_numpy(), np.ndarray), np.ndarray)
    check(assert_type(tensor.to_tensor(), pa.Tensor), pa.Tensor)
    check(assert_type(tensor.__dlpack_device__(), tuple[int, int]), tuple)
    check(assert_type(tensor.__dlpack__(max_version=(1, 0)), CapsuleType),
          CapsuleType)


def test_invalid_usage() -> None:
    s = pa.scalar(1)
    if TYPE_CHECKING:
        s.equals(1)  # type: ignore[arg-type]
        s.cast(1)  # type: ignore[arg-type]
        s.as_py(maps_as_pydicts="loose")  # type: ignore[arg-type]
        pa.scalar(1, type=1)  # type: ignore[arg-type]
        # Scalars don't have the methods of their subclasses
        s.value  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

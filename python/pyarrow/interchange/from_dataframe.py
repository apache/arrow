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

from __future__ import annotations

from typing import (
    Any,
)

from pyarrow.interchange.column import (
    DtypeKind,
    ColumnBuffers,
    ColumnNullType,
)

import ctypes
import numpy as np
import pyarrow as pa
import re

from pyarrow.interchange.column import Dtype


# A typing protocol could be added later to let Mypy validate code using
# `from_dataframe` better.
DataFrameObject = Any
ColumnObject = Any
BufferObject = Any


_PYARROW_DTYPES: dict[DtypeKind, dict[int, Any]] = {
    DtypeKind.INT: {8: pa.int8(),
                    16: pa.int16(),
                    32: pa.int32(),
                    64: pa.int64()},
    DtypeKind.UINT: {8: pa.uint8(),
                     16: pa.uint16(),
                     32: pa.uint32(),
                     64: pa.uint64()},
    DtypeKind.FLOAT: {16: pa.float16(),
                      32: pa.float32(),
                      64: pa.float64()},
    DtypeKind.BOOL: {1: pa.bool_()},
    DtypeKind.STRING: {8: pa.string()},
}


def from_dataframe(df: DataFrameObject, allow_copy=True) -> pa.Table:
    """
    Build a ``pa.Table`` from any DataFrame supporting the interchange
    protocol.

    Parameters
    ----------
    df : DataFrameObject
        Object supporting the interchange protocol, i.e. `__dataframe__`
        method.
    allow_copy : bool, default: True
        Whether to allow copying the memory to perform the conversion
        (if false then zero-copy approach is requested).

    Returns
    -------
    pa.Table
    """
    if isinstance(df, pa.Table):
        return df

    if not hasattr(df, "__dataframe__"):
        raise ValueError("`df` does not support __dataframe__")

    return _from_dataframe(df.__dataframe__(allow_copy=allow_copy))


def _from_dataframe(df: DataFrameObject, allow_copy=True):
    """
    Build a ``pa.Table`` from the DataFrame interchange object.

    Parameters
    ----------
    df : DataFrameObject
        Object supporting the interchange protocol, i.e. `__dataframe__`
        method.
    allow_copy : bool, default: True
        Whether to allow copying the memory to perform the conversion
        (if false then zero-copy approach is requested).

    Returns
    -------
    pa.Table
    """
    batches = []
    for chunk in df.get_chunks():
        batch = protocol_df_chunk_to_pyarrow(chunk, allow_copy)
        batches.append(batch)

    table = pa.Table.from_batches(batches)
    return table


def protocol_df_chunk_to_pyarrow(
    df: DataFrameObject,
    allow_copy: bool = True
) -> pa.Table:
    """
    Convert interchange protocol chunk to ``pa.RecordBatch``.

    Parameters
    ----------
    df : DataFrameObject

    Returns
    -------
    pa.RecordBatch
    """
    # We need a dict of columns here, with each column being a PyArrow
    # or NumPy array.
    columns: dict[str, pa.Array | np.ndarray] = {}
    buffers = []  # hold on to buffers, keeps memory alive
    for name in df.column_names():
        if not isinstance(name, str):
            raise ValueError(f"Column {name} is not a string")
        if name in columns:
            raise ValueError(f"Column {name} is not unique")
        col = df.get_column_by_name(name)
        dtype = col.dtype[0]
        if dtype in (
            DtypeKind.INT,
            DtypeKind.UINT,
            DtypeKind.FLOAT,
            DtypeKind.STRING,
        ):
            columns[name], buf = column_to_array(col, allow_copy)
        elif dtype == DtypeKind.BOOL:
            columns[name], buf = bool_8_column_to_array(col, allow_copy)
        elif dtype == DtypeKind.CATEGORICAL:
            columns[name], buf = categorical_column_to_dictionary(
                col, allow_copy)
        elif dtype == DtypeKind.DATETIME:
            columns[name], buf = datetime_column_to_array(col, allow_copy)
        else:
            raise NotImplementedError(f"Data type {dtype} not handled yet")

        buffers.append(buf)

    return pa.RecordBatch.from_pydict(columns)


def column_to_array(
    col: ColumnObject,
    allow_copy: bool = True
) -> tuple[pa.Array, Any]:
    """
    Convert a column holding one of the primitive dtypes to a PyArrow array.
    A primitive type is one of: int, uint, float, bool (1 bit).

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    tuple
        Tuple of pa.Array holding the data and the memory owner object
        that keeps the memory alive.
    """
    buffers = col.get_buffers()
    null_count = col.null_count
    data = buffers_to_array(buffers, col.size(),
                            col.describe_null,
                            col.dtype, col.offset,
                            allow_copy, null_count)
    return data, buffers


def bool_8_column_to_array(
    col: ColumnObject,
    allow_copy: bool = True
) -> tuple[pa.Array, Any]:
    """
    Convert a column holding boolean dtype with bit width = 8 to a
    PyArrow array.

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    tuple
        Tuple of pa.Array holding the data and the memory owner object
        that keeps the memory alive.
    """
    if not allow_copy:
        raise RuntimeError(
            "PyArrow Array will always be created out of a NumPy ndarray "
            "and a copy is required which is forbidden by allow_copy=False"
        )
    buffers = col.get_buffers()

    # Data buffer
    data_buff, data_dtype = buffers["data"]
    offset = col.offset
    data_bit_width = data_dtype[1]

    data = buffer_to_ndarray(data_buff,
                             data_bit_width,
                             offset)

    # Validity buffer
    try:
        validity_buff, validity_dtype = buffers["validity"]
    except TypeError:
        validity_buff = None

    null_kind, sentinel_val = col.describe_null
    bytemask = None

    if validity_buff:
        if null_kind in (ColumnNullType.USE_BYTEMASK,
                         ColumnNullType.USE_BITMASK):

            validity_bit_width = validity_dtype[1]
            bytemask = buffer_to_ndarray(validity_buff,
                                         validity_bit_width,
                                         offset)
            if sentinel_val == 0:
                bytemask = np.invert(bytemask)
        else:
            raise NotImplementedError(f"{null_kind} null representation "
                                      "is not yet supported for boolean "
                                      "dtype.")
    # Output
    return pa.array(data, mask=bytemask), buffers


def categorical_column_to_dictionary(
    col: ColumnObject,
    allow_copy: bool = True
) -> tuple[pa.DictionaryArray, Any]:
    """
    Convert a column holding categorical data to a pa.DictionaryArray.

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    tuple
        Tuple of pa.DictionaryArray holding the data and the memory owner
        object that keeps the memory alive.
    """
    categorical = col.describe_categorical
    null_kind, sentinel_val = col.describe_null

    if not categorical["is_dictionary"]:
        raise NotImplementedError(
            "Non-dictionary categoricals not supported yet")

    cat_column = categorical["categories"]
    dictionary = column_to_array(cat_column)[0]

    buffers = col.get_buffers()
    indices = buffers_to_array(buffers, col.size(),
                               col.describe_null,
                               col.dtype, col.offset,
                               allow_copy)

    if null_kind == ColumnNullType.USE_SENTINEL:
        if not allow_copy:
            raise RuntimeError(
                "PyArrow Array will always be created out of a NumPy ndarray "
                "and a copy is required which is forbidden by allow_copy=False"
            )
        bytemask = [value == sentinel_val for value in indices.to_pylist()]
        indices = pa.array(indices.to_pylist(), mask=bytemask)

    dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
    return dict_array, buffers


def datetime_column_to_array(
    col: ColumnObject,
    allow_copy: bool = True
) -> tuple[pa.Array, Any]:
    """
    Convert a column holding DateTime data to a NumPy array.

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    tuple
        Tuple of pa.Array holding the data and the memory owner object
        that keeps the memory alive.
    """
    buffers = col.get_buffers()
    data_buff, data_type = buffers["data"]
    try:
        validity_buff, validity_dtype = buffers["validity"]
    except TypeError:
        validity_buff = None

    format_str = data_type[2]
    unit, tz = parse_datetime_format_str(format_str)
    data_dtype = pa.timestamp(unit, tz=tz)

    # Data buffer
    data_pa_buffer = pa.foreign_buffer(data_buff.ptr, data_buff.bufsize)

    # Validity buffer
    validity_pa_buff = validity_buff
    bytemask = None
    if validity_pa_buff:
        validity_pa_buff, bytemask = validity_buffer(validity_buff,
                                                     validity_dtype,
                                                     col.describe_null,
                                                     col.offset)

    null_kind, sentinel_val = col.describe_null
    # If sentinel values are used to represent missing values
    # then validity buffer and bytemask are None and we will
    # substitute the mask buffer with the sentinel mask buffer
    if null_kind == ColumnNullType.USE_SENTINEL:
        import pyarrow.compute as pc

        int_arr = pa.Array.from_buffers(pa.int64(),
                                        col.size(),
                                        [None, data_pa_buffer],
                                        offset=col.offset)
        bytemask = pc.equal(int_arr, sentinel_val)
        # Will need to invert once using the mask buffer
        # to construct an array:
        # sentinel_arr = pc.equal(int_arr, sentinel_val)
        # bytemask = pc.invert(sentinel_arr)

    # Constructing a pa.Array from data and validity buffer
    array = pa.Array.from_buffers(
        data_dtype,
        col.size(),
        [validity_pa_buff, data_pa_buffer],
        offset=col.offset,
    )



    # In case a bytemask was constructed with validity_buffer() call
    # or with sentinel_value then we have to add the mask to the array
    if bytemask is not None:
        if not allow_copy:
            raise RuntimeError(
                "PyArrow Array will always be created out of a NumPy ndarray "
                "and a copy is required which is forbidden by allow_copy=False"
            )
        return pa.array(array.to_pylist(), mask=bytemask), buffers
    else:
        return array, buffers


def parse_datetime_format_str(format_str):
    """Parse datetime `format_str` to interpret the `data`."""

    # timestamp 'ts{unit}:tz'
    timestamp_meta = re.match(r"ts([smun]):(.*)", format_str)
    if timestamp_meta:
        unit, tz = timestamp_meta.group(1), timestamp_meta.group(2)
        if unit != "s":
            # the format string describes only a first letter of the unit, so
            # add one extra letter to convert the unit to numpy-style:
            # 'm' -> 'ms', 'u' -> 'us', 'n' -> 'ns'
            unit += "s"

        return unit, tz

    raise NotImplementedError(f"DateTime kind is not supported: {format_str}")


def buffers_to_array(
    buffers: ColumnBuffers,
    length: int,
    describe_null: ColumnNullType,
    validity_dtype: Dtype,
    offset: int = 0,
    allow_copy: bool = True,
    null_count: int = -1
) -> pa.Array:
    """
    Build a PyArrow array from the passed buffer.

    Parameters
    ----------
    buffer : ColumnBuffers
        Dictionary containing tuples of underlying buffers and
        their associated dtype.
    length : int
        The number of values in the array.
    describe_null: ColumnNullType
        Null representation the column dtype uses,
        as a tuple ``(kind, value)``
    validity_dtype : Dtype,
        Dtype description as a tuple ``(kind, bit-width, format string,
        endianness)``.
    offset : int, default: 0
        Number of elements to offset from the start of the buffer.
    allow_copy : bool, default: True
        Whether to allow copying the memory to perform the conversion
        (if false then zero-copy approach is requested).
    null_count : int, default: -1

    Returns
    -------
    pa.Array

    Notes
    -----
    The returned array doesn't own the memory. The caller of this function
    is responsible for keeping the memory owner object alive as long as
    the returned PyArrow array is being used.
    """
    data_buff, data_type = buffers["data"]
    try:
        validity_buff, validity_dtype = buffers["validity"]
    except TypeError:
        validity_buff = None
    try:
        offset_buff, _ = buffers["offsets"]
    except TypeError:
        offset_buff = None

    kind, bit_width, _, _ = data_type

    data_dtype = _PYARROW_DTYPES.get(kind, {}).get(bit_width, None)
    if data_dtype is None:
        raise NotImplementedError(
            f"Conversion for {data_type} is not yet supported.")

    # Construct a pyarrow Buffer
    data_pa_buffer = pa.foreign_buffer(data_buff.ptr, data_buff.bufsize)

    # Construct a validity pyarrow Buffer or a bytemask, if exists
    validity_pa_buff = validity_buff
    _, _, validity_f_string, _ = validity_dtype
    bytemask = None
    if validity_pa_buff:
        validity_pa_buff, bytemask = validity_buffer(validity_buff,
                                                     validity_dtype,
                                                     describe_null,
                                                     offset)

    # Construct a pyarrow Array from buffers
    if offset_buff:
        # If an offset buffer exists, construct an offset pyarrow Buffer
        # and add it to the construction of an array
        offset_pa_buffer = pa.py_buffer(offset_buff._x)
        if validity_f_string == 'U':
            if not null_count:
                null_count = -1
            array = pa.LargeStringArray.from_buffers(
                length,
                offset_pa_buffer,
                data_pa_buffer,
                validity_pa_buff,
                null_count=null_count,
                offset=offset,
            )
        else:
            array = pa.Array.from_buffers(
                pa.string(),
                length,
                [validity_pa_buff, offset_pa_buffer, data_pa_buffer],
                offset=offset,
            )
    else:
        array = pa.Array.from_buffers(
            data_dtype,
            length,
            [validity_pa_buff, data_pa_buffer],
            offset=offset,
        )

    # In case a bytemask was constructed with validity_buffer() call
    # then we have to add the mask to the array
    if bytemask is not None:
        if not allow_copy:
            raise RuntimeError(
                "PyArrow Array will always be created out of a NumPy ndarray "
                "and a copy is required which is forbidden by allow_copy=False"
            )
        return pa.array(array.to_pylist(), mask=bytemask)
    else:
        return array


def validity_buffer(
    validity_buff: BufferObject,
    validity_dtype: Dtype,
    describe_null: ColumnNullType,
    offset: int = 0,
) -> tuple[pa.Buffer, np.ndarray]:
    """
    Build a PyArrow buffer or a NumPy array from the passed buffer.

    Parameters
    ----------
    validity_buff : BufferObject
        Tuple of underlying validity buffer and associated dtype.
    validity_dtype : Dtype
        Dtype description as a tuple ``(kind, bit-width, format string,
        endianness)``.
    describe_null : ColumnNullType
        Null representation the column dtype uses,
        as a tuple ``(kind, value)``
    offset : int, default: 0
        Number of elements to offset from the start of the buffer.

    Returns
    -------
    tuple
        Tuple of a pyarrow buffer and a np.ndarray defining a mask.
        Ony one of both can be not None.
    """
    null_kind, sentinel_val = describe_null
    validity_kind, validity_bit_width, _, _ = validity_dtype
    assert validity_kind == DtypeKind.BOOL

    bytemask = None
    if null_kind == ColumnNullType.USE_BYTEMASK:

        bytemask = buffer_to_ndarray(validity_buff,
                                     validity_bit_width,
                                     offset)
        if sentinel_val == 0:
            bytemask = np.invert(bytemask)
        validity_pa_buff = None

    elif null_kind == ColumnNullType.USE_BITMASK and sentinel_val == 0:
        validity_pa_buff = pa.foreign_buffer(
            validity_buff.ptr, validity_buff.bufsize)
    else:
        raise NotImplementedError(
            f"{describe_null} null representation is not yet supported.")

    return validity_pa_buff, bytemask


def buffer_to_ndarray(
    validity_buff: BufferObject,
    bit_width: int,
    offset: int = 0,
) -> np.ndarray:
    """
    Build a NumPy array from the passed buffer.

    Parameters
    ----------
    validity_buff : BufferObject
        Tuple of underlying validity buffer and associated dtype.
    bit_width : int
        The number of bits of the buffer dtype as an integer.
    offset : int, default: 0
        Number of elements to offset from the start of the buffer.

    Returns
    -------
    np.ndarray
    """
    # TODO: No DLPack yet, so need to construct a new ndarray from
    # the data pointer and size in the buffer plus the dtype on the
    # column. Use DLPack as NumPy supports it since
    # https://github.com/numpy/numpy/pull/19083
    ctypes_type = np.ctypeslib.as_ctypes_type(bool)
    data_pointer = ctypes.cast(
        validity_buff.ptr + (offset * bit_width // 8),
        ctypes.POINTER(ctypes_type)
    )

    data = np.ctypeslib.as_array(
        data_pointer, shape=(validity_buff.bufsize // (bit_width // 8),)
    )

    return data

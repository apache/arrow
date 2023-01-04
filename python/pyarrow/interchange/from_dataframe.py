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

import pyarrow as pa
import re

import pyarrow.compute as pc
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
    DtypeKind.BOOL: {8: pa.uint8()},
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
        batch = protocol_df_chunk_to_pyarrow(chunk)
        batches.append(batch)

    table = pa.Table.from_batches(batches)
    return table


def protocol_df_chunk_to_pyarrow(
    df: DataFrameObject,
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
    # We need a dict of columns here, with each column being a pa.Array
    columns: dict[str, pa.Array] = {}
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
            columns[name] = column_to_array(col)
        elif dtype == DtypeKind.BOOL:
            columns[name] = bool_column_to_array(col)
        elif dtype == DtypeKind.CATEGORICAL:
            columns[name] = categorical_column_to_dictionary(col)
        elif dtype == DtypeKind.DATETIME:
            columns[name] = datetime_column_to_array(col)
        else:
            raise NotImplementedError(f"Data type {dtype} not handled yet")

    return pa.RecordBatch.from_pydict(columns)


def column_to_array(
    col: ColumnObject,
) -> pa.Array:
    """
    Convert a column holding one of the primitive dtypes to a PyArrow array.
    A primitive type is one of: int, uint, float, bool (1 bit).

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    pa.Array
    """
    buffers = col.get_buffers()
    null_count = col.null_count
    data = buffers_to_array(buffers, col.size(),
                            col.describe_null,
                            col.offset, null_count)
    return data


def bool_column_to_array(
    col: ColumnObject,
) -> pa.Array:
    """
    Convert a column holding boolean dtype to a PyArrow array.

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    pa.Array
    """
    buffers = col.get_buffers()
    data = buffers_to_array(buffers, col.size(),
                            col.describe_null,
                            col.offset)
    data = pc.cast(data, pa.bool_())

    return data


def categorical_column_to_dictionary(
    col: ColumnObject,
) -> pa.DictionaryArray:
    """
    Convert a column holding categorical data to a pa.DictionaryArray.

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    pa.DictionaryArray
    """
    categorical = col.describe_categorical

    if not categorical["is_dictionary"]:
        raise NotImplementedError(
            "Non-dictionary categoricals not supported yet")

    cat_column = categorical["categories"]
    dictionary = column_to_array(cat_column)

    buffers = col.get_buffers()

    indices = buffers_to_array(buffers, col.size(),
                               col.describe_null,
                               col.offset)

    # Constructing a pa.DictionaryArray
    dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)

    return dict_array


def datetime_column_to_array(
    col: ColumnObject,
) -> pa.Array:
    """
    Convert a column holding DateTime data to a pa.Array.

    Parameters
    ----------
    col : ColumnObject

    Returns
    -------
    pa.Array
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
    data_pa_buffer = pa.foreign_buffer(data_buff.ptr, data_buff.bufsize,
                                       base=data_buff)

    # Validity buffer
    validity_pa_buff = validity_buff
    null_kind, sentinel_val = col.describe_null
    if null_kind == ColumnNullType.USE_SENTINEL:
        int_arr = pa.Array.from_buffers(pa.int64(),
                                        col.size(),
                                        [None, data_pa_buffer],
                                        offset=col.offset)
        sentinel_arr = pc.equal(int_arr, sentinel_val)
        mask_bool = pc.invert(sentinel_arr)
        validity_pa_buff = mask_bool.buffers()[1]
    elif null_kind == ColumnNullType.NON_NULLABLE:
        # sliced array can have a NON_NULLABLE ColumnNullType due
        # to no missing values in that slice of an array though the bitmask
        # exists and validity_buff must be set to None in this case
        validity_buff = validity_pa_buff = None
    elif validity_buff:
        validity_pa_buff = validity_buffer(validity_buff,
                                           validity_dtype,
                                           col.describe_null,
                                           col.size(),
                                           col.offset)

    # Constructing a pa.Array from data and validity buffer
    return pa.Array.from_buffers(data_dtype, col.size(),
                                 [validity_pa_buff, data_pa_buffer],
                                 offset=col.offset)


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
    offset: int = 0,
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
    offset : int, default: 0
        Number of elements to offset from the start of the buffer.
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
        offset_buff, offset_dtype = buffers["offsets"]
    except TypeError:
        offset_buff = None

    kind, bit_width, f_string, _ = data_type

    data_dtype = _PYARROW_DTYPES.get(kind, {}).get(bit_width, None)
    if data_dtype is None:
        raise NotImplementedError(
            f"Conversion for {data_type} is not yet supported.")

    # Construct a pyarrow Buffer
    data_pa_buffer = pa.foreign_buffer(data_buff.ptr, data_buff.bufsize,
                                       base=data_buff)

    # Construct a validity pyarrow Buffer or a bytemask, if exists
    validity_pa_buff = validity_buff
    null_kind, sentinel_val = describe_null

    # Check for float NaN values
    if null_kind == ColumnNullType.USE_NAN:
        if kind == DtypeKind.FLOAT and bit_width == 16:
            raise NotImplementedError(
                f"{data_type} with {null_kind} is not yet supported.")
        else:
            pyarrow_data = pa.Array.from_buffers(
                data_dtype,
                length,
                [None, data_pa_buffer],
                offset=offset,
            )
            mask = pc.is_nan(pyarrow_data)
            mask = pc.invert(mask)
            validity_pa_buff = mask.buffers()[1]
    # Check for sentinel values values
    elif null_kind == ColumnNullType.USE_SENTINEL:
        int_arr = pa.Array.from_buffers(data_dtype,
                                        length,
                                        [None, data_pa_buffer],
                                        offset=offset)
        sentinel_arr = pc.equal(int_arr, sentinel_val)
        mask_bool = pc.invert(sentinel_arr)
        validity_pa_buff = mask_bool.buffers()[1]
    elif null_kind == ColumnNullType.NON_NULLABLE:
        # sliced array can have a NON_NULLABLE ColumnNullType due
        # to no missing values in that slice of an array though the bitmask
        # exists and validity_buff must be set to None in this case
        validity_buff = validity_pa_buff = None
    elif validity_buff:
        validity_pa_buff = validity_buffer(validity_buff,
                                           validity_dtype,
                                           describe_null,
                                           length,
                                           offset)

    # Construct a pyarrow Array from buffers
    if offset_buff:
        _, offset_bit_width, _, _ = offset_dtype
        # If an offset buffer exists, construct an offset pyarrow Buffer
        # and add it to the construction of an array
        offset_pa_buffer = pa.foreign_buffer(offset_buff.ptr,
                                             offset_buff.bufsize,
                                             base=offset_buff)

        if f_string == 'U':
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
            if offset_bit_width == 64:
                string_type = pa.large_string()
            else:
                string_type = pa.string()
            array = pa.Array.from_buffers(
                string_type,
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

    return array


def validity_buffer(
    validity_buff: BufferObject,
    validity_dtype: Dtype,
    describe_null: ColumnNullType,
    length: int,
    offset: int = 0,
) -> pa.Buffer:
    """
    Build a PyArrow buffer from the passed buffer.

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
    length : int
        The number of values in the array.
    offset : int, default: 0
        Number of elements to offset from the start of the buffer.

    Returns
    -------
    pa.Buffer
    """
    null_kind, sentinel_val = describe_null
    validity_kind, _, _, _ = validity_dtype
    assert validity_kind == DtypeKind.BOOL

    if null_kind == ColumnNullType.USE_BYTEMASK or (
        null_kind == ColumnNullType.USE_BITMASK and sentinel_val == 1
    ):
        validity_pa_buff = pa.foreign_buffer(validity_buff.ptr,
                                             validity_buff.bufsize,
                                             base=validity_buff)

        if null_kind == ColumnNullType.USE_BYTEMASK:
            mask = pa.Array.from_buffers(pa.int8(), length,
                                         [None, validity_pa_buff],
                                         offset=offset)
            mask_bool = pc.cast(mask, pa.bool_())
        else:
            mask_bool = pa.Array.from_buffers(pa.bool_(), length,
                                              [None, validity_pa_buff],
                                              offset=offset)

        if sentinel_val == 1:
            mask_bool = pc.invert(mask_bool)
        validity_buff = mask_bool.buffers()[1]

    elif null_kind == ColumnNullType.USE_BITMASK and sentinel_val == 0:
        validity_buff = pa.foreign_buffer(validity_buff.ptr,
                                          validity_buff.bufsize,
                                          base=validity_buff)
    else:
        raise NotImplementedError(
            f"{describe_null} null representation is not yet supported.")

    return validity_buff

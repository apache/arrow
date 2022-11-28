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

from pyarrow.interchange.buffer import _PyArrowBuffer
from pyarrow.interchange.column import (
    _PyArrowColumn,
    DtypeKind,
)
from pyarrow.interchange.dataframe import _PyArrowDataFrame

import numpy as np
import pyarrow as pa


# A typing protocol could be added later to let Mypy validate code using
# `from_dataframe` better.
DataFrameObject = Any
ColumnObject = Any
BufferObject = Any


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
    pass


def protocol_df_chunk_to_pyarrow(df: DataFrameObject) -> pa.Table:
    """
    Convert interchange protocol chunk to ``pd.DataFrame``.
    Parameters
    ----------
    df : DataFrameObject
    Returns
    -------
    pa.Table
    """
    # We need a dict of columns here, with each column being a NumPy array
    # (at least for now, deal with non-NumPy dtypes later).
    columns: dict[str, Any] = {}
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
            DtypeKind.BOOL,
        ):
            columns[name], buf = primitive_column_to_ndarray(col)
        elif dtype == DtypeKind.CATEGORICAL:
            columns[name], buf = categorical_column_to_dictionary(col)
        elif dtype == DtypeKind.STRING:
            columns[name], buf = string_column_to_ndarray(col)
        elif dtype == DtypeKind.DATETIME:
            columns[name], buf = datetime_column_to_ndarray(col)
        else:
            raise NotImplementedError(f"Data type {dtype} not handled yet")

        buffers.append(buf)

    pass


def primitive_column_to_array(col: ColumnObject) -> tuple[pa.Array, Any]:
    """
    Convert a column holding one of the primitive dtypes to a PyArrow array.
    A primitive type is one of: int, uint, float, bool.
    Parameters
    ----------
    col : ColumnObject
    Returns
    -------
    tuple
        Tuple of pa.Array holding the data and the memory owner object
        that keeps the memory alive.
    """
    pass


def categorical_column_to_dictionary(
    col: ColumnObject
) -> tuple[pa.Array, Any]:
    """
    Convert a column holding categorical data to a pandas Series.
    Parameters
    ----------
    col : ColumnObject
    Returns
    -------
    tuple
        Tuple of pa.Array holding the data and the memory owner object
        that keeps the memory alive.
    """
    pass


def string_column_to_array(col: ColumnObject) -> tuple[pa.Array, Any]:
    """
    Convert a column holding string data to a NumPy array.
    Parameters
    ----------
    col : ColumnObject
    Returns
    -------
    tuple
        Tuple of pa.Array holding the data and the memory owner object
        that keeps the memory alive.
    """
    pass


def parse_datetime_format_str(format_str, data):
    """Parse datetime `format_str` to interpret the `data`."""
    pass


def datetime_column_to_array(col: ColumnObject) -> tuple[pa.Array, Any]:
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
    pass


def buffer_to_array(
    buffer: BufferObject,
    dtype: tuple[DtypeKind, int, str, str],
    offset: int = 0,
    length: int | None = None,
) -> pa.Array:
    """
    Build a NumPy array from the passed buffer.
    Parameters
    ----------
    buffer : BufferObject
        Buffer to build a PyArrow array from.
    dtype : tuple
        Data type of the buffer conforming protocol dtypes format.
    offset : int, default: 0
        Number of elements to offset from the start of the buffer.
    length : int, optional
        If the buffer is a bit-mask, specifies a number of bits to read
        from the buffer. Has no effect otherwise.
    Returns
    -------
    pa.Array

    Notes
    -----
    The returned array doesn't own the memory. The caller of this function
    is responsible for keeping the memory owner object alive as long as
    the returned NumPy array is being used.
    """
    pass


def bitmask_to_bool_array(
    bitmask: np.ndarray, mask_length: int, first_byte_offset: int = 0
) -> pa.Array:
    """
    Convert bit-mask to a boolean NumPy array.
    Parameters
    ----------
    bitmask : np.ndarray[uint8]
        NumPy array of uint8 dtype representing the bitmask.
    mask_length : int
        Number of elements in the mask to interpret.
    first_byte_offset : int, default: 0
        Number of elements to offset from the start of the first byte.
    Returns
    -------
    pa.Array[bool]
    """
    pass

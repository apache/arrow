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


from pyarrow._compute import (  # noqa
    FilterOptions,
    Function,
    FunctionRegistry,
    function_registry,
    call_function
)
import pyarrow._compute as _pc


def cast(arr, target_type, safe=True):
    """
    Cast array values to another data type. Can also be invoked as an array
    instance method.

    Parameters
    ----------
    arr : Array or ChunkedArray
    target_type : DataType or type string alias
        Type to cast to
    safe : bool, default True
        Check for overflows or other unsafe conversions

    Examples
    --------
    >>> from datetime import datetime
    >>> import pyarrow as pa
    >>> arr = pa.array([datetime(2010, 1, 1), datetime(2015, 1, 1)])
    >>> arr.type
    TimestampType(timestamp[us])

    You can use ``pyarrow.DataType`` objects to specify the target type:

    >>> cast(arr, pa.timestamp('ms'))
    <pyarrow.lib.TimestampArray object at 0x7fe93c0f6910>
    [
      2010-01-01 00:00:00.000,
      2015-01-01 00:00:00.000
    ]

    >>> cast(arr, pa.timestamp('ms')).type
    TimestampType(timestamp[ms])

    Alternatively, it is also supported to use the string aliases for these
    types:

    >>> arr.cast('timestamp[ms]')
    <pyarrow.lib.TimestampArray object at 0x10420eb88>
    [
      1262304000000,
      1420070400000
    ]
    >>> arr.cast('timestamp[ms]').type
    TimestampType(timestamp[ms])

    Returns
    -------
    casted : Array
    """
    if target_type is None:
        raise ValueError("Cast target type must not be None")
    if safe:
        options = _pc.CastOptions.safe(target_type)
    else:
        options = _pc.CastOptions.unsafe(target_type)
    return call_function("cast", [arr], options)


def sum(array):
    """
    Sum the values in a numerical (chunked) array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray

    Returns
    -------
    sum : pyarrow.Scalar
    """
    return call_function('sum', [array])

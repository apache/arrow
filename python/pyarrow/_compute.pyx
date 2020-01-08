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

# cython: language_level = 3

from pyarrow.lib cimport (
    Array,
    wrap_datum,
    _context,
    check_status,
    ChunkedArray
)
from pyarrow.includes.libarrow cimport CDatum, Sum


cdef _sum_array(array: Array):
    cdef CDatum out

    with nogil:
        check_status(Sum(_context(), CDatum(array.sp_array), &out))

    return wrap_datum(out)


cdef _sum_chunked_array(array: ChunkedArray):
    cdef CDatum out

    with nogil:
        check_status(Sum(_context(), CDatum(array.sp_chunked_array), &out))

    return wrap_datum(out)


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
    if isinstance(array, Array):
        return _sum_array(array)
    elif isinstance(array, ChunkedArray):
        return _sum_chunked_array(array)
    else:
        raise ValueError(
            "Only pyarrow.Array and pyarrow.ChunkedArray supported as"
            " an input, passed {}".format(type(array))
        )

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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

import datetime
import decimal as _pydecimal
import json
import numpy as np
import os
import six

from pyarrow.compat import frombytes, tobytes, ordered_dict

from cython.operator cimport dereference as deref
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.common cimport PyObject_to_object
cimport pyarrow.includes.libarrow as libarrow
cimport cpython as cp

# Initialize NumPy C API
arrow_init_numpy()
# Initialize PyArrow C++ API
# (used from some of our C++ code, see e.g. ARROW-5260)
import_pyarrow()
set_numpy_nan(np.nan)


def cpu_count():
    """
    Return the number of threads to use in parallel operations.

    The number of threads is determined at startup by inspecting the
    ``OMP_NUM_THREADS`` and ``OMP_THREAD_LIMIT`` environment variables.
    If neither is present, it will default to the number of hardware threads
    on the system.  It can be modified at runtime by calling
    :func:`set_cpu_count()`.
    """
    return GetCpuThreadPoolCapacity()


def set_cpu_count(int count):
    """
    Set the number of threads to use in parallel operations.
    """
    if count < 1:
        raise ValueError("CPU count must be strictly positive")
    check_status(SetCpuThreadPoolCapacity(count))


Type_NA = _Type_NA
Type_BOOL = _Type_BOOL
Type_UINT8 = _Type_UINT8
Type_INT8 = _Type_INT8
Type_UINT16 = _Type_UINT16
Type_INT16 = _Type_INT16
Type_UINT32 = _Type_UINT32
Type_INT32 = _Type_INT32
Type_UINT64 = _Type_UINT64
Type_INT64 = _Type_INT64
Type_HALF_FLOAT = _Type_HALF_FLOAT
Type_FLOAT = _Type_FLOAT
Type_DOUBLE = _Type_DOUBLE
Type_DECIMAL = _Type_DECIMAL
Type_DATE32 = _Type_DATE32
Type_DATE64 = _Type_DATE64
Type_TIMESTAMP = _Type_TIMESTAMP
Type_TIME32 = _Type_TIME32
Type_TIME64 = _Type_TIME64
Type_BINARY = _Type_BINARY
Type_STRING = _Type_STRING
Type_FIXED_SIZE_BINARY = _Type_FIXED_SIZE_BINARY
Type_LIST = _Type_LIST
Type_STRUCT = _Type_STRUCT
Type_UNION = _Type_UNION
Type_DICTIONARY = _Type_DICTIONARY
Type_MAP = _Type_MAP

UnionMode_SPARSE = _UnionMode_SPARSE
UnionMode_DENSE = _UnionMode_DENSE

# pandas API shim
include "pandas-shim.pxi"

# Exception types
include "error.pxi"

# Memory pools and allocation
include "memory.pxi"

# DataType, Field, Schema
include "types.pxi"

# Array scalar values
include "scalar.pxi"

# Array types
include "array.pxi"

# Builders
include "builder.pxi"

# Column, Table, Record Batch
include "table.pxi"

# File IO
include "io.pxi"
include "io-hdfs.pxi"

# IPC / Messaging
include "ipc.pxi"

# Feather format
include "feather.pxi"

# Python serialization
include "serialization.pxi"

# Micro-benchmark routines
include "benchmark.pxi"

# Public API
include "public-api.pxi"

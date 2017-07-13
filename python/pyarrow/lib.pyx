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

from cython.operator cimport dereference as deref
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.common cimport PyObject_to_object
cimport pyarrow.includes.libarrow as libarrow
cimport cpython as cp


import datetime
import decimal as _pydecimal
import numpy as np
import six
from pyarrow.compat import frombytes, tobytes, PandasSeries, Categorical

cdef _pandas():
    import pandas as pd
    return pd


arrow_init_numpy()

import numpy as np
set_numpy_nan(np.nan)

import multiprocessing
import os
cdef int CPU_COUNT = int(
    os.environ.get('OMP_NUM_THREADS',
                   max(multiprocessing.cpu_count() // 2, 1)))


def cpu_count():
    """
    Returns
    -------
    count : Number of CPUs to use by default in parallel operations. Default is
      max(1, multiprocessing.cpu_count() / 2), but can be overridden by the
      OMP_NUM_THREADS environment variable. For the default, we divide the CPU
      count by 2 because most modern computers have hyperthreading turned on,
      so doubling the CPU count beyond the number of physical cores does not
      help.
    """
    return CPU_COUNT

def set_cpu_count(count):
    global CPU_COUNT
    CPU_COUNT = max(int(count), 1)


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
Type_DICTIONARY = _Type_DICTIONARY


# Exception types
include "error.pxi"

# Memory pools and allocation
include "memory.pxi"

# Array types
include "array.pxi"

# Column, Table, Record Batch
include "table.pxi"

# File IO
include "io.pxi"

# IPC / Messaging
include "ipc.pxi"

# Feather format
include "feather.pxi"

#----------------------------------------------------------------------
# Public API

include "public-api.pxi"

#!/usr/bin/env python3
#
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

import os
import pyarrow as pa
from pyarrow.cffi import ffi

def make_schema():
    return pa.schema([('ints', pa.list_(pa.int32()))],
                     metadata={b'key1': b'value1'})

def make_batch():
    return pa.record_batch([[[1], [2, 42]]], make_schema())


def load_lib():
    libext = 'so'
    if os.name == 'nt':
        libext = 'dll'

    ffi.cdef(
        """
        void importSchema(uintptr_t ptr);
        void importRecordBatch(uintptr_t scptr, uintptr_t rbptr);
        """)
    return ffi.dlopen('./cgotest.{}'.format(libext))


c_schema = ffi.new("struct ArrowSchema*")
ptr_schema = int(ffi.cast("uintptr_t", c_schema))
make_schema()._export_to_c(ptr_schema)

cgotest = load_lib()
cgotest.importSchema(ptr_schema)

c_array = ffi.new("struct ArrowArray*")
ptr_array = int(ffi.cast("uintptr_t", c_array))

batch = make_batch()
batch._export_to_c(ptr_array)

cgotest.importRecordBatch(ptr_schema, ptr_array)

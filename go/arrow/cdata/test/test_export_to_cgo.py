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

import contextlib
import gc
import os
import unittest

import pyarrow as pa
from pyarrow.cffi import ffi


def load_cgotest():
    # XXX what about Darwin?
    libext = 'so'
    if os.name == 'nt':
        libext = 'dll'

    ffi.cdef(
        """
        void importSchema(uintptr_t ptr);
        void importRecordBatch(uintptr_t scptr, uintptr_t rbptr);
        void runGC();
        """)
    return ffi.dlopen(f'./cgotest.{libext}')


cgotest = load_cgotest()


class TestPythonToGo(unittest.TestCase):

    def setUp(self):
        self.c_schema = ffi.new("struct ArrowSchema*")
        self.ptr_schema = int(ffi.cast("uintptr_t", self.c_schema))
        self.c_array = ffi.new("struct ArrowArray*")
        self.ptr_array = int(ffi.cast("uintptr_t", self.c_array))

    def make_schema(self):
        return pa.schema([('ints', pa.list_(pa.int32()))],
                         metadata={b'key1': b'value1'})

    def make_batch(self):
        return pa.record_batch([[[1], [], None, [2, 42]]],
                               self.make_schema())

    def run_gc(self):
        # Several Go GC runs can be required to run all finalizers
        for i in range(5):
            cgotest.runGC()
        gc.collect()

    @contextlib.contextmanager
    def assert_pyarrow_memory_released(self):
        self.run_gc()
        old_allocated = pa.total_allocated_bytes()
        yield
        self.run_gc()
        diff = pa.total_allocated_bytes() - old_allocated
        self.assertEqual(
            pa.total_allocated_bytes(), old_allocated,
            f"PyArrow memory was not adequately released: {diff} bytes lost")

    def test_schema(self):
        with self.assert_pyarrow_memory_released():
            self.make_schema()._export_to_c(self.ptr_schema)
            # Will panic if expectations are not met
            cgotest.importSchema(self.ptr_schema)

    def test_record_batch(self):
        with self.assert_pyarrow_memory_released():
            self.make_schema()._export_to_c(self.ptr_schema)
            self.make_batch()._export_to_c(self.ptr_array)
            # Will panic if expectations are not met
            cgotest.importRecordBatch(self.ptr_schema, self.ptr_array)


if __name__ == '__main__':
    unittest.main(verbosity=2)

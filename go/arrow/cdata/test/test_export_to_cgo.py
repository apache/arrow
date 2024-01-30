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
        long long totalAllocated();
        void importSchema(uintptr_t ptr);
        void importRecordBatch(uintptr_t scptr, uintptr_t rbptr);
        void runGC();
        void exportSchema(uintptr_t ptr);
        void exportRecordBatch(uintptr_t schema, uintptr_t record);
        void importThenExportSchema(uintptr_t input, uintptr_t output);
        void importThenExportRecord(uintptr_t schemaIn, uintptr_t arrIn, 
                                    uintptr_t schemaOut, uintptr_t arrOut);
        void roundtripArray(uintptr_t arrIn, uintptr_t schema, uintptr_t arrOut);
        """)
    return ffi.dlopen(f'./cgotest.{libext}')


cgotest = load_cgotest()

class BaseTestGoPython(unittest.TestCase):
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
        old_go_allocated = cgotest.totalAllocated()
        yield
        self.run_gc()
        diff = pa.total_allocated_bytes() - old_allocated
        godiff = cgotest.totalAllocated() - old_go_allocated
        self.assertEqual(
            pa.total_allocated_bytes(), old_allocated,
            f"PyArrow memory was not adequately released: {diff} bytes lost")
        self.assertEqual(
            cgotest.totalAllocated(), old_go_allocated,
            f"Go memory was not properly released: {godiff} bytes lost")
        

class TestPythonToGo(BaseTestGoPython):
    
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


class TestGoToPython(BaseTestGoPython):

    def test_get_schema(self):
        with self.assert_pyarrow_memory_released():
            cgotest.exportSchema(self.ptr_schema)

            sc = pa.Schema._import_from_c(self.ptr_schema)
            assert sc == self.make_schema()
    
    def test_get_batch(self):
        with self.assert_pyarrow_memory_released():
            cgotest.exportRecordBatch(self.ptr_schema, self.ptr_array)
            arrnew = pa.RecordBatch._import_from_c(self.ptr_array, self.ptr_schema)
            assert arrnew == self.make_batch()
            del arrnew
    
class TestRoundTrip(BaseTestGoPython):

    def test_schema_roundtrip(self):
        with self.assert_pyarrow_memory_released():
            # make sure that Python -> Go -> Python ends up with
            # the same exact schema
            schema = self.make_schema()
            schema._export_to_c(self.ptr_schema)
            del schema
            
            c_schema = ffi.new("struct ArrowSchema*")
            ptr_schema = int(ffi.cast("uintptr_t", c_schema))

            cgotest.importThenExportSchema(self.ptr_schema, ptr_schema)
            schema_new = pa.Schema._import_from_c(ptr_schema)
            assert schema_new == self.make_schema()
            del c_schema

    def test_batch_roundtrip(self):
        with self.assert_pyarrow_memory_released():
            # make sure that Python -> Go -> Python for record
            # batches works correctly and gets the same data in the end
            schema = self.make_schema()
            batch = self.make_batch()
            schema._export_to_c(self.ptr_schema)
            batch._export_to_c(self.ptr_array)
            del schema
            del batch

            c_schema = ffi.new("struct ArrowSchema*")
            c_batch = ffi.new("struct ArrowArray*")
            ptr_schema = int(ffi.cast("uintptr_t", c_schema))
            ptr_batch = int(ffi.cast("uintptr_t", c_batch))

            cgotest.importThenExportRecord(self.ptr_schema, self.ptr_array, 
                                           ptr_schema, ptr_batch)
            batch_new = pa.RecordBatch._import_from_c(ptr_batch, ptr_schema)
            assert batch_new == self.make_batch()
            del batch_new
            del c_schema
            del c_batch

    # commented out types can be uncommented after
    # GH-14875 is addressed
    _test_pyarrow_types = [
        pa.null(),
        pa.bool_(),
        pa.int32(),
        pa.time32("s"),
        pa.time64("us"),
        pa.date32(),
        pa.timestamp("us"),
        pa.timestamp("us", tz="UTC"),
        pa.timestamp("us", tz="Europe/Paris"),
        pa.duration("s"),
        pa.duration("ms"),
        pa.duration("us"),
        pa.duration("ns"),
        pa.float16(),
        pa.float32(),
        pa.float64(),
        pa.decimal128(19, 4),        
        pa.string(),
        pa.binary(),
        pa.binary(10),
        pa.large_string(),
        pa.large_binary(),
        pa.list_(pa.int32()),
        pa.list_(pa.int32(), 2),
        pa.large_list(pa.uint16()),
        pa.struct([
            pa.field("a", pa.int32()),
            pa.field("b", pa.int8()),
            pa.field("c", pa.string()),
        ]),
        pa.struct([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.int8(), nullable=False),
            pa.field("c", pa.string()),
        ]),
        pa.dictionary(pa.int8(), pa.int64()),
        pa.dictionary(pa.int8(), pa.string()),
        pa.map_(pa.string(), pa.int32()),
        pa.map_(pa.int64(), pa.int32()),
        # pa.run_end_encoded(pa.int16(), pa.int64()),
    ]

    def test_empty_roundtrip(self):
        for typ in self._test_pyarrow_types:
            with self.subTest(typ=typ):
                with self.assert_pyarrow_memory_released():
                    a = pa.array([], typ)
                    a._export_to_c(self.ptr_array)
                    typ._export_to_c(self.ptr_schema)
                    
                    c_arr = ffi.new("struct ArrowArray*")
                    ptr_arr = int(ffi.cast("uintptr_t", c_arr))

                    cgotest.roundtripArray(self.ptr_array, self.ptr_schema, ptr_arr)
                    b = pa.Array._import_from_c(ptr_arr, typ)
                    b.validate(full=True)
                    assert a.to_pylist() == b.to_pylist()
                    assert a.type == b.type
                    del a
                    del b

if __name__ == '__main__':
    unittest.main(verbosity=2)

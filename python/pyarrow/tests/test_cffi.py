# -*- coding: utf-8 -*-
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

import pyarrow as pa
try:
    from pyarrow.cffi import ffi
except ImportError:
    ffi = None

import pytest

needs_cffi = pytest.mark.skipif(ffi is None,
                                reason="test needs cffi package installed")


@needs_cffi
def test_export_import_array():
    c = ffi.new("struct ArrowArray*")
    ptr = int(ffi.cast("uintptr_t", c))

    old_allocated = pa.total_allocated_bytes()

    arr = pa.array([1, 2, 42])
    py_value = arr.to_pylist()
    arr._export_to_c(ptr)
    new_allocated = pa.total_allocated_bytes()
    assert new_allocated > old_allocated
    # Delete C++ object and recreate new C++ object from exported pointer
    del arr
    arr_new = pa.Array._import_from_c(ptr)
    assert arr_new.to_pylist() == py_value
    assert pa.total_allocated_bytes() == new_allocated
    del arr_new
    assert pa.total_allocated_bytes() == old_allocated


@needs_cffi
def test_export_import_batch():
    c = ffi.new("struct ArrowArray*")
    ptr = int(ffi.cast("uintptr_t", c))

    old_allocated = pa.total_allocated_bytes()

    batch = pa.record_batch(
        [pa.array([1, 2, None]), pa.array([True, False, False])],
        names=['ints', 'bools'])
    py_value = batch.to_pydict()
    batch._export_to_c(ptr)
    new_allocated = pa.total_allocated_bytes()
    assert new_allocated > old_allocated
    # Delete C++ object and recreate new C++ object from exported pointer
    del batch
    batch_new = pa.RecordBatch._import_from_c(ptr)
    assert batch_new.to_pydict() == py_value
    assert pa.total_allocated_bytes() == new_allocated
    del batch_new
    assert pa.total_allocated_bytes() == old_allocated

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

import gc
import pytest

import pyarrow as pa


try:
    pa.jemalloc_memory_pool()
    HAVE_JEMALLOC = True
except ImportError:
    HAVE_JEMALLOC = False


jemalloc = pytest.mark.skipif(not HAVE_JEMALLOC,
                              reason='jemalloc support not built')


@jemalloc
def test_different_memory_pool():
    gc.collect()
    bytes_before_default = pa.total_allocated_bytes()
    bytes_before_jemalloc = pa.jemalloc_memory_pool().bytes_allocated()

    # it works
    array = pa.array([1, None, 3, None],  # noqa
                     memory_pool=pa.jemalloc_memory_pool())
    gc.collect()
    assert pa.total_allocated_bytes() == bytes_before_default
    assert (pa.jemalloc_memory_pool().bytes_allocated() >
            bytes_before_jemalloc)


@jemalloc
def test_default_memory_pool():
    gc.collect()
    bytes_before_default = pa.total_allocated_bytes()
    bytes_before_jemalloc = pa.jemalloc_memory_pool().bytes_allocated()

    old_memory_pool = pa.default_memory_pool()
    pa.set_memory_pool(pa.jemalloc_memory_pool())

    array = pa.array([1, None, 3, None])  # noqa

    pa.set_memory_pool(old_memory_pool)
    gc.collect()

    assert pa.total_allocated_bytes() == bytes_before_default

    assert (pa.jemalloc_memory_pool().bytes_allocated() >
            bytes_before_jemalloc)

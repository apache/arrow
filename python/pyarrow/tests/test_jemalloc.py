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

try:
    import pyarrow.jemalloc
    HAVE_JEMALLOC = True
except ImportError:
    HAVE_JEMALLOC = False

jemalloc = pytest.mark.skipif(not HAVE_JEMALLOC,
                              reason='jemalloc support not built')


@jemalloc
def test_different_memory_pool():
    gc.collect()
    bytes_before_default = pyarrow.total_allocated_bytes()
    bytes_before_jemalloc = pyarrow.jemalloc.default_pool().bytes_allocated()

    # it works
    array = pyarrow.from_pylist([1, None, 3, None],  # noqa
                                memory_pool=pyarrow.jemalloc.default_pool())
    gc.collect()
    assert pyarrow.total_allocated_bytes() == bytes_before_default
    assert (pyarrow.jemalloc.default_pool().bytes_allocated() >
            bytes_before_jemalloc)


@jemalloc
def test_default_memory_pool():
    gc.collect()
    bytes_before_default = pyarrow.total_allocated_bytes()
    bytes_before_jemalloc = pyarrow.jemalloc.default_pool().bytes_allocated()

    old_memory_pool = pyarrow.memory.default_pool()
    pyarrow.memory.set_default_pool(pyarrow.jemalloc.default_pool())

    array = pyarrow.from_pylist([1, None, 3, None])  # noqa

    pyarrow.memory.set_default_pool(old_memory_pool)
    gc.collect()

    assert pyarrow.total_allocated_bytes() == bytes_before_default

    assert (pyarrow.jemalloc.default_pool().bytes_allocated() >
            bytes_before_jemalloc)

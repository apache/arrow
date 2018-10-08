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

import pyarrow as pa


@contextlib.contextmanager
def allocate_bytes(pool, nbytes):
    """
    Temporarily allocate *nbytes* from the given *pool*.
    """
    arr = pa.array([b"x" * nbytes], type=pa.binary(), memory_pool=pool)
    # Fetch the values buffer from the varbinary array and release the rest,
    # to get the desired allocation amount
    buf = arr.buffers()[2]
    arr = None
    assert len(buf) == nbytes
    try:
        yield
    finally:
        buf = None


def check_allocated_bytes(pool):
    """
    Check allocation stats on *pool*.
    """
    allocated_before = pool.bytes_allocated()
    max_mem_before = pool.max_memory()
    with allocate_bytes(pool, 512):
        assert pool.bytes_allocated() == allocated_before + 512
        new_max_memory = pool.max_memory()
        assert pool.max_memory() >= max_mem_before
    assert pool.bytes_allocated() == allocated_before
    assert pool.max_memory() == new_max_memory


def test_default_allocated_bytes():
    pool = pa.default_memory_pool()
    with allocate_bytes(pool, 1024):
        check_allocated_bytes(pool)
        assert pool.bytes_allocated() == pa.total_allocated_bytes()


def test_proxy_memory_pool():
    pool = pa.proxy_memory_pool(pa.default_memory_pool())
    check_allocated_bytes(pool)


def test_logging_memory_pool(capfd):
    pool = pa.logging_memory_pool(pa.default_memory_pool())
    check_allocated_bytes(pool)
    out, err = capfd.readouterr()
    assert err == ""
    assert out.count("Allocate:") > 0
    assert out.count("Allocate:") == out.count("Free:")


def test_set_memory_pool():
    old_pool = pa.default_memory_pool()
    pool = pa.proxy_memory_pool(old_pool)
    pa.set_memory_pool(pool)
    try:
        allocated_before = pool.bytes_allocated()
        with allocate_bytes(None, 512):
            assert pool.bytes_allocated() == allocated_before + 512
        assert pool.bytes_allocated() == allocated_before
    finally:
        pa.set_memory_pool(old_pool)

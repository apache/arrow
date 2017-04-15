.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

jemalloc MemoryPool
===================

Arrow's default :class:`~pyarrow.memory.MemoryPool` uses the system's allocator
through the POSIX APIs. Although this already provides aligned allocation, the
POSIX interface doesn't support aligned reallocation. The default reallocation
strategy is to allocate a new region, copy over the old data and free the
previous region. Using `jemalloc <http://jemalloc.net/>`_ we can simply extend
the existing memory allocation to the requested size. While this may still be
linear in the size of allocated memory, it is magnitudes faster as only the page
mapping in the kernel is touched, not the actual data.

The :mod:`~pyarrow.jemalloc` allocator is not enabled by default to allow the
use of the system allocator and/or other allocators like ``tcmalloc``. You can
either explicitly make it the default allocator or pass it only to single
operations.

.. code:: python

    import pyarrow as pa

    jemalloc_pool = pyarrow.jemalloc_memory_pool()

    # Explicitly use jemalloc for allocating memory for an Arrow Table object
    array = pa.Array.from_pylist([1, 2, 3], memory_pool=jemalloc_pool)

    # Set the global pool
    pyarrow.set_memory_pool(jemalloc_pool)
    # This operation has no explicit MemoryPool specified and will thus will
    # also use jemalloc for its allocations.
    array = pa.Array.from_pylist([1, 2, 3])

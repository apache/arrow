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

.. currentmodule:: pyarrow

.. _api.memory:

Buffers and Memory
==================

In-Memory Buffers
-----------------

Factory Functions
~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: ../generated/

   allocate_buffer
   py_buffer
   foreign_buffer

Classes
~~~~~~~

.. autosummary::
   :toctree: ../generated/

   Buffer
   ResizableBuffer

Miscellaneous
~~~~~~~~~~~~~

.. autosummary::
   :toctree: ../generated/

   compress
   decompress

.. _api.memory_pool:

Memory Pools
------------

.. autosummary::
   :toctree: ../generated/

   MemoryPool
   default_memory_pool
   total_allocated_bytes
   set_memory_pool
   log_memory_allocations

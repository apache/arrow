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

.. currentmodule:: pyarrow.cuda

CUDA Integration
================

.. ifconfig:: not cuda_enabled

   .. error::
      This documentation was built without CUDA enabled.  The CUDA
      API docs are not available.

.. NOTE We still generate those API docs (with empty docstrings)
.. when CUDA is disabled and `pyarrow.cuda` mocked (see conf.py).
.. Otherwise we'd get autodoc warnings, see https://github.com/sphinx-doc/sphinx/issues/4770

CUDA Contexts
-------------

.. autosummary::
   :toctree: ../generated/

   Context

CUDA Buffers
------------

.. autosummary::
   :toctree: ../generated/

   CudaBuffer
   new_host_buffer
   HostBuffer
   BufferReader
   BufferWriter

Serialization and IPC
---------------------

.. autosummary::
   :toctree: ../generated/

   serialize_record_batch
   read_record_batch
   read_message
   IpcMemHandle

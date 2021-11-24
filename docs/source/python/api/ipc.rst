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

.. _api.ipc:

Serialization and IPC
=====================

Inter-Process Communication
---------------------------

.. autosummary::
   :toctree: ../generated/

   ipc.new_file
   ipc.open_file
   ipc.new_stream
   ipc.open_stream
   ipc.read_message
   ipc.read_record_batch
   ipc.get_record_batch_size
   ipc.read_tensor
   ipc.write_tensor
   ipc.get_tensor_size
   ipc.IpcWriteOptions
   ipc.Message
   ipc.MessageReader
   ipc.RecordBatchFileReader
   ipc.RecordBatchFileWriter
   ipc.RecordBatchStreamReader
   ipc.RecordBatchStreamWriter

Serialization
-------------

.. warning::

   The serialization functionality is deprecated in pyarrow 2.0, and will
   be removed in a future version. Use the standard library ``pickle`` or
   the IPC functionality of pyarrow (see :ref:`ipc`).


.. autosummary::
   :toctree: ../generated/

   serialize
   serialize_to
   deserialize
   deserialize_components
   deserialize_from
   read_serialized
   SerializedPyObject
   SerializationContext

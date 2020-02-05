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

   ipc.open_file
   ipc.open_stream
   Message
   MessageReader
   RecordBatchFileReader
   RecordBatchFileWriter
   RecordBatchStreamReader
   RecordBatchStreamWriter
   read_message
   read_record_batch
   get_record_batch_size
   read_tensor
   write_tensor
   get_tensor_size

Serialization
-------------

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

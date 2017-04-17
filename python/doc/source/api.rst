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
.. _api:

*************
API Reference
*************

.. _api.functions:

Type Metadata and Schemas
-------------------------

.. autosummary::
   :toctree: generated/

   null
   bool_
   int8
   int16
   int32
   int64
   uint8
   uint16
   uint32
   uint64
   float16
   float32
   float64
   timestamp
   date32
   date64
   binary
   string
   decimal
   list_
   struct
   dictionary
   field
   DataType
   Field
   Schema
   schema

Scalar Value Types
------------------

.. autosummary::
   :toctree: generated/

   NA
   NAType
   Scalar
   ArrayValue
   Int8Value
   Int16Value
   Int32Value
   Int64Value
   UInt8Value
   UInt16Value
   UInt32Value
   UInt64Value
   FloatValue
   DoubleValue
   ListValue
   BinaryValue
   StringValue
   FixedSizeBinaryValue

Array Types
-----------

.. autosummary::
   :toctree: generated/

   Array
   NullArray
   NumericArray
   IntegerArray
   FloatingPointArray
   BooleanArray
   Int8Array
   Int16Array
   Int32Array
   Int64Array
   UInt8Array
   UInt16Array
   UInt32Array
   UInt64Array
   DictionaryArray
   StringArray

Tables and Record Batches
-------------------------

.. autosummary::
   :toctree: generated/

   Column
   RecordBatch
   Table

Tensor type and Functions
-------------------------

.. autosummary::
   :toctree: generated/

   Tensor
   write_tensor
   get_tensor_size
   read_tensor

Input / Output and Shared Memory
--------------------------------

.. autosummary::
   :toctree: generated/

   Buffer
   BufferReader
   InMemoryOutputStream
   NativeFile
   MemoryMappedFile
   memory_map
   create_memory_map
   PythonFileInterface

Interprocess Communication and Messaging
----------------------------------------

.. autosummary::
   :toctree: generated/

   FileReader
   FileWriter
   StreamReader
   StreamWriter

Memory Pools
------------

.. autosummary::
   :toctree: generated/

   MemoryPool
   default_memory_pool
   jemalloc_memory_pool
   total_allocated_bytes
   set_memory_pool

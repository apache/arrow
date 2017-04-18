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

Type and Schema Factory Functions
---------------------------------

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
   time32
   time64
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
   schema
   from_numpy_dtype

Scalar Value Types
------------------

.. autosummary::
   :toctree: generated/

   NA
   NAType
   Scalar
   ArrayValue
   BooleanValue
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
   Date32Value
   Date64Value
   TimestampValue
   DecimalValue


Array Types and Constructors
----------------------------

.. autosummary::
   :toctree: generated/

   array
   Array
   BooleanArray
   DictionaryArray
   FloatingPointArray
   IntegerArray
   Int8Array
   Int16Array
   Int32Array
   Int64Array
   NullArray
   NumericArray
   UInt8Array
   UInt16Array
   UInt32Array
   UInt64Array
   BinaryArray
   FixedSizeBinaryArray
   StringArray
   Time32Array
   Time64Array
   Date32Array
   Date64Array
   TimestampArray
   DecimalArray
   ListArray

Tables and Record Batches
-------------------------

.. autosummary::
   :toctree: generated/

   ChunkedArray
   Column
   RecordBatch
   Table
   get_record_batch_size

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
   PythonFile

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

Type Classes
------------

.. autosummary::
   :toctree: generated/

   DataType
   DecimalType
   DictionaryType
   FixedSizeBinaryType
   Time32Type
   Time64Type
   TimestampType
   Field
   Schema

.. currentmodule:: pyarrow.parquet

Apache Parquet
--------------

.. autosummary::
   :toctree: generated/

   ParquetDataset
   ParquetFile
   read_table
   write_metadata
   write_table

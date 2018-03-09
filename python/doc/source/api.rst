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

.. _api.types:

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
   decimal128
   list_
   struct
   dictionary
   field
   schema
   from_numpy_dtype

.. currentmodule:: pyarrow.types
.. _api.types.checking:

Type checking functions
-----------------------

.. autosummary::
   :toctree: generated/

   is_boolean
   is_integer
   is_signed_integer
   is_unsigned_integer
   is_int8
   is_int16
   is_int32
   is_int64
   is_uint8
   is_uint16
   is_uint32
   is_uint64
   is_floating
   is_float16
   is_float32
   is_float64
   is_decimal
   is_list
   is_struct
   is_union
   is_nested
   is_temporal
   is_timestamp
   is_date
   is_date32
   is_date64
   is_time
   is_time32
   is_time64
   is_null
   is_binary
   is_unicode
   is_string
   is_fixed_size_binary
   is_map
   is_dictionary

.. currentmodule:: pyarrow

.. _api.value:

Scalar Value Types
------------------

.. autosummary::
   :toctree: generated/

   NA
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

.. _api.array:

.. currentmodule:: pyarrow.lib

Array Types
-----------

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
   Decimal128Array
   ListArray

.. _api.table:

.. currentmodule:: pyarrow

Tables and Record Batches
-------------------------

.. autosummary::
   :toctree: generated/

   column
   chunked_array
   concat_tables
   ChunkedArray
   Column
   RecordBatch
   Table

.. _api.tensor:

Tensor type and Functions
-------------------------

.. autosummary::
   :toctree: generated/

   Tensor

.. _api.io:

Input / Output and Shared Memory
--------------------------------

.. autosummary::
   :toctree: generated/

   allocate_buffer
   compress
   decompress
   frombuffer
   foreign_buffer
   Buffer
   ResizableBuffer
   BufferReader
   BufferOutputStream
   NativeFile
   MemoryMappedFile
   memory_map
   create_memory_map
   PythonFile

File Systems
------------

.. autosummary::
   :toctree: generated/

   hdfs.connect
   LocalFileSystem

.. class:: HadoopFileSystem
   :noindex:

.. _api.ipc:

Serialization and IPC
---------------------

.. autosummary::
   :toctree: generated/

   Message
   MessageReader
   RecordBatchFileReader
   RecordBatchFileWriter
   RecordBatchStreamReader
   RecordBatchStreamWriter
   open_file
   open_stream
   read_message
   read_record_batch
   get_record_batch_size
   read_tensor
   write_tensor
   get_tensor_size
   serialize
   serialize_to
   deserialize
   deserialize_components
   deserialize_from
   read_serialized
   SerializedPyObject
   SerializationContext

.. _api.feather:

Feather Format
~~~~~~~~~~~~~~

.. currentmodule:: pyarrow.feather

.. _api.memory_pool:

.. autosummary::
   :toctree: generated/

   read_feather
   write_feather

Memory Pools
------------

.. currentmodule:: pyarrow

.. autosummary::
   :toctree: generated/

   MemoryPool
   default_memory_pool
   total_allocated_bytes
   set_memory_pool
   log_memory_allocations

.. _api.type_classes:

.. currentmodule:: pyarrow.lib

Type Classes
------------

.. autosummary::
   :toctree: generated/

   DataType
   Field
   Schema

.. currentmodule:: pyarrow.plasma

.. _api.plasma:

In-Memory Object Store
----------------------

.. autosummary::
   :toctree: generated/

   ObjectID
   PlasmaClient
   PlasmaBuffer

.. currentmodule:: pyarrow.parquet

.. _api.parquet:

Apache Parquet
--------------

.. autosummary::
   :toctree: generated/

   ParquetDataset
   ParquetFile
   ParquetWriter
   read_table
   read_metadata
   read_pandas
   read_schema
   write_metadata
   write_table

.. currentmodule:: pyarrow

Using with C extensions
-----------------------

.. autosummary::
   :toctree: generated/

   get_include
   get_libraries
   get_library_dirs

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

.. default-domain:: cpp
.. highlight:: cpp

.. cpp:namespace:: parquet::arrow

=================================
Reading and writing Parquet files
=================================

The Parquet C++ library is part of the Apache Arrow project and benefits
from tight integration with Arrow C++.

Reading
=======

The Parquet :class:`FileReader` requires a :class:`::arrow::io::RandomAccessFile`
instance representing the input file.

.. code-block:: cpp

   #include "arrow/parquet/arrow/reader.h"

   {
      // ...
      arrow::Status st;
      arrow::MemoryPool* pool = default_memory_pool();
      std::shared_ptr<arrow::io::RandomAccessFile> input = ...;

      // Open Parquet file reader
      std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
      st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
      if (!st.ok()) {
         // Handle error instantiating file reader...
      }

      // Read entire file as a single Arrow table
      std::shared_ptr<arrow::Table> table;
      st = arrow_reader->ReadTable(&table);
      if (!st.ok()) {
         // Handle error reading Parquet data...
      }
   }

Finer-grained options are available through the :class:`FileReaderBuilder`
helper class.

.. TODO write section about performance and memory efficiency

Writing
=======

TODO: write this

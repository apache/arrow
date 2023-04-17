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

.. cpp:namespace:: arrow::csv

=============================
Reading and Writing CSV files
=============================

Arrow provides a fast CSV reader allowing ingestion of external data
to create Arrow Tables or a stream of Arrow RecordBatches.

.. seealso::
   :ref:`CSV reader/writer API reference <cpp-api-csv>`.

Reading CSV files
=================

Data in a CSV file can either be read in as a single Arrow Table using
:class:`~arrow::csv::TableReader` or streamed as RecordBatches using
:class:`~arrow::csv::StreamingReader`. See :ref:`Tradeoffs <cpp-csv-tradeoffs>` for a
discussion of the tradeoffs between the two methods.

Both these readers require an :class:`arrow::io::InputStream` instance
representing the input file. Their behavior can be customized using a
combination of :class:`~arrow::csv::ReadOptions`,
:class:`~arrow::csv::ParseOptions`, and :class:`~arrow::csv::ConvertOptions`.

TableReader
-----------

.. code-block:: cpp

   #include "arrow/csv/api.h"

   {
      // ...
      arrow::io::IOContext io_context = arrow::io::default_io_context();
      std::shared_ptr<arrow::io::InputStream> input = ...;

      auto read_options = arrow::csv::ReadOptions::Defaults();
      auto parse_options = arrow::csv::ParseOptions::Defaults();
      auto convert_options = arrow::csv::ConvertOptions::Defaults();

      // Instantiate TableReader from input stream and options
      auto maybe_reader =
        arrow::csv::TableReader::Make(io_context,
                                      input,
                                      read_options,
                                      parse_options,
                                      convert_options);
      if (!maybe_reader.ok()) {
        // Handle TableReader instantiation error...
      }
      std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

      // Read table from CSV file
      auto maybe_table = reader->Read();
      if (!maybe_table.ok()) {
        // Handle CSV read error
        // (for example a CSV syntax error or failed type conversion)
      }
      std::shared_ptr<arrow::Table> table = *maybe_table;
   }

StreamingReader
---------------

.. code-block:: cpp

   #include "arrow/csv/api.h"

   {
      // ...
      arrow::io::IOContext io_context = arrow::io::default_io_context();
      std::shared_ptr<arrow::io::InputStream> input = ...;

      auto read_options = arrow::csv::ReadOptions::Defaults();
      auto parse_options = arrow::csv::ParseOptions::Defaults();
      auto convert_options = arrow::csv::ConvertOptions::Defaults();

      // Instantiate StreamingReader from input stream and options
      auto maybe_reader =
        arrow::csv::StreamingReader::Make(io_context,
                                          input,
                                          read_options,
                                          parse_options,
                                          convert_options);
      if (!maybe_reader.ok()) {
        // Handle StreamingReader instantiation error...
      }
      std::shared_ptr<arrow::csv::StreamingReader> reader = *maybe_reader;

      // Set aside a RecordBatch pointer for re-use while streaming
      std::shared_ptr<RecordBatch> batch;

      while (true) {
          // Attempt to read the first RecordBatch
          arrow::Status status = reader->ReadNext(&batch);

          if (!status.ok()) {
            // Handle read error
          }

          if (batch == NULL) {
            // Handle end of file
            break;
          }

          // Do something with the batch
      }
   }

.. _cpp-csv-tradeoffs:

Tradeoffs
---------

The choice between using :class:`~arrow::csv::TableReader` or
:class:`~arrow::csv::StreamingReader` will ultimately depend on the use case
but there are a few tradeoffs to be aware of:

1. **Memory usage:** :class:`~arrow::csv::TableReader` loads all of the data
   into memory at once and, depending on the amount of data, may require
   considerably more memory than :class:`~arrow::csv::StreamingReader` which
   only loads one :class:`~arrow::RecordBatch` at a time. This is likely to be
   the most significant tradeoff for users.
2. **Speed:** When reading the entire contents of a CSV,
   :class:`~arrow::csv::TableReader` will tend to be faster than
   :class:`~arrow::csv::StreamingReader` because it makes better use of
   available cores. See :ref:`Performance <cpp-csv-performance>` for more
   details.
3. **Flexibility:** :class:`~arrow::csv::StreamingReader` might be considered
   less flexible than :class:`~arrow::csv::TableReader` because it performs type
   inference only on the first block that's read in, after which point the types
   are frozen and any data in subsequent blocks that cannot be converted to
   those types will cause an error. Note that this can be remedied either by
   setting :member:`ReadOptions::block_size` to a large enough value or by using
   :member:`ConvertOptions::column_types` to set the desired data types
   explicitly.

Writing CSV files
=================

A CSV file is written to a :class:`~arrow::io::OutputStream`.

.. code-block:: cpp

   #include <arrow/csv/api.h>
   {
       // Oneshot write
       // ...
       std::shared_ptr<arrow::io::OutputStream> output = ...;
       auto write_options = arrow::csv::WriteOptions::Defaults();
       if (WriteCSV(table, write_options, output.get()).ok()) {
           // Handle writer error...
       }
   }
   {
       // Write incrementally
       // ...
       std::shared_ptr<arrow::io::OutputStream> output = ...;
       auto write_options = arrow::csv::WriteOptions::Defaults();
       auto maybe_writer = arrow::csv::MakeCSVWriter(output, schema, write_options);
       if (!maybe_writer.ok()) {
           // Handle writer instantiation error...
       }
       std::shared_ptr<arrow::ipc::RecordBatchWriter> writer = *maybe_writer;

       // Write batches...
       if (!writer->WriteRecordBatch(*batch).ok()) {
           // Handle write error...
       }

       if (!writer->Close().ok()) {
           // Handle close error...
       }
       if (!output->Close().ok()) {
           // Handle file close error...
       }
   }

.. note:: The writer does not yet support all Arrow types.

Column names
============

There are three possible ways to infer column names from the CSV file:

* By default, the column names are read from the first row in the CSV file
* If :member:`ReadOptions::column_names` is set, it forces the column
  names in the table to these values (the first row in the CSV file is
  read as data)
* If :member:`ReadOptions::autogenerate_column_names` is true, column names
  will be autogenerated with the pattern "f0", "f1"... (the first row in the
  CSV file is read as data)

Column selection
================

By default, Arrow reads all columns in the CSV file.  You can narrow the
selection of columns with the :member:`ConvertOptions::include_columns`
option.  If some columns in :member:`ConvertOptions::include_columns`
are missing from the CSV file, an error will be emitted unless
:member:`ConvertOptions::include_missing_columns` is true, in which case
the missing columns are assumed to contain all-null values.

Interaction with column names
-----------------------------

If both :member:`ReadOptions::column_names` and
:member:`ConvertOptions::include_columns` are specified,
the :member:`ReadOptions::column_names` are assumed to map to CSV columns,
and :member:`ConvertOptions::include_columns` is a subset of those column
names that will part of the Arrow Table.

Data types
==========

By default, the CSV reader infers the most appropriate data type for each
column.  Type inference considers the following data types, in order:

* Null
* Int64
* Boolean
* Date32
* Time32 (with seconds unit)
* Timestamp (with seconds unit)
* Timestamp (with nanoseconds unit)
* Float64
* Dictionary<String> (if :member:`ConvertOptions::auto_dict_encode` is true)
* Dictionary<Binary> (if :member:`ConvertOptions::auto_dict_encode` is true)
* String
* Binary

It is possible to override type inference for select columns by setting
the :member:`ConvertOptions::column_types` option.  Explicit data types
can be chosen from the following list:

* Null
* All Integer types
* Float32 and Float64
* Decimal128
* Boolean
* Date32 and Date64
* Time32 and Time64
* Timestamp
* Binary and Large Binary
* String and Large String (with optional UTF8 input validation)
* Fixed-Size Binary
* Dictionary with index type Int32 and value type one of the following:
  Binary, String, LargeBinary, LargeString,  Int32, UInt32, Int64, UInt64,
  Float32, Float64, Decimal128

Other data types do not support conversion from CSV values and will error out.

Dictionary inference
--------------------

If type inference is enabled and :member:`ConvertOptions::auto_dict_encode`
is true, the CSV reader first tries to convert string-like columns to a
dictionary-encoded string-like array.  It switches to a plain string-like
array when the threshold in :member:`ConvertOptions::auto_dict_max_cardinality`
is reached.

Timestamp inference/parsing
---------------------------

If type inference is enabled, the CSV reader first tries to interpret
string-like columns as timestamps. If all rows have some zone offset
(e.g. ``Z`` or ``+0100``), even if the offsets are inconsistent, then the
inferred type will be UTC timestamp. If no rows have a zone offset, then the
inferred type will be timestamp without timezone. A mix of rows with/without
offsets will result in a string column.

If the type is explicitly specified as a timestamp with/without timezone, then
the reader will error on values without/with zone offsets in that column. Note
that this means it isn't currently possible to have the reader parse a column
of timestamps without zone offsets as local times in a particular timezone;
instead, parse the column as timestamp without timezone, then convert the
values afterwards using the ``assume_timezone`` compute function.

+-------------------+------------------------------+-------------------+
| Specified Type    | Input CSV                    | Result Type       |
+===================+==============================+===================+
| (inferred)        | ``2021-01-01T00:00:00``      | timestamp[s]      |
|                   +------------------------------+-------------------+
|                   | ``2021-01-01T00:00:00Z``     | timestamp[s, UTC] |
|                   +------------------------------+                   |
|                   | ``2021-01-01T00:00:00+0100`` |                   |
|                   +------------------------------+-------------------+
|                   | ::                           | string            |
|                   |                              |                   |
|                   |     2021-01-01T00:00:00      |                   |
|                   |     2021-01-01T00:00:00Z     |                   |
+-------------------+------------------------------+-------------------+
| timestamp[s]      | ``2021-01-01T00:00:00``      | timestamp[s]      |
|                   +------------------------------+-------------------+
|                   | ``2021-01-01T00:00:00Z``     | (error)           |
|                   +------------------------------+                   |
|                   | ``2021-01-01T00:00:00+0100`` |                   |
|                   +------------------------------+                   |
|                   | ::                           |                   |
|                   |                              |                   |
|                   |     2021-01-01T00:00:00      |                   |
|                   |     2021-01-01T00:00:00Z     |                   |
+-------------------+------------------------------+-------------------+
| timestamp[s, UTC] | ``2021-01-01T00:00:00``      | (error)           |
|                   +------------------------------+-------------------+
|                   | ``2021-01-01T00:00:00Z``     | timestamp[s, UTC] |
|                   +------------------------------+                   |
|                   | ``2021-01-01T00:00:00+0100`` |                   |
|                   +------------------------------+-------------------+
|                   | ::                           | (error)           |
|                   |                              |                   |
|                   |     2021-01-01T00:00:00      |                   |
|                   |     2021-01-01T00:00:00Z     |                   |
+-------------------+------------------------------+-------------------+
| timestamp[s,      | ``2021-01-01T00:00:00``      | (error)           |
| America/New_York] +------------------------------+-------------------+
|                   | ``2021-01-01T00:00:00Z``     | timestamp[s,      |
|                   +------------------------------+ America/New_York] |
|                   | ``2021-01-01T00:00:00+0100`` |                   |
|                   +------------------------------+-------------------+
|                   | ::                           | (error)           |
|                   |                              |                   |
|                   |     2021-01-01T00:00:00      |                   |
|                   |     2021-01-01T00:00:00Z     |                   |
+-------------------+------------------------------+-------------------+

Nulls
-----

Null values are recognized from the spellings stored in
:member:`ConvertOptions::null_values`.  The :func:`ConvertOptions::Defaults`
factory method will initialize a number of conventional null spellings such
as ``N/A``.

Character encoding
------------------

CSV files are expected to be encoded in UTF8.  However, non-UTF8 data
is accepted for Binary columns.

Write Options
=============

The format of written CSV files can be customized via :class:`~arrow::csv::WriteOptions`.
Currently few options are available; more will be added in future releases.

.. _cpp-csv-performance:

Performance
===========

By default, :class:`~arrow::csv::TableReader` will parallelize reads in order to
exploit all CPU cores on your machine.  You can change this setting in
:member:`ReadOptions::use_threads`.  A reasonable expectation is at least
100 MB/s per core on a performant desktop or laptop computer (measured in
source CSV bytes, not target Arrow data bytes).

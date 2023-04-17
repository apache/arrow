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

=======
Dataset
=======

.. warning::

    Experimental: The Java module ``dataset`` is currently under early
    development. API might be changed in each release of Apache Arrow until it
    gets mature.

Dataset is an universal layer in Apache Arrow for querying data in different
formats or in different partitioning strategies. Usually the data to be queried
is supposed to be located from a traditional file system, however Arrow Dataset
is not designed only for querying files but can be extended to serve all
possible data sources such as from inter-process communication or from other
network locations, etc.

.. contents::

Getting Started
===============

Currently supported file formats are:

- Apache Arrow (``.arrow``)
- Apache ORC (``.orc``)
- Apache Parquet (``.parquet``)
- Comma-Separated Values (``.csv``)

Below shows a simplest example of using Dataset to query a Parquet file in Java:

.. code-block:: Java

    // read data from file /opt/example.parquet
    String uri = "file:/opt/example.parquet";
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
        List<ArrowRecordBatch> batches = new ArrayList<>();
        while (reader.loadNextBatch()) {
            try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                final VectorUnloader unloader = new VectorUnloader(root);
                batches.add(unloader.getRecordBatch());
            }
        }

        // do something with read record batches, for example:
        analyzeArrowData(batches);

        // finished the analysis of the data, close all resources:
        AutoCloseables.close(batches);
    } catch (Exception e) {
        e.printStackTrace();
    }

.. note::
    ``ArrowRecordBatch`` is a low-level composite Arrow data exchange format
    that doesn't provide API to read typed data from it directly.
    It's recommended to use utilities ``VectorLoader`` to load it into a schema
    aware container ``VectorSchemaRoot`` by which user could be able to access
    decoded data conveniently in Java.

    The ``ScanOptions batchSize`` argument takes effect only if it is set to a value
    smaller than the number of rows in the recordbatch.

.. seealso::
   Load record batches with :doc:`VectorSchemaRoot <vector_schema_root>`.

Schema
======

Schema of the data to be queried can be inspected via method
``DatasetFactory#inspect()`` before actually reading it. For example:

.. code-block:: Java

    // read data from local file /opt/example.parquet
    String uri = "file:/opt/example.parquet";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory factory = new FileSystemDatasetFactory(allocator,
        NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);

    // inspect schema
    Schema schema = factory.inspect();

For some of the data format that is compatible with a user-defined schema, user
can use method ``DatasetFactory#inspect(Schema schema)`` to create the dataset:

.. code-block:: Java

    Schema schema = createUserSchema()
    Dataset dataset = factory.finish(schema);

Otherwise when the non-parameter method ``DatasetFactory#inspect()`` is called,
schema will be inferred automatically from data source. The same as the result
of ``DatasetFactory#inspect()``.

Also, if projector is specified during scanning (see next section
:ref:`java-dataset-projection`), the actual schema of output data can be got
within method ``Scanner::schema()``:

.. code-block:: Java

    Scanner scanner = dataset.newScan(
        new ScanOptions(32768, Optional.of(new String[] {"id", "name"})));
    Schema projectedSchema = scanner.schema();

.. _java-dataset-projection:

Projection
==========

User can specify projections in ScanOptions. For ``FileSystemDataset``, only
column projection is allowed for now, which means, only column names
in the projection list will be accepted. For example:

.. code-block:: Java

    String[] projection = new String[] {"id", "name"};
    ScanOptions options = new ScanOptions(32768, Optional.of(projection));

If no projection is needed, leave the optional projection argument absent in
ScanOptions:

.. code-block:: Java

    ScanOptions options = new ScanOptions(32768, Optional.empty());

Or use shortcut construtor:

.. code-block:: Java

    ScanOptions options = new ScanOptions(32768);

Then all columns will be emitted during scanning.

Read Data from HDFS
===================

``FileSystemDataset`` supports reading data from non-local file systems. HDFS
support is included in the official Apache Arrow Java package releases and
can be used directly without re-building the source code.

To access HDFS data using Dataset API, pass a general HDFS URI to
``FilesSystemDatasetFactory``:

.. code-block:: Java

    String uri = "hdfs://{hdfs_host}:{port}/data/example.parquet";
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    DatasetFactory factory = new FileSystemDatasetFactory(allocator,
        NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);

Native Memory Management
========================

To gain better performance and reduce code complexity, Java
``FileSystemDataset`` internally relys on C++
``arrow::dataset::FileSystemDataset`` via JNI.
As a result, all Arrow data read from ``FileSystemDataset`` is supposed to be
allocated off the JVM heap. To manage this part of memory, an utility class
``NativeMemoryPool`` is provided to users.

As a basic example, by using a listenable ``NativeMemoryPool``, user can pass
a listener hooking on C++ buffer allocation/deallocation:

.. code-block:: Java

    AtomicLong reserved = new AtomicLong(0L);
    ReservationListener listener = new ReservationListener() {
      @Override
      public void reserve(long size) {
        reserved.getAndAdd(size);
      }

      @Override
      public void unreserve(long size) {
        reserved.getAndAdd(-size);
      }
    };
    NativeMemoryPool pool = NativeMemoryPool.createListenable(listener);
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(allocator,
        pool, FileFormat.PARQUET, uri);


Also, it's a very common case to reserve the same amount of JVM direct memory
for the data read from datasets. For this use a built-in utility
class ``DirectReservationListener`` is provided:

.. code-block:: Java

    NativeMemoryPool pool = NativeMemoryPool.createListenable(
        DirectReservationListener.instance());

This way, once the allocated byte count of Arrow buffers reaches the limit of
JVM direct memory, ``OutOfMemoryError: Direct buffer memory`` will
be thrown during scanning.

.. note::
    The default instance ``NativeMemoryPool.getDefaultMemoryPool()`` does
    nothing on buffer allocation/deallocation. It's OK to use it in
    the case of POC or testing, but for production use in complex environment,
    it's recommended to manage memory by using a listenable memory pool.

.. note::
    The ``BufferAllocator`` instance passed to ``FileSystemDatasetFactory``'s
    constructor is also aware of the overall memory usage of the produced
    dataset instances. Once the Java buffers are created the passed allocator
    will become their parent allocator.

Usage Notes
===========

Native Object Resource Management
---------------------------------

As another result of relying on JNI, all components related to
``FileSystemDataset`` should be closed manually or use try-with-resources to
release the corresponding native objects after using. For example:

.. code-block:: Java

    String uri = "file:/opt/example.parquet";
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory factory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, uri);
        Dataset dataset = factory.finish();
        Scanner scanner = dataset.newScan(options)
    ) {

        // do something

    } catch (Exception e) {
        e.printStackTrace();
    }

If user forgets to close them then native object leakage might be caused.

BatchSize
---------

The ``batchSize`` argument of ``ScanOptions`` is a limit on the size of an individual batch.

For example, let's try to read a Parquet file with gzip compression and 3 row groups:

.. code-block::

   # Let configure ScanOptions as:
   ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

   $ parquet-tools meta data4_3rg_gzip.parquet
   file schema: schema
   age:         OPTIONAL INT64 R:0 D:1
   name:        OPTIONAL BINARY L:STRING R:0 D:1
   row group 1: RC:4 TS:182 OFFSET:4
   row group 2: RC:4 TS:190 OFFSET:420
   row group 3: RC:3 TS:179 OFFSET:838

Here, we set the batchSize in ScanOptions to 32768. Because that's greater
than the number of rows in the next batch, which is 4 rows because the first
row group has only 4 rows, then the program gets only 4 rows. The scanner
will not combine smaller batches to reach the limit, but it will split
large batches to stay under the limit. So in the case the row group had more
than 32768 rows, it would get split into blocks of 32768 rows or less.

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

.. _cpp-dataset:

================
Tabular Datasets
================

.. seealso::
   :doc:`Dataset API reference <api/dataset>`

.. warning::

    The ``arrow::dataset`` namespace is experimental, and a stable API
    is not yet guaranteed.

The Arrow Datasets library provides functionality to efficiently work with
tabular, potentially larger than memory, and multi-file datasets. This includes:

* A unified interface that supports different sources and file formats and
  different file systems (local, cloud).
* Discovery of sources (crawling directories, handling partitioned datasets with
  various partitioning schemes, basic schema normalization, ...)
* Optimized reading with predicate pushdown (filtering rows), projection
  (selecting and deriving columns), and optionally parallel reading.

The supported file formats currently are Parquet, Feather / Arrow IPC, CSV and
ORC (note that ORC datasets can currently only be read and not yet written).
The goal is to expand support to other file formats and data sources
(e.g. database connections) in the future.

.. _cpp-dataset-reading:

Reading Datasets
----------------

For the examples below, let's create a small dataset consisting
of a directory with two parquet files:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Reading Datasets)
   :end-before: (Doc section: Reading Datasets)
   :linenos:
   :lineno-match:

(See the full example at bottom: :ref:`cpp-dataset-full-example`.)

Dataset discovery
~~~~~~~~~~~~~~~~~

A :class:`arrow::dataset::Dataset` object can be created using the various
:class:`arrow::dataset::DatasetFactory` objects. Here, we'll use the
:class:`arrow::dataset::FileSystemDatasetFactory`, which can create a dataset
given a base directory path:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Dataset discovery)
   :end-before: (Doc section: Dataset discovery)
   :emphasize-lines: 6-11
   :linenos:
   :lineno-match:

We're also passing the filesystem to use and the file format to use for reading.
This lets us choose between (for example) reading local files or files in Amazon
S3, or between Parquet and CSV.

In addition to searching a base directory, we can list file paths manually.

Creating a :class:`arrow::dataset::Dataset` does not begin reading the data
itself. It only crawls the directory to find all the files (if needed), which can
be retrieved with :func:`arrow::dataset::FileSystemDataset::files`:

.. code-block:: cpp

   // Print out the files crawled (only for FileSystemDataset)
   for (const auto& filename : dataset->files()) {
     std::cout << filename << std::endl;
   }

…and infers the dataset's schema (by default from the first file):

.. code-block:: cpp

   std::cout << dataset->schema()->ToString() << std::endl;

Using the :func:`arrow::dataset::Dataset::NewScan` method, we can build a
:class:`arrow::dataset::Scanner` and read the dataset (or a portion of it) into
a :class:`arrow::Table` with the :func:`arrow::dataset::Scanner::ToTable`
method:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Dataset discovery)
   :end-before: (Doc section: Dataset discovery)
   :emphasize-lines: 16-19
   :linenos:
   :lineno-match:

.. TODO: iterative loading not documented pending API changes
.. note:: Depending on the size of your dataset, this can require a lot of
          memory; see :ref:`cpp-dataset-filtering-data` below on
          filtering/projecting.

Reading different file formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above examples use Parquet files on local disk, but the Dataset API
provides a consistent interface across multiple file formats and filesystems.
(See :ref:`cpp-dataset-cloud-storage` for more information on the latter.)
Currently, Parquet, ORC, Feather / Arrow IPC, and CSV file formats are
supported; more formats are planned in the future.

If we save the table as Feather files instead of Parquet files:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Reading different file formats)
   :end-before: (Doc section: Reading different file formats)
   :linenos:
   :lineno-match:

…then we can read the Feather file by passing an :class:`arrow::dataset::IpcFileFormat`:

.. code-block:: cpp

    auto format = std::make_shared<ds::ParquetFileFormat>();
    // ...
    auto factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options)
                       .ValueOrDie();

Customizing file formats
~~~~~~~~~~~~~~~~~~~~~~~~

:class:`arrow::dataset::FileFormat` objects have properties that control how
files are read. For example::

  auto format = std::make_shared<ds::ParquetFileFormat>();
  format->reader_options.dict_columns.insert("a");

Will configure column ``"a"`` to be dictionary-encoded when read. Similarly,
setting :member:`arrow::dataset::CsvFileFormat::parse_options` lets us change
things like reading comma-separated or tab-separated data.

Additionally, passing an :class:`arrow::dataset::FragmentScanOptions` to
:func:`arrow::dataset::ScannerBuilder::FragmentScanOptions` offers fine-grained
control over data scanning. For example, for CSV files, we can change what values
are converted into Boolean true and false at scan time.

.. _cpp-dataset-filtering-data:

Filtering data
--------------

So far, we've been reading the entire dataset, but if we need only a subset of the
data, this can waste time or memory reading data we don't need. The
:class:`arrow::dataset::Scanner` offers control over what data to read.

In this snippet, we use :func:`arrow::dataset::ScannerBuilder::Project` to select
which columns to read:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Filtering data)
   :end-before: (Doc section: Filtering data)
   :emphasize-lines: 16
   :linenos:
   :lineno-match:

Some formats, such as Parquet, can reduce I/O costs here by reading only the
specified columns from the filesystem.

A filter can be provided with :func:`arrow::dataset::ScannerBuilder::Filter`, so
that rows which do not match the filter predicate will not be included in the
returned table. Again, some formats, such as Parquet, can use this filter to
reduce the amount of I/O needed.

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Filtering data)
   :end-before: (Doc section: Filtering data)
   :emphasize-lines: 17
   :linenos:
   :lineno-match:

.. TODO Expressions not documented pending renamespacing

Projecting columns
------------------

In addition to selecting columns, :func:`arrow::dataset::ScannerBuilder::Project`
can also be used for more complex projections, such as renaming columns, casting
them to other types, and even deriving new columns based on evaluating
expressions.

In this case, we pass a vector of expressions used to construct column values
and a vector of names for the columns:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Projecting columns)
   :end-before: (Doc section: Projecting columns)
   :emphasize-lines: 18-28
   :linenos:
   :lineno-match:

This also determines the column selection; only the given columns will be
present in the resulting table. If you want to include a derived column in
*addition* to the existing columns, you can build up the expressions from the
dataset schema:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Projecting columns #2)
   :end-before: (Doc section: Projecting columns #2)
   :emphasize-lines: 17-27
   :linenos:
   :lineno-match:

.. note:: When combining filters and projections, Arrow will determine all
          necessary columns to read. For instance, if you filter on a column that
          isn't ultimately selected, Arrow will still read the column to evaluate
          the filter.

Reading and writing partitioned data
------------------------------------

So far, we've been working with datasets consisting of flat directories with
files. Oftentimes, a dataset will have one or more columns that are frequently
filtered on. Instead of having to read and then filter the data, by organizing the
files into a nested directory structure, we can define a partitioned dataset,
where sub-directory names hold information about which subset of the data is
stored in that directory. Then, we can more efficiently filter data by using that
information to avoid loading files that don't match the filter.

For example, a dataset partitioned by year and month may have the following layout:

.. code-block:: text

   dataset_name/
     year=2007/
       month=01/
          data0.parquet
          data1.parquet
          ...
       month=02/
          data0.parquet
          data1.parquet
          ...
       month=03/
       ...
     year=2008/
       month=01/
       ...
     ...

The above partitioning scheme is using "/key=value/" directory names, as found in
Apache Hive. Under this convention, the file at
``dataset_name/year=2007/month=01/data0.parquet`` contains only data for which
``year == 2007`` and ``month == 01``.

Let's create a small partitioned dataset. For this, we'll use Arrow's dataset
writing functionality.

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Reading and writing partitioned data)
   :end-before: (Doc section: Reading and writing partitioned data)
   :emphasize-lines: 25-42
   :linenos:
   :lineno-match:

The above created a directory with two subdirectories ("part=a" and "part=b"),
and the Parquet files written in those directories no longer include the "part"
column.

Reading this dataset, we now specify that the dataset should use a Hive-like
partitioning scheme:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Reading and writing partitioned data #2)
   :end-before: (Doc section: Reading and writing partitioned data #2)
   :emphasize-lines: 7,9-11
   :linenos:
   :lineno-match:

Although the partition fields are not included in the actual Parquet files,
they will be added back to the resulting table when scanning this dataset:

.. code-block:: text

   $ ./debug/dataset_documentation_example file:///tmp parquet_hive partitioned
   Found fragment: /tmp/parquet_dataset/part=a/part0.parquet
   Partition expression: (part == "a")
   Found fragment: /tmp/parquet_dataset/part=b/part1.parquet
   Partition expression: (part == "b")
   Read 20 rows
   a: int64
     -- field metadata --
     PARQUET:field_id: '1'
   b: double
     -- field metadata --
     PARQUET:field_id: '2'
   c: int64
     -- field metadata --
     PARQUET:field_id: '3'
   part: string
   ----
   # snip...

We can now filter on the partition keys, which avoids loading files
altogether if they do not match the filter:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: (Doc section: Reading and writing partitioned data #3)
   :end-before: (Doc section: Reading and writing partitioned data #3)
   :emphasize-lines: 15-18
   :linenos:
   :lineno-match:

Different partitioning schemes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above example uses a Hive-like directory scheme, such as "/year=2009/month=11/day=15".
We specified this by passing the Hive partitioning factory. In this case, the types of
the partition keys are inferred from the file paths.

It is also possible to directly construct the partitioning and explicitly define
the schema of the partition keys. For example:

.. code-block:: cpp

    auto part = std::make_shared<ds::HivePartitioning>(arrow::schema({
        arrow::field("year", arrow::int16()),
        arrow::field("month", arrow::int8()),
        arrow::field("day", arrow::int32())
    }));

Arrow supports another partitioning scheme, "directory partitioning", where the
segments in the file path represent the values of the partition keys without
including the name (the field names are implicit in the segment's index). For
example, given field names "year", "month", and "day", one path might be
"/2019/11/15".

Since the names are not included in the file paths, these must be specified
when constructing a directory partitioning:

.. code-block:: cpp

    auto part = ds::DirectoryPartitioning::MakeFactory({"year", "month", "day"});

Directory partitioning also supports providing a full schema rather than inferring
types from file paths.

Partitioning performance considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Partitioning datasets has two aspects that affect performance: it increases the number of
files and it creates a directory structure around the files. Both of these have benefits
as well as costs. Depending on the configuration and the size of your dataset, the costs 
can outweigh the benefits. 

Because partitions split up the dataset into multiple files, partitioned datasets can be 
read and written with parallelism. However, each additional file adds a little overhead in 
processing for filesystem interaction. It also increases the overall dataset size since 
each file has some shared metadata. For example, each parquet file contains the schema and
group-level statistics. The number of partitions is a floor for the number of files. If 
you partition a dataset by date with a year of data, you will have at least 365 files. If 
you further partition by another dimension with 1,000 unique values, you will have up to 
365,000 files. This fine of partitioning often leads to small files that mostly consist of
metadata.

Partitioned datasets create nested folder structures, and those allow us to prune which 
files are loaded in a scan. However, this adds overhead to discovering files in the dataset,
as we'll need to recursively "list directory" to find the data files. Too fine
partitions can cause problems here: Partitioning a dataset by date for a years worth
of data will require 365 list calls to find all the files; adding another column with 
cardinality 1,000 will make that 365,365 calls.

The most optimal partitioning layout will depend on your data, access patterns, and which
systems will be reading the data. Most systems, including Arrow, should work across a 
range of file sizes and partitioning layouts, but there are extremes you should avoid. These
guidelines can help avoid some known worst cases:

* Avoid files smaller than 20MB and larger than 2GB.
* Avoid partitioning layouts with more than 10,000 distinct partitions.

For file formats that have a notion of groups within a file, such as Parquet, similar
guidelines apply. Row groups can provide parallelism when reading and allow data skipping
based on statistics, but very small groups can cause metadata to be a significant portion
of file size. Arrow's file writer provides sensible defaults for group sizing in most cases.

Reading from other data sources
-------------------------------

Reading in-memory data
~~~~~~~~~~~~~~~~~~~~~~

If you already have data in memory that you'd like to use with the Datasets API
(e.g. to filter/project data, or to write it out to a filesystem), you can wrap it
in an :class:`arrow::dataset::InMemoryDataset`:

.. code-block:: cpp

   auto table = arrow::Table::FromRecordBatches(...);
   auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(std::move(table));
   // Scan the dataset, filter, it, etc.
   auto scanner_builder = dataset->NewScan();

In the example, we used the InMemoryDataset to write our example data to local
disk which was used in the rest of the example:

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :start-after: Reading and writing partitioned data
   :end-before: Reading and writing partitioned data
   :emphasize-lines: 24-28
   :linenos:
   :lineno-match:

.. _cpp-dataset-cloud-storage:

Reading from cloud storage
~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to local files, Arrow Datasets also support reading from cloud
storage systems, such as Amazon S3, by passing a different filesystem.

See the :ref:`filesystem <cpp-filesystems>` docs for more details on the available
filesystems.

.. _cpp-dataset-full-example:

A note on transactions & ACID guarantees
----------------------------------------

The dataset API offers no transaction support or any ACID guarantees.  This affects
both reading and writing.  Concurrent reads are fine.  Concurrent writes or writes
concurring with reads may have unexpected behavior.  Various approaches can be used
to avoid operating on the same files such as using a unique basename template for
each writer, a temporary directory for new files, or separate storage of the file
list instead of relying on directory discovery.

Unexpectedly killing the process while a write is in progress can leave the system
in an inconsistent state.  Write calls generally return as soon as the bytes to be
written have been completely delivered to the OS page cache.  Even though a write
operation has been completed it is possible for part of the file to be lost if
there is a sudden power loss immediately after the write call.

Most file formats have magic numbers which are written at the end.  This means a
partial file write can safely be detected and discarded.  The CSV file format does
not have any such concept and a partially written CSV file may be detected as valid.

Full Example
------------

.. literalinclude:: ../../../cpp/examples/arrow/dataset_documentation_example.cc
   :language: cpp
   :linenos:

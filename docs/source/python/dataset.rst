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

.. ipython:: python
    :suppress:

    # set custom tmp working directory for files that create data
    import os
    import tempfile

    orig_working_dir = os.getcwd()
    temp_working_dir = tempfile.mkdtemp(prefix="pyarrow-")
    os.chdir(temp_working_dir)

.. currentmodule:: pyarrow.dataset

.. _dataset:

Tabular Datasets
================

The ``pyarrow.dataset`` module provides functionality to efficiently work with
tabular, potentially larger than memory, and multi-file datasets. This includes:

* A unified interface that supports different sources and file formats and
  different file systems (local, cloud).
* Discovery of sources (crawling directories, handle directory-based partitioned
  datasets, basic schema normalization, ..)
* Optimized reading with predicate pushdown (filtering rows), projection
  (selecting and deriving columns), and optionally parallel reading.

The supported file formats currently are Parquet, Feather / Arrow IPC, CSV and
ORC (note that ORC datasets can currently only be read and not yet written).
The goal is to expand support to other file formats and data sources
(e.g. database connections) in the future.

For those familiar with the existing :class:`pyarrow.parquet.ParquetDataset` for
reading Parquet datasets: ``pyarrow.dataset``'s goal is similar but not specific
to the Parquet format and not tied to Python: the same datasets API is exposed
in the R bindings or Arrow. In addition ``pyarrow.dataset`` boasts improved
performance and new features (e.g. filtering within files rather than only on
partition keys).


Reading Datasets
----------------

.. TODO Full blown example with NYC taxi data to show off, afterwards explain all parts:

For the examples below, let's create a small dataset consisting
of a directory with two parquet files:

.. ipython:: python

    import tempfile
    import pathlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    import numpy as np

    base = pathlib.Path(tempfile.mkdtemp(prefix="pyarrow-"))
    (base / "parquet_dataset").mkdir(exist_ok=True)

    # creating an Arrow Table
    table = pa.table({'a': range(10), 'b': np.random.randn(10), 'c': [1, 2] * 5})

    # writing it into two parquet files
    pq.write_table(table.slice(0, 5), base / "parquet_dataset/data1.parquet")
    pq.write_table(table.slice(5, 10), base / "parquet_dataset/data2.parquet")

Dataset discovery
~~~~~~~~~~~~~~~~~

A :class:`Dataset` object can be created with the :func:`dataset` function. We
can pass it the path to the directory containing the data files:

.. ipython:: python

    import pyarrow.dataset as ds
    dataset = ds.dataset(base / "parquet_dataset", format="parquet")
    dataset

In addition to searching a base directory, :func:`dataset` accepts a path to a
single file or a list of file paths.

Creating a :class:`Dataset` object does not begin reading the data itself. If
needed, it only crawls the directory to find all the files:

.. ipython:: python

    dataset.files

... and infers the dataset's schema (by default from the first file):

.. ipython:: python

    print(dataset.schema.to_string(show_field_metadata=False))

Using the :meth:`Dataset.to_table` method we can read the dataset (or a portion
of it) into a pyarrow Table (note that depending on the size of your dataset
this can require a lot of memory, see below on filtering / iterative loading):

.. ipython:: python

    dataset.to_table()
    # converting to pandas to see the contents of the scanned table
    dataset.to_table().to_pandas()

Reading different file formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above examples use Parquet files as dataset sources but the Dataset API
provides a consistent interface across multiple file formats and filesystems.
Currently, Parquet, ORC, Feather / Arrow IPC, and CSV file formats are
supported; more formats are planned in the future.

If we save the table as Feather files instead of Parquet files:

.. ipython:: python

    import pyarrow.feather as feather

    feather.write_feather(table, base / "data.feather")

…then we can read the Feather file using the same functions, but with specifying
``format="feather"``:

.. ipython:: python

    dataset = ds.dataset(base / "data.feather", format="feather")
    dataset.to_table().to_pandas().head()

Customizing file formats
~~~~~~~~~~~~~~~~~~~~~~~~

The format name as a string, like::

    ds.dataset(..., format="parquet")

is short hand for a default constructed :class:`ParquetFileFormat`::

    ds.dataset(..., format=ds.ParquetFileFormat())

The :class:`FileFormat` objects can be customized using keywords. For example::

    parquet_format = ds.ParquetFileFormat(read_options={'dictionary_columns': ['a']})
    ds.dataset(..., format=parquet_format)

Will configure column ``"a"`` to be dictionary encoded on scan.

.. _py-filter-dataset:

Filtering data
--------------

To avoid reading all data when only needing a subset, the ``columns`` and
``filter`` keywords can be used.

The ``columns`` keyword can be used to only read the specified columns:

.. ipython:: python

    dataset = ds.dataset(base / "parquet_dataset", format="parquet")
    dataset.to_table(columns=['a', 'b']).to_pandas()

With the ``filter`` keyword, rows which do not match the filter predicate will
not be included in the returned table. The keyword expects a boolean
:class:`Expression` referencing at least one of the columns:

.. ipython:: python

    dataset.to_table(filter=ds.field('a') >= 7).to_pandas()
    dataset.to_table(filter=ds.field('c') == 2).to_pandas()

The easiest way to construct those :class:`Expression` objects is by using the
:func:`field` helper function. Any column - not just partition columns - can be
referenced using the :func:`field` function (which creates a
:class:`FieldExpression`). Operator overloads are provided to compose filters
including the comparisons (equal, larger/less than, etc), set membership
testing, and boolean combinations (``&``, ``|``, ``~``):

.. ipython:: python

    ds.field('a') != 3
    ds.field('a').isin([1, 2, 3])
    (ds.field('a') > ds.field('b')) & (ds.field('b') > 1)

Note that :class:`Expression` objects can **not** be combined by python logical
operators ``and``, ``or`` and ``not``.

Projecting columns
------------------

The ``columns`` keyword can be used to read a subset of the columns of the
dataset by passing it a list of column names. The keyword can also be used
for more complex projections in combination with expressions.

In this case, we pass it a dictionary with the keys being the resulting
column names and the values the expression that is used to construct the column
values:

.. ipython:: python

    projection = {
        "a_renamed": ds.field("a"),
        "b_as_float32": ds.field("b").cast("float32"),
        "c_1": ds.field("c") == 1,
    }
    dataset.to_table(columns=projection).to_pandas().head()

The dictionary also determines the column selection (only the keys in the
dictionary will be present as columns in the resulting table). If you want
to include a derived column in *addition* to the existing columns, you can
build up the dictionary from the dataset schema:

.. ipython:: python

    projection = {col: ds.field(col) for col in dataset.schema.names}
    projection.update({"b_large": ds.field("b") > 1})
    dataset.to_table(columns=projection).to_pandas().head()


Reading partitioned data
------------------------

Above, a dataset consisting of a flat directory with files was shown. However, a
dataset can exploit a nested directory structure defining a partitioned dataset,
where the sub-directory names hold information about which subset of the data is
stored in that directory.

For example, a dataset partitioned by year and month may look like on disk:

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

The above partitioning scheme is using "/key=value/" directory names, as found
in Apache Hive.

Let's create a small partitioned dataset. The :func:`~pyarrow.parquet.write_to_dataset`
function can write such hive-like partitioned datasets.

.. ipython:: python

    table = pa.table({'a': range(10), 'b': np.random.randn(10), 'c': [1, 2] * 5,
                      'part': ['a'] * 5 + ['b'] * 5})
    pq.write_to_dataset(table, "parquet_dataset_partitioned",
                        partition_cols=['part'])

The above created a directory with two subdirectories ("part=a" and "part=b"),
and the Parquet files written in those directories no longer include the "part"
column.

Reading this dataset with :func:`dataset`, we now specify that the dataset
should use a hive-like partitioning scheme with the ``partitioning`` keyword:

.. ipython:: python

    dataset = ds.dataset("parquet_dataset_partitioned", format="parquet",
                         partitioning="hive")
    dataset.files

Although the partition fields are not included in the actual Parquet files,
they will be added back to the resulting table when scanning this dataset:

.. ipython:: python

    dataset.to_table().to_pandas().head(3)

We can now filter on the partition keys, which avoids loading files
altogether if they do not match the filter:

.. ipython:: python

    dataset.to_table(filter=ds.field("part") == "b").to_pandas()


Different partitioning schemes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above example uses a hive-like directory scheme, such as "/year=2009/month=11/day=15".
We specified this passing the ``partitioning="hive"`` keyword. In this case,
the types of the partition keys are inferred from the file paths.

It is also possible to explicitly define the schema of the partition keys
using the :func:`partitioning` function. For example:

.. code-block:: python

    part = ds.partitioning(
        pa.schema([("year", pa.int16()), ("month", pa.int8()), ("day", pa.int32())]),
        flavor="hive"
    )
    dataset = ds.dataset(..., partitioning=part)

"Directory partitioning" is also supported, where the segments in the file path
represent the values of the partition keys without including the name (the
field name are implicit in the segment's index). For example, given field names
"year", "month", and "day", one path might be "/2019/11/15".

Since the names are not included in the file paths, these must be specified
when constructing a directory partitioning:

.. code-block:: python

    part = ds.partitioning(field_names=["year", "month", "day"])

Directory partitioning also supports providing a full schema rather than inferring
types from file paths.


Reading from cloud storage
--------------------------

In addition to local files, pyarrow also supports reading from cloud storage.
Currently, :class:`HDFS <pyarrow.fs.HadoopFileSystem>` and
:class:`Amazon S3-compatible storage <pyarrow.fs.S3FileSystem>` are supported.

When passing a file URI, the file system will be inferred. For example,
specifying a S3 path:

.. code-block:: python

    dataset = ds.dataset("s3://voltrondata-labs-datasets/nyc-taxi/")

Typically, you will want to customize the connection parameters, and then
a file system object can be created and passed to the ``filesystem`` keyword:

.. code-block:: python

    from pyarrow import fs

    s3  = fs.S3FileSystem(region="us-east-2")
    dataset = ds.dataset("voltrondata-labs-datasets/nyc-taxi/", filesystem=s3)

The currently available classes are :class:`~pyarrow.fs.S3FileSystem` and
:class:`~pyarrow.fs.HadoopFileSystem`. See the :ref:`filesystem` docs for more
details.


Reading from Minio
------------------

In addition to cloud storage, pyarrow also supports reading from a
`MinIO <https://github.com/minio/minio>`_ object storage instance emulating S3
APIs. Paired with `toxiproxy <https://github.com/shopify/toxiproxy>`_, this is
useful for testing or benchmarking.

.. code-block:: python

    from pyarrow import fs

    # By default, MinIO will listen for unencrypted HTTP traffic.
    minio = fs.S3FileSystem(scheme="http", endpoint_override="localhost:9000")
    dataset = ds.dataset("voltrondata-labs-datasets/nyc-taxi/", filesystem=minio)


Working with Parquet Datasets
-----------------------------

While the Datasets API provides a unified interface to different file formats,
some specific methods exist for Parquet Datasets.

Some processing frameworks such as Dask (optionally) use a ``_metadata`` file
with partitioned datasets which includes information about the schema and the
row group metadata of the full dataset. Using such a file can give a more
efficient creation of a parquet Dataset, since it does not need to infer the
schema and crawl the directories for all Parquet files (this is especially the
case for filesystems where accessing files is expensive). The
:func:`parquet_dataset` function allows us to create a Dataset from a partitioned
dataset with a ``_metadata`` file:

.. code-block:: python

    dataset = ds.parquet_dataset("/path/to/dir/_metadata")

By default, the constructed :class:`Dataset` object for Parquet datasets maps
each fragment to a single Parquet file. If you want fragments mapping to each
row group of a Parquet file, you can use the ``split_by_row_group()`` method of
the fragments:

.. code-block:: python

    fragments = list(dataset.get_fragments())
    fragments[0].split_by_row_group()

This method returns a list of new Fragments mapping to each row group of
the original Fragment (Parquet file). Both ``get_fragments()`` and
``split_by_row_group()`` accept an optional filter expression to get a
filtered list of fragments.


Manual specification of the Dataset
-----------------------------------

The :func:`dataset` function allows easy creation of a Dataset viewing a directory,
crawling all subdirectories for files and partitioning information. However
sometimes discovery is not required and the dataset's files and partitions
are already known (for example, when this information is stored in metadata).
In this case it is possible to create a Dataset explicitly without any
automatic discovery or inference.

For the example here, we are going to use a dataset where the file names contain
additional partitioning information:

.. ipython:: python

    # creating a dummy dataset: directory with two files
    table = pa.table({'col1': range(3), 'col2': np.random.randn(3)})
    (base / "parquet_dataset_manual").mkdir(exist_ok=True)
    pq.write_table(table, base / "parquet_dataset_manual" / "data_2018.parquet")
    pq.write_table(table, base / "parquet_dataset_manual" / "data_2019.parquet")

To create a Dataset from a list of files, we need to specify the paths, schema,
format, filesystem, and partition expressions manually:

.. ipython:: python

    from pyarrow import fs

    schema = pa.schema([("year", pa.int64()), ("col1", pa.int64()), ("col2", pa.float64())])

    dataset = ds.FileSystemDataset.from_paths(
        ["data_2018.parquet", "data_2019.parquet"], schema=schema, format=ds.ParquetFileFormat(),
        filesystem=fs.SubTreeFileSystem(str(base / "parquet_dataset_manual"), fs.LocalFileSystem()),
        partitions=[ds.field('year') == 2018, ds.field('year') == 2019])

Since we specified the "partition expressions" for our files, this information
is materialized as columns when reading the data and can be used for filtering:

.. ipython:: python

    dataset.to_table().to_pandas()
    dataset.to_table(filter=ds.field('year') == 2019).to_pandas()

Another benefit of manually listing the files is that the order of the files
controls the order of the data.  When performing an ordered read (or a read to
a table) then the rows returned will match the order of the files given.  This
only applies when the dataset is constructed with a list of files.  There
are no order guarantees given when the files are instead discovered by scanning
a directory.

Iterative (out of core or streaming) reads
------------------------------------------

The previous examples have demonstrated how to read the data into a table using :func:`~Dataset.to_table`.  This is
useful if the dataset is small or there is only a small amount of data that needs to
be read.  The dataset API contains additional methods to read and process large amounts
of data in a streaming fashion.

The easiest way to do this is to use the method :meth:`Dataset.to_batches`.  This
method returns an iterator of record batches.  For example, we can use this method to
calculate the average of a column without loading the entire column into memory:

.. ipython:: python

    import pyarrow.compute as pc

    col2_sum = 0
    count = 0
    for batch in dataset.to_batches(columns=["col2"], filter=~ds.field("col2").is_null()):
        col2_sum += pc.sum(batch.column("col2")).as_py()
        count += batch.num_rows
    mean_a = col2_sum/count

Customizing the batch size
~~~~~~~~~~~~~~~~~~~~~~~~~~

An iterative read of a dataset is often called a "scan" of the dataset and pyarrow
uses an object called a :class:`Scanner` to do this.  A Scanner is created for you
automatically by the :func:`~Dataset.to_table` and :func:`~Dataset.to_batches` method of the dataset.
Any arguments you pass to these methods will be passed on to the Scanner constructor.

One of those parameters is the ``batch_size``.  This controls the maximum size of the
batches returned by the scanner.  Batches can still be smaller than the ``batch_size``
if the dataset consists of small files or those files themselves consist of small
row groups.  For example, a parquet file with 10,000 rows per row group will yield
batches with, at most, 10,000 rows unless the ``batch_size`` is set to a smaller value.

The default batch size is one million rows and this is typically a good default but
you may want to customize it if you are reading a large number of columns.

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

Writing Datasets
----------------

The dataset API also simplifies writing data to a dataset using :func:`write_dataset` .  This can be useful when
you want to partition your data or you need to write a large amount of data.  A
basic dataset write is similar to writing a table except that you specify a directory
instead of a filename.

.. ipython:: python

    table = pa.table({"a": range(10), "b": np.random.randn(10), "c": [1, 2] * 5})
    ds.write_dataset(table, "sample_dataset", format="parquet")

The above example will create a single file named part-0.parquet in our sample_dataset
directory.

.. warning::

    If you run the example again it will replace the existing part-0.parquet file.
    Appending files to an existing dataset requires specifying a new
    ``basename_template`` for each call to ``ds.write_dataset``
    to avoid overwrite.

Writing partitioned data
~~~~~~~~~~~~~~~~~~~~~~~~

A partitioning object can be used to specify how your output data should be partitioned.
This uses the same kind of partitioning objects we used for reading datasets.  To write
our above data out to a partitioned directory we only need to specify how we want the
dataset to be partitioned.  For example:

.. ipython:: python

    part = ds.partitioning(
        pa.schema([("c", pa.int16())]), flavor="hive"
    )
    ds.write_dataset(table, "partitioned_dataset", format="parquet", partitioning=part)

This will create two files.  Half our data will be in the dataset_root/c=1 directory and
the other half will be in the dataset_root/c=2 directory.

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

Configuring files open during a write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When writing data to the disk, there are a few parameters that can be 
important to optimize the writes, such as the number of rows per file and
the maximum number of open files allowed during the write.

Set the maximum number of files opened with the ``max_open_files`` parameter of
:meth:`write_dataset`.

If  ``max_open_files`` is set greater than 0 then this will limit the maximum 
number of files that can be left open. This only applies to writing partitioned
datasets, where rows are dispatched to the appropriate file depending on their
partition values. If an attempt is made to open too many  files then the least
recently used file will be closed.  If this setting is set too low you may end
up fragmenting your data into many small files.

If your process is concurrently using other file handlers, either with a 
dataset scanner or otherwise, you may hit a system file handler limit. For 
example, if you are scanning a dataset with 300 files and writing out to
900 files, the total of 1200 files may be over a system limit. (On Linux,
this might be a "Too Many Open Files" error.) You can either reduce this
``max_open_files`` setting or increase the file handler limit on your
system. The default value is 900 which allows some number of files
to be open by the scanner before hitting the default Linux limit of 1024. 

Another important configuration used in :meth:`write_dataset` is ``max_rows_per_file``. 

Set the maximum number of rows written in each file with the ``max_rows_per_files``
parameter of :meth:`write_dataset`.

If ``max_rows_per_file`` is set greater than 0 then this will limit how many 
rows are placed in any single file. Otherwise there will be no limit and one
file will be created in each output directory unless files need to be closed to respect
``max_open_files``. This setting is the primary way to control file size.
For workloads writing a lot of data, files can get very large without a
row count cap, leading to out-of-memory errors in downstream readers. The
relationship between row count and file size depends on the dataset schema
and how well compressed (if at all) the data is.

Configuring rows per group during a write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The volume of data written to the disk per each group can be configured.
This configuration includes a lower and an upper bound.
The minimum number of rows required to form a row group is 
defined with the ``min_rows_per_group`` parameter of :meth:`write_dataset`.

.. note::
    If ``min_rows_per_group`` is set greater than 0 then this will cause the 
    dataset writer to batch incoming data and only write the row groups to the 
    disk when sufficient rows have accumulated. The final row group size may be 
    less than this value if other options such as ``max_open_files`` or 
    ``max_rows_per_file`` force smaller row group sizes.

The maximum number of rows allowed per group is defined with the
``max_rows_per_group`` parameter of :meth:`write_dataset`.

If ``max_rows_per_group`` is set greater than 0 then the dataset writer may split 
up large incoming batches into multiple row groups.  If this value is set then 
``min_rows_per_group`` should also be set or else you may end up with very small 
row groups (e.g. if the incoming row group size is just barely larger than this value).

Row groups are built into the Parquet and IPC/Feather formats but don't affect JSON or CSV.
When reading back Parquet and IPC formats in Arrow, the row group boundaries become the
record batch boundaries, determining the default batch size of downstream readers.
Additionally, row groups in Parquet files have column statistics which can help readers
skip irrelevant data but can add size to the file. As an extreme example, if one sets
max_rows_per_group=1 in Parquet, they will have large files because most of the files
will be row group statistics.

Writing large amounts of data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The above examples wrote data from a table.  If you are writing a large amount of data
you may not be able to load everything into a single in-memory table.  Fortunately, the
:func:`~Dataset.write_dataset` method also accepts an iterable of record batches.  This makes it really
simple, for example, to repartition a large dataset without loading the entire dataset
into memory:

.. ipython:: python

    old_part = ds.partitioning(
        pa.schema([("c", pa.int16())]), flavor="hive"
    )
    new_part = ds.partitioning(
        pa.schema([("c", pa.int16())]), flavor=None
    )
    input_dataset = ds.dataset("partitioned_dataset", partitioning=old_part)
    # A scanner can act as an iterator of record batches but you could also receive
    # data from the network (e.g. via flight), from your own scanning, or from any
    # other method that yields record batches.  In addition, you can pass a dataset
    # into write_dataset directly but this method is useful if you want to customize
    # the scanner (e.g. to filter the input dataset or set a maximum batch size)
    scanner = input_dataset.scanner()

    ds.write_dataset(scanner, "repartitioned_dataset", format="parquet", partitioning=new_part)

After the above example runs our data will be in dataset_root/1 and dataset_root/2
directories.  In this simple example we are not changing the structure of the data
(only the directory naming schema) but you could also use this mechnaism to change
which columns are used to partition the dataset.  This is useful when you expect to
query your data in specific ways and you can utilize partitioning to reduce the
amount of data you need to read.

Customizing & inspecting written files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default the dataset API will create files named "part-i.format" where "i" is a integer
generated during the write and "format" is the file format specified in the write_dataset
call.  For simple datasets it may be possible to know which files will be created but for
larger or partitioned datasets it is not so easy.  The ``file_visitor`` keyword can be used 
to supply a visitor that will be called as each file is created:

.. ipython:: python

    def file_visitor(written_file):
        print(f"path={written_file.path}")
        print(f"size={written_file.size} bytes")
        print(f"metadata={written_file.metadata}")

.. ipython:: python

    ds.write_dataset(table, "dataset_visited", format="parquet", partitioning=part,
                     file_visitor=file_visitor)

This will allow you to collect the filenames that belong to the dataset and store them elsewhere
which can be useful when you want to avoid scanning directories the next time you need to read
the data.  It can also be used to generate the _metadata index file used by other tools such as
Dask or Spark to create an index of the dataset.

Configuring format-specific parameters during a write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to the common options shared by all formats there are also format specific options
that are unique to a particular format.  For example, to allow truncated timestamps while writing
Parquet files:

.. ipython:: python

    parquet_format = ds.ParquetFileFormat()
    write_options = parquet_format.make_write_options(allow_truncated_timestamps=True)
    ds.write_dataset(table, "sample_dataset2", format="parquet", partitioning=part,
                     file_options=write_options)


.. ipython:: python
    :suppress:

    # clean-up custom working directory
    import os
    import shutil

    os.chdir(orig_working_dir)
    shutil.rmtree(temp_working_dir, ignore_errors=True)

    # also clean-up custom base directory used in some examples
    shutil.rmtree(str(base), ignore_errors=True)

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

.. currentmodule:: pyarrow.dataset

.. _dataset:

Tabular Datasets
================

.. warning::

    The ``pyarrow.dataset`` module is experimental (specifically the classes),
    and a stable API is not yet guaranteed.

The ``pyarrow.dataset`` module provides functionality to efficiently work with
tabular, potentially larger than memory and multi-file datasets:

* A unified interface for different sources: supporting different sources and
  file formats (Parquet, Feather files) and different file systems (local,
  cloud).
* Discovery of sources (crawling directories, handle directory-based partitioned
  datasets, basic schema normalization, ..)
* Optimized reading with predicate pushdown (filtering rows), projection
  (selecting columns), parallel reading or fine-grained managing of tasks.

Currently, only Parquet and Feather / Arrow IPC files are supported. The goal
is to expand this in the future to other file formats and data sources (e.g.
database connections).

For those familiar with the existing :class:`pyarrow.parquet.ParquetDataset` for
reading Parquet datasets: ``pyarrow.dataset``'s goal is similar but not specific
to the Parquet format and not tied to Python: the same datasets API is exposed
in the R bindings or Arrow. In addition ``pyarrow.dataset`` boasts improved
perfomance and new features (e.g. filtering within files rather than only on
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

    base = pathlib.Path(tempfile.gettempdir())
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

In addition to a base directory path, :func:`dataset` accepts a path to a single
file or a list of file paths.

Creating a :class:`Dataset` object loads nothing into memory, it only crawls the
directory to find all the files:

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

The above examples use Parquet files as dataset source but the Dataset API
provides a consistent interface across multiple file formats and sources.
Currently, Parquet and Feather / Arrow IPC file format are supported; more
formats are planned in the future.

If we save the table as a Feather file instead of Parquet files:

.. ipython:: python

    import pyarrow.feather as feather

    feather.write_feather(table, base / "data.feather")

then we can read the Feather file using the same functions, but with specifying
``format="feather"``:

.. ipython:: python

    dataset = ds.dataset(base / "data.feather", format="feather")
    dataset.to_table().to_pandas().head()

Customizing file formats
~~~~~~~~~~~~~~~~~~~~~~~~

The format name as a string, like::

    ds.dataset(..., format="parquet")

is short hand for a default constructed :class:`ParquetFileFormat`::

    ds.dataset(..., format=ds.ParquetFileForma())

The :class:`FileFormat` objects can be customized using keywords. For example::

    parquet_format = ds.ParquetFileFormat(read_options={'dictionary_columns': ['a']})
    ds.dataset(..., format=parquet_format)

Will configure column ``"a"`` to be dictionary encoded on scan.

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
testing, and boolean combinations (and, or, not):

.. ipython:: python

    ds.field('a') != 3
    ds.field('a').isin([1, 2, 3])
    (ds.field('a') > ds.field('b')) & (ds.field('b') > 1)


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
    pq.write_to_dataset(table, str(base / "parquet_dataset_partitioned"),
                        partition_cols=['part'])

The above created a directory with two subdirectories ("part=a" and "part=b"),
and the Parquet files written in those directories no longer include the "part"
column.

Reading this dataset with :func:`dataset`, we now specify that the dataset
uses a hive-like partitioning scheme with the `partitioning` keyword:

.. ipython:: python

    dataset = ds.dataset(str(base / "parquet_dataset_partitioned"), format="parquet",
                         partitioning="hive")
    dataset.files

Although the partition fields are not included in the actual Parquet files,
they will be added back to the resulting table when scanning this dataset:

.. ipython:: python

    dataset.to_table().to_pandas().head(3)

We can now filter on the partition keys, which avoids loading files
altogether if they do not match the predicate:

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

    dataset = ds.dataset("s3://ursa-labs-taxi-data/", partitioning=["year", "month"])

Typically, you will want to customize the connection parameters, and then
a file system object can be created and passed to the ``filesystem`` keyword:

.. code-block:: python

    from pyarrow import fs

    s3  = fs.S3FileSystem(region="us-east-2")
    dataset = ds.dataset("ursa-labs-taxi-data/", filesystem=s3,
                         partitioning=["year", "month"])

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
    minio = fs.S3FileSystem(scheme="http", endpoint="localhost:9000")
    dataset = ds.dataset("ursa-labs-taxi-data/", filesystem=minio,
                         partitioning=["year", "month"])


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


Manual scheduling
-----------------

..
    Possible content:
    - fragments (get_fragments)
    - scan / scan tasks / iterators of record batches

The :func:`~Dataset.to_table` method loads all selected data into memory
at once resulting in a pyarrow Table. Alternatively, a dataset can also be
scanned one RecordBatch at a time in an iterative manner using the
:func:`~Dataset.scan` method::

    for scan_task in dataset.scan(columns=[...], filter=...):
        for record_batch in scan_task.execute():
            # process the record batch

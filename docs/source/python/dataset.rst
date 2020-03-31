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
tabular, potentially larger than memory and multi-file, datasets:

* A unified interface for different sources: supporting different sources
  (files, database connection, ..), different file systems (local, cloud) and
  different file formats (Parquet, CSV, JSON, Feather, ..)
* Discovery of sources (crawling directories, handle directory-based partitioned
  datasets, basic schema normalization, ..)
* Optimized reading with pedicate pushdown (filtering rows), projection
  (selecting columns), parallel reading or fine-grained managing of tasks.


For those familiar with the existing :class:`pyarrow.parquet.ParquetDataset` for
reading Parquet datasets: the goal of `pyarrow.dataset` is to provide similar
functionality, but not specific to the Parquet format, not tied to Python (for
example the R bindings of Arrow also have an interface for the Dateset API) and
with improved functionality (e.g. filtering on the file level and not only
partition keys).



Reading Datasets
----------------


Full blown example with NYC taxi data to show off, afterwards explain all parts:

...



Dataset discovery
~~~~~~~~~~~~~~~~~

- creating dataset
- get schema / files


Filtering data
~~~~~~~~~~~~~~

- columns / filter expressions



Reading different file formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- show feather



Reading partitioned data
------------------------

- hive vs directory partitioning supported right now
- how to specify the schema with ds.partitioning()


Reading from cloud storage
--------------------------

- example with s3 filesystem / hdfs filesystem



Manual specification of the Dataset
-----------------------------------

The :func:`dataset` function allows to easily create a Dataset from a directory,
crawling all subdirectories for files and partitioning information. However,
when the dataset files and partitions are already known (for example, when this
information is stored in metadata), it is also possible to create a Dataset
explicitly using the lower level API without any automatic discovery or
inference.

For the example here, we are going to use a dataset where the file names contain
additional partitioning information:

.. ipython:: python

    # creating a dummy dataset: directory with two files
    table = pa.table({'col1': range(3), 'col2': np.random.randn(3)})
    pq.write_table(table, "parquet_dataset_manual/data_file1.parquet")
    pq.write_table(table, "parquet_dataset_manual/data_file2.parquet")

To create a Dataset from a list of files, we need to specify the schema, format,
filesystem, and paths manually:

.. ipython:: python

    import pyarrow.fs

    schema = pa.schema([("file", pa.int64()), ("col1", pa.int64()), ("col2", pa.float64())])

    dataset = ds.FileSystemDataset(
        schema, None, ds.ParquetFileFormat(), pa.fs.LocalFileSystem(),
        ["parquet_dataset_manual/data_file1.parquet", "parquet_dataset_manual/data_file2.parquet"],
        [ds.field('file') == 1, ds.field('file') == 2])

We also specified the "partition expressions" for our files, so this information
is included when reading the data and can be used for filtering:

.. ipython:: python

    dataset.to_table().to_pandas()
    dataset.to_table(filter=ds.field('file') == 1).to_pandas()


Manual scheduling
-----------------

- fragments (get_fragments)
- scan / scan tasks / iterators of record batches

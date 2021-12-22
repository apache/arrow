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

.. _getstarted:

Getting Started
===============

Arrow manages data in arrays (:class:`pyarrow.Array`), which can be
grouped in tables (:class:`pyarrow.Table`) to represent columns of data
in tabular data.

Arrow also provides support for various formats to get those tabular
data in and out of disk and networks. Most commonly used formats are
Parquet (:ref:`parquet`) and the IPC format (:ref:`ipc`). 

Creating Arrays and Tables
--------------------------

Arrays in Arrow are collections of data of uniform type. That allows
Arrow to use the best performing implementation to store the data and
perform computations on it. So each array is meant to have data and
a type

.. ipython:: python

    import pyarrow as pa

    days = pa.array([1, 12, 17, 23, 28], type=pa.int8())

Multiple arrays can be combined in tables to form the columns
in tabular data when attached to a column name

.. ipython:: python

    months = pa.array([1, 3, 5, 7, 1], type=pa.int8())
    years = pa.array([1990, 2000, 1995, 2000, 1995], type=pa.int16())

    birthdays_table = pa.table([days, months, years],
                               names=["days", "months", "years"])
    
    birthdays_table

See :ref:`data` for more details.

Saving and Loading Tables
-------------------------

Once you have tabular data, Arrow provides out of the box
the features to save and restore that data for common formats
like Parquet:

.. ipython:: python   

    import pyarrow.parquet as pq

    pq.write_table(birthdays_table, 'birthdays.parquet')

Once you have your data on disk, loading it back is a single function call,
and Arrow is heavily optimized for memory and speed so loading
data will be as quick as possible

.. ipython:: python

    reloaded_birthdays = pq.read_table('birthdays.parquet')

    reloaded_birthdays

Saving and loading back data in arrow is usually done through
:ref:`Parquet <parquet>`, :ref:`IPC format <ipc>` (:ref:`feather`), 
:ref:`CSV <py-csv>` or :ref:`Line-Delimited JSON <json>` formats.

Performing Computations
-----------------------

Arrow ships with a bunch of compute functions that can be applied
to its arrays and tables, so through the compute functions 
it's possible to apply transformations to the data

.. ipython:: python

    import pyarrow.compute as pc

    pc.value_counts(birthdays_table["years"])

See :ref:`compute` for a list of available compute functions and
how to use them.

Working with large data
-----------------------

Arrow also provides the :class:`pyarrow.dataset` API to work with
large data, which will handle for you partitioning of your data in
smaller chunks

.. ipython:: python

    import pyarrow.dataset as ds

    ds.write_dataset(birthdays_table, "savedir", format="parquet", 
                     partitioning=ds.partitioning(
                        pa.schema([birthdays_table.schema.field("years")])
                    ))

Loading back the partitioned dataset will detect the chunks

.. ipython:: python

    birthdays_dataset = ds.dataset("savedir", format="parquet", partitioning=["years"])

    birthdays_dataset.files

and will lazily load chunks of data only when iterating over them

.. ipython:: python

    import datetime

    current_year = datetime.datetime.utcnow().year
    for table_chunk in birthdays_dataset.to_batches():
        print("AGES", pc.subtract(current_year, table_chunk["years"]))

For further details on how to work with big datasets, how to filter them,
how to project them, etc., refer to :ref:`dataset` documentation.

Continuining from here
----------------------

For digging further into Arrow, you might want to read the 
:doc:`PyArrow Documentation <./index>` itself or the 
`Arrow Python Cookbook <https://arrow.apache.org/cookbook/py/>`_


.. ipython:: python
    :suppress:

    # clean-up custom working directory
    import os
    import shutil

    os.chdir(orig_working_dir)
    shutil.rmtree(temp_working_dir, ignore_errors=True)

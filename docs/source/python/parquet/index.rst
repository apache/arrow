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
.. _parquet:

Reading and Writing the Apache Parquet Format
=============================================

The `Apache Parquet <http://parquet.apache.org/>`_ project provides a
standardized open-source columnar storage format for use in data analysis
systems. It was created originally for use in `Apache Hadoop
<http://hadoop.apache.org/>`_ with systems like `Apache Drill
<http://drill.apache.org>`_, `Apache Hive <http://hive.apache.org>`_, `Apache
Impala <http://impala.apache.org>`_, and `Apache Spark
<http://spark.apache.org>`_ adopting it as a shared standard for high
performance data IO.

Apache Arrow is an ideal in-memory transport layer for data that is being read
or written with Parquet files. We have been concurrently developing the `C++
implementation of
Apache Parquet <https://github.com/apache/arrow/tree/main/cpp/tools/parquet>`_,
which includes a native, multithreaded C++ adapter to and from in-memory Arrow
data. PyArrow includes Python bindings to this code, which thus enables reading
and writing Parquet files with pandas as well.

Obtaining pyarrow with Parquet Support
--------------------------------------

If you installed ``pyarrow`` with pip or conda, it should be built with Parquet
support bundled:

.. code-block:: python

   >>> import pyarrow.parquet as pq

If you are building ``pyarrow`` from source, you must use ``-DARROW_PARQUET=ON``
when compiling the C++ libraries and enable the Parquet extensions when
building ``pyarrow``. If you want to use Parquet Encryption, then you must
use ``-DPARQUET_REQUIRE_ENCRYPTION=ON`` too when compiling the C++ libraries.
See the :ref:`Python Development <python-development>` page for more details.


Reading and Writing Single Files
--------------------------------

.. toctree::
   :maxdepth: 2

   parquet

Data Type Handling
------------------

.. toctree::
   :maxdepth: 2

   parquet_type_handling


Partitioned Datasets (Multiple Files)
-------------------------------------

.. toctree::
   :maxdepth: 2

   parquet_datasets

Parquet Modular Encryption
--------------------------

.. toctree::
   :maxdepth: 2

   parquet_encryption

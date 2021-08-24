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

====================
DataFusion in Python
====================

This is a Python library that binds to `Apache Arrow <https://arrow.apache.org/>`_ in-memory query engine `DataFusion <https://github.com/apache/arrow/tree/master/rust/datafusion>`_.

Like pyspark, it allows you to build a plan through SQL or a DataFrame API against in-memory data, parquet or CSV files, run it in a multi-threaded environment, and obtain the result back in Python.

It also allows you to use UDFs and UDAFs for complex operations.

The major advantage of this library over other execution engines is that this library achieves zero-copy between Python and its execution engine: there is no cost in using UDFs, UDAFs, and collecting the results to Python apart from having to lock the GIL when running those operations.

Its query engine, DataFusion, is written in `Rust <https://www.rust-lang.org>`_), which makes strong assumptions about thread safety and lack of memory leaks.

Technically, zero-copy is achieved via the `c data interface <https://arrow.apache.org/docs/format/CDataInterface.html>`_.

How to use it
=============

Simple usage:

.. code-block:: python

   import datafusion
   import pyarrow

   # an alias
   f = datafusion.functions

   # create a context
   ctx = datafusion.ExecutionContext()

   # create a RecordBatch and a new DataFrame from it
   batch = pyarrow.RecordBatch.from_arrays(
       [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
       names=["a", "b"],
   )
   df = ctx.create_dataframe([[batch]])

   # create a new statement
   df = df.select(
       f.col("a") + f.col("b"),
       f.col("a") - f.col("b"),
   )

   # execute and collect the first (and only) batch
   result = df.collect()[0]

   assert result.column(0) == pyarrow.array([5, 7, 9])
   assert result.column(1) == pyarrow.array([-3, -3, -3])


UDFs
----

.. code-block:: python

   def is_null(array: pyarrow.Array) -> pyarrow.Array:
       return array.is_null()

   udf = f.udf(is_null, [pyarrow.int64()], pyarrow.bool_())

   df = df.select(udf(f.col("a")))


UDAF
----

.. code-block:: python

   import pyarrow
   import pyarrow.compute


   class Accumulator:
       """
       Interface of a user-defined accumulation.
       """
       def __init__(self):
           self._sum = pyarrow.scalar(0.0)

       def to_scalars(self) -> [pyarrow.Scalar]:
           return [self._sum]

       def update(self, values: pyarrow.Array) -> None:
           # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
           self._sum = pyarrow.scalar(self._sum.as_py() + pyarrow.compute.sum(values).as_py())

       def merge(self, states: pyarrow.Array) -> None:
           # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
           self._sum = pyarrow.scalar(self._sum.as_py() + pyarrow.compute.sum(states).as_py())

       def evaluate(self) -> pyarrow.Scalar:
           return self._sum


   df = ...

   udaf = f.udaf(Accumulator, pyarrow.float64(), pyarrow.float64(), [pyarrow.float64()])

   df = df.aggregate(
       [],
       [udaf(f.col("a"))]
   )


How to install (from pip)
=========================

.. code-block:: shell

   pip install datafusion


How to develop
==============

This assumes that you have rust and cargo installed. We use the workflow recommended by `pyo3 <https://github.com/PyO3/pyo3>`_ and `maturin <https://github.com/PyO3/maturin>`_.

Bootstrap:

.. code-block:: shell

   # fetch this repo
   git clone git@github.com:apache/arrow-datafusion.git

   cd arrow-datafusion/python

   # prepare development environment (used to build wheel / install in development)
   python3 -m venv venv
   # activate the venv
   source venv/bin/activate
   pip install -r requirements.txt


Whenever rust code changes (your changes or via `git pull`):

.. code-block:: shell

   # make sure you activate the venv using "source venv/bin/activate" first
   maturin develop
   python -m pytest


How to update dependencies
==========================

To change test dependencies, change the `requirements.in` and run

.. code-block:: shell

   # install pip-tools (this can be done only once), also consider running in venv
   pip install pip-tools

   # change requirements.in and then run
   pip-compile --generate-hashes


To update dependencies, run

.. code-block:: shell

   pip-compile update


More details `here <https://github.com/jazzband/pip-tools>`_


.. toctree::
   :maxdepth: 2

   api

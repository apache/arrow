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

Extending PyArrow Datasets
==========================

.. warn::

    This protocol is currently experimental.

PyArrow provides a core protocol for datasets, so third-party libraries can both
produce and consume classes that conform to useful subset of the PyArrow dataset
API. This subset provides enough functionality to provide projection
pushdown. The subset of the API is contained in ``pyarrow.dataset.protocol``.

.. image:: pyarrow_dataset_protocol.svg
   :alt: A diagram showing the workflow for using the PyArrow Dataset protocol.
         There are two flows shown, one for stream and one for tasks. The stream
         case shows a linear flow from a producer class, to a dataset, to a 
         scanner, and finally to a RecordBatchReader. The tasks case shows a
         similar diagram, except the dataset is split into fragments, which are
         then distributed to tasks, which each create their own scanner and
         RecordBatchReader.

Producers are responsible for outputting a class that conforms to the protocol.

Consumers are responsible for calling methods on the protocol to get the data
out of the dataset. The protocol supports getting data as a single stream or
as a series of tasks which may be distributed.

As an example, from the perspective of the user this is what the code looks like
to retrieve a Delta Lake table as a dataset and use it in DuckDB: 

.. code-block:: python
    :emphasize-lines: 2,6

    from deltalake import DeltaTable
    table = DeltaTable("path/to/table")
    dataset = table.to_pyarrow_dataset()

    import duckdb
    df = duckdb.arrow(dataset)
    df.project("y")

Here, the DuckDB would pass the the projection of ``y`` down to the producer
through the dataset protocol. The deltalake scanner would then only read the
column ``y``. Thus, the user gets to enjoy the performance benefits of pushing
down projections while being able to specify those in their preferred query engine.


Dataset Producers
-----------------

If you are a library implementing a new data source, you'll want to be able to
produce a PyArrow-compatible dataset. Your dataset could be backed by the classes
implemented in PyArrow or you could implement your own classes. Either way, you
should implement the protocol below.

Dataset Consumers
-----------------

If you are a query engine, you'll want to be able to
consume any PyArrow datasets. To make sure your integration is compatible
with any dataset, you should only call methods that are included in the 
protocol. Dataset implementations provided by PyArrow implements additional
options and methods beyond those, but they should not be relied upon without
checking for specific classes.

There are two general patterns for consuming PyArrow datasets: reading a single
stream or creating a scan task per fragment.

If you have a streaming execution model, you can receive a single stream
of data by calling ``dataset.scanner(columns=...).to_reader()``.
This will return a RecordBatchReader, which can be exported over the 
:ref:`C Stream Interface <c-stream-interface>`. The record batches yield 
from the stream can then be passed to worker threads for parallelism.

If you are using a task-based model, you can split the dataset into fragments 
and then distribute those fragments into tasks that create their own scanners
and readers. In this case, the code looks more like:

.. code-block:: python

    fragments = list(dataset.get_fragments(columns=...))

    def scan_partition(i):
        fragment = fragments[i]
        scanner = fragment.scanner()
        return reader = scanner.to_reader()

Fragments are pickleable, so they can be passed to remote workers in a 
distributed system.

If your engine supports projection (column) pushdown,
you can pass those down to the dataset by passing them to the ``scanner``.
Column pushdown is limited to selecting a subset of columns from the schema.
Some implementations, including PyArrow may also support projecting and
renaming columns, but this is not part of the protocol.


The protocol
------------

This module can be imported starting in PyArrow ``13.0.0`` at
``pyarrow.dataset.protocol``. The protocol is defined with ``typing.Protocol``
classes. They can be checked at runtime with ``isinstance`` but can also be
checked statically with Python type checkers like ``mypy``.

.. literalinclude:: ../../../../python/pyarrow/dataset/protocol.py
   :language: python

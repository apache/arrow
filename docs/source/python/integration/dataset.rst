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

Extending PyArrow Datasets
==========================

PyArrow provides a core protocol for datasets, so third-party libraries can both
produce and consume PyArrow datasets.

The idea is that any library can have a method that returns their dataset as a
PyArrow dataset. Then, any query engine can consume that dataset and push down
filters and projections.

.. image:: pyarrow_dataset_protocol.svg
   :alt: A diagram showing the workflow for using the PyArrow Dataset protocol.
         There are two flows shown, one for streams and one for tasks. The stream
         case shows a linear flow from a producer class, to a dataset, to a 
         scanner, and finally to a RecordBatchReader. The tasks case shows a
         similar diagram, except the dataset is split into fragments, which are
         then distributed to tasks, which each create their own scanner and
         RecordBatchReader.

Producers are responsible for outputting a class that conforms to the protocol.

Consumers are responsible for calling methods on the protocol to get the data
out of the dataset. The protocol supports getting data as a single stream or
as a series of tasks which may be distributed.

From the perspective of a user, this looks something like

.. code-block:: python

    dataset = producer_library.get_dataset(...)
    df = consumer_library.read_dataset(dataset)
    df.filter("x > 0").select("y")

Here, the consumer would pass the filter ``x > 0`` and the projection of ``y`` down
to the producer through the dataset protocol. Thus, the user gets to enjoy the
performance benefits of pushing down filters and projections while being able
to specify those in their preferred query engine.


Dataset Producers
-----------------

If you are a library implementing a new data source, you'll want to be able to
produce a PyArrow-compatible dataset. Your dataset could be backed by the classes
implemented in PyArrow or you could implement your own classes. Either way, you
should implement the protocol below.

When implementing the dataset, consider the following:

* Filters passed down should be fully executed. While other systems have scanners
  that are "best-effort", only executing the parts of the filter that it can, PyArrow
  datasets should always remove all rows that don't match the filter.
* The API does not require that a dataset has metadata about all fragments
  loaded into memory. Indeed, to scale to very large Datasets, don't eagerly
  load all the fragment metadata into memory. Instead, load fragment metadata
  once a filter is passed. This allows you to skip loading metadata about
  fragments that aren't relevant to queries. For example, if you have a dataset
  that uses Hive-style paritioning for a column ``date`` and the user passes a
  filter for ``date=2023-01-01``, then you can skip listing directory for HIVE
  partitions that don't match that date.


Dataset Consumers
-----------------

If you are a query engine, you'll want to be able to
consume any PyArrow datasets. To make sure your integration is compatible
with any dataset, you should only call methods that are included in the 
protocol. Dataset implementations provided by PyArrow implements additional
options and methods beyond those, but they should not be relied upon without
checking for specific classes.

There are two general patterns for consuming PyArrow datasets: reading a single
stream or reading a stream per fragment.

If you have a streaming execution model, you can recieve a single stream
of data by calling ``dataset.scanner(filter=..., columns=...).to_reader()``.
This will return a RecordBatchReader, which can be exported over the 
:ref:`C Stream Interface <c-stream-interface>`. The record batches yield 
from the stream can then be passed to worker threads for parallelism.

If you are using a partition-based or distributed model, you can split the
dataset into fragments and then distribute those fragments into tasks that
create their own scanners and readers. In this case, the code looks more
like:

.. code-block:: python

    fragments = list(dataset.get_fragments(filter=..., columns=...))

    def scan_partition(i):
        fragment = fragments[i]
        scanner = fragment.scanner()
        return reader = scanner.to_reader()

Fragments are pickleable, so they can be passed to remote workers in a 
distributed system.

If your engine supports predicate (filter) and projection (column) pushdown,
you can pass those down to the dataset by passing them to the ``scanner``.


The protocol
------------

This module can be imported starting in PyArrow ``13.0.0`` at
``pyarrow.dataset.protocol``. The protocol is defined with ``typing.Protocol``
classes. They can be checked at runtime with ``isinstance`` but can also be
checked statically with Python type checkers like ``mypy``.

.. literalinclude:: ../../../../python/pyarrow/dataset/protocol.py
   :language: python

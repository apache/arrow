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

Dataset Producers
-----------------

If you are a library implementing a new data source, you'll want to be able to
produce a PyArrow-compatible dataset. Your dataset could be backed by the classes
implemented in PyArrow or you could implement your own classes. Either way, you
should implement the protocol below.

When implementing the dataset, consider the following:

* To scale to very large dataset, don't eagerly load all the fragments into memory.
  Instead, load fragments once a filter is passed. This allows you to skip loading
  metadata about fragments that aren't relevant to queries. For example, if you
  have a dataset that uses Hive-style paritioning for a column ``date`` and the
  user passes a filter for ``date=2023-01-01``, then you can skip listing directory
  for HIVE partitions that don't match that date.
* Filters passed down should be fully executed. While other systems have scanners
  that are "best-effort", only executing the parts of the filter that it can, PyArrow
  datasets should always remove all rows that don't match the filter.


Dataset Consumers
-----------------

If you are a query engine, you'll want to be able to
consume any PyArrow datasets. To make sure your integration is compatible
with any dataset, you should only call methods that are included in the 
protocol. Dataset implementations provided by PyArrow implements additional
options and methods beyond those, but they should not be relied upon.

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

.. literalinclude:: ../../python/pyarrow/dataset/protocol.py
   :language: python

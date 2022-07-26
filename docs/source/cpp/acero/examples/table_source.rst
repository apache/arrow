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

============
Table Source
============

.. _stream_execution_table_source_docs:

In the previous example, :ref:`source node <stream_execution_source_docs>`, a source node
was used to input the data.  But when developing an application, if the data is already in memory
as a table, it is much easier, and more performant to use :class:`arrow::compute::TableSourceNodeOptions`.
Here the input data can be passed as a ``std::shared_ptr<arrow::Table>`` along with a ``max_batch_size``. 
The ``max_batch_size`` is to break up large record batches so that they can be processed in parallel.
It is important to note that the table batches will not get merged to form larger batches when the source
table has a smaller batch size.

Example of using ``table_source``

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Table Source Example)
  :end-before: (Doc section: Table Source Example)
  :linenos:
  :lineno-match:

.. _stream_execution_filter_docs:

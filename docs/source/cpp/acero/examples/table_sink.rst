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

.. default-domain:: cpp
.. highlight:: cpp
.. cpp:namespace:: arrow::compute

.. _stream_execution_table_sink_docs:

==========
Table Sink
==========

The ``table_sink`` node provides the ability to receive the output as an in-memory table. 
This is simpler to use than the other sink nodes provided by the streaming execution engine
but it only makes sense when the output fits comfortably in memory.
The node is created using :class:`arrow::compute::TableSinkNodeOptions`.

Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Table Sink Example)
  :end-before: (Doc section: Table Sink Example)
  :linenos:
  :lineno-match:

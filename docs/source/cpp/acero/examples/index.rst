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

Examples
========

Constructing ``ExecNode`` using Options
---------------------------------------

:class:`ExecNode` is the component we use as a building block 
containing in-built operations with various functionalities. 

This is the list of operations associated with the execution plan:

.. list-table:: Operations and Options
   :widths: 50 50
   :header-rows: 1

   * - Operation
     - Options
   * - ``source``
     - :class:`arrow::compute::SourceNodeOptions`
   * - ``table_source``
     - :class:`arrow::compute::TableSourceNodeOptions`
   * - ``filter``
     - :class:`arrow::compute::FilterNodeOptions`
   * - ``project``
     - :class:`arrow::compute::ProjectNodeOptions`
   * - ``aggregate``
     - :class:`arrow::compute::AggregateNodeOptions`
   * - ``sink``
     - :class:`arrow::compute::SinkNodeOptions`
   * - ``consuming_sink``
     - :class:`arrow::compute::ConsumingSinkNodeOptions`
   * - ``order_by_sink``
     - :class:`arrow::compute::OrderBySinkNodeOptions`
   * - ``select_k_sink``
     - :class:`arrow::compute::SelectKSinkNodeOptions`
   * - ``scan``
     - :class:`arrow::dataset::ScanNodeOptions` 
   * - ``hash_join``
     - :class:`arrow::compute::HashJoinNodeOptions`
   * - ``write``
     - :class:`arrow::dataset::WriteNodeOptions`
   * - ``union``
     - N/A
   * - ``table_sink``
     - :class:`arrow::compute::TableSinkNodeOptions`

.. toctree::
   :maxdepth: 1

   source
   table_source
   filter
   project
   aggregate
   sink
   consuming_sink
   order_by_sink
   select_k_sink
   scan
   hash_join
   write
   union
   table_sink

Summary
-------

There are examples of these nodes which can be found in 
``cpp/examples/arrow/execution_plan_documentation_examples.cc`` in the Arrow source.

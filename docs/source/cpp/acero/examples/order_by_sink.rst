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

.. _stream_execution_order_by_sink_docs:

=============
Order by sink
=============

``order_by_sink`` operation is an extension to the ``sink`` operation. 
This operation provides the ability to guarantee the ordering of the 
stream by providing the :class:`arrow::compute::OrderBySinkNodeOptions`. 
Here the :class:`arrow::compute::SortOptions` are provided to define which columns 
are used for sorting and whether to sort by ascending or descending values.

.. note:: This node is a "pipeline breaker" and will fully materialize the dataset in memory.
          In the future, spillover mechanisms will be added which should alleviate this 
          constraint.


Order-By-Sink example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: OrderBySink Example)
  :end-before: (Doc section: OrderBySink Example)
  :linenos:
  :lineno-match:
  
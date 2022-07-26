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

.. _stream_execution_aggregate_docs:

=========
Aggregate
=========

The ``aggregate`` node computes various types of aggregates over data.

Arrow supports two types of aggregates: "scalar" aggregates, and
"hash" aggregates. Scalar aggregates reduce an array or scalar input
to a single scalar output (e.g. computing the mean of a column). Hash
aggregates act like ``GROUP BY`` in SQL and first partition data based
on one or more key columns, then reduce the data in each
partition. The ``aggregate`` node supports both types of computation,
and can compute any number of aggregations at once.

:class:`arrow::compute::AggregateNodeOptions` is used to define the
aggregation criteria.  It takes a list of aggregation functions and
their options; a list of target fields to aggregate, one per function;
and a list of names for the output fields, one per function.
Optionally, it takes a list of columns that are used to partition the
data, in the case of a hash aggregation.  The aggregation functions
can be selected from :ref:`this list of aggregation functions
<aggregation-option-list>`.

.. note:: This node is a "pipeline breaker" and will fully materialize
          the dataset in memory.  In the future, spillover mechanisms
          will be added which should alleviate this constraint.

The aggregation can provide results as a group or scalar. For instances,
an operation like `hash_count` provides the counts per each unique record
as a grouped result while an operation like `sum` provides a single record. 

Scalar Aggregation example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Scalar Aggregate Example)
  :end-before: (Doc section: Scalar Aggregate Example)
  :linenos:
  :lineno-match:

Group Aggregation example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: Group Aggregate Example)
  :end-before: (Doc section: Group Aggregate Example)
  :linenos:
  :lineno-match:
  
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

===============================
The Gandiva Expression Compiler
===============================

Gandiva is a runtime expression compiler that uses `LLVM`_ to generate
efficient native code for projections and filters on Arrow record batches.
Gandiva only handles projections and filters. For other transformations, see
:ref:`Compute Functions <compute-cpp>`.

Gandiva was designed to take advantage of the Arrow memory format and modern
hardware. Compiling expressions using LLVM allows the execution to be optimized
to the local runtime environment and hardware, including available SIMD
instructions. To reduce optimization overhead, many Gandiva functions are
pre-compiled into LLVM IR (intermediate representation).

.. _LLVM: https://llvm.org/


Building Expressions
====================

Gandiva provides a general expression representation where expressions are
represented by a tree of nodes. The expression trees are built using
:class:`gandiva::TreeExprBuilder`. The leaves of the expression tree are typically
field references, created by :func:`gandiva::TreeExprBuilder::MakeField`, and
literal values, created by :func:`gandiva::TreeExprBuilder::MakeLiteral`. Nodes
can be combined into more complex expression trees using:

* :func:`gandiva::TreeExprBuilder::MakeFunction` to create a function
  node.
* :func:`gandiva::TreeExprBuilder::MakeIf` to create if-else logic.
* :func:`gandiva::TreeExprBuilder::MakeAnd` and :func:`gandiva::TreeExprBuilder::MakeOr`
  to create boolean expressions. (For "not", use the ``not(bool)`` function in ``MakeFunction``.)
* :func:`gandiva::TreeExprBuilder::MakeInExpressionInt32` and the other "in expression"
  functions to create set membership tests.

Once an expression tree is built, they are wrapped in either :class:`gandiva::Expression`
or :class:`gandiva::Condition`, depending on how they will be used.
``Expression`` is used in projections while ``Condition`` is used filters.

As an example, here is how to create an Expression representing ``x + 3`` and a
Condition representing ``x < 3``:

.. code-block:: cpp

   auto field_x_raw = arrow::field("x", arrow::int32());
   auto field_x = TreeExprBuilder::MakeField(field_x_raw);
   auto literal_3 = TreeExprBuilder::MakeLiteral(3);
   auto field_result = arrow::field("result", arrow::int32());

   auto add_node = TreeExprBuilder::MakeFunction("add", {field_x, literal_3}, arrow::int32());
   auto expression = TreeExprBuilder::MakeExpression(add_node, field_result);

   auto less_than_node = TreeExprBuilder::MakeFunction("less_than", {field_x, literal_3},
                                                       boolean());
   auto condition = TreeExprBuilder::MakeCondition(less_than_node);

For simpler expressions, there are also convenience functions that allow you to
use functions directly in ``MakeExpression`` and ``MakeCondition``:

.. code-block:: cpp

   auto expression = TreeExprBuilder::MakeExpression("add", {field_x, literal_3}, field_result);

   auto condition = TreeExprBuilder::MakeCondition("less_than", {field_x, literal_3});


Projectors and Filters
======================

Gandiva's two execution kernels are :class:`gandiva::Projector` and
:class:`gandiva::Filter`. ``Projector`` consumes a record batch and projects
into a new record batch. ``Filter`` consumes a record batch and produces a
:class:`gandiva::SelectionVector` containing the indices that matched the condition.

For both ``Projector`` and ``Filter``, optimization of the expression IR happens
when creating instances. They are compiled against a static schema, so the
schema of the record batches must be known at this point.

Continuing with the ``expression`` and ``condition`` created in the previous
section, here is an example of creating a Projector and a Filter:

.. code-block:: cpp

   auto schema = arrow::schema({field_x});
   std::shared_ptr<Projector> projector;
   auto status = Projector::Make(schema, {expression}, &projector);
   ARROW_CHECK_OK(status);

   std::shared_ptr<Filter> filter;
   status = Filter::Make(schema, condition, &filter);
   ARROW_CHECK_OK(status);


Once a Projector or Filter is created, it can be evaluated on Arrow record batches.
These execution kernels are single-threaded on their own, but are designed to be
reused to process distinct record batches in parallel.

Execution is performed with :func:`gandiva::Projector::Evaluate` and
:func:`gandiva::Filter::Evaluate`. Filters produce :class:`gandiva::SelectionVector`,
a vector of row indices that matched the filter condition. When filtering and
projecting record batches, you can pass the selection vector into the projector
so that the projection is only evaluated on matching rows.

Here is an example of evaluating the Filter and Projector created above:

.. code-block:: cpp

   auto pool = arrow::default_memory_pool();
   int num_records = 4;
   arrow::Int32Buider builder;
   int32_t values[4] = {1, 2, 3, 4};
   ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array, builder.Finish());
   auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array});

   // Just project
   arrow::ArrayVector outputs;
   status = projector->Evaluate(*in_batch, pool, &outputs);
   ARROW_CHECK_OK(status);

   // Evaluate filter
   gandiva::SelectionVector result_indices;
   status = filter->Evaluate(*in_batch, &result_indices);
   ARROW_CHECK_OK(status);

   // Project with filter
   arrow::ArrayVector outputs_filtered;
   status = projector->Evaluate(*in_batch, selection_vector.get(),
                                pool, &outputs_filtered);
   ARROW_CHECK_OK(status);

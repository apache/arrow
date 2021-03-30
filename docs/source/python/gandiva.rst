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

.. currentmodule:: pyarrow.gandiva
.. _gandiva:

The Gandiva Expression Compiler
===============================

Gandiva is an expression compiler that uses `LLVM`_ to generate efficient native
code for projections and filters on Arrow record batches.

Using Gandiva involves three steps. First, you use the :py:class:`TreeExprBuilder` to 
build an expression to filter or a set of expressions to project rows. The builder provides
a library of functions that can be composed into complex expressions. Second,
you create a :py:class:`Filter` or :py:class:`Projector` from the expression. This step
compiles your expression into efficient native code. Finally, you use the filter or 
projector you created to operate on Arrow record batches.

.. _LLVM: https://llvm.org/

.. contents:: Contents
  :depth: 3

.. note::

   As present, Gandiva is only built on PyArrow Conda distributions, not PyPI wheels.


Building Gandiva Expressions
----------------------------

Gandiva expressions are built in PyArrow with :py:class:`TreeExprBuilder`. For example,
to express ``x`` is between 2 and 3:

.. code-block:: python

  import pyarrow as pa
  from pyarrow.gandiva import TreeExprBuilder

  builder = TreeExprBuilder()

  field_x = builder.make_field(pa.field('x', pa.float64()))
  scalar_2 = builder.make_literal(2.0, pa.float64())
  scalar_3 = builder.make_literal(3.0, pa.float64())

  expr = builder.make_and([
      builder.make_function('greater_than', [field_x, scalar_2], pa.bool_()),
      builder.make_function('less_than', [field_x, scalar_3], pa.bool_())
  ])

Each of the builder methods returns a new :py:class:`Node` in the expression tree. This
includes:

- :py:meth:`TreeExprBuilder.make_field` creates a field node: a reference to a column in the record batches.
  The type must match the type of the array in that column.

- :py:meth:`TreeExprBuilder.make_literal` creates a literal node: a literal value hardcoded.

- :py:meth:`TreeExprBuilder.make_function` creates a function node, which may take other nodes as arguments.
  The available functions are listed in the API docs.

- :py:meth:`TreeExprBuilder.make_if`, :py:meth:`TreeExprBuilder.make_and`, and :py:meth:`TreeExprBuilder.make_or` create new nodes based on if-else, 
  boolean "and" and boolean "or" logic.


Executing Filter Expressions
----------------------------

To create a filter, you can convert a boolean expression node into a :py:class:`Condition`,
and instantiate the Gandiva filter using :py:func:`make_filter`. When creating the filter
instance, the expression is converted into LLVM IR and optimized, allowing it to
be reused for multiple record batches.

.. code-block:: python

  import pyarrow as pa
  from pyarrow.gandiva import TreeExprBuilder, make_filter

  builder = TreeExprBuilder()

  field_x = builder.make_field(pa.field('x', pa.float64()))
  scalar_2 = builder.make_literal(2.0, pa.float64())
  expr = builder.make_function('greater_than', [field_x, scalar_2], pa.bool_())

  condition = builder.make_condition(expr)

  record_batch = pa.record_batch([pa.array([1.0, 3.0], pa.float64())], ['x'])
  filter_executor = make_filter(record_batch.schema, condition)

The :py:meth:`Filter.evaluate` method runs the filter on a record batch. It returns a
:py:class:`SelectionVector`, which contains the indicies of the matching rows. This can 
either be used later in a :py:meth:`Projector.evaluate` or can be converted into an
Arrow array with :py:meth:`SelectionVector.to_array` and run again the record batch
with the :py:meth:`pyarrow.RecordBatch.take` method.

.. code-block:: python

  selection_vector = filter_executor.evaluate(record_batch, pa.default_memory_pool())
  result = record_batch.take(selection_vector.to_array())


Executing Projection Expressions
--------------------------------

To create a :py:class:`Projector`, convert nodes into expressions using 
:py:meth:`TreeExprBuilder.make_expression` and then pass that list into the 
:py:func:`make_projector` function. For example, to project a record batch
with a float ``x`` into one with ``x`` and the logarithm of ``x``:

.. code-block:: python

  import pyarrow as pa
  from pyarrow.gandiva import TreeExprBuilder, make_projector

  builder = TreeExprBuilder()

  field_x = builder.make_field(pa.field('x', pa.float64()))
  log_x = builder.make_function('log', [field_x], pa.float64())

  expressions = [
    builder.make_expression(field_x, pa.field('x', pa.float64())),
    builder.make_expression(log_x, pa.field('log_x', pa.float64()))
  ]

  record_batch = pa.record_batch([pa.array([1.0, 3.0], pa.float64())], ['x'])
  projector = make_projector(record_batch.schema, expressions, pa.default_memory_pool())

  result = projector.evaluate(record_batch)
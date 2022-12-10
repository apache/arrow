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
.. cpp:namespace:: gandiva

===============================
The Gandiva Expression Compiler
===============================

Gandiva is a runtime expression compiler that uses `LLVM`_ to generate
efficient native code for compute on Arrow record batches.
Gandiva only handles projections and filters; for other transformations, see
:ref:`Compute Functions <compute-cpp>`.

Gandiva was designed to take advantage of the Arrow memory format and modern
hardware. From the Arrow memory model, since Arrow arrays have separate buffers for values and 
validity bitmaps, values and their null status can often be processed 
independently, allowing for better instruction pipelining. On modern hardware,
compiling expressions using LLVM allows the execution to be optimized
to the local runtime environment and hardware, including available SIMD
instructions. To reduce optimization overhead, many Gandiva functions are
pre-compiled into LLVM IR (intermediate representation).

.. _LLVM: https://llvm.org/


Building Expressions
====================

Gandiva provides a general expression representation where expressions are
represented by a tree of nodes. The expression trees are built using
:class:`TreeExprBuilder`. The leaves of the expression tree are typically
field references, created by :func:`TreeExprBuilder::MakeField`, and
literal values, created by :func:`TreeExprBuilder::MakeLiteral`. Nodes
can be combined into more complex expression trees using:

* :func:`TreeExprBuilder::MakeFunction` to create a function
  node. (You can call :func:`GetRegisteredFunctionSignatures` to 
  get a list of valid function signatures.)
* :func:`TreeExprBuilder::MakeIf` to create if-else logic.
* :func:`TreeExprBuilder::MakeAnd` and :func:`TreeExprBuilder::MakeOr`
  to create boolean expressions. (For "not", use the ``not(bool)`` function in ``MakeFunction``.)
* :func:`TreeExprBuilder::MakeInExpressionInt32` and the other "in expression"
  functions to create set membership tests.

Each of these functions create new composite nodes, which contain the leaf nodes
(literals and field references) or other composite nodes as children. By 
composing these, you can create arbitrarily complex expression trees.

Once an expression tree is built, they are wrapped in either :class:`Expression`
or :class:`Condition`, depending on how they will be used.
``Expression`` is used in projections while ``Condition`` is used in filters.

As an example, here is how to create an Expression representing ``x + 3`` and a
Condition representing ``x < 3``:

.. literalinclude:: ../../../cpp/examples/arrow/gandiva_example.cc
   :language: cpp
   :start-after: (Doc section: Create expressions)
   :end-before: (Doc section: Create expressions)
   :dedent: 2


Projectors and Filters
======================

Gandiva's two execution kernels are :class:`Projector` and
:class:`Filter`. ``Projector`` consumes a record batch and projects
into a new record batch. ``Filter`` consumes a record batch and produces a
:class:`SelectionVector` containing the indices that matched the condition.

For both ``Projector`` and ``Filter``, optimization of the expression IR happens
when creating instances. They are compiled against a static schema, so the
schema of the record batches must be known at this point.

Continuing with the ``expression`` and ``condition`` created in the previous
section, here is an example of creating a Projector and a Filter:

.. literalinclude:: ../../../cpp/examples/arrow/gandiva_example.cc
   :language: cpp
   :start-after: (Doc section: Create projector and filter)
   :end-before: (Doc section: Create projector and filter)
   :dedent: 2

Once a Projector or Filter is created, it can be evaluated on Arrow record batches.
These execution kernels are single-threaded on their own, but are designed to be
reused to process distinct record batches in parallel.

Evaluating projections
----------------------

Execution is performed with :func:`Projector::Evaluate`. This outputs 
a vector of arrays, which can be passed along with the output schema to
:func:`arrow::RecordBatch::Make()`.

.. literalinclude:: ../../../cpp/examples/arrow/gandiva_example.cc
   :language: cpp
   :start-after: (Doc section: Evaluate projection)
   :end-before: (Doc section: Evaluate projection)
   :dedent: 2

Evaluating filters
------------------

:func:`Filter::Evaluate` produces :class:`SelectionVector`,
a vector of row indices that matched the filter condition. The selection vector
is a wrapper around an arrow integer array, parameterized by bitwidth. When 
creating the selection vector (you must initialize it *before* passing to 
``Evaluate()``), you must choose the bitwidth, which determines the max index 
value it can hold, and the max number of slots, which determines how many indices
it may contain. In general, the max number of slots should be set to your batch 
size and the bitwidth the smallest integer size that can represent all integers 
less than the batch size. For example, if your batch size is 100k, set the 
maximum number of slots to 100k and the bitwidth to 32 (since 2^16 = 64k which 
would be too small).

Once ``Evaluate()`` has been run and the :class:`SelectionVector` is
populated, use the :func:`SelectionVector::ToArray()` method to get
the underlying array and then :func:`::arrow::compute::Take()` to materialize the
output record batch.

.. literalinclude:: ../../../cpp/examples/arrow/gandiva_example.cc
   :language: cpp
   :start-after: (Doc section: Evaluate filter)
   :end-before: (Doc section: Evaluate filter)
   :dedent: 2

Evaluating projections and filters
----------------------------------

Finally, you can also project while apply a selection vector, with 
:func:`Projector::Evaluate()`. To do so, first make sure to initialize the
:class:`Projector` with :func:`SelectionVector::GetMode()` so that the projector
compiles with the correct bitwidth. Then you can pass the 
:class:`SelectionVector` into the :func:`Projector::Evaluate()` method.


.. literalinclude:: ../../../cpp/examples/arrow/gandiva_example.cc
   :language: cpp
   :start-after: (Doc section: Evaluate filter and projection)
   :end-before: (Doc section: Evaluate filter and projection)
   :dedent: 2

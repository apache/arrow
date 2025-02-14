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


===================
Arrow Compute Layer
===================

Overview
========

Apache Arrow supports the definition and execution of analytical functions on arbitrary
Arrow data structures. These functions and the infrastructure to define and execute them
are altogether referred to as the **compute layer**. Functions in this API primarily
process columnar data (such as scalar or Arrow-based array inputs) and they are intended
for use inside query engines, data frame libraries, etc.

All functions accept whole arrays as input. Many functions perform element-wise or scalar
operations, producing an output element for each input element provided. Other functions,
such as data filtering or group-based analysis, produce a number of output elements that
differ from the quantity of input elements provided. Finally, there are functions that
take produce only a single output value.

Detailed information about the compute layer can be found in a few places, distinguished
by learing objective:

* :doc:`Compute Layer Reference <../compute>` -- A reference for the functions defined in the
  compute layer, available to any project built on top of Arrow. This reference contains a
  list of each function, their documented behavior, and information about their signatures
  and options.

* :doc:`Compute Layer Development Guide <../developers/author_compute_fn>` -- A guide for developing
  new compute functions for the compute layer to be included in Arrow.

Resources
---------

An introduction to the compute API is provided in the :doc:`Compute Functions
<compute>` user guide, which contains documentation for many of the functions, what types
of arguments they accept, and what types of outputs they return.


Overview
--------


.. seealso::
   :doc:`User Guide for the compute API <compute>`

All functions accept whole arrays as input. Many functions perform element-wise or scalar
operations, producing an output element for each input element provided. Other functions,
such as data filtering or group-based analysis, produce a number of output elements that
differ from the quantity of input elements provided. Finally, there are functions that
take produce only a single output value.

This documentation aims to detail the process for writing a compute function and adding it
to the Apache Arrow library so that it can be made available to any project built atop
Arrow. We begin with an overview of the compute API, then we discuss a more concrete
example. Additionally, a cookbook is being developed and will be available in the `Apache
Arrow Cookbook`_.

.. _Apache Arrow Cookbook: https://arrow.apache.org/cookbook/cpp/


Terminology
-----------

Before digging into the details, here is a brief listing of important terminology used in
the compute API:

* **standard function** -- For the clarity in this documentation, this is a standard C++
  function.

* **compute function** -- A logical representation of a function that is defined and
  invoked using Arrow's compute API.

* **compute kernel** -- A specific implementation of a **compute function**. This is most
  often written as a standard function or a class, depending on the implementation
  complexity and whether the compute kernel is stateful. A compute kernel must be
  associated with a compute function by using **AddKernel** (as in
  :cpp:func:`ScalarFunction::AddKernel <ScalarFunction::AddKernel()>`).

* **function registry** -- A data structure, like a dictionary, where functions are
  listed.

* **dispatch** -- Selection of a specific compute kernel when a compute function is
  invoked. When calling a compute function, choosing the appropriate compute kernel
  depends on the types of input arguments and how many arguments are provided.


Parts of a Compute Function
---------------------------

Principal attributes of a compute :struct:`Function`:

* A unique :func:`name <arrow::compute::Function::name()>` used for function invocation
  and language bindings.

* A :cpp:enum:`Kind <Function::Kind>` which describes the relationship between the
  cardinality of input elements and output elements. More concretely, a function kind
  could be "element-wise," or "scalar," if it calculates an output element for each input
  element.

* A return value of :struct:`OutputType` and a **shape**: :struct:`Scalar`,
  :struct:`Array`, or :struct:`ChunkedArray`.

* An :struct:`Arity`, or argument cardinality, that represents how many arguments the
  function accepts. Commonly: :cpp:func:`Nullary <Arity::Nullary()>`, :cpp:func:`Unary
  <Arity::Unary()>`, :cpp:func:`Binary <Arity::Binary()>`, :cpp:func:`Ternary
  <Arity::Ternary()>`, or :cpp:func:`Variadic <Arity::VarArgs()>`.

* A number of arguments, each having an :struct:`InputType` and a **shape**:
  :struct:`Scalar`, :struct:`Array`, or :struct:`ChunkedArray`.

* A :struct:`FunctionDoc` which documents functionality and behavior.

Compute functions can also be further categorized based on the type of operation
performed. For example, **Scalar Arithmetic** functions accept scalar, numeric arguments and
return a scalar, numeric value. Similarly, **Scalar String** functions accept scalar
arguments and return a scalar value; but, expects arguments to be strings and returns
a string value.

Compute functions (see :doc:`FunctionImpl and subclasses <../api/compute>`) are associated
with a set of :struct:`Kernels <Kernel>`. Each kernel is similar to a real function and
implements logic for the function for a specific argument signature.

Optionally, compute functions may accept :struct:`FunctionOptions`, which provides a
mechanism to alter behavior of a function kernel instead of creating new functions or
kernels for each desirable behavior.

Function and Kernel Signatures
------------------------------

There are a few important parts of a compute function: (1) arguments, (2) result, and
(3) kernels.

**Arguments.** The number of arguments for a compute function is defined by the
:struct:`Arity` class. Typically, a compute function's arity is up to 3 or "variable"
(meaning it can be any number). Each argument has a type, such as :struct:`UInt64Type`,
and a shape, such as :struct:`Array`.

Commonly: :cpp:func:`Nullary <Arity::Nullary()>`, :cpp:func:`Unary
  <Arity::Unary()>`, :cpp:func:`Binary <Arity::Binary()>`, :cpp:func:`Ternary
  <Arity::Ternary()>`, or :cpp:func:`Variadic <Arity::VarArgs()>`.

**Result.** A compute function typically has a result because Arrow is designed around
immutable data: if a compute function didn't have a result and isn't supposed to mutate
its arguments, then it would have no effect. What's most notable about a compute
function's result, is that its relationships with the compute function's arguments
determines the compute function's :cpp:enum:`Kind <Function::Kind>`--a
:cpp:enumerator:`SCALAR <Function::Kind::SCALAR>` function is one where the number of
elements in its result is the same as in all of its arguments.

* A :cpp:enum:`Kind <Function::Kind>` which describes the relationship between the
  cardinality of input elements and output elements. More concretely, a function kind
  could be "element-wise," or "scalar," if it calculates an output element for each input
  element.

**Kernels.** A compute function is defined by its behavior, for example an **Add**
function should combine two or more inputs according to *some* definition of addition.
However, the actual logic that is executed for actual inputs is defined by a compute
kernel. If we want to execute an **Add** function on 2 arrays of 32-bit integers, then two
steps must be taken: (1) the **Add** function should have an appropriate compute kernel
defined and (2) something should select that compute kernel and execute it. The first step
is done when writing a compute function, described in the :doc:`Authoring Compute
Functions <../developers/author_compute_fn>` guide. The second step is done by Arrow's
compute layer and relies on how the compute function is defined in step one.

* **compute kernel** -- A specific implementation of a **compute function**. This is most
  often written as a standard function or a class, depending on the implementation
  complexity and whether the compute kernel is stateful. A compute kernel must be
  associated with a compute function by using **AddKernel** (as in
  :cpp:func:`ScalarFunction::AddKernel <ScalarFunction::AddKernel()>`).


Many
functions perform element-wise or scalar operations, producing an output element for each
input element provided. Other functions, such as data filtering or group-based analysis,
produce a number of output elements that differ from the quantity of input elements
provided. Finally, there are functions that take produce only a single output value.


Function Kinds
--------------

Arrow uses an enumerated type, :cpp:enum:`Kind <Function::Kind>`, to define expectations
of how a compute function produces outputs. There are 4 primary **kinds**: :ref:`Scalar
<reflabel-fnkind-scalar>`, :ref:`Vector <reflabel-fnkind-vector>`, :ref:`Aggregate
<reflabel-fnkind-aggregate>`, and :ref:`Meta <reflabel-fnkind-meta>`.

In the Arrow repo, compute functions are grouped in source files based on their *kind*,
such as **Add** (a scalar, convenience function) in `api_scalar.h`_. Compute kernel
implementations are also separated by kind, in addition to other aspects, such as a
compute kernel for **Add(<timestamp>, <date>)** (a scalar, arithmetic compute kernel) in
`scalar_arithmetic.cc`_.

.. _api_scalar.h:
   https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.h#L537-L545

.. _scalar_arithmetic.cc:
   https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/kernels/scalar_arithmetic.cc#L1972-L1982


.. _reflabel-fnkind-scalar:

Scalar
~~~~~~

An "element-wise" function that returns a value of the same shape as the arguments. A
scalar function can accept scalar or array values, but every argument has the same shape
and the return value also has the same shape. In other words, every input must have the
same cardinality and the output contains an element corresponding to an element of the
input. Some scalar functions allow for a mix of array and scalar inputs, but the scalar
input is treated as an array with the scalar value repeated.

**Categories of Scalar Functions**

* Arithmetic

* Comparisons

* Logical

* String

    * Predicates

    * Transforms

    * Trimming

    * Splitting

    * Extraction

* Containment Tests

* Structural Transforms

* Conversions


A simple way to determine if a function is scalar is to answer a couple questions:

* Do all inputs have the same (broadcasted) length?

* Does the Nth element in the output only depend on the Nth element of each input?


.. _reflabel-fnkind-vector:

Vector
~~~~~~

A function with array input and output whose behavior depends on combinations
of values at different locations in the input arrays, rather than the independent
computations on scalar values at the same location in input arrays.

**Categories of Vector Functions**

* Associative Transforms

* Selections

* Sorts and Partitions

* Structural Transforms


.. _reflabel-fnkind-aggregate:

Aggregates
~~~~~~~~~~

There are 2 kinds of aggregates we describe here: :cpp:enumerator:`scalar
<Function::Kind::SCALAR_AGGREGATE>` and :cpp:enumerator:`hash
<Function::Kind::HASH_AGGREGATE>`. **Scalar Aggregate.** A function that computes scalar
summary statistics from array input. **Hash Aggregate.** A function that computes grouped
summary statistics from array input and an array of group identifiers.


.. _reflabel-fnkind-meta:

Meta
~~~~

A function that dispatches to other functions and does not contain its own kernels.



Kernels
-------

Compute functions are associated with some number of :term:`compute kernel` instances. In
simple cases, an :term:`execution kernel` can be implemented as a :term:`standard
function` and does not need a paired :term:`init kernel`. In more complex cases, an
:term:`execution kernel` may be a member function of a class (standard function), delegate
work to helper functions, and store state that can be accessed across invocations of the
execution kernel (e.g. for ChunkedArrays). Such an execution kernel could be paired with
an :term:`init kernel` that initializes kernel state before the first invocation of the
execution kernel.

Compute functions (see :doc:`FunctionImpl and subclasses <../api/compute>`) are associated
with a set of :struct:`Kernels <Kernel>`. Each kernel is similar to a real function and
implements logic for the function for a specific argument signature.

Compute functions are associated with some number of :term:`compute kernel` instances. In
simple cases, a kernel is similar to a :term:`standard function` and implements logic for
the :term:`compute function` for a specific argument signature.

Kernels are simple ``structs`` containing only function pointers (the "methods" of the
kernel) and attribute flags. Each function kind corresponds to a :struct:`Kernel` with
methods representing each stage of the function's execution. For example,
:struct:`ScalarKernel` includes (optionally) :member:`ScalarKernel::init` to initialize
any state necessary for execution and :member:`ScalarKernel::exec` to perform the
computation.

Since many kernels are closely related in operation and differ only in their input types,
it's frequently useful to leverage c++'s powerful template system to efficiently generate
kernels methods. For example, the "add" compute function accepts all numeric types and its
kernel methods are instantiations of the same function template.


Kernels are simple structs containing only function pointers (the "methods" of the
kernel) and attribute flags. Each function kind corresponds to a :struct:`Kernel` with
methods representing each stage of the function's execution.


A :term:`compute function`, itself, is instantiated as a ...

For example,
:struct:`ScalarKernel` includes (optionally) :member:`ScalarKernel::init` to initialize
any state necessary for execution and :member:`ScalarKernel::exec` to perform the
computation.

In simple cases, an :term:`execution kernel` can be implemented as a :term:`standard
function` and does not need a paired :term:`init kernel`. In more complex cases, an
:term:`execution kernel` may be a member function of a class (standard function), delegate
work to helper functions, and store state that can be accessed across invocations of the
execution kernel (e.g. for ChunkedArrays). Such an execution kernel could be paired with
an :term:`init kernel` that initializes kernel state before the first invocation of the
execution kernel.

Function options
----------------

:struct:`FunctionOptions` provides a mechanism for changing behavior of a compute
function without having to have a different compute kernel for each behavior.


Function documentation
----------------------

:struct:`FunctionDoc` provides a mechanism for documenting a compute function
programmatically.



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


===========================
Authoring Compute Functions
===========================

Overview
========

This documentation aims to detail the process for writing a compute function and adding it
to the Apache Arrow library so that it can be made available to any project built atop
Arrow. For other detailed information about the compute layer, here are some other
resources that are distinguished by learning objective:

* :doc:`Compute Layer Reference <../compute>` -- A reference for the functions defined in the
  compute layer, available to any project built on top of Arrow. This reference contains a
  list of each function, their documented behavior, and information about their signatures
  and options.

* `Apache Arrow Cookbook`_ -- A cookbook recipe that provides a concise example for how to
  develop a new compute function. The example used in the recipe is the same as the
  example discussed in this documentation, which is intended to provide a more thorough
  discussion of the code organization and design.

.. _Apache Arrow Cookbook:
   https://arrow.apache.org/cookbook/cpp/

This documentation is organized in a somewhat "bottom-up" fashion, beginning with a
:ref:`glossary <reflabel-glossary>` and some :ref:`assumptions <reflabel-assums>`. Then,
we use a concrete example to discuss the code organization and design of the compute
layer in three main sections. The majority of code needed will be for implementing each
:term:`compute kernel` and any supporting code, discussed in :ref:`the kernel section
<reflabel-compute-kernel>`. Then, the :ref:`next section <reflabel-compute-function>`
shows how to define a :term:`compute function`, which is mostly logical, and how to
associate compute kernels with it. Then, shown in the :ref:`final section
<reflabel-compute-addfunction>`, the compute function can finally be added to a registry.

Compute Functions
=================

Terms
-----

When writing a new compute function, it is easiest to start with a brief listing of
important terminology used in the compute layer:

.. _reflabel-glossary:

.. glossary::

  standard function
    Used in this documentation to refer to a standard C++ function.

  compute function
    A logical representation of a function that is defined and invoked using Arrow's
    compute layer.

  compute kernel
    A specific implementation of a :term:`compute function`. Typically written as a
    standard function or a class, depending on the implementation complexity and whether
    the compute kernel is stateful.

  execution kernel
    A compute kernel that executes the processing, or execution, logic for a compute
    function.

  init kernel
    A compute kernel that initializes some state that will be used by the execution
    kernel.

  function registry
    A data structure, like a dictionary, where functions are listed.

  registered function
    A compute function that has been added to a function registry.

  registered kernel
    A compute kernel that has been associated with a compute function (as in
    :cpp:func:`ScalarFunction::AddKernel <ScalarFunction::AddKernel()>`).

  dispatch
    Selection of a specific :term:`registered kernel` when invoking a compute function.

  function arity
    The cardinality of input arguments for the compute function. A compute function that
    takes a single input argument is a **unary** compute function and its arity is one.

  function kind
    A category that describes how input arguments participate in the function result. If
    each element of a compute function argument is independently processed and the
    function result has the same number of elements, that compute function is a **scalar**
    compute function.

  function options
    A class whose attributes are used by compute kernels to change function behavior.


Intro
-----

.. _reflabel-assums:

There are a lot of things to consider when writing a compute function. Also, the compute
layer provides a lot of infrastructure, but the design is fairly complex. To make writing
a compute function a bit easier, here are some initial simplifying assumptions we can
establish:

* Compute functions accept whole arrays as input. If a scalar value is provided as input
  to a compute function, it should be wrapped in an array.

* Compute functions cannot modify input data in-place. Results should be written elsewhere
  and returned in an :struct:`ExecResult <arrow::compute::ExecResult>`.

.. I think this will be a good section to accumulate best practices, etc. but not sure if,
   or how, that should be explicitly mentioned.
   

.. _reflabel-compute-kernel:

Defining the Function Kernels
-----------------------------

Compute functions are associated with some number of :term:`compute kernel` instances. A
compute kernel is a function pointer, pointing to a standard function (either a class
method or a global function). Compute kernels come in two flavors, depending on their
purpose: A :term:`execution kernel` implements the main logic for a :term:`compute
function`. If the execution kernel is stateful, then it may have an associated :term:`init
kernel` that initializes its state. Stateful execution may be useful when invoking a
compute function on many columns of a table, on many batches of a table, or even many
times on the same input.

Principal attributes of an :term:`execution kernel` are:

* Returns :struct:`arrow::Status`

* Accepts:

  * a :struct:`KernelContext` pointer

  * an :struct:`ExecSpan` reference

  * an :struct:`ExecResult` pointer

**Return Status.** The Status returned by the kernel indicates the kernel's success or
failure. This provides an explicit mechanism for execution status without relying on the
interpretation of the function result.

**KernelContext.** The context wraps an :class:`arrow::compute::ExecContext` and other
metadata about the environment in which the kernel function should be executed.

**ExecSpan.** The input arguments are contained within an :struct:`ExecSpan` (newly added
in place of :struct:`ExecBatch`), which holds non-owning references to argument data.

**ExecResult.** The :class:`arrow::compute::ExecResult` pointer is an output argument,
pointing to a place to write function results. ExecResult holds *either* an
:struct:`ArraySpan` value or an :struct:`ArrayData` shared pointer. If the function
results are a view into data (they are not owned), then the results should be stored as an
ArraySpan. If the function results are newly allocated data (or ownership has been taken),
then the results should be stored as shared pointer to an ArrayData instance.

As an example, we implement a scalar execution kernel, `ScalarHash::Exec`, as a class
method. It does not access the input :struct:`KernelContext`. It validates the shape of
the input :struct:`ExecSpan`, then iterates over each element in the enclosed input array.
The function result is allocated using :func:`AllocateBuffer` and then moved into a new
:struct:`ArrayData` instance. Finally, a pointer to the ArrayData instance is assigned to
the :struct:`ExecResult` and an :cpp:enumerator:`OK` status is returned.

Since many kernels are closely related in operation and differ only in their input types,
it's frequently useful to leverage C++'s powerful template system to efficiently generate
kernels methods. For example, the "add" compute function accepts all numeric types and its
kernel methods are instantiations of the same function template.


.. _reflabel-compute-function:

Defining the Function
---------------------

Principal attributes of a compute :struct:`Function`:

* A :cpp:enum:`Kind <Function::Kind>` which describes how input values participate in
  computing output values. This must be known in order to instantiate the correct function
  object. For a :cpp:enumerator:`SCALAR <Function::Kind::SCALAR>` function, we create an
  instance of :struct:`ScalarFunction`.

* A unique :func:`name <arrow::compute::Function::name()>` used to identify the function
  from a registry for invocation. For a scalar hash function, we use the name
  "scalar_hash".

* An :struct:`Arity`, or cardinality of function arguments (how many arguments the
  function accepts). For a unary compute function, we use :cpp:func:`Unary
  <Arity::Unary()>`.

* A :struct:`FunctionDoc` which documents functionality and behavior.


**Code Organization.** Compute functions are organized into source files based on
:term:`function kind`. If a source file becomes large or complex, then it may be further
split. For a new, scalar hash function, we might want to create a new source file,
`scalar_hash.cc`_. This new file clearly indicates that it holds scalar functions that
calculate, or help calculate, hash values.

.. _scalar_hash.cc:
   https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/scalar_hash.cc

In addition to the source file containing the function and kernels, there are source files
that organize convenience functions, some :term:`function options`, and other higher-level
or shared types. For a convenience function, :func:`ScalarHash`, we add a standard
function to `api_scalar.h`_ and `api_scalar.cc`_. ScalarHash wraps the invocation of the
compute function from the default :term:`function registry` and provides an easy,
convenient method to call our compute function.

.. _api_scalar.h:
   https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.h

.. _api_scalar.cc:
   https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.cc


**Function Name.** Decide on a unique function name that best represents the function's
behavior. Longer, descriptive names are preferred because they help with clarity and make
it easier to add future variants of compute functions. Names can also be multiple words,
separated with an underscore.

For existing compute functions, see :ref:`available compute functions
<compute-function-list>`. Note that the names listed are the unique function name, not the
name of a convenience function. Typically, the name of a convenience function (if it
exists) should be the unique function name in pascal case (camel case with first letter
capitalized). For a compute function named "scalar_hash", we name the convenience function
"ScalarHash".


**Function Arity.** Decide how many arguments the compute function should accept. To some
extent this is less of a choice because it is inherent in the function behavior. For
example, :func:`Add` is naturally a :cpp:func:`Binary <Arity::Binary()>` function.
However, it is possible to write :func:`Add` as a :cpp:func:`Unary <Arity::Unary()>`
function that produces a sum of an input array (although, this would be better named as
`Sum` rather than `Add`). Another consideration is whether arguments could, or should, be
provided via :term:`function options` instead. Ultimately, there are a lot of approaches;
so, it is encouraged to choose one approach that feels right and to let discussion on a
pull request determine if a different approach should be used.


**Function Doc.** Each compute function has documentation which includes a summary,
description, and argument types of its operation. A :struct:`FunctionDoc` is individually
instantiated, then referenced when adding a :term:`compute function` to a :term`function
registry`.


* A number of arguments, each having an :struct:`InputType` and a **shape**:
  :struct:`Scalar`, :struct:`Array`, or :struct:`ChunkedArray`.

* A return value of :struct:`OutputType` and a **shape**: :struct:`Scalar`,
  :struct:`Array`, or :struct:`ChunkedArray`.


**Arity** types : :cpp:func:`Nullary
<Arity::Nullary()>`, :cpp:func:`Unary <Arity::Unary()>`, :cpp:func:`Binary
<Arity::Binary()>`, :cpp:func:`Ternary <Arity::Ternary()>`, and :cpp:func:`Variadic
<Arity::VarArgs()>`.

Compute functions can also be further categorized based on the type of operation
performed. For example, **Scalar Arithmetic** functions accept scalar, numeric arguments and
return a scalar, numeric value. Similarly, **Scalar String** functions accept scalar
arguments and return a scalar value; but, expects arguments to be strings and returns
a string value.

Compute functions (see :doc:`FunctionImpl and subclasses <../api/compute>`) are associated
with a set of :struct:`Kernels <Kernel>`. Each kernel is similar to a real function and
implements logic for the function for a specific argument signature.

Compute functions are associated with some number of :term:`compute kernel` instances. In
simple cases, a kernel is similar to a :term:`standard function` and implements logic for
the :term:`compute function` for a specific argument signature.

Optionally, compute functions may accept :struct:`FunctionOptions`, which provides a
mechanism to alter behavior of a function kernel instead of creating new functions or
kernels for each desirable behavior.


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

Function options
----------------

:struct:`FunctionOptions` provides a mechanism for changing behavior of a compute
function without having to have a different compute kernel for each behavior.




Code Organization
=================

This section describes the general structure and organization of the compute layer. We
use a hypothetical example to better illustrate what source files are likely to be
modified and the overall process for adding a new compute function.

For our example, we would like to add a new compute function, called "scalar_hash," which
takes a single array as input and produces a single array as output. This function will
calculate a hash value for each element in the input, which means the result will have the
same number of entries as the input (same "cardinality"). In other words, **scalar_hash**
will be a scalar, unary compute function.

Although **scalar_hash** does not yet exist, there is some code we can reuse to implement
a our compute kernels. Specifically, we want to use `ComputeHash`_ to calculate each hash
value.

.. _ComputeHash:
   https://github.com/apache/arrow/blob/master/cpp/src/arrow/util/hashing.h#L83-L102

Resources
---------

* arrow/util/hashing.h - defines utility functions

    * Function definitions suffixed with *WithOverflow* to support "safe math" for
      arithmetic kernels. Helper macros are included to create the definitions which
      invoke the corresponding operation in
      [`portable_snippets`](https://github.com/apache/arrow/blob/master/cpp/src/arrow/vendored/portable-snippets/safe-math.h)
      library.

* compute/api_scalar.h - contains

    * Subclasses of `FunctionOptions` for specific categories of compute functions

    * API/prototypes for all `Scalar` compute functions. Note that there is a single API
      version for each compute function.

* *compute/api_scalar.cc* - defines `Scalar` compute functions as wrappers over
  :func:`CallFunction` (one-shot function). Arrow provides macros to easily define compute
  functions based on their `arity` and invocation mode.

    * Macros of the form `SCALAR_EAGER_*` invoke `CallFunction` directly and only require
      one function name.

    * Macros of the form `SCALAR_*` invoke `CallFunction` and require two function names:
      default (behaves like `SCALAR_EAGER_*`) and a `_checked` variant (checks for
      overflow).

* compute/kernels/scalar_arithmetic.cc - contains kernel definitions for "Scalar
  Arithmetic" compute functions. Kernel definitions are defined via a class with literal
  name of compute function and containing methods named `Call` that are parameterized for
  specific input types (signed/unsigned integer and floating-point).

    * For compute functions that may trigger overflow the "checked" variant is a class
      suffixed with `Checked` and makes use of assertions and overflow checks. If overflow
      occurs, kernel returns zero and sets that `Status*` error flag.

        * For compute functions that do not have a valid mathematical operation for
          specific datatypes (e.g., negate an unsigned integer), the kernel for those
          types is provided but should trigger an error with `DCHECK(false) << This is
          included only for the purposes of instantiability from the "arithmetic kernel
          generator"` and return zero.


Kernel dispatcher
-----------------

* compute/exec.h

    * Defines variants of `CallFunction` which are the one-shot functions for invoking
      compute functions. A compute function should invoke `CallFunction` in its
      definition.

    * Defines `ExecContext` class

    * ScalarExecutor applies scalar function to batch

    * ExecBatchIterator::Make

* `DispatchBest`

* `FunctionRegistry` is the class representing a function registry. By default there is a
  single global registry where all kernels reside. `ExecContext` maintains a reference to
  the registry, if reference is NULL then the default registry is used.

* aggregate_basic.cc, aggregate_basic_internal.h - example of passing options to kernel

    * scalaraggregator


Portable snippets for safe (integer) math
-----------------------------------------

Arithmetic functions which can trigger integral overflow use the vendored library
`portable_snippets` to perform "safe math" operations (e.g., arithmetic, logical shifts,
casts).

Kernel implementations suffixed with `WithOverflow` need to be defined in
https://github.com/apache/arrow/blob/master/cpp/src/arrow/util/int_util_internal.h for
each primitive datatype supported. Use the helper macros of the form `*OPS_WITH_OVERFLOW`
to automatically generate the definitions. This file also contains helper functions for
performing safe integral arithmetic for the kernel's default variant.

The short-hand name maps to the predefined operation names in
https://github.com/apache/arrow/blob/master/cpp/src/arrow/vendored/portable-snippets/safe-math.h#L1028-L1033.
For example, `OPS_WITH_OVERFLOW(AddWithOverflow, add)` uses short-hand name `add`.


Adding a new compute function
=============================


Define kernels of compute function
----------------------------------

Define the kernel implementations in the corresponding source file based on the compute
function's "kind" and category. For example, a "Scalar" arithmetic function has kernels
defined in
https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/kernels/scalar_arithmetic.cc.


.. _reflabel-compute-addkernel:

Register kernels of compute function
------------------------------------

1. Before registering the kernels, check that the available kernel generators support the
   `arity` and data types allowed for the new compute function. Kernel generators are not
   of the same form for all the kernel `kinds`. For example, in the "Scalar Arithmetic"
   kernels, registration functions have names of the form `MakeArithmeticFunction` and
   `MakeArithmeticFunctionNotNull`. If not available, you will need to define them for
   your particular case.

1. Create the kernels by invoking the kernel generators.

1. Register the kernels in the corresponding registry along with its `FunctionDoc`.


.. _reflabel-compute-addfunction:

Register function in registry
-----------------------------

TODO

Testing
-------

Arrow uses Google test framework. All kernels should have tests to ensure stability of the
compute layer. Tests should at least cover ordinary inputs, corner cases, extreme values,
nulls, different data types, and invalid tests. Moreover, there can be kernel-specific
tests. For example, for arithmetic kernels, tests should include `NaN` and `Inf` inputs.
The test files are located alongside the kernel source files and suffixed with `_test`.
Tests are grouped by compute function `kind` and categories.

`TYPED_TEST(test suite name, compute function)` - wrapper to define tests for the given
compute function. The `test suite name` is associated with a set of data types that are
used for the test suite (`TYPED_TEST_SUITE`). Tests from multiple compute functions can be
placed in the same test suite. For example, `TYPED_TEST(TestBinaryArithmeticFloating,
Sub)` and `TYPED_TEST(TestBinaryArithmeticFloating, Mul)`.


Helpers
=======

* `MakeArray` - convert a `Datum` to an ...

* `ArrayFromJSON(type_id, format string)` -  `ArrayFromJSON(float32, "[1.3, 10.80, NaN,
  Inf, null]")`


Benchmarking
------------


Example of Unary Arithmetic Function: Absolute Value
====================================================

Identify the principal attributes.

1. Name

    * String literal: "absolute_value"

    * C++ function names: `AbsoluteValue`

1. Input/output types: Numerical (signed and unsigned, integral and floating-point)

1. Input/output shapes: operate on scalars or element-wise for arrays

1. Kind: Scalar

    * Category: Arithmetic

1. Arity: Unary


Define compute function
-----------------------

Add compute function's prototype to
https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.h

.. code-block:: cpp

  ARROW_EXPORT
  Result<Datum>
  AbsoluteValue(const Datum& arg
                ,ArithmeticOptions options = ArithmeticOptions()
                ,ExecContext* ctx = NULLPTR);

Add compute function's definition to
https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.cc
Recall that "Arithmetic" functions create two kernel variants: default and
overflow-checking. Therefore, we use the `SCALAR_ARITHMETIC_UNARY` macro which requires
two function names (with and without "_checked" suffix).

.. code-block:: cpp

  // TODO: omit this from this doc article
  SCALAR_ARITHMETIC_UNARY(AbsoluteValue, "absolute_value", "absolute_value_checked")


Define kernels of compute function
----------------------------------

The absolute value operation can overflow for signed integral inputs, so we need to define
"safe" functions using the `portable_snippets` library.

.. code-block:: cpp

  SIGNED_UNARY_OPS_WITH_OVERFLOW(AbsoluteValueWithOverflow, abs)


Given that this is a "Scalar Arithmetic" function, its kernels will be defined in
https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/kernels/scalar_arithmetic.cc.

.. code-block:: cpp

  struct AbsoluteValue {
    template <typename T, typename Arg>
    static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status*) {
      return (arg < static_cast<T>(0)) ? -arg : arg;
    }

    template <typename T, typename Arg>
    static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, Arg arg, Status*) {
      return arg;
    }

    template <typename T, typename Arg>
    static constexpr enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status* st) {
      return (arg < static_cast<T>(0)) ? arrow::internal::SafeSignedNegate(arg) : arg;
    }
  };

  struct AbsoluteValueChecked {
    template <typename T, typename Arg>
    static enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status* st) {
      static_assert(std::is_same<T, Arg>::value, "");
      if (arg < static_cast<T>(0)) {
          T result = 0;
          if (ARROW_PREDICT_FALSE(NegateWithOverflow(arg, &result))) {
            *st = Status::Invalid("overflow");
          }
          return result;
      }
      return arg;
    }

    template <typename T, typename Arg>
    static enable_if_unsigned_integer<T> Call(KernelContext* ctx, Arg arg, Status* st) {
      static_assert(std::is_same<T, Arg>::value, "");
      return arg;
    }

    template <typename T, typename Arg>
    static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status* st) {
      static_assert(std::is_same<T, Arg>::value, "");
      return (arg < static_cast<T>(0)) ? -arg : arg;
    }
  };


Create compute function documentation (`FunctionDoc` object)
------------------------------------------------------------

.. code-block:: cpp

  const FunctionDoc absolute_value_doc {
     "Calculate the absolute value of the argument element-wise"
    ,(
        "Results will wrap around on integer overflow.\n"
        "Use function 'absolute_value_checked' if you want overflow\n"
        "to return an error."
     )
    ,{"x"}
  };

  const FunctionDoc absolute_value_checked_doc {
     "Calculate the absolute value of the argument element-wise"
    ,(
        "This function returns an error on overflow.  For a variant that\n"
        "doesn't fail on overflow, use function 'absolute_value_checked'."
     )
    ,{"x"}
  };

Register kernels of compute function
------------------------------------

1. For the case of absolute value, the kernel generator
   `MakeUnaryArithmeticFunctionNotNull` was not available so it was added.


1. Create the kernels by invoking the kernel generators.

.. code-block:: cpp

  auto absolute_value = MakeUnaryArithmeticFunction<AbsoluteValue>(
    "absolute_value", &absolute_value_doc
  );

  auto absolute_value_checked = MakeUnaryArithmeticFunctionNotNull<AbsoluteValueChecked>(
    "absolute_value_checked", &absolute_value_checked_doc
  );


1. Register the kernels in the corresponding registry along with its `FunctionDoc`.


.. code-block:: cpp

  DCHECK_OK(registry->AddFunction(std::move(absolute_value)));
  DCHECK_OK(registry->AddFunction(std::move(absolute_value_checked)));


Example of Unary String Kernel: ASCII Reverse
=============================================

1. Name

    * String literal: "ascii_reverse"

    * C++ function names: `AsciiReverse`

1. Input/output types: String-like (Printable ASCII)

1. Input/output shapes: operate on scalars or element-wise for arrays

1. Kind: Scalar

    * Category: String predicate

1. Arity: Unary


Example of Binary Arithmetic Kernel: Hypotenuse of Right-angled Triangle
========================================================================

1. Name

    * String literal: "hypotenuse"

    * C++ function names: `Hypotenuse`

1. Input/output types: Numerical (signed and unsigned, integral and floating-point)

1. Input/output shapes: operate on scalars or element-wise for arrays

1. Kind: Scalar

    * Category: Arithmetic

1. Arity: Binary (length of each leg)


Define compute function
-----------------------

Add compute function's prototype to
https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.h

.. code-block:: cpp

  ARROW_EXPORT
  Result<Datum>
  Hypotenuse( const Datum& arg
             ,ArithmeticOptions options = ArithmeticOptions()
             ,ExecContext* ctx = NULLPTR);

Add compute function's definition to
https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.cc

Recall that "Arithmetic" functions create two kernel variants: default and
overflow-checking. Therefore, we use the `SCALAR_ARITHMETIC_BINARY` macro which requires
two function names (with and without "_checked" suffix).

.. code-block:: cpp

  // TODO: omit this from this doc article
  SCALAR_ARITHMETIC_BINARY(Hypotenuse, "hypotenuse", "hypotenuse_checked")


Q&A
===

1. How does one decides between "utility function" and "compute function"?
   https://lists.apache.org/thread.html/rf585cd4aed1f01a490702951b48b11124153b7e9e1dc477845129e81%40%3Cdev.arrow.apache.org%3E

2. If a related or variant form of a compute function is to be added in the future, is
   the current name extensible or specific enough to allow room for clear
   differentiation? For example, `str_length` is not a good name because there are
   different types of strings, so in this case it is preferable to be specific with
   `ascii_length` and `utf8_length`.

3. Identify the input/output types/shapes

    * What are the input types/shapes supported?

    * If multiple inputs are expected, are they the same type/shape?

4. Identify the compute function "kind" based on its operation and #2.

    * Does the codebase of the "kind" provides full support for the new compute function?

        * If not, is it straightforward to add the missing parts or can the new compute
          function be supported by another "kind"?

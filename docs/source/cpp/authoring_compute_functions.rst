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

Compute Functions
=================

An introduction to compute functions is provided in https://arrow.apache.org/docs/cpp/compute.html.

The [compute submodule](https://github.com/edponce/arrow/tree/master/cpp/src/arrow/compute) contains analytical functions that process primarily columnar data for either scalar or Arrow-based array inputs. These are intended for use inside query engines, data frame libraries, etc.

Many functions have SQL-like semantics in that they perform element-wise or scalar operations on whole arrays at a time. Other functions are not SQL-like and compute results that may be a different length or whose results depend on the order of the values.

Terminology:
* The term compute "function" refers to a particular general operation that may have many different implementations corresponding to different combinations of types or function behavior options.
* A specific implementation of a function is a "kernel". Selecting a viable kernel for executing a function is referred to as "dispatching". When executing a function on inputs, we must first select a suitable kernel corresponding to the value types of the inputs is selected.
* Functions along with their kernel implementations are collected in a "function registry". Given a function name and argument types, we can look up that function and dispatch to a compatible kernel.

[Compute functions](https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/function.h) have the following principal attributes:
* A unique ["name"](https://arrow.apache.org/docs/cpp/api/compute.html#_CPPv4NK5arrow7compute8Function4nameEv) used for function invocation and language bindings
* A ["kind"](https://arrow.apache.org/docs/cpp/api/compute.html#_CPPv4N5arrow7compute8Function4KindE)
  which indicates in what context it is valid for use
    * Input/output [types](https://arrow.apache.org/docs/cpp/compute.html#type-categories) and [shapes](https://arrow.apache.org/docs/cpp/compute.html#input-shapes)
    * Compute functions can also be further "categorized" based on the type of operation performed. For example, `Scalar Arithmetic` vs `Scalar String`.
* Compute functions (see [FunctionImpl and subclasses](https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/function.h)) contain ["kernels"](https://github.com/edponce/arrow/tree/master/cpp/src/arrow/compute/kernels) which are implementations for specific argument signatures.
* An ["arity"](https://arrow.apache.org/docs/cpp/api/compute.html#_CPPv4N5arrow7compute5ArityE) which states the number of required arguments
for its core operation. Functions are commonly nullary, unary, binary, or ternary, but can also be variadic.
* ["Documentation"](https://arrow.apache.org/docs/cpp/api/compute.html#_CPPv4N5arrow7compute11FunctionDocE) describing the function's functionality and behavior
* ["Options"](https://arrow.apache.org/docs/cpp/api/compute.html#_CPPv4N5arrow7compute15FunctionOptionsE) specifying configuration of the function's behavior.

Compute functions are grouped in source files based on their "kind" in https://github.com/apache/arrow/tree/master/cpp/src/arrow/compute.
Kernels of compute functions are grouped in source files based on their "kind" and category, see https://github.com/edponce/arrow/tree/master/cpp/src/arrow/compute/kernels.


Kinds of compute functions
--------------------------

Arrow uses an enumerated type to identify the kind of a compute function, refer to
https://github.com/edponce/arrow/tree/master/cpp/src/arrow/compute/function.h

Scalar
~~~~~~

A function that performs scalar data operations on whole arrays of
data. Can generally process Array or Scalar values. The size of the
output will be the same as the size (or broadcasted size, in the case
of mixing Array and Scalar inputs) of the input.

https://arrow.apache.org/docs/cpp/compute.html#arithmetic-functions

**Categories of Scalar functions**

* Arithmetic
* Comparisons
* Logical
* String
    * predicates
    * transforms
    * trimming
    * splitting
    * extraction
* Containment tests
* Structural transforms
* Conversions


Vector
~~~~~~

A function with array input and output whose behavior depends on the
values of the entire arrays passed, rather than the value of each scalar value.

**Categories of Vector functions**

* Associative transforms
* Selections
* Sorts and partitions
* Structural transforms


Scalar aggregate
~~~~~~~~~~~~~~~~

A function that computes scalar summary statistics from array input.

### Hash aggregate

A function that computes grouped summary statistics from array input
and an array of group identifiers.

Meta
~~~~

A function that dispatches to other functions and does not contain its own kernels.



Kernels
-------

Kernels are simple ``structs`` containing only function pointers (the "methods" of the kernel) and attribute flags. Each function kind corresponds to a class of Kernel with methods representing each stage of the function's execution. For example, :struct:`ScalarKernel` includes (optionally) :member:`ScalarKernel::init` to initialize any state necessary for execution and :member:`ScalarKernel::exec` to perform the computation.

Since many kernels are closely related in operation and differ only in their input types, it's frequently useful to leverage c++'s powerful template system to efficiently generate kernels' methods. For example, the "add" compute function accepts all numeric types and its kernels' methods are instantiations of the same function template.

Function options
----------------

[FunctionOptions](https://arrow.apache.org/docs/cpp/api/compute.html#_CPPv4N5arrow7compute15FunctionOptionsE)


Function documentation
----------------------

[FunctionDoc](https://arrow.apache.org/docs/cpp/api/compute.html#_CPPv4N5arrow7compute11FunctionDocE)


Files and structures of the computer layer
==========================================

This section describes the general structure of files/directory and principal code structures of the compute layer.

* arrow/util/int_util_internal.h - defines utility functions
    * Function definitions suffixed with `WithOverflow` to support "safe math" for arithmetic kernels. Helper macros are included to create the definitions which invoke the corresponding operation in [`portable_snippets`](https://github.com/apache/arrow/blob/master/cpp/src/arrow/vendored/portable-snippets/safe-math.h) library.

* compute/api_scalar.h - contains
    * Subclasses of `FunctionOptions` for specific categories of compute functions
    * API/prototypes for all `Scalar` compute functions. Note that there is a single API version for each compute function.
* *compute/api_scalar.cc* - defines `Scalar` compute functions as wrappers over ["CallFunction"](https://arrow.apache.org/docs/cpp/api/compute.html?highlight=one%20shot#_CPPv412CallFunctionRKNSt6stringERKNSt6vectorI5DatumEEPK15FunctionOptionsP11ExecContext) (one-shot function). Arrow provides macros to easily define compute functions based on their `arity` and invocation mode.
    * Macros of the form `SCALAR_EAGER_*` invoke `CallFunction` directly and only require one function name.
    * Macros of the form `SCALAR_*` invoke `CallFunction` after checking for overflow and require two function names (default and `_checked` variant).

* compute/kernels/scalar_arithmetic.cc - contains kernel definitions for "Scalar Arithmetic" compute functions. Kernel definitions are defined via a class with literal name of compute function and containing methods named `Call` that are parameterized for specific input types (signed/unsigned integer and floating-point).
    * For compute functions that may trigger overflow the "checked" variant is a class suffixed with `Checked` and makes use of assertions and overflow checks. If overflow occurs, kernel returns zero and sets that `Status*` error flag.
        * For compute functions that do not have a valid mathematical operation for specific datatypes (e.g., negate an unsigned integer), the kernel for those types is provided but should trigger an error with `DCHECK(false) << This is included only for the purposes of instantiability from the "arithmetic kernel generator"` and return zero.


Kernel dispatcher
-----------------

* compute/exec.h
    * Defines variants of `CallFunction` which are the one-shot functions for invoking compute functions. A compute function should invoke `CallFunction` in its definition.
    * Defines `ExecContext` class
    * ScalarExecutor applies scalar function to batch
    * ExecBatchIterator::Make

* `DispatchBest`

* `FunctionRegistry` is the class representing a function registry. By default there is a single global registry where all kernels reside. `ExecContext` maintains a reference to the registry, if reference is NULL then the default registry is used.

* aggregate_basic.cc, aggregate_basic_internal.h - example of passing options to kernel
    * scalaraggregator


Portable snippets for safe (integer) math
-----------------------------------------

Arithmetic functions which can trigger integral overflow use the vendored library `portable_snippets` to perform "safe math" operations (e.g., arithmetic, logical shifts, casts).
Kernel implementations suffixed with `WithOverflow` need to be defined in https://github.com/apache/arrow/blob/master/cpp/src/arrow/util/int_util_internal.h for each primitive datatype supported. Use the helper macros of the form `*OPS_WITH_OVERFLOW` to automatically generate the definitions. This file also contains helper functions for performing safe integral arithmetic for the kernels' default variant.

The short-hand name maps to the predefined operation names in https://github.com/apache/arrow/blob/master/cpp/src/arrow/vendored/portable-snippets/safe-math.h#L1028-L1033. For example, `OPS_WITH_OVERFLOW(AddWithOverflow, add)` uses short-hand name `add`.


Adding a new compute function
=============================

This section describes the process for adding a new compute function and associated kernel implementations.

First, you should identify the principal attributes of the new compute function.
The following series of steps help guide the design process.

1. Decide on a unique name that fully represents the function's operation

   Browse the [available compute functions](https://arrow.apache.org/docs/cpp/compute.html#available-functions) to prevent a name collision. Note that the long form of names is preferred, and multi-word names are allowed due to the fact that string versions use an underscore instead of whitespace and C++ function names use camel case convention.
     * What is a representative and unambiguous name for the operation performed by the compute function?
     * If a related or variant form of a compute function is to be added in the future, is the current name extensible or specific enough to allow room for clear differentiation? For example, `str_length` is not a good name because there are different types of strings, so in this case it is preferable to be specific with `ascii_length` and `utf8_length`.

1. Identify the input/output types/shapes
    * What are the input types/shapes supported?
    * If multiple inputs are expected, are they the same type/shape?

1. Identify the compute function "kind" based on its operation and #2.
    * Does the codebase of the "kind" provides full support for the new compute function?
        * If not, is it straightforward to add the missing parts or can the new compute function be supported by another "kind"?


Define compute function
-----------------------

Add the compute function prototype and definition to the corresponding source files based on its "kind". For example the API of a "Scalar" function is found in https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.h and its definition in https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.cc.



Define kernels of compute function
----------------------------------

Define the kernel implementations in the corresponding source file based on the compute function's "kind" and category. For example, a "Scalar" arithmetic function has kernels defined in https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/kernels/scalar_arithmetic.cc.

Create compute function documentation (`FunctionDoc` object)
------------------------------------------------------------

Each compute function has documentation which includes a summary, description, and argument types of its operation. A `FunctionDoc` object is instantiated and used in the registration step. Note that for compute functions that can overflow, another `FunctionDoc` is required for the `_checked` variant.

Register kernels of compute function
------------------------------------

1. Before registering the kernels, check that the available kernel generators support the `arity` and data types allowed for the new compute function. Kernel generators are not of the same form for all the kernel `kinds`. For example, in the "Scalar Arithmetic" kernels, registration functions have names of the form `MakeArithmeticFunction` and `MakeArithmeticFunctionNotNull`. If not available, you will need to define them for your particular case.

1. Create the kernels by invoking the kernel generators.

1. Register the kernels in the corresponding registry along with its `FunctionDoc`.


Testing
-------

Arrow uses Google test framework. All kernels should have tests to ensure stability of the compute layer. Tests should at least cover ordinary inputs, corner cases, extreme values, nulls, different data types, and invalid tests. Moreover, there can be kernel-specific tests. For example, for arithmetic kernels, tests should include `NaN` and `Inf` inputs. The test files are located alongside the kernel source files and suffixed with `_test`. Tests are grouped by compute function `kind` and categories.

`TYPED_TEST(test suite name, compute function)` - wrapper to define tests for the given compute function. The `test suite name` is associated with a set of data types that are used for the test suite (`TYPED_TEST_SUITE`). Tests from multiple compute functions can be placed in the same test suite. For example, `TYPED_TEST(TestBinaryArithmeticFloating, Sub)` and `TYPED_TEST(TestBinaryArithmeticFloating, Mul)`.

Helpers
=======

* `MakeArray` - convert a `Datum` to an ...
* `ArrayFromJSON(type_id, format string)` -  `ArrayFromJSON(float32, "[1.3, 10.80, NaN, Inf, null]")`


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

Add compute function's prototype to https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.h

```C++
ARROW_EXPORT
Result<Datum> AbsoluteValue(const Datum& arg, ArithmeticOptions options = ArithmeticOptions(), ExecContext* ctx = NULLPTR);
```

Add compute function's definition to https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.cc
Recall that "Arithmetic" functions create two kernel variants: default and overflow-checking. Therefore, we use the `SCALAR_ARITHMETIC_UNARY` macro which requires two function names (with and without "_checked" suffix).

```C++
SCALAR_ARITHMETIC_UNARY(AbsoluteValue, "absolute_value", "absolute_value_checked")
```

Define kernels of compute function
----------------------------------

The absolute value operation can overflow for signed integral inputs, so we need to define "safe" functions using the `portable_snippets` library.

```C++
SIGNED_UNARY_OPS_WITH_OVERFLOW(AbsoluteValueWithOverflow, abs)
```

Given that this is a "Scalar Arithmetic" function, its kernels will be defined in https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/kernels/scalar_arithmetic.cc.

```C++
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
```

Create compute function documentation (`FunctionDoc` object)
------------------------------------------------------------

```C++
const FunctionDoc absolute_value_doc{"Calculate the absolute value of the argument element-wise",
                             ("Results will wrap around on integer overflow.\n"
                              "Use function \"absolute_value_checked\" if you want overflow\n"
                              "to return an error."),
                             {"x"}};

const FunctionDoc absolute_value_checked_doc{
    "Calculate the absolute value of the argument element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"absolute_value_checked\"."),
    {"x"}};
```

Register kernels of compute function
------------------------------------

1. For the case of absolute value, the kernel generator `MakeUnaryArithmeticFunctionNotNull` was not available so it was added.


1. Create the kernels by invoking the kernel generators.
```C++
  auto absolute_value = MakeUnaryArithmeticFunction<AbsoluteValue>("absolute_value", &absolute_value_doc);
  auto absolute_value_checked = MakeUnaryArithmeticFunctionNotNull<AbsoluteValueChecked>("absolute_value_checked", &absolute_value_checked_doc);
```

1. Register the kernels in the corresponding registry along with its `FunctionDoc`.
```C++
  DCHECK_OK(registry->AddFunction(std::move(absolute_value)));
  DCHECK_OK(registry->AddFunction(std::move(absolute_value_checked)));
```

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

Add compute function's prototype to https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.h

```C++
ARROW_EXPORT
Result<Datum> Hypotenuse(const Datum& arg, ArithmeticOptions options = ArithmeticOptions(), ExecContext* ctx = NULLPTR);
```

Add compute function's definition to https://github.com/apache/arrow/blob/master/cpp/src/arrow/compute/api_scalar.cc
Recall that "Arithmetic" functions create two kernel variants: default and overflow-checking. Therefore, we use the `SCALAR_ARITHMETIC_BINARY` macro which requires two function names (with and without "_checked" suffix).

```C++
SCALAR_ARITHMETIC_BINARY(Hypotenuse, "hypotenuse", "hypotenuse_checked")
```

Q&A
===

1. How does one decides between "utility function" and "compute function"?
https://lists.apache.org/thread.html/rf585cd4aed1f01a490702951b48b11124153b7e9e1dc477845129e81%40%3Cdev.arrow.apache.org%3E

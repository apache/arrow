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

.. cpp:namespace:: arrow

Conventions
===========

The Arrow C++ API follows a few simple guidelines.  As with many rules,
there may be exceptions.

Language version
----------------

Arrow is C++11-compatible.  A few backports are used for newer functionality,
for example the :class:`std::string_view` class.

Namespacing
-----------

All the Arrow API (except macros) is namespaced inside a ``arrow`` namespace,
and nested namespaces thereof.

Safe pointers
-------------

Arrow objects are usually passed and stored using safe pointers -- most of
the time :class:`std::shared_ptr` but sometimes also :class:`std::unique_ptr`.

Immutability
------------

Many Arrow objects are immutable: once constructed, their logical properties
cannot change anymore.  This makes it possible to use them in multi-threaded
scenarios without requiring tedious and error-prone synchronization.

There are obvious exceptions to this, such as IO objects or mutable data buffers.

Error reporting
---------------

Most APIs indicate a successful or erroneous outcome by returning a
:class:`arrow::Status` instance.  Arrow doesn't throw exceptions of its
own, but third-party exceptions might propagate through, especially
:class:`std::bad_alloc` (but Arrow doesn't use the standard allocators for
large data).

When an API can return either an error code or a successful value, it usually
does so by returning the template class
:class:`arrow::Result <template\<class T\> arrow::Result>`.  However,
some APIs (usually deprecated) return :class:`arrow::Status` and pass the
result value as an out-pointer parameter.

Here is an example of checking the outcome of an operation::

   const int64_t buffer_size = 4096;

   auto maybe_buffer = arrow::AllocateBuffer(buffer_size, &buffer);
   if (!maybe_buffer.ok()) {
      // ... handle error
   } else {
      std::shared_ptr<arrow::Buffer> buffer = *maybe_buffer;
      // ... use allocated buffer
   }

If the caller function itself returns a :class:`arrow::Result` or
:class:`arrow::Status` and wants to propagate any non-successful outcome, two
convenience macros are available:

* :c:macro:`ARROW_RETURN_NOT_OK` takes a :class:`arrow::Status` parameter
  and returns it if not successful.

* :c:macro:`ARROW_ASSIGN_OR_RAISE` takes a :class:`arrow::Result` parameter,
  assigns its result to a *lvalue* if successful, or returns the corresponding
  :class:`arrow::Status` on error.

For example::

   arrow::Status DoSomething() {
      const int64_t buffer_size = 4096;
      std::shared_ptr<arrow::Buffer> buffer;
      ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateBuffer(buffer_size));
      // ... allocation successful, do something with buffer below

      // return success at the end
      return Status::OK();
   }

.. seealso::
   :doc:`API reference for error reporting <api/support>`


.. TODO: Maybe move below to Datatypes.rst?

Type Traits
-----------

Writing code that can handle all Arrow types would be verbose, if it weren't for
type traits. They are empty structs with type declarations that map the Arrow 
DataTypes to the specialized array, scalar, builder, and other associated types. 
For example, boolean type has traits:

.. TODO: Should code blocks go in a place where they are executed?

.. code-block:: cpp

   template <>
   struct TypeTraits<BooleanType> {
     using ArrayType = BooleanArray;
     using BuilderType = BooleanBuilder;
     using ScalarType = BooleanScalar;
     using CType = bool;

     static constexpr int64_t bytes_required(int64_t elements) {
       return bit_util::BytesForBits(elements);
     }
     constexpr static bool is_parameter_free = true;
     static inline std::shared_ptr<DataType> type_singleton() { return boolean(); }
   };

See the :ref:`type-traits` for an explanation of each of these fields.

Using type traits, one can write template functions that can handle a variety
of Arrow types. For example, to write a function that creates an array of nulls
for any type:

.. code-block:: cpp

   template<typename DataType, typename BuilderType=DataType::BuilderType, typename ArrayType=DataType::ArrayType>
   ArrayType make_nulls(int32_t n) {
      BuilderType builder = BuilderType::Make();
      for (int32_t i = 0; i < n; ++i) {
         builder.AppendNull();
      }
      ArrayType out;
      builder.Finish(&out);
      return out;
   }

For some common cases, there are type associations on the classes themselves. Use:

* ``Scalar::TypeClass`` to get data type class of a scalar
* ``Array::TypeClass`` to get data type class of an array
* ``DataType::c_type`` to get associated C type of an Arrow data type

There are also template type definitions for constraining template functions to a 
subset of Arrow types. For example, to write a sum function for any numeric
(integer or float) array:

.. code-block:: cpp

   template <typename ArrayType, typename DataType = typename ArrayType::TypeClass>
   arrow::enable_if_number<DataType, DataType::c_type> Visit(const ArrayType& array) {
     DataType::c_type sum = 0;
     for (arrow::util::optional<typename DataType::c_type> value : array) {
       if (value.has_value()) {
         sum += value.value();
       }
     }
     return sum;
   }

See :ref:`type-predicates-api` for a list of these.


:ref:`building them yourself <type-predicates-api>`.
.. TODO: test these snippets

Visitor Pattern
---------------

Several types, including :class:`arrow::DataType`, :class:`arrow::Scalar`, and
:class:`arrow::Array`, have sub-types specialized to their corresponding Arrow
types. For example, for the Arrow boolean type there is :class:`arrow::BooleanType`,
:class:`arrow::BooleanScalar`, and :class:`arrow::BooleanArray`. In order to 
process these entities, you may need to write write logic that specializes based 
on the particular Arrow type. In these cases, use the
`visitor pattern <https://en.wikipedia.org/wiki/Visitor_pattern>`_.

Arrow provides abstract visitor classes (:class:`arrow::TypeVisitor`,
:class:`arrow::ScalarVisitor`, :class:`arrow::ArrayVisitor`) and an ``Accept()``
method on each of the corresponding base types (e.g. :func:`arrow::Array::Accept`).

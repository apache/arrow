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

Data Types
==========

.. seealso::
   :doc:`Datatype API reference <api/datatype>`.

Data types govern how physical data is interpreted.  Their :ref:`specification
<format_columnar>` allows binary interoperability between different Arrow
implementations, including from different programming languages and runtimes
(for example it is possible to access the same data, without copying, from
both Python and Java using the :py:mod:`pyarrow.jvm` bridge module).

Information about a data type in C++ can be represented in three ways:

1. Using a :class:`arrow::DataType` instance (e.g. as a function argument)
2. Using a :class:`arrow::DataType` concrete subclass (e.g. as a template
   parameter)
3. Using a :type:`arrow::Type::type` enum value (e.g. as the condition of
   a switch statement)

The first form (using a :class:`arrow::DataType` instance) is the most idiomatic
and flexible.  Runtime-parametric types can only be fully represented with
a DataType instance.  For example, a :class:`arrow::TimestampType` needs to be
constructed at runtime with a :type:`arrow::TimeUnit::type` parameter; a
:class:`arrow::Decimal128Type` with *scale* and *precision* parameters;
a :class:`arrow::ListType` with a full child type (itself a
:class:`arrow::DataType` instance).

The two other forms can be used where performance is critical, in order to
avoid paying the price of dynamic typing and polymorphism.  However, some
amount of runtime switching can still be required for parametric types.
It is not possible to reify all possible types at compile time, since Arrow
data types allows arbitrary nesting.

Creating data types
-------------------

To instantiate data types, it is recommended to call the provided
:ref:`factory functions <api-type-factories>`::

   std::shared_ptr<arrow::DataType> type;

   // A 16-bit integer type
   type = arrow::int16();
   // A 64-bit timestamp type (with microsecond granularity)
   type = arrow::timestamp(arrow::TimeUnit::MICRO);
   // A list type of single-precision floating-point values
   type = arrow::list(arrow::float32());



Type Traits
-----------

Writing code that can handle concrete :class:`arrow::DataType` subclasses would 
be verbose, if it weren't for type traits. Arrow's type traits map the Arrow 
data types to the specialized array, scalar, builder, and other associated types.
For example, the Boolean type has traits:

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
of Arrow types. For example, to write a function that creates an array of 
Fibonacci values for any Arrow numeric type:

.. code-block:: cpp

   template <typename DataType,
             typename BuilderType = typename arrow::TypeTraits<DataType>::BuilderType,
             typename ArrayType = typename arrow::TypeTraits<DataType>::ArrayType,
             typename CType = typename arrow::TypeTraits<DataType>::CType>
   arrow::Result<std::shared_ptr<ArrayType>> MakeFibonacci(int32_t n) {
     BuilderType builder;
     CType val = 0;
     CType next_val = 1;
     for (int32_t i = 0; i < n; ++i) {
       builder.Append(val);
       CType temp = val + next_val;
       val = next_val;
       next_val = temp;
     }
     std::shared_ptr<ArrayType> out;
     ARROW_RETURN_NOT_OK(builder.Finish(&out));
     return out;
   }

For some common cases, there are type associations on the classes themselves. Use:

* ``Scalar::TypeClass`` to get data type class of a scalar
* ``Array::TypeClass`` to get data type class of an array
* ``DataType::c_type`` to get associated C type of an Arrow data type

Similar to the type traits provided in
`std::type_traits <https://en.cppreference.com/w/cpp/header/type_traits>`_,
Arrow provides type predicates such as ``is_number_type`` as well as 
corresponding templates that wrap ``std::enable_if_t`` such as ``enable_if_number``.
These can constrain template functions to only compile for relevant types, which
is useful if other overloads need to be implemented. For example, to write a sum
function for any numeric (integer or float) array:

.. code-block:: cpp

   template <typename ArrayType, typename DataType = typename ArrayType::TypeClass,
             typename CType = typename DataType::c_type>
   arrow::enable_if_number<DataType, CType> SumArray(const ArrayType& array) {
     CType sum = 0;
     for (arrow::util::optional<CType> value : array) {
       if (value.has_value()) {
         sum += value.value();
       }
     }
     return sum;
   }

See :ref:`type-predicates-api` for a list of these.


Visitor Pattern
---------------

In order to process :class:`arrow::DataType`, :class:`arrow::Scalar`, or
:class:`arrow::Array`, you may need to write write logic that specializes based 
on the particular Arrow type. In these cases, use the
`visitor pattern <https://en.wikipedia.org/wiki/Visitor_pattern>`_. Arrow provides
the template functions:

* :func:`arrow::VisitTypeInline`
* :func:`arrow::VisitScalarInline`
* :func:`arrow::VisitArrayInline`

To use these, implement ``Status Visit()`` methods for each specialized type, then
pass the class instance to the inline visit function. To avoid repetitive code,
use type traits as documented in the previous section. As a brief example,
here is how one might sum across columns of arbitrary numeric types:

.. code-block:: cpp

   class TableSummation {
     double partial = 0.0;
    public:
   
     arrow::Result<double> Compute(std::shared_ptr<arrow::RecordBatch> batch) {
       for (std::shared_ptr<arrow::Array> array : batch->columns()) {
         ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array, this));
       }
       return partial;
     }
   
     // Default implementation
     arrow::Status Visit(const arrow::Array& array) {
       return arrow::Status::NotImplemented("Can not compute sum for array of type ",
                                            array.type()->ToString());
     }
   
     template <typename ArrayType, typename T = typename ArrayType::TypeClass>
     arrow::enable_if_number<T, arrow::Status> Visit(const ArrayType& array) {
       for (arrow::util::optional<typename T::c_type> value : array) {
         if (value.has_value()) {
           partial += static_cast<double>(value.value());
         }
       }
       return arrow::Status::OK();
     }
   };

Arrow also provides abstract visitor classes (:class:`arrow::TypeVisitor`,
:class:`arrow::ScalarVisitor`, :class:`arrow::ArrayVisitor`) and an ``Accept()``
method on each of the corresponding base types (e.g. :func:`arrow::Array::Accept`).
However, these are not able to be implemented using template functions, so you
will typically prefer using the inline type visitors.
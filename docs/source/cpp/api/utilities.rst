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

=========
Utilities
=========

Decimal Numbers
===============

.. doxygenclass:: arrow::BasicDecimal128
   :members:

.. doxygenclass:: arrow::Decimal128
   :members:

.. doxygenclass:: arrow::BasicDecimal256
   :members:

.. doxygenclass:: arrow::Decimal256
   :members:

Iterators
=========

.. doxygenclass:: arrow::Iterator
   :members:

.. doxygenclass:: arrow::VectorIterator
   :members:

Compression
===========

.. doxygenenum:: arrow::Compression::type

.. doxygenclass:: arrow::util::Codec
   :members:

.. doxygenclass:: arrow::util::Compressor
   :members:

.. doxygenclass:: arrow::util::Decompressor
   :members:

Visitors
========

.. doxygenfunction:: arrow::VisitTypeInline
   :project: arrow_cpp

.. doxygenfunction:: arrow::VisitTypeIdInline
   :project: arrow_cpp

.. doxygenfunction:: arrow::VisitScalarInline
   :project: arrow_cpp

.. doxygenfunction:: arrow::VisitArrayInline
   :project: arrow_cpp


.. _type-traits:

Type Traits
===========

These types provide relationships between Arrow types at compile time. :cpp:type:`TypeTraits`
maps Arrow DataTypes to other types, and :cpp:type:`CTypeTraits ` maps C types to
Arrow types.

TypeTraits
----------

Each specialized type defines the following associated types:

.. cpp:type:: TypeTraits::ArrayType

  Corresponding :doc:`Arrow array type </cpp/api/array.rst>`

.. cpp:type:: TypeTraits::BuilderType

  Corresponding :doc:`array builder type </cpp/api/builders.rst>`

.. cpp:type:: TypeTraits::ScalarType

  Corresponding :doc:`Arrow scalar type </cpp/api/scalar.rst>`

.. cpp:var:: TypeTraits::is_parameter_free

  Whether the type has any type parameters, such as field types in nested types
  or scale and precision in decimal types.


In addition, the following are defined for many but not all of the types:

.. cpp:type:: TypeTraits::CType

  Corresponding C type. For example, ``int64_t`` for ``Int64Array``.

.. cpp:type:: TypeTraits::TensorType

  Corresponding :doc:`Arrow tensor type </cpp/api/tensor.rst>`

.. cpp:function:: static inline constexpr int64_t bytes_required(int64_t elements)

  Return the number of bytes required for given number of elements. Defined for 
  types with a fixed size.

.. cpp:function:: static inline std::shared_ptr<DataType> TypeTraits::type_singleton()

  For types where is_parameter_free is true, returns an instance of the data type.


.. doxygengroup:: type-traits
   :content-only:
   :members:
   :undoc-members:

CTypeTraits
-----------

Each specialized type defines the following associated types:

.. cpp:type:: CTypeTraits::ArrowType

  Corresponding :doc:`Arrow type </cpp/api/datatype.rst>`

.. doxygengroup:: c-type-traits
   :content-only:
   :members:
   :undoc-members:


.. _type-predicates-api:

Type Predicates
---------------

Type predicates that can be used with templates. Predicates of the form ``is_XXX`` 
resolve to constant boolean values, while predicates of the form ``enable_if_XXX`` 
resolve to the second type parameter ``R`` if the first parameter ``T`` passes 
the test.

Example usage:

.. code-block:: cpp

  template<typename TypeClass>
  arrow::enable_if_number<TypeClass, RETURN_TYPE> MyFunction(const TypeClass& type) {
    ..
  }

  template<typename ArrayType, typename TypeClass=ArrayType::TypeClass>
  arrow::enable_if_number<TypeClass, RETURN_TYPE> MyFunction(const ArrayType& array) {
    ..
  }


.. doxygengroup:: type-predicates
   :content-only:
   :members:
   :undoc-members:


Runtime Type Predicates
-----------------------

Type predicates that can be applied at runtime.

.. doxygengroup:: runtime-type-predicates
   :content-only:
   :members:
   :undoc-members:
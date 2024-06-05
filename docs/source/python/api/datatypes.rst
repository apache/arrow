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

.. _api.types:
.. currentmodule:: pyarrow

Data Types and Schemas
======================

Factory Functions
-----------------

These should be used to create Arrow data types and schemas.

.. autosummary::
   :toctree: ../generated/

   null
   bool_
   int8
   int16
   int32
   int64
   uint8
   uint16
   uint32
   uint64
   float16
   float32
   float64
   time32
   time64
   timestamp
   date32
   date64
   duration
   month_day_nano_interval
   binary
   string
   utf8
   large_binary
   large_string
   large_utf8
   binary_view
   string_view
   decimal128
   list_
   large_list
   list_view
   large_list_view
   map_
   struct
   dictionary
   run_end_encoded
   field
   schema
   from_numpy_dtype

Utility Functions
-----------------

.. autosummary::
   :toctree: ../generated/

   unify_schemas

.. _api.type_classes:
.. currentmodule:: pyarrow

Type Classes
------------

Do not instantiate these classes directly.  Instead, call one of the factory
functions above.

.. autosummary::
   :toctree: ../generated/

   DataType
   DictionaryType
   ListType
   MapType
   StructType
   UnionType
   TimestampType
   Time32Type
   Time64Type
   FixedSizeBinaryType
   Decimal128Type
   Field
   Schema
   RunEndEncodedType

Specific classes and functions for extension types.

.. autosummary::
   :toctree: ../generated/

   ExtensionType
   PyExtensionType
   register_extension_type
   unregister_extension_type


.. _api.types.checking:
.. currentmodule:: pyarrow.types

Type Checking
-------------

These functions are predicates to check whether a :class:`DataType` instance
represents a given data type (such as ``int32``) or general category
(such as "is a signed integer").

.. autosummary::
   :toctree: ../generated/

   is_boolean
   is_integer
   is_signed_integer
   is_unsigned_integer
   is_int8
   is_int16
   is_int32
   is_int64
   is_uint8
   is_uint16
   is_uint32
   is_uint64
   is_floating
   is_float16
   is_float32
   is_float64
   is_decimal
   is_decimal128
   is_decimal256
   is_list
   is_large_list
   is_fixed_size_list
   is_list_view
   is_large_list_view
   is_struct
   is_union
   is_nested
   is_run_end_encoded
   is_temporal
   is_timestamp
   is_date
   is_date32
   is_date64
   is_time
   is_time32
   is_time64
   is_duration
   is_interval
   is_null
   is_binary
   is_unicode
   is_string
   is_large_binary
   is_large_unicode
   is_large_string
   is_binary_view
   is_string_view
   is_fixed_size_binary
   is_map
   is_dictionary
   is_primitive

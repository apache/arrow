// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_STL_H
#define ARROW_STL_H

#include <string>
#include <tuple>
#include <vector>

#include "arrow/type.h"

namespace arrow {

class Schema;

namespace stl {

/// Traits meta class to map standard C/C++ types to equivalent Arrow types.
template <typename T>
struct ConversionTraits {};

#define ARROW_STL_CONVERSION(c_type, ArrowType_) \
  template <>                                    \
  struct ConversionTraits<c_type> {              \
    using ArrowType = ArrowType_;                \
    constexpr static bool nullable = false;      \
  };

ARROW_STL_CONVERSION(bool, BooleanType)
ARROW_STL_CONVERSION(int8_t, Int8Type)
ARROW_STL_CONVERSION(int16_t, Int16Type)
ARROW_STL_CONVERSION(int32_t, Int32Type)
ARROW_STL_CONVERSION(int64_t, Int64Type)
ARROW_STL_CONVERSION(uint8_t, UInt8Type)
ARROW_STL_CONVERSION(uint16_t, UInt16Type)
ARROW_STL_CONVERSION(uint32_t, UInt32Type)
ARROW_STL_CONVERSION(uint64_t, UInt64Type)
ARROW_STL_CONVERSION(float, FloatType)
ARROW_STL_CONVERSION(double, DoubleType)
ARROW_STL_CONVERSION(std::string, StringType)

template <typename value_c_type>
struct ConversionTraits<std::vector<value_c_type>> {
  using ArrowType = meta::ListType<typename ConversionTraits<value_c_type>::ArrowType>;
  constexpr static bool nullable = false;
};

/// Build an arrow::Schema based upon the types defined in a std::tuple-like structure.
///
/// While the type information is available at compile-time, we still need to add the
/// column names at runtime, thus these methods are not constexpr.
template <typename Tuple, std::size_t N = std::tuple_size<Tuple>::value>
struct SchemaFromTuple {
  using Element = typename std::tuple_element<N - 1, Tuple>::type;
  using ArrowType = typename ConversionTraits<Element>::ArrowType;

  // Implementations that take a vector-like object for the column names.

  /// Recursively build a vector of arrow::Field from the defined types.
  ///
  /// In most cases MakeSchema is the better entrypoint for the Schema creation.
  static std::vector<std::shared_ptr<Field>> MakeSchemaRecursion(
      const std::vector<std::string>& names) {
    std::vector<std::shared_ptr<Field>> ret =
        SchemaFromTuple<Tuple, N - 1>::MakeSchemaRecursion(names);
    ret.push_back(field(names[N - 1], std::make_shared<ArrowType>(),
                        ConversionTraits<Element>::nullable));
    return ret;
  }

  /// Build a Schema from the types of the tuple-like structure passed in as template
  /// parameter assign the column names at runtime.
  ///
  /// An example usage of this API can look like the following:
  ///
  /// \code{.cpp}
  /// using TupleType = std::tuple<int, std::vector<std::string>>;
  /// std::shared_ptr<Schema> schema =
  ///   SchemaFromTuple<TupleType>::MakeSchema({"int_column", "list_of_strings_column"});
  /// \endcode
  static std::shared_ptr<Schema> MakeSchema(const std::vector<std::string>& names) {
    return std::make_shared<Schema>(MakeSchemaRecursion(names));
  }

  // Implementations that take a tuple-like object for the column names.

  /// Recursively build a vector of arrow::Field from the defined types.
  ///
  /// In most cases MakeSchema is the better entrypoint for the Schema creation.
  template <typename NamesTuple>
  static std::vector<std::shared_ptr<Field>> MakeSchemaRecursionT(
      const NamesTuple& names) {
    std::vector<std::shared_ptr<Field>> ret =
        SchemaFromTuple<Tuple, N - 1>::MakeSchemaRecursionT(names);
    ret.push_back(field(std::get<N - 1>(names), std::make_shared<ArrowType>(),
                        ConversionTraits<Element>::nullable));
    return ret;
  }

  /// Build a Schema from the types of the tuple-like structure passed in as template
  /// parameter assign the column names at runtime.
  ///
  /// An example usage of this API can look like the following:
  ///
  /// \code{.cpp}
  /// using TupleType = std::tuple<int, std::vector<std::string>>;
  /// std::shared_ptr<Schema> schema =
  ///   SchemaFromTuple<TupleType>::MakeSchema({"int_column", "list_of_strings_column"});
  /// \endcode
  template <typename NamesTuple>
  static std::shared_ptr<Schema> MakeSchema(const NamesTuple& names) {
    return std::make_shared<Schema>(MakeSchemaRecursionT<NamesTuple>(names));
  }
};

template <typename Tuple>
struct SchemaFromTuple<Tuple, 0> {
  static std::vector<std::shared_ptr<Field>> MakeSchemaRecursion(
      const std::vector<std::string>& names) {
    std::vector<std::shared_ptr<Field>> ret;
    ret.reserve(names.size());
    return ret;
  }

  template <typename NamesTuple>
  static std::vector<std::shared_ptr<Field>> MakeSchemaRecursionT(
      const NamesTuple& names) {
    std::vector<std::shared_ptr<Field>> ret;
    ret.reserve(std::tuple_size<NamesTuple>::value);
    return ret;
  }
};
/// @endcond

}  // namespace stl
}  // namespace arrow

#endif  // ARROW_STL_H

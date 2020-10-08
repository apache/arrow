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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/type.h>

namespace cpp11 {
R6 r6_DataType(const std::shared_ptr<arrow::DataType>& type) {
  if (type == nullptr) return R_NilValue;

  using arrow::Type;

  switch (type->id()) {
    case Type::NA:
      return r6(type, "Null");
    case Type::BOOL:
      return r6(type, "Boolean");
    case Type::UINT8:
      return r6(type, "UInt8");
    case Type::UINT16:
      return r6(type, "UInt16");
    case Type::UINT32:
      return r6(type, "UInt32");
    case Type::UINT64:
      return r6(type, "UInt64");

    case Type::INT8:
      return r6(type, "Int8");
    case Type::INT16:
      return r6(type, "Int16");
    case Type::INT32:
      return r6(type, "Int32");
    case Type::INT64:
      return r6(type, "Int64");

    case Type::HALF_FLOAT:
      return r6(type, "Float16");
    case Type::FLOAT:
      return r6(type, "Float32");
    case Type::DOUBLE:
      return r6(type, "Float64");

    case Type::STRING:
      return r6(type, "Utf8");
    case Type::LARGE_STRING:
      return r6(type, "LargeUtf8");

    case Type::BINARY:
      return r6(type, "Binary");
    case Type::FIXED_SIZE_BINARY:
      return r6(type, "FixedSizeBinary");
    case Type::LARGE_BINARY:
      return r6(type, "LargeBinary");

    case Type::DATE32:
      return r6(type, "Date32");
    case Type::DATE64:
      return r6(type, "Date64");
    case Type::TIMESTAMP:
      return r6(type, "Timestamp");

    case Type::TIME32:
      return r6(type, "Time32");
    case Type::TIME64:
      return r6(type, "Time64");

    case Type::DECIMAL:
      return r6(type, "Decimal128Type");

    case Type::LIST:
      return r6(type, "ListType");
    case Type::LARGE_LIST:
      return r6(type, "LargeListType");
    case Type::FIXED_SIZE_LIST:
      return r6(type, "FixedSizeListType");

    case Type::STRUCT:
      return r6(type, "StructType");
    case Type::DICTIONARY:
      return r6(type, "DictionaryType");

    default:
      break;
  };

  return r6(type, "DataType");

  // switch(names(Type)[self$id + 1],
  //
  //        INTERVAL = stop("Type INTERVAL not implemented yet"),
  //        SPARSE_UNION = stop("Type SPARSE_UNION not implemented yet"),
  //        DENSE_UNION = stop("Type DENSE_UNION not implemented yet"),
  //        MAP = stop("Type MAP not implemented yet"),
  //        EXTENSION = stop("Type EXTENSION not implemented yet"),
  //        DURATION = stop("Type DURATION not implemented yet"),
}

}  // namespace cpp11

// [[arrow::export]]
R6 Int8__initialize() { return cpp11::r6(arrow::int8(), "Int8"); }

// [[arrow::export]]
R6 Int16__initialize() { return cpp11::r6(arrow::int16(), "Int16"); }

// [[arrow::export]]
R6 Int32__initialize() { return cpp11::r6(arrow::int32(), "Int32"); }

// [[arrow::export]]
R6 Int64__initialize() { return cpp11::r6(arrow::int64(), "Int64"); }

// [[arrow::export]]
R6 UInt8__initialize() { return cpp11::r6(arrow::uint8(), "UInt8"); }

// [[arrow::export]]
R6 UInt16__initialize() { return cpp11::r6(arrow::uint16(), "UInt16"); }

// [[arrow::export]]
R6 UInt32__initialize() { return cpp11::r6(arrow::uint32(), "UInt32"); }

// [[arrow::export]]
R6 UInt64__initialize() { return cpp11::r6(arrow::uint64(), "UInt64"); }

// [[arrow::export]]
R6 Float16__initialize() { return cpp11::r6(arrow::float16(), "Float16"); }

// [[arrow::export]]
R6 Float32__initialize() { return cpp11::r6(arrow::float32(), "Float32"); }

// [[arrow::export]]
R6 Float64__initialize() { return cpp11::r6(arrow::float64(), "Float64"); }

// [[arrow::export]]
R6 Boolean__initialize() { return cpp11::r6(arrow::boolean(), "Boolean"); }

// [[arrow::export]]
R6 Utf8__initialize() { return cpp11::r6(arrow::utf8(), "Utf8"); }

// [[arrow::export]]
R6 LargeUtf8__initialize() { return cpp11::r6(arrow::large_utf8(), "LargeUtf8"); }

// [[arrow::export]]
R6 Binary__initialize() { return cpp11::r6(arrow::binary(), "Binary"); }

// [[arrow::export]]
R6 LargeBinary__initialize() { return cpp11::r6(arrow::large_binary(), "LargeBinary"); }

// [[arrow::export]]
R6 Date32__initialize() { return cpp11::r6(arrow::date32(), "Date32"); }

// [[arrow::export]]
R6 Date64__initialize() { return cpp11::r6(arrow::date64(), "Date64"); }

// [[arrow::export]]
R6 Null__initialize() { return cpp11::r6(arrow::null(), "Null"); }

// [[arrow::export]]
R6 Decimal128Type__initialize(int32_t precision, int32_t scale) {
  // Use the builder that validates inputs
  return cpp11::r6(ValueOrStop(arrow::Decimal128Type::Make(precision, scale)),
                   "Decimal128Type");
}

// [[arrow::export]]
R6 FixedSizeBinary__initialize(R_xlen_t byte_width) {
  if (byte_width == NA_INTEGER) {
    cpp11::stop("'byte_width' cannot be NA");
  }
  if (byte_width < 1) {
    cpp11::stop("'byte_width' must be > 0");
  }
  return cpp11::r6(arrow::fixed_size_binary(byte_width), "FixedSizeBinary");
}

// [[arrow::export]]
R6 Timestamp__initialize(arrow::TimeUnit::type unit, const std::string& timezone) {
  return cpp11::r6(arrow::timestamp(unit, timezone), "Timestamp");
}

// [[arrow::export]]
R6 Time32__initialize(arrow::TimeUnit::type unit) {
  return cpp11::r6(arrow::time32(unit), "Time32");
}

// [[arrow::export]]
R6 Time64__initialize(arrow::TimeUnit::type unit) {
  return cpp11::r6(arrow::time64(unit), "Time64");
}

// [[arrow::export]]
R6 list__(SEXP x) {
  if (Rf_inherits(x, "Field")) {
    auto field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(x);
    return cpp11::r6(arrow::list(field), "ListType");
  }

  if (Rf_inherits(x, "DataType")) {
    auto type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(x);
    return cpp11::r6(arrow::list(type), "ListType");
  }

  cpp11::stop("incompatible");
  return R_NilValue;
}

// [[arrow::export]]
R6 large_list__(SEXP x) {
  if (Rf_inherits(x, "Field")) {
    auto field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(x);
    return cpp11::r6(arrow::large_list(field), "LargeListType");
  }

  if (Rf_inherits(x, "DataType")) {
    auto type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(x);
    return cpp11::r6(arrow::large_list(type), "LargeListType");
  }

  cpp11::stop("incompatible");
  return R_NilValue;
}

// [[arrow::export]]
R6 fixed_size_list__(SEXP x, int list_size) {
  if (Rf_inherits(x, "Field")) {
    auto field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(x);
    return cpp11::r6(arrow::fixed_size_list(field, list_size), "FixedSizeListType");
  }

  if (Rf_inherits(x, "DataType")) {
    auto type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(x);
    return cpp11::r6(arrow::fixed_size_list(type, list_size), "FixedSizeListType");
  }

  cpp11::stop("incompatible");
  return R_NilValue;
}

// [[arrow::export]]
R6 struct__(const std::vector<std::shared_ptr<arrow::Field>>& fields) {
  return cpp11::r6(arrow::struct_(fields), "StructType");
}

// [[arrow::export]]
std::string DataType__ToString(const std::shared_ptr<arrow::DataType>& type) {
  return type->ToString();
}

// [[arrow::export]]
std::string DataType__name(const std::shared_ptr<arrow::DataType>& type) {
  return type->name();
}

// [[arrow::export]]
bool DataType__Equals(const std::shared_ptr<arrow::DataType>& lhs,
                      const std::shared_ptr<arrow::DataType>& rhs) {
  return lhs->Equals(*rhs);
}

// [[arrow::export]]
int DataType__num_fields(const std::shared_ptr<arrow::DataType>& type) {
  return type->num_fields();
}

// [[arrow::export]]
cpp11::list DataType__fields(const std::shared_ptr<arrow::DataType>& type) {
  return arrow::r::to_r_list(type->fields(), cpp11::r6_Field);
}

// [[arrow::export]]
arrow::Type::type DataType__id(const std::shared_ptr<arrow::DataType>& type) {
  return type->id();
}

// [[arrow::export]]
std::string ListType__ToString(const std::shared_ptr<arrow::ListType>& type) {
  return type->ToString();
}

// [[arrow::export]]
int FixedWidthType__bit_width(const std::shared_ptr<arrow::FixedWidthType>& type) {
  return type->bit_width();
}

// [[arrow::export]]
arrow::DateUnit DateType__unit(const std::shared_ptr<arrow::DateType>& type) {
  return type->unit();
}

// [[arrow::export]]
arrow::TimeUnit::type TimeType__unit(const std::shared_ptr<arrow::TimeType>& type) {
  return type->unit();
}

// [[arrow::export]]
int32_t DecimalType__precision(const std::shared_ptr<arrow::DecimalType>& type) {
  return type->precision();
}

// [[arrow::export]]
int32_t DecimalType__scale(const std::shared_ptr<arrow::DecimalType>& type) {
  return type->scale();
}

// [[arrow::export]]
std::string TimestampType__timezone(const std::shared_ptr<arrow::TimestampType>& type) {
  return type->timezone();
}

// [[arrow::export]]
arrow::TimeUnit::type TimestampType__unit(
    const std::shared_ptr<arrow::TimestampType>& type) {
  return type->unit();
}

// [[arrow::export]]
R6 DictionaryType__initialize(const std::shared_ptr<arrow::DataType>& index_type,
                              const std::shared_ptr<arrow::DataType>& value_type,
                              bool ordered) {
  auto type = ValueOrStop(arrow::DictionaryType::Make(index_type, value_type, ordered));
  return cpp11::r6(type, "DictionaryType");
}

// [[arrow::export]]
R6 DictionaryType__index_type(const std::shared_ptr<arrow::DictionaryType>& type) {
  return cpp11::r6_DataType(type->index_type());
}

// [[arrow::export]]
R6 DictionaryType__value_type(const std::shared_ptr<arrow::DictionaryType>& type) {
  return cpp11::r6_DataType(type->value_type());
}

// [[arrow::export]]
std::string DictionaryType__name(const std::shared_ptr<arrow::DictionaryType>& type) {
  return type->name();
}

// [[arrow::export]]
bool DictionaryType__ordered(const std::shared_ptr<arrow::DictionaryType>& type) {
  return type->ordered();
}

// [[arrow::export]]
R6 StructType__GetFieldByName(const std::shared_ptr<arrow::StructType>& type,
                              const std::string& name) {
  return cpp11::r6(type->GetFieldByName(name), "Field");
}

// [[arrow::export]]
int StructType__GetFieldIndex(const std::shared_ptr<arrow::StructType>& type,
                              const std::string& name) {
  return type->GetFieldIndex(name);
}

// [[arrow::export]]
R6 ListType__value_field(const std::shared_ptr<arrow::ListType>& type) {
  return cpp11::r6(type->value_field(), "Field");
}

// [[arrow::export]]
R6 ListType__value_type(const std::shared_ptr<arrow::ListType>& type) {
  return cpp11::r6_DataType(type->value_type());
}

// [[arrow::export]]
R6 LargeListType__value_field(const std::shared_ptr<arrow::LargeListType>& type) {
  return cpp11::r6(type->value_field(), "Field");
}

// [[arrow::export]]
R6 LargeListType__value_type(const std::shared_ptr<arrow::LargeListType>& type) {
  return cpp11::r6_DataType(type->value_type());
}

// [[arrow::export]]
R6 FixedSizeListType__value_field(const std::shared_ptr<arrow::FixedSizeListType>& type) {
  return cpp11::r6(type->value_field(), "Field");
}

// [[arrow::export]]
R6 FixedSizeListType__value_type(const std::shared_ptr<arrow::FixedSizeListType>& type) {
  return cpp11::r6_DataType(type->value_type());
}

// [[arrow::export]]
int FixedSizeListType__list_size(const std::shared_ptr<arrow::FixedSizeListType>& type) {
  return type->list_size();
}

#endif

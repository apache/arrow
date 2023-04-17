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

#include <arrow/type.h>

namespace cpp11 {

const char* r6_class_name<arrow::DataType>::get(
    const std::shared_ptr<arrow::DataType>& type) {
  using arrow::Type;

  switch (type->id()) {
    case Type::NA:
      return "Null";
    case Type::BOOL:
      return "Boolean";
    case Type::UINT8:
      return "UInt8";
    case Type::UINT16:
      return "UInt16";
    case Type::UINT32:
      return "UInt32";
    case Type::UINT64:
      return "UInt64";

    case Type::INT8:
      return "Int8";
    case Type::INT16:
      return "Int16";
    case Type::INT32:
      return "Int32";
    case Type::INT64:
      return "Int64";

    case Type::HALF_FLOAT:
      return "Float16";
    case Type::FLOAT:
      return "Float32";
    case Type::DOUBLE:
      return "Float64";

    case Type::STRING:
      return "Utf8";
    case Type::LARGE_STRING:
      return "LargeUtf8";

    case Type::BINARY:
      return "Binary";
    case Type::FIXED_SIZE_BINARY:
      return "FixedSizeBinary";
    case Type::LARGE_BINARY:
      return "LargeBinary";

    case Type::DATE32:
      return "Date32";
    case Type::DATE64:
      return "Date64";
    case Type::TIMESTAMP:
      return "Timestamp";

    case Type::TIME32:
      return "Time32";
    case Type::TIME64:
      return "Time64";
    case Type::DURATION:
      return "DurationType";

    case Type::DECIMAL128:
      return "Decimal128Type";
    case Type::DECIMAL256:
      return "Decimal256Type";

    case Type::LIST:
      return "ListType";
    case Type::LARGE_LIST:
      return "LargeListType";
    case Type::FIXED_SIZE_LIST:
      return "FixedSizeListType";

    case Type::MAP:
      return "MapType";

    case Type::STRUCT:
      return "StructType";
    case Type::DICTIONARY:
      return "DictionaryType";
    case Type::EXTENSION:
      return "ExtensionType";

    default:
      break;
  }

  // No R6 classes are defined for:
  //    INTERVAL
  //    SPARSE_UNION
  //    DENSE_UNION
  //    MAP
  //    EXTENSION
  //    DURATION
  //
  // If a c++ function returns one it will be wrapped as a DataType.

  return "DataType";
}

}  // namespace cpp11

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Int8__initialize() { return arrow::int8(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Int16__initialize() { return arrow::int16(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Int32__initialize() { return arrow::int32(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Int64__initialize() { return arrow::int64(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> UInt8__initialize() { return arrow::uint8(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> UInt16__initialize() { return arrow::uint16(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> UInt32__initialize() { return arrow::uint32(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> UInt64__initialize() { return arrow::uint64(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Float16__initialize() { return arrow::float16(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Float32__initialize() { return arrow::float32(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Float64__initialize() { return arrow::float64(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Boolean__initialize() { return arrow::boolean(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Utf8__initialize() { return arrow::utf8(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> LargeUtf8__initialize() { return arrow::large_utf8(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Binary__initialize() { return arrow::binary(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> LargeBinary__initialize() {
  return arrow::large_binary();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Date32__initialize() { return arrow::date32(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Date64__initialize() { return arrow::date64(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Null__initialize() { return arrow::null(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Decimal128Type__initialize(int32_t precision,
                                                            int32_t scale) {
  // Use the builder that validates inputs
  return ValueOrStop(arrow::Decimal128Type::Make(precision, scale));
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Decimal256Type__initialize(int32_t precision,
                                                            int32_t scale) {
  // Use the builder that validates inputs
  return ValueOrStop(arrow::Decimal256Type::Make(precision, scale));
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> DayTimeInterval__initialize() {
  return arrow::day_time_interval();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> FixedSizeBinary__initialize(R_xlen_t byte_width) {
  if (byte_width == NA_INTEGER) {
    cpp11::stop("'byte_width' cannot be NA");
  }
  if (byte_width < 1) {
    cpp11::stop("'byte_width' must be > 0");
  }
  return arrow::fixed_size_binary(byte_width);
}

// [[arrow::export]]
int FixedSizeBinary__byte_width(const std::shared_ptr<arrow::FixedSizeBinaryType>& type) {
  return type->byte_width();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Timestamp__initialize(arrow::TimeUnit::type unit,
                                                       const std::string& timezone) {
  return arrow::timestamp(unit, timezone);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Time32__initialize(arrow::TimeUnit::type unit) {
  return arrow::time32(unit);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Time64__initialize(arrow::TimeUnit::type unit) {
  return arrow::time64(unit);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Duration__initialize(arrow::TimeUnit::type unit) {
  return arrow::duration(unit);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> list__(SEXP x) {
  if (Rf_inherits(x, "Field")) {
    auto field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(x);
    return arrow::list(field);
  }

  if (!Rf_inherits(x, "DataType")) {
    cpp11::stop("incompatible");
  }

  auto type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(x);
  return arrow::list(type);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> large_list__(SEXP x) {
  if (Rf_inherits(x, "Field")) {
    auto field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(x);
    return arrow::large_list(field);
  }

  if (!Rf_inherits(x, "DataType")) {
    cpp11::stop("incompatible");
  }

  auto type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(x);
  return arrow::large_list(type);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> fixed_size_list__(SEXP x, int list_size) {
  if (Rf_inherits(x, "Field")) {
    auto field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(x);
    return arrow::fixed_size_list(field, list_size);
  }

  if (!Rf_inherits(x, "DataType")) {
    cpp11::stop("incompatible");
  }

  auto type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(x);
  return arrow::fixed_size_list(type, list_size);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> map__(SEXP key, SEXP item, bool keys_sorted = false) {
  std::shared_ptr<arrow::Field> key_field;
  std::shared_ptr<arrow::Field> item_field;

  if (Rf_inherits(key, "DataType")) {
    auto key_type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(key);
    key_field = std::make_shared<arrow::Field>("key", key_type, /* nullable = */ false);
  } else if (Rf_inherits(key, "Field")) {
    key_field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(key);
    if (key_field->nullable()) cpp11::stop("key field cannot be nullable.");
  } else {
    cpp11::stop("key must be a DataType or Field.");
  }

  if (Rf_inherits(item, "DataType")) {
    auto item_type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(item);
    item_field = std::make_shared<arrow::Field>("value", item_type);
  } else if (Rf_inherits(item, "Field")) {
    item_field = cpp11::as_cpp<std::shared_ptr<arrow::Field>>(item);
  } else {
    cpp11::stop("item must be a DataType or Field.");
  }

  return std::make_shared<arrow::MapType>(key_field, item_field);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> struct__(
    const std::vector<std::shared_ptr<arrow::Field>>& fields) {
  return arrow::struct_(fields);
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
                      const std::shared_ptr<arrow::DataType>& rhs, bool check_metadata) {
  return lhs->Equals(*rhs, check_metadata);
}

// [[arrow::export]]
int DataType__num_fields(const std::shared_ptr<arrow::DataType>& type) {
  return type->num_fields();
}

// [[arrow::export]]
cpp11::list DataType__fields(const std::shared_ptr<arrow::DataType>& type) {
  return arrow::r::to_r_list(type->fields());
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
arrow::TimeUnit::type DurationType__unit(
    const std::shared_ptr<arrow::DurationType>& type) {
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
std::shared_ptr<arrow::DataType> DictionaryType__initialize(
    const std::shared_ptr<arrow::DataType>& index_type,
    const std::shared_ptr<arrow::DataType>& value_type, bool ordered) {
  return ValueOrStop(arrow::DictionaryType::Make(index_type, value_type, ordered));
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> DictionaryType__index_type(
    const std::shared_ptr<arrow::DictionaryType>& type) {
  return type->index_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> DictionaryType__value_type(
    const std::shared_ptr<arrow::DictionaryType>& type) {
  return type->value_type();
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
std::shared_ptr<arrow::Field> StructType__GetFieldByName(
    const std::shared_ptr<arrow::StructType>& type, const std::string& name) {
  return type->GetFieldByName(name);
}

// [[arrow::export]]
int StructType__GetFieldIndex(const std::shared_ptr<arrow::StructType>& type,
                              const std::string& name) {
  return type->GetFieldIndex(name);
}

// [[arrow::export]]
std::vector<std::string> StructType__field_names(
    const std::shared_ptr<arrow::StructType>& type) {
  auto num_fields = type->num_fields();
  std::vector<std::string> out(num_fields);
  for (int i = 0; i < num_fields; i++) {
    out[i] = type->field(i)->name();
  }
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> ListType__value_field(
    const std::shared_ptr<arrow::ListType>& type) {
  return type->value_field();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ListType__value_type(
    const std::shared_ptr<arrow::ListType>& type) {
  return type->value_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> LargeListType__value_field(
    const std::shared_ptr<arrow::LargeListType>& type) {
  return type->value_field();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> LargeListType__value_type(
    const std::shared_ptr<arrow::LargeListType>& type) {
  return type->value_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> FixedSizeListType__value_field(
    const std::shared_ptr<arrow::FixedSizeListType>& type) {
  return type->value_field();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> FixedSizeListType__value_type(
    const std::shared_ptr<arrow::FixedSizeListType>& type) {
  return type->value_type();
}

// [[arrow::export]]
int FixedSizeListType__list_size(const std::shared_ptr<arrow::FixedSizeListType>& type) {
  return type->list_size();
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> MapType__key_field(
    const std::shared_ptr<arrow::MapType>& type) {
  return type->key_field();
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> MapType__item_field(
    const std::shared_ptr<arrow::MapType>& type) {
  return type->item_field();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> MapType__key_type(
    const std::shared_ptr<arrow::MapType>& type) {
  return type->key_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> MapType__item_type(
    const std::shared_ptr<arrow::MapType>& type) {
  return type->item_type();
}

// [[arrow::export]]
bool MapType__keys_sorted(const std::shared_ptr<arrow::MapType>& type) {
  return type->keys_sorted();
}

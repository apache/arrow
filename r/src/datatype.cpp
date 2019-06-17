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

using Rcpp::CharacterVector;
using Rcpp::List;
using Rcpp::stop;
using Rcpp::wrap;

#if defined(ARROW_R_WITH_ARROW)

// [[arrow::export]]
bool shared_ptr_is_null(SEXP xp) {
  return reinterpret_cast<std::shared_ptr<void>*>(EXTPTR_PTR(xp))->get() == nullptr;
}

// [[arrow::export]]
bool unique_ptr_is_null(SEXP xp) {
  return reinterpret_cast<std::unique_ptr<void>*>(EXTPTR_PTR(xp))->get() == nullptr;
}

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
std::shared_ptr<arrow::DataType> Date32__initialize() { return arrow::date32(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Date64__initialize() { return arrow::date64(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Null__initialize() { return arrow::null(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Decimal128Type__initialize(int32_t precision,
                                                            int32_t scale) {
  return arrow::decimal(precision, scale);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> FixedSizeBinary__initialize(int32_t byte_width) {
  return arrow::fixed_size_binary(byte_width);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Timestamp__initialize1(arrow::TimeUnit::type unit) {
  return arrow::timestamp(unit);
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Timestamp__initialize2(arrow::TimeUnit::type unit,
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
SEXP list__(SEXP x) {
  if (Rf_inherits(x, "arrow::Field")) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::Field>> field(x);
    return wrap(arrow::list(field));
  }

  if (Rf_inherits(x, "arrow::DataType")) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::DataType>> type(x);
    return wrap(arrow::list(type));
  }

  stop("incompatible");
  return R_NilValue;
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> struct_(List fields) {
  return arrow::struct_(arrow::r::List_to_shared_ptr_vector<arrow::Field>(fields));
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
int DataType__num_children(const std::shared_ptr<arrow::DataType>& type) {
  return type->num_children();
}

// [[arrow::export]]
List DataType__children_pointer(const std::shared_ptr<arrow::DataType>& type) {
  return List(type->children().begin(), type->children().end());
}

// [[arrow::export]]
arrow::Type::type DataType__id(const std::shared_ptr<arrow::DataType>& type) {
  return type->id();
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> schema_(List fields) {
  return arrow::schema(arrow::r::List_to_shared_ptr_vector<arrow::Field>(fields));
}

// [[arrow::export]]
std::string Schema__ToString(const std::shared_ptr<arrow::Schema>& s) {
  return s->ToString();
}

// [[arrow::export]]
int Schema__num_fields(const std::shared_ptr<arrow::Schema>& s) {
  return s->num_fields();
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> Schema__field(const std::shared_ptr<arrow::Schema>& s,
                                            int i) {
  if (i >= s->num_fields() || i < 0) {
    Rcpp::stop("Invalid field index for schema.");
  }

  return s->field(i);
}

// [[arrow::export]]
CharacterVector Schema__names(const std::shared_ptr<arrow::Schema>& schema) {
  auto fields = schema->fields();
  return CharacterVector(
      fields.begin(), fields.end(),
      [](const std::shared_ptr<arrow::Field>& field) { return field->name(); });
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
std::shared_ptr<arrow::DataType> DictionaryType__initialize(
    const std::shared_ptr<arrow::DataType>& index_type,
    const std::shared_ptr<arrow::DataType>& value_type, bool ordered) {
  return arrow::dictionary(index_type, value_type, ordered);
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

#endif

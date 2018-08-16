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

#include "arrow_types.h"

using namespace Rcpp;

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Int8_initialize() { return arrow::int8(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Int16_initialize() { return arrow::int16(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Int32_initialize() { return arrow::int32(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Int64_initialize() { return arrow::int64(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> UInt8_initialize() { return arrow::uint8(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> UInt16_initialize() { return arrow::uint16(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> UInt32_initialize() { return arrow::uint32(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> UInt64_initialize() { return arrow::uint64(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Float16_initialize() { return arrow::float16(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Float32_initialize() { return arrow::float32(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Float64_initialize() { return arrow::float64(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Boolean_initialize() { return arrow::boolean(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Utf8_initialize() { return arrow::utf8(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Date32_initialize() { return arrow::date32(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Date64_initialize() { return arrow::date64(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Null_initialize() { return arrow::null(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Decimal128Type_initialize(int32_t precision, int32_t scale) {
  return arrow::decimal(precision, scale);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> FixedSizeBinary_initialize(int32_t byte_width) {
  return arrow::fixed_size_binary(byte_width);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Timestamp_initialize1(arrow::TimeUnit::type unit) {
  return arrow::timestamp(unit);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Timestamp_initialize2(arrow::TimeUnit::type unit, const std::string& timezone) {
  return arrow::timestamp(unit, timezone);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Time32_initialize(arrow::TimeUnit::type unit) {
  return arrow::time32(unit);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Time64_initialize(arrow::TimeUnit::type unit) {
  return arrow::time64(unit);
}

// [[Rcpp::export]]
SEXP list__(SEXP x) {
  if (Rf_inherits(x, "arrow::Field")) {
    return wrap(arrow::list(*xptr_Field(x)));
  }

  if (Rf_inherits(x, "arrow::DataType")) {
    return wrap(arrow::list(*xptr_DataType(x)));
  }

  stop("incompatible");
  return R_NilValue;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> struct_(Rcpp::ListOf<xptr_Field> fields) {
  int n = fields.size();
  std::vector<std::shared_ptr<arrow::Field>> vec_fields;
  for (int i = 0; i < n; i++) {
    vec_fields.emplace_back(*fields[i]);
  }

  return arrow::struct_(vec_fields);
}

// [[Rcpp::export]]
std::string DataType_ToString(xptr_DataType type) {
  std::shared_ptr<arrow::DataType> ptr(*type);
  return ptr->ToString();
}

// [[Rcpp::export]]
std::string DataType_name(xptr_DataType type) {
  return std::shared_ptr<arrow::DataType>(*type)->name();
}

// [[Rcpp::export]]
bool DataType_Equals(xptr_DataType lhs, xptr_DataType rhs) {
  return std::shared_ptr<arrow::DataType>(*lhs)->Equals(*rhs);
}

// [[Rcpp::export]]
int DataType_num_children(xptr_DataType type) {
  return std::shared_ptr<arrow::DataType>(*type)->num_children();
}

// [[Rcpp::export]]
List DataType_children_pointer(xptr_DataType type) {
  const std::vector<std::shared_ptr<arrow::Field>>& kids = std::shared_ptr<arrow::DataType>(*type)->children();

  int n = kids.size();
  List out(n);
  for (int i=0; i<n; i++) {
    out[i] = xptr_Field(new std::shared_ptr<arrow::Field>(kids[i])) ;
  }
  return out;
}

// [[Rcpp::export]]
arrow::Type::type DataType_id(xptr_DataType type) {
  return std::shared_ptr<arrow::DataType>(*type)->id();
}


// [[Rcpp::export]]
xptr_Schema schema_(ListOf<xptr_Field> fields) {
  int n = fields.size();
  std::vector<std::shared_ptr<arrow::Field>> vec_fields;
  for (int i = 0; i < n; i++) {
    vec_fields.emplace_back(*fields[i]);
  }

  xptr_Schema s(new std::shared_ptr<arrow::Schema>(arrow::schema(vec_fields)));
  s.attr("class") = CharacterVector::create("arrow::Schema");
  return s;
}

// [[Rcpp::export]]
std::string Schema_ToString(xptr_Schema type) {
  std::shared_ptr<arrow::Schema> ptr(*type);
  return ptr->ToString();
}

// [[Rcpp::export]]
std::string ListType_ToString(xptr_ListType type) {
  std::shared_ptr<arrow::ListType> ptr(*type);
  return ptr->ToString();
}

// [[Rcpp::export]]
int FixedWidthType_bit_width(xptr_FixedWidthType type){
  return std::shared_ptr<arrow::FixedWidthType>(*type)->bit_width();
}

// [[Rcpp::export]]
arrow::DateUnit DateType_unit(xptr_DateType type){
  return std::shared_ptr<arrow::DateType>(*type)->unit();
}

// [[Rcpp::export]]
arrow::TimeUnit::type TimeType_unit(xptr_TimeType type){
  return std::shared_ptr<arrow::TimeType>(*type)->unit();
}


// [[Rcpp::export]]
int32_t DecimalType_precision(xptr_DecimalType type) {
  return std::shared_ptr<arrow::DecimalType>(*type)->precision();
}

// [[Rcpp::export]]
int32_t DecimalType_scale(xptr_DecimalType type) {
  return std::shared_ptr<arrow::DecimalType>(*type)->scale();
}

// [[Rcpp::export]]
std::string TimestampType_timezone(xptr_TimestampType type) {
  return std::shared_ptr<arrow::TimestampType>(*type)->timezone();
}

// [[Rcpp::export]]
arrow::TimeUnit::type TimestampType_unit(xptr_TimestampType type) {
  return std::shared_ptr<arrow::TimestampType>(*type)->unit();
}

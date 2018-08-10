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

#include <Rcpp.h>
#include "arrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp;

template <typename... String>
xptr_DataType metadata(const std::shared_ptr<arrow::DataType>& ptr, String... strings) {
  xptr_DataType res(new std::shared_ptr<arrow::DataType>(ptr));
  res.attr("class") = CharacterVector::create(ptr->name(), strings...);
  return res;
}

xptr_DataType metadata_integer(const std::shared_ptr<arrow::DataType>& ptr) {
  return metadata(ptr, "arrow::Integer", "arrow::Number", "arrow::PrimitiveCType",
                  "arrow::FixedWidthType", "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType Int8_initialize() { return metadata_integer(arrow::int8()); }

// [[Rcpp::export]]
xptr_DataType Int16_initialize() { return metadata_integer(arrow::int16()); }

// [[Rcpp::export]]
xptr_DataType Int32_initialize() { return metadata_integer(arrow::int32()); }

// [[Rcpp::export]]
xptr_DataType Int64_initialize() { return metadata_integer(arrow::int64()); }

// [[Rcpp::export]]
xptr_DataType UInt8_initialize() { return metadata_integer(arrow::uint8()); }

// [[Rcpp::export]]
xptr_DataType UInt16_initialize() { return metadata_integer(arrow::uint16()); }

// [[Rcpp::export]]
xptr_DataType UInt32_initialize() { return metadata_integer(arrow::uint32()); }

// [[Rcpp::export]]
xptr_DataType UInt64_initialize() { return metadata_integer(arrow::uint64()); }

xptr_DataType metadata_float(const std::shared_ptr<arrow::DataType>& ptr) {
  return metadata(ptr, "arrow::FloatingPoint", "arrow::Number", "arrow::PrimitiveCType",
                  "arrow::FixedWidthType", "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType Float16_initialize() { return metadata_float(arrow::float16()); }

// [[Rcpp::export]]
xptr_DataType Float32_initialize() { return metadata_float(arrow::float32()); }

// [[Rcpp::export]]
xptr_DataType Float64_initialize() { return metadata_float(arrow::float64()); }

// [[Rcpp::export]]
xptr_DataType Boolean_initialize() {
  return metadata(arrow::boolean(), "arrow::BooleanType", "arrow::FixedWidthType",
                  "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType Utf8_initialize() {
  return metadata(arrow::utf8(), "arrow::StringType", "arrow::BinaryType",
                  "arrow::DataType");
}

// binary ?

xptr_DataType metadata_date(const std::shared_ptr<arrow::DataType>& ptr) {
  return metadata(ptr, "arrow::DateType", "arrow::FixedWidthType", "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType Date32_initialize() { return metadata_date(arrow::date32()); }

// [[Rcpp::export]]
xptr_DataType Date64_initialize() { return metadata_date(arrow::date64()); }

// [[Rcpp::export]]
xptr_DataType Null_initialize() {
  return metadata(arrow::null(), "arrow::NullType", "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType DecimalType_initialize(int32_t precision, int32_t scale) {
  return metadata(arrow::decimal(precision, scale), "arrow::NullType", "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType FixedSizeBinary_initialize(int32_t byte_width) {
  return metadata(arrow::fixed_size_binary(byte_width), "arrow::FixedSizeBinaryType",
                  "arrow::FixedWidthType", "arrow::DataType");
}

namespace Rcpp {
template <>
arrow::TimeUnit::type as<arrow::TimeUnit::type>(SEXP x) {
  if (!Rf_inherits(x, "arrow::TimeUnit::type")) stop("incompatible");
  return static_cast<arrow::TimeUnit::type>(as<int>(x));
}
}  // namespace Rcpp

// [[Rcpp::export]]
xptr_DataType Timestamp_initialize1(arrow::TimeUnit::type unit) {
  return metadata(arrow::timestamp(unit), "arrow::TimestampType", "arrow::FixedWidthType",
                  "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType Timestamp_initialize2(arrow::TimeUnit::type unit, const std::string& timezone) {
  return metadata(arrow::timestamp(unit, timezone), "arrow::TimestampType",
                  "arrow::FixedWidthType", "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType Time32_initialize(arrow::TimeUnit::type unit) {
  return metadata(arrow::time32(unit), "arrow::Time32Type", "arrow::TimeType",
                  "arrow::FixedWidthType", "arrow::DataType");
}

// [[Rcpp::export]]
xptr_DataType Time64_initialize(arrow::TimeUnit::type unit) {
  return metadata(arrow::time64(unit), "arrow::Time64Type", "arrow::TimeType",
                  "arrow::FixedWidthType", "arrow::DataType");
}

// [[Rcpp::export]]
SEXP list__(SEXP x) {
  if (Rf_inherits(x, "arrow::Field")) {
    return metadata(arrow::list(*xptr_Field(x)), "arrow::ListType", "arrow::NestedType",
                    "arrow::DataType");
  }

  if (Rf_inherits(x, "arrow::DataType")) {
    return metadata(arrow::list(*xptr_DataType(x)), "arrow::ListType",
                    "arrow::NestedType", "arrow::DataType");
  }

  stop("incompatible");
  return R_NilValue;
}

// [[Rcpp::export]]
xptr_DataType struct_(ListOf<xptr_Field> fields) {
  int n = fields.size();
  std::vector<std::shared_ptr<arrow::Field>> vec_fields;
  for (int i = 0; i < n; i++) {
    vec_fields.emplace_back(*fields[i]);
  }

  std::shared_ptr<arrow::DataType> s(arrow::struct_(vec_fields));
  return metadata(s, "arrow::StructType", "arrow::NestedType", "arrow::DataType");
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

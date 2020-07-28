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

#include <arrow/array.h>
#include <arrow/util/bitmap_reader.h>

void arrow::r::validate_slice_offset(arrow::r::Index offset, int64_t len) {
  if (offset == NA_INTEGER) {
    cpp11::stop("Slice 'offset' cannot be NA");
  }
  if (offset < 0) {
    cpp11::stop("Slice 'offset' cannot be negative");
  }
  if (offset > len) {
    cpp11::stop("Slice 'offset' greater than array length");
  }
}

void arrow::r::validate_slice_length(arrow::r::Index length, int64_t available) {
  if (length == NA_INTEGER) {
    cpp11::stop("Slice 'length' cannot be NA");
  }
  if (length < 0) {
    cpp11::stop("Slice 'length' cannot be negative");
  }
  if (length > available) {
    cpp11::warning("Slice 'length' greater than available length");
  }
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Slice1(const std::shared_ptr<arrow::Array>& array,
                                            arrow::r::Index offset) {
  arrow::r::validate_slice_offset(offset, array->length());
  return array->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Slice2(const std::shared_ptr<arrow::Array>& array,
                                            arrow::r::Index offset,
                                            arrow::r::Index length) {
  arrow::r::validate_slice_offset(offset, array->length());
  arrow::r::validate_slice_length(length, array->length() - offset);
  return array->Slice(offset, length);
}

void arrow::r::validate_index(int i, int len) {
  if (i == NA_INTEGER) {
    cpp11::stop("'i' cannot be NA");
  }
  if (i < 0 || i >= len) {
    cpp11::stop("subscript out of bounds");
  }
}

// [[arrow::export]]
bool Array__IsNull(const std::shared_ptr<arrow::Array>& x, arrow::r::Index i) {
  arrow::r::validate_index(i, x->length());
  return x->IsNull(i);
}

// [[arrow::export]]
bool Array__IsValid(const std::shared_ptr<arrow::Array>& x, arrow::r::Index i) {
  arrow::r::validate_index(i, x->length());
  return x->IsValid(i);
}

// [[arrow::export]]
int Array__length(const std::shared_ptr<arrow::Array>& x) { return x->length(); }

// [[arrow::export]]
int Array__offset(const std::shared_ptr<arrow::Array>& x) { return x->offset(); }

// [[arrow::export]]
int Array__null_count(const std::shared_ptr<arrow::Array>& x) { return x->null_count(); }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Array__type(const std::shared_ptr<arrow::Array>& x) {
  return x->type();
}

// [[arrow::export]]
std::string Array__ToString(const std::shared_ptr<arrow::Array>& x) {
  return x->ToString();
}

// [[arrow::export]]
arrow::Type::type Array__type_id(const std::shared_ptr<arrow::Array>& x) {
  return x->type_id();
}

// [[arrow::export]]
bool Array__Equals(const std::shared_ptr<arrow::Array>& lhs,
                   const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->Equals(rhs);
}

// [[arrow::export]]
bool Array__ApproxEquals(const std::shared_ptr<arrow::Array>& lhs,
                         const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->ApproxEquals(rhs);
}

// [[arrow::export]]
std::shared_ptr<arrow::ArrayData> Array__data(
    const std::shared_ptr<arrow::Array>& array) {
  return array->data();
}

// [[arrow::export]]
bool Array__RangeEquals(const std::shared_ptr<arrow::Array>& self,
                        const std::shared_ptr<arrow::Array>& other,
                        arrow::r::Index start_idx, arrow::r::Index end_idx,
                        arrow::r::Index other_start_idx) {
  if (start_idx == NA_INTEGER) {
    cpp11::stop("'start_idx' cannot be NA");
  }
  if (end_idx == NA_INTEGER) {
    cpp11::stop("'end_idx' cannot be NA");
  }
  if (other_start_idx == NA_INTEGER) {
    cpp11::stop("'other_start_idx' cannot be NA");
  }
  return self->RangeEquals(*other, start_idx, end_idx, other_start_idx);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__View(const std::shared_ptr<arrow::Array>& array,
                                          const std::shared_ptr<arrow::DataType>& type) {
  return ValueOrStop(array->View(type));
}

// [[arrow::export]]
void Array__Validate(const std::shared_ptr<arrow::Array>& array) {
  StopIfNotOk(array->Validate());
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> DictionaryArray__indices(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->indices();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> DictionaryArray__dictionary(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->dictionary();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> StructArray__field(
    const std::shared_ptr<arrow::StructArray>& array, int i) {
  return array->field(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> StructArray__GetFieldByName(
    const std::shared_ptr<arrow::StructArray>& array, const std::string& name) {
  return array->GetFieldByName(name);
}

// [[arrow::export]]
arrow::ArrayVector StructArray__Flatten(
    const std::shared_ptr<arrow::StructArray>& array) {
  return ValueOrStop(array->Flatten());
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ListArray__value_type(
    const std::shared_ptr<arrow::ListArray>& array) {
  return array->value_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> LargeListArray__value_type(
    const std::shared_ptr<arrow::LargeListArray>& array) {
  return array->value_type();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ListArray__values(
    const std::shared_ptr<arrow::ListArray>& array) {
  return array->values();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> LargeListArray__values(
    const std::shared_ptr<arrow::LargeListArray>& array) {
  return array->values();
}

// [[arrow::export]]
int32_t ListArray__value_length(const std::shared_ptr<arrow::ListArray>& array,
                                int64_t i) {
  return array->value_length(i);
}

// [[arrow::export]]
int64_t LargeListArray__value_length(const std::shared_ptr<arrow::LargeListArray>& array,
                                     int64_t i) {
  return array->value_length(i);
}

// [[arrow::export]]
int64_t FixedSizeListArray__value_length(
    const std::shared_ptr<arrow::FixedSizeListArray>& array, int64_t i) {
  return array->value_length(i);
}

// [[arrow::export]]
int32_t ListArray__value_offset(const std::shared_ptr<arrow::ListArray>& array,
                                int64_t i) {
  return array->value_offset(i);
}

// [[arrow::export]]
int64_t LargeListArray__value_offset(const std::shared_ptr<arrow::LargeListArray>& array,
                                     int64_t i) {
  return array->value_offset(i);
}

// [[arrow::export]]
int64_t FixedSizeListArray__value_offset(
    const std::shared_ptr<arrow::FixedSizeListArray>& array, int64_t i) {
  return array->value_offset(i);
}

// [[arrow::export]]
Rcpp::IntegerVector ListArray__raw_value_offsets(
    const std::shared_ptr<arrow::ListArray>& array) {
  auto offsets = array->raw_value_offsets();
  return Rcpp::IntegerVector(offsets, offsets + array->length());
}

// [[arrow::export]]
Rcpp::IntegerVector LargeListArray__raw_value_offsets(
    const std::shared_ptr<arrow::LargeListArray>& array) {
  auto offsets = array->raw_value_offsets();
  return Rcpp::IntegerVector(offsets, offsets + array->length());
}

#endif

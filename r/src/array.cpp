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

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/util/bitmap_reader.h>
#include <arrow/util/byte_size.h>

namespace cpp11 {

const char* r6_class_name<arrow::Array>::get(const std::shared_ptr<arrow::Array>& array) {
  auto type = array->type_id();
  switch (type) {
    case arrow::Type::DICTIONARY:
      return "DictionaryArray";
    case arrow::Type::STRUCT:
      return "StructArray";
    case arrow::Type::LIST:
      return "ListArray";
    case arrow::Type::LARGE_LIST:
      return "LargeListArray";
    case arrow::Type::FIXED_SIZE_LIST:
      return "FixedSizeListArray";
    case arrow::Type::MAP:
      return "MapArray";
    case arrow::Type::EXTENSION:
      return "ExtensionArray";

    default:
      return "Array";
  }
}

}  // namespace cpp11

void arrow::r::validate_slice_offset(R_xlen_t offset, int64_t len) {
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

void arrow::r::validate_slice_length(R_xlen_t length, int64_t available) {
  if (length == NA_INTEGER) {
    cpp11::stop("Slice 'length' cannot be NA");
  }
  if (length < 0) {
    cpp11::stop("Slice 'length' cannot be negative");
  }
  if (length > available) {
    // For an unknown reason, cpp11::warning() crashes here; however, this
    // should throw an exception if Rf_warning() jumps, so we need
    // cpp11::safe[]().
    cpp11::safe[Rf_warning]("Slice 'length' greater than available length");
  }
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Slice1(const std::shared_ptr<arrow::Array>& array,
                                            R_xlen_t offset) {
  arrow::r::validate_slice_offset(offset, array->length());
  return array->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__Slice2(const std::shared_ptr<arrow::Array>& array,
                                            R_xlen_t offset, R_xlen_t length) {
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
bool Array__IsNull(const std::shared_ptr<arrow::Array>& x, R_xlen_t i) {
  arrow::r::validate_index(i, x->length());
  return x->IsNull(i);
}

// [[arrow::export]]
bool Array__IsValid(const std::shared_ptr<arrow::Array>& x, R_xlen_t i) {
  arrow::r::validate_index(i, x->length());
  return x->IsValid(i);
}

// [[arrow::export]]
r_vec_size Array__length(const std::shared_ptr<arrow::Array>& x) {
  return r_vec_size(x->length());
}

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
std::string Array__Diff(const std::shared_ptr<arrow::Array>& lhs,
                        const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->Diff(*rhs);
}

// [[arrow::export]]
std::shared_ptr<arrow::ArrayData> Array__data(
    const std::shared_ptr<arrow::Array>& array) {
  return array->data();
}

// [[arrow::export]]
bool Array__RangeEquals(const std::shared_ptr<arrow::Array>& self,
                        const std::shared_ptr<arrow::Array>& other, R_xlen_t start_idx,
                        R_xlen_t end_idx, R_xlen_t other_start_idx) {
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
std::shared_ptr<arrow::StructArray> StructArray__from_RecordBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  return ValueOrStop(
      arrow::StructArray::Make(batch->columns(), batch->schema()->field_names()));
}

// [[arrow::export]]
cpp11::list StructArray__Flatten(const std::shared_ptr<arrow::StructArray>& array) {
  return arrow::r::to_r_list(ValueOrStop(array->Flatten()));
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
r_vec_size LargeListArray__value_length(
    const std::shared_ptr<arrow::LargeListArray>& array, int64_t i) {
  return r_vec_size(array->value_length(i));
}

// [[arrow::export]]
r_vec_size FixedSizeListArray__value_length(
    const std::shared_ptr<arrow::FixedSizeListArray>& array, int64_t i) {
  return r_vec_size(array->value_length(i));
}

// [[arrow::export]]
int32_t ListArray__value_offset(const std::shared_ptr<arrow::ListArray>& array,
                                int64_t i) {
  return array->value_offset(i);
}

// [[arrow::export]]
r_vec_size LargeListArray__value_offset(
    const std::shared_ptr<arrow::LargeListArray>& array, int64_t i) {
  return r_vec_size(array->value_offset(i));
}

// [[arrow::export]]
r_vec_size FixedSizeListArray__value_offset(
    const std::shared_ptr<arrow::FixedSizeListArray>& array, int64_t i) {
  return r_vec_size(array->value_offset(i));
}

// [[arrow::export]]
cpp11::writable::integers ListArray__raw_value_offsets(
    const std::shared_ptr<arrow::ListArray>& array) {
  auto offsets = array->raw_value_offsets();
  return cpp11::writable::integers(offsets, offsets + array->length());
}

// [[arrow::export]]
cpp11::writable::integers LargeListArray__raw_value_offsets(
    const std::shared_ptr<arrow::LargeListArray>& array) {
  auto offsets = array->raw_value_offsets();
  return cpp11::writable::integers(offsets, offsets + array->length());
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> MapArray__keys(
    const std::shared_ptr<arrow::MapArray>& array) {
  return array->keys();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> MapArray__items(
    const std::shared_ptr<arrow::MapArray>& array) {
  return array->items();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> MapArray__keys_nested(
    const std::shared_ptr<arrow::MapArray>& array) {
  return ValueOrStop(arrow::ListArray::FromArrays(*(array->offsets()), *(array->keys())));
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> MapArray__items_nested(
    const std::shared_ptr<arrow::MapArray>& array) {
  return ValueOrStop(
      arrow::ListArray::FromArrays(*(array->offsets()), *(array->items())));
}

// [[arrow::export]]
bool Array__Same(const std::shared_ptr<arrow::Array>& x,
                 const std::shared_ptr<arrow::Array>& y) {
  return x.get() == y.get();
}

// [[arrow::export]]
r_vec_size Array__ReferencedBufferSize(const std::shared_ptr<arrow::Array>& x) {
  return r_vec_size(ValueOrStop(arrow::util::ReferencedBufferSize(*x)));
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> arrow__Concatenate(cpp11::list dots) {
  arrow::ArrayVector vector;
  vector.reserve(dots.size());

  for (const cpp11::sexp& item : dots) {
    vector.push_back(cpp11::as_cpp<std::shared_ptr<arrow::Array>>(item));
  }

  return ValueOrStop(arrow::Concatenate(vector));
}

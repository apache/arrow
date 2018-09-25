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
using namespace arrow;

namespace arrow {
namespace r {

template <int RTYPE, typename Vec = Rcpp::Vector<RTYPE>>
class SimpleRBuffer : public arrow::Buffer {
 public:
  SimpleRBuffer(Vec vec)
      : Buffer(reinterpret_cast<const uint8_t*>(vec.begin()),
               vec.size() * sizeof(typename Vec::stored_type)),
        vec_(vec) {}

 private:
  // vec_ holds the memory
  Vec vec_;
};

template <int RTYPE, typename Type>
std::shared_ptr<arrow::Array> SimpleArray(SEXP x) {
  Rcpp::Vector<RTYPE> vec(x);
  std::vector<std::shared_ptr<arrow::Buffer>> buffers{
      nullptr, std::make_shared<SimpleRBuffer<RTYPE>>(vec)};

  int null_count = 0;
  if (RTYPE != RAWSXP) {
    std::shared_ptr<arrow::Buffer> null_bitmap;

    auto first_na = std::find_if(vec.begin(), vec.end(), Rcpp::Vector<RTYPE>::is_na);
    if (first_na < vec.end()) {
      R_ERROR_NOT_OK(arrow::AllocateBuffer(vec.size(), &null_bitmap));
      auto null_bitmap_data = null_bitmap->mutable_data();
      arrow::internal::FirstTimeBitmapWriter bitmap_writer(null_bitmap_data, 0,
                                                           vec.size());

      // first loop to clear all the bits before the first NA
      auto j = std::distance(vec.begin(), first_na);
      int i = 0;
      for (; i < j; i++, bitmap_writer.Next()) {
        bitmap_writer.Set();
      }

      // then finish
      for (; i < vec.size(); i++, bitmap_writer.Next()) {
        if (Rcpp::Vector<RTYPE>::is_na(vec[i])) {
          bitmap_writer.Clear();
          null_count++;
        } else {
          bitmap_writer.Set();
        }
      }

      bitmap_writer.Finish();
      buffers[0] = std::move(null_bitmap);
    }
  }

  auto data = ArrayData::Make(
      std::make_shared<Type>(), LENGTH(x), std::move(buffers), null_count, 0 /*offset*/
  );

  // return the right Array class
  return std::make_shared<arrow::NumericArray<Type>>(data);
}

}  // namespace r
}  // namespace arrow

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__from_vector(SEXP x) {
  switch (TYPEOF(x)) {
    case INTSXP:
      if (Rf_isFactor(x)) {
        break;
      }
      return arrow::r::SimpleArray<INTSXP, arrow::Int32Type>(x);
    case REALSXP:
      // TODO: Dates, ...
      return arrow::r::SimpleArray<REALSXP, arrow::DoubleType>(x);
    case RAWSXP:
      return arrow::r::SimpleArray<RAWSXP, arrow::Int8Type>(x);
    default:
      break;
  }

  stop("not handled");
  return nullptr;
}

template <int RTYPE>
inline SEXP simple_Array_to_Vector(const std::shared_ptr<arrow::Array>& array) {
  using stored_type = typename Rcpp::Vector<RTYPE>::stored_type;
  auto start = reinterpret_cast<const stored_type*>(
      array->data()->buffers[1]->data() + array->offset() * sizeof(stored_type));

  size_t n = array->length();
  Rcpp::Vector<RTYPE> vec(start, start + n);
  if (array->null_count() && RTYPE != RAWSXP) {
    // TODO: not sure what to do with RAWSXP since
    //       R raw vector do not have a concept of missing data

    arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                                array->offset(), n);
    for (size_t i = 0; i < n; i++, bitmap_reader.Next()) {
      if (bitmap_reader.IsNotSet()) {
        vec[i] = Rcpp::Vector<RTYPE>::get_na();
      }
    }
  }

  return vec;
}

// [[Rcpp::export]]
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array) {
  switch (array->type_id()) {
    case Type::INT8:
      return simple_Array_to_Vector<RAWSXP>(array);
    case Type::INT32:
      return simple_Array_to_Vector<INTSXP>(array);
    case Type::DOUBLE:
      return simple_Array_to_Vector<REALSXP>(array);
    default:
      break;
  }

  stop(tfm::format("cannot handle Array of type %d", array->type_id()));
  return R_NilValue;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__Slice1(const std::shared_ptr<arrow::Array>& array,
                                            int offset) {
  return array->Slice(offset);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__Slice2(const std::shared_ptr<arrow::Array>& array,
                                            int offset, int length) {
  return array->Slice(offset, length);
}

// [[Rcpp::export]]
bool Array__IsNull(const std::shared_ptr<arrow::Array>& x, int i) { return x->IsNull(i); }

// [[Rcpp::export]]
bool Array__IsValid(const std::shared_ptr<arrow::Array>& x, int i) {
  return x->IsValid(i);
}

// [[Rcpp::export]]
int Array__length(const std::shared_ptr<arrow::Array>& x) { return x->length(); }

// [[Rcpp::export]]
int Array__offset(const std::shared_ptr<arrow::Array>& x) { return x->offset(); }

// [[Rcpp::export]]
int Array__null_count(const std::shared_ptr<arrow::Array>& x) { return x->null_count(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Array__type(const std::shared_ptr<arrow::Array>& x) {
  return x->type();
}

// [[Rcpp::export]]
std::string Array__ToString(const std::shared_ptr<arrow::Array>& x) {
  return x->ToString();
}

// [[Rcpp::export]]
arrow::Type::type Array__type_id(const std::shared_ptr<arrow::Array>& x) {
  return x->type_id();
}

// [[Rcpp::export]]
bool Array__Equals(const std::shared_ptr<arrow::Array>& lhs,
                   const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->Equals(rhs);
}

// [[Rcpp::export]]
bool Array__ApproxEquals(const std::shared_ptr<arrow::Array>& lhs,
                         const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->ApproxEquals(rhs);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ArrayData> Array__data(
    const std::shared_ptr<arrow::Array>& array) {
  return array->data();
}

// [[Rcpp::export]]
bool Array__RangeEquals(const std::shared_ptr<arrow::Array>& self,
                        const std::shared_ptr<arrow::Array>& other, int start_idx,
                        int end_idx, int other_start_idx) {
  return self->RangeEquals(*other, start_idx, end_idx, other_start_idx);
}

// [[Rcpp::export]]
LogicalVector Array__Mask(const std::shared_ptr<arrow::Array>& array) {
  if (array->null_count() == 0) {
    return LogicalVector(array->length(), true);
  }

  auto n = array->length();
  LogicalVector res(no_init(n));
  arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                              array->offset(), n);
  for (size_t i = 0; i < array->length(); i++, bitmap_reader.Next()) {
    res[i] = bitmap_reader.IsSet();
  }
  return res;
}

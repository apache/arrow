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

template <int RTYPE>
inline SEXP simple_ChunkedArray_to_Vector(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  using value_type = typename Rcpp::Vector<RTYPE>::stored_type;
  Rcpp::Vector<RTYPE> out = no_init(chunked_array->length());
  auto p = out.begin();

  int k = 0;
  for (int i = 0; i < chunked_array->num_chunks(); i++) {
    auto chunk = chunked_array->chunk(i);
    auto n = chunk->length();

    // copy the data
    auto q = p;
    auto p_chunk =
        arrow::r::GetValuesSafely<value_type>(chunk->data(), 1, chunk->offset());
    STOP_IF_NULL(p_chunk);
    p = std::copy_n(p_chunk, n, p);

    // set NA using the bitmap
    auto bitmap_data = chunk->null_bitmap();
    if (bitmap_data && RTYPE != RAWSXP) {
      arrow::internal::BitmapReader bitmap_reader(bitmap_data->data(), chunk->offset(),
                                                  n);

      for (int j = 0; j < n; j++, bitmap_reader.Next()) {
        if (bitmap_reader.IsNotSet()) {
          q[k + j] = Rcpp::Vector<RTYPE>::get_na();
        }
      }
    }

    k += chunk->length();
  }
  return out;
}

// [[Rcpp::export]]
int ChunkedArray__length(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->length();
}

// [[Rcpp::export]]
int ChunkedArray__null_count(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->null_count();
}

// [[Rcpp::export]]
int ChunkedArray__num_chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->num_chunks();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> ChunkedArray__chunk(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int i) {
  return chunked_array->chunk(i);
}

// [[Rcpp::export]]
List ChunkedArray__chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return wrap(chunked_array->chunks());
}

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> ChunkedArray__type(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->type();
}

// [[Rcpp::export]]
SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  switch (chunked_array->type()->id()) {
    case Type::INT8:
      return simple_ChunkedArray_to_Vector<RAWSXP>(chunked_array);
    case Type::INT32:
      return simple_ChunkedArray_to_Vector<INTSXP>(chunked_array);
    case Type::DOUBLE:
      return simple_ChunkedArray_to_Vector<REALSXP>(chunked_array);
    default:
      break;
  }

  stop(tfm::format("cannot handle Array of type %d", chunked_array->type()->id()));
  return R_NilValue;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkArray__Slice1(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int offset) {
  return chunked_array->Slice(offset);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkArray__Slice2(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int offset, int length) {
  return chunked_array->Slice(offset, length);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__from_list(List chunks) {
  std::vector<std::shared_ptr<arrow::Array>> vec;
  for (SEXP chunk : chunks) {
    vec.push_back(Array__from_vector(chunk));
  }
  return std::make_shared<arrow::ChunkedArray>(std::move(vec));
}

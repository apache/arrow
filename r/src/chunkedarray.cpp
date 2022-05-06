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

#include <arrow/builder.h>
#include <arrow/chunked_array.h>
#include <arrow/util/byte_size.h>

// [[arrow::export]]
int ChunkedArray__length(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->length();
}

// [[arrow::export]]
int ChunkedArray__null_count(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->null_count();
}

// [[arrow::export]]
int ChunkedArray__num_chunks(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->num_chunks();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ChunkedArray__chunk(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, int i) {
  arrow::r::validate_index(i, chunked_array->num_chunks());
  return chunked_array->chunk(i);
}

// [[arrow::export]]
cpp11::list ChunkedArray__chunks(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return arrow::r::to_r_list(chunked_array->chunks());
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ChunkedArray__type(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return chunked_array->type();
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Slice1(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, R_xlen_t offset) {
  arrow::r::validate_slice_offset(offset, chunked_array->length());
  return chunked_array->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__Slice2(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array, R_xlen_t offset,
    R_xlen_t length) {
  arrow::r::validate_slice_offset(offset, chunked_array->length());
  arrow::r::validate_slice_length(length, chunked_array->length() - offset);
  return chunked_array->Slice(offset, length);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__View(
    const std::shared_ptr<arrow::ChunkedArray>& array,
    const std::shared_ptr<arrow::DataType>& type) {
  return ValueOrStop(array->View(type));
}

// [[arrow::export]]
void ChunkedArray__Validate(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  StopIfNotOk(chunked_array->Validate());
}

// [[arrow::export]]
bool ChunkedArray__Equals(const std::shared_ptr<arrow::ChunkedArray>& x,
                          const std::shared_ptr<arrow::ChunkedArray>& y) {
  return x->Equals(y);
}

// [[arrow::export]]
std::string ChunkedArray__ToString(const std::shared_ptr<arrow::ChunkedArray>& x) {
  return x->ToString();
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__from_list(cpp11::list chunks,
                                                             SEXP s_type) {
  std::vector<std::shared_ptr<arrow::Array>> vec;

  // the type might be NULL, in which case we need to infer it from the data
  // we keep track of whether it was inferred or supplied
  bool type_inferred = Rf_isNull(s_type);
  R_xlen_t n = XLENGTH(chunks);

  std::shared_ptr<arrow::DataType> type;
  if (type_inferred) {
    if (n == 0) {
      cpp11::stop("type must be specified for empty list");
    }
    type = arrow::r::InferArrowType(VECTOR_ELT(chunks, 0));
  } else {
    type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(s_type);
  }

  if (n == 0) {
    std::shared_ptr<arrow::Array> array;
    std::unique_ptr<arrow::ArrayBuilder> type_builder;
    StopIfNotOk(arrow::MakeBuilder(gc_memory_pool(), type, &type_builder));
    StopIfNotOk(type_builder->Finish(&array));
    vec.push_back(array);
  } else {
    // the first - might differ from the rest of the loop
    // because we might have inferred the type from the first element of the list
    //
    // this only really matters for dictionary arrays
    auto chunked_array =
        arrow::r::vec_to_arrow_ChunkedArray(chunks[0], type, type_inferred);
    for (const auto& chunk : chunked_array->chunks()) {
      vec.push_back(chunk);
    }

    for (R_xlen_t i = 1; i < n; i++) {
      chunked_array = arrow::r::vec_to_arrow_ChunkedArray(chunks[i], type, false);
      for (const auto& chunk : chunked_array->chunks()) {
        vec.push_back(chunk);
      }
    }
  }

  // Use Make so we validate that chunk types are all the same
  return ValueOrStop(arrow::ChunkedArray::Make(std::move(vec)));
}

// [[arrow::export]]
int64_t ChunkedArray__ReferencedBufferSize(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return ValueOrStop(arrow::util::ReferencedBufferSize(*chunked_array));
}

#endif

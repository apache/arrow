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

#pragma once

#include <cpp11/R.hpp>

#include "./arrow_cpp11.h"

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/buffer.h>  // for RBuffer definition below
#include <arrow/c/bridge.h>
#include <arrow/result.h>
#include <arrow/type_fwd.h>

#include <limits>
#include <memory>
#include <utility>
#include <vector>

SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array);
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array);
std::shared_ptr<arrow::Array> Array__from_vector(SEXP x, SEXP type);
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays(SEXP, SEXP);

namespace arrow {

static inline void StopIfNotOk(const Status& status) {
  if (!(status.ok())) {
    cpp11::stop(status.ToString());
  }
}

template <typename R>
auto ValueOrStop(R&& result) -> decltype(std::forward<R>(result).ValueOrDie()) {
  StopIfNotOk(result.status());
  return std::forward<R>(result).ValueOrDie();
}

namespace r {

std::shared_ptr<arrow::DataType> InferArrowType(SEXP x);

Status count_fields(SEXP lst, int* out);

std::shared_ptr<arrow::Array> Array__from_vector(
    SEXP x, const std::shared_ptr<arrow::DataType>& type, bool type_inferred);

void inspect(SEXP obj);

// the integer64 sentinel
constexpr int64_t NA_INT64 = std::numeric_limits<int64_t>::min();

template <typename RVector>
class RBuffer : public MutableBuffer {
 public:
  explicit RBuffer(RVector vec)
      : MutableBuffer(reinterpret_cast<uint8_t*>(DATAPTR(vec)),
                      vec.size() * sizeof(typename RVector::value_type)),
        vec_(vec) {}

 private:
  // vec_ holds the memory
  RVector vec_;
};

std::shared_ptr<arrow::DataType> InferArrowTypeFromFactor(SEXP);

void validate_slice_offset(R_xlen_t offset, int64_t len);

void validate_slice_length(R_xlen_t length, int64_t available);

void validate_index(int i, int len);

template <typename Lambda>
void TraverseDots(cpp11::list dots, int num_fields, Lambda lambda) {
  cpp11::strings names(dots.attr(R_NamesSymbol));

  for (R_xlen_t i = 0, j = 0; j < num_fields; i++) {
    auto name_i = names[i];

    if (name_i.size() == 0) {
      cpp11::list x_i = dots[i];
      cpp11::strings names_x_i(x_i.attr(R_NamesSymbol));
      R_xlen_t n_i = x_i.size();
      for (R_xlen_t k = 0; k < n_i; k++, j++) {
        lambda(j, x_i[k], names_x_i[k]);
      }
    } else {
      lambda(j, dots[i], name_i);
      j++;
    }
  }
}

arrow::Status InferSchemaFromDots(SEXP lst, SEXP schema_sxp, int num_fields,
                                  std::shared_ptr<arrow::Schema>& schema);

arrow::Status AddMetadataFromDots(SEXP lst, int num_fields,
                                  std::shared_ptr<arrow::Schema>& schema);

}  // namespace r
}  // namespace arrow

#endif

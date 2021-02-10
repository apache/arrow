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

#include <memory>

#include "./arrow_types.h"
#include "./arrow_vctrs.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/array/array_base.h>
#include <arrow/builder.h>
#include <arrow/chunked_array.h>
#include <arrow/util/bitmap_writer.h>
#include <arrow/visitor_inline.h>

using arrow::internal::checked_cast;

namespace arrow {
namespace r {

template <typename T>
inline bool is_na(T value) {
  return false;
}

template <>
inline bool is_na<int64_t>(int64_t value) {
  return value == NA_INT64;
}

template <>
inline bool is_na<double>(double value) {
  return ISNA(value);
}

template <>
inline bool is_na<int>(int value) {
  return value == NA_INTEGER;
}

template <typename T>
int64_t time_cast(T value);

template <>
inline int64_t time_cast<int>(int value) {
  return static_cast<int64_t>(value) * 1000;
}

template <>
inline int64_t time_cast<double>(double value) {
  return static_cast<int64_t>(value * 1000);
}

}  // namespace r
}  // namespace arrow

// ---------------- new api

namespace arrow {

namespace internal {

template <typename T, typename Target,
          typename std::enable_if<std::is_signed<Target>::value, Target>::type = 0>
Status int_cast(T x, Target* out) {
  if (static_cast<int64_t>(x) < std::numeric_limits<Target>::min() ||
      static_cast<int64_t>(x) > std::numeric_limits<Target>::max()) {
    return Status::Invalid("Value is too large to fit in C integer type");
  }
  *out = static_cast<Target>(x);
  return Status::OK();
}

template <typename T>
struct usigned_type;

template <typename T, typename Target,
          typename std::enable_if<std::is_unsigned<Target>::value, Target>::type = 0>
Status int_cast(T x, Target* out) {
  // we need to compare between unsigned integers
  uint64_t x64 = x;
  if (x64 < 0 || x64 > std::numeric_limits<Target>::max()) {
    return Status::Invalid("Value is too large to fit in C integer type");
  }
  *out = static_cast<Target>(x);
  return Status::OK();
}

template <typename Int>
Status double_cast(Int x, double* out) {
  *out = static_cast<double>(x);
  return Status::OK();
}

template <>
Status double_cast<int64_t>(int64_t x, double* out) {
  constexpr int64_t kDoubleMax = 1LL << 53;
  constexpr int64_t kDoubleMin = -(1LL << 53);

  if (x < kDoubleMin || x > kDoubleMax) {
    return Status::Invalid("integer value ", x, " is outside of the range exactly",
                           " representable by a IEEE 754 double precision value");
  }
  *out = static_cast<double>(x);
  return Status::OK();
}

// used for int and int64_t
template <typename T>
Status float_cast(T x, float* out) {
  constexpr int64_t kHalfFloatMax = 1LL << 24;
  constexpr int64_t kHalfFloatMin = -(1LL << 24);

  int64_t x64 = static_cast<int64_t>(x);
  if (x64 < kHalfFloatMin || x64 > kHalfFloatMax) {
    return Status::Invalid("integer value ", x, " is outside of the range exactly",
                           " representable by a IEEE 754 half precision value");
  }

  *out = static_cast<float>(x);
  return Status::OK();
}

template <>
Status float_cast<double>(double x, float* out) {
  // TODO: is there some sort of floating point overflow ?
  *out = static_cast<float>(x);
  return Status::OK();
}

}  // namespace internal

}  // namespace arrow

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
    vec.push_back(arrow::r::vec_to_arrow(chunks[0], type, type_inferred));

    for (R_xlen_t i = 1; i < n; i++) {
      vec.push_back(arrow::r::vec_to_arrow(chunks[i], type, false));
    }
  }

  return std::make_shared<arrow::ChunkedArray>(std::move(vec));
}

#endif

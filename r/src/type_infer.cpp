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
#include <arrow/chunked_array.h>

namespace arrow {
namespace r {

static inline std::shared_ptr<arrow::DataType> IndexTypeForFactors(int n_factors) {
  if (n_factors < INT8_MAX) {
    return arrow::int8();
  } else if (n_factors < INT16_MAX) {
    return arrow::int16();
  } else {
    return arrow::int32();
  }
}

std::shared_ptr<arrow::DataType> InferArrowTypeFromFactor(SEXP factor) {
  SEXP factors = Rf_getAttrib(factor, R_LevelsSymbol);
  auto index_type = IndexTypeForFactors(Rf_length(factors));
  bool is_ordered = Rf_inherits(factor, "ordered");
  return dictionary(index_type, arrow::utf8(), is_ordered);
}

template <int VectorType>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector(SEXP x) {
  cpp11::stop("Unknown vector type: ", VectorType);
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<ENVSXP>(SEXP x) {
  if (Rf_inherits(x, "Array")) {
    return cpp11::as_cpp<std::shared_ptr<arrow::Array>>(x)->type();
  }

  cpp11::stop("Unrecognized vector instance for type ENVSXP");
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<LGLSXP>(SEXP x) {
  return Rf_inherits(x, "vctrs_unspecified") ? null() : boolean();
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<INTSXP>(SEXP x) {
  if (Rf_isFactor(x)) {
    return InferArrowTypeFromFactor(x);
  } else if (Rf_inherits(x, "Date")) {
    return date32();
  } else if (Rf_inherits(x, "POSIXct")) {
    auto tzone_sexp = Rf_getAttrib(x, symbols::tzone);
    if (Rf_isNull(tzone_sexp)) {
      return timestamp(TimeUnit::MICRO);
    } else {
      return timestamp(TimeUnit::MICRO, CHAR(STRING_ELT(tzone_sexp, 0)));
    }
  }
  return int32();
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<REALSXP>(SEXP x) {
  if (Rf_inherits(x, "Date")) {
    return date32();
  }
  if (Rf_inherits(x, "POSIXct")) {
    auto tzone_sexp = Rf_getAttrib(x, symbols::tzone);
    if (Rf_isNull(tzone_sexp)) {
      return timestamp(TimeUnit::MICRO);
    } else {
      return timestamp(TimeUnit::MICRO, CHAR(STRING_ELT(tzone_sexp, 0)));
    }
  }
  if (Rf_inherits(x, "integer64")) {
    return int64();
  }
  if (Rf_inherits(x, "difftime")) {
    return time32(TimeUnit::SECOND);
  }
  return float64();
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<STRSXP>(SEXP x) {
  return cpp11::unwind_protect([&] {
    R_xlen_t n = XLENGTH(x);

    int64_t size = 0;

    for (R_xlen_t i = 0; i < n; i++) {
      size += arrow::r::unsafe::r_string_size(STRING_ELT(x, i));
      if (size > arrow::kBinaryMemoryLimit) {
        // Exceeds 2GB capacity of utf8 type, so use large
        return large_utf8();
      }
    }

    return utf8();
  });
}

static inline std::shared_ptr<arrow::DataType> InferArrowTypeFromDataFrame(
    cpp11::list x) {
  R_xlen_t n = x.size();
  cpp11::strings names(x.attr(R_NamesSymbol));
  std::vector<std::shared_ptr<arrow::Field>> fields(n);
  for (R_xlen_t i = 0; i < n; i++) {
    fields[i] = arrow::field(names[i], InferArrowType(x[i]));
  }
  return arrow::struct_(std::move(fields));
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<VECSXP>(SEXP x) {
  if (Rf_inherits(x, "data.frame") || Rf_inherits(x, "POSIXlt")) {
    return InferArrowTypeFromDataFrame(x);
  } else {
    // some known special cases
    if (Rf_inherits(x, "arrow_fixed_size_binary")) {
      SEXP byte_width = Rf_getAttrib(x, symbols::byte_width);
      if (Rf_isNull(byte_width) || TYPEOF(byte_width) != INTSXP ||
          XLENGTH(byte_width) != 1) {
        cpp11::stop("malformed arrow_fixed_size_binary object");
      }
      return arrow::fixed_size_binary(INTEGER(byte_width)[0]);
    }

    if (Rf_inherits(x, "arrow_binary")) {
      return arrow::binary();
    }

    if (Rf_inherits(x, "arrow_large_binary")) {
      return arrow::large_binary();
    }

    SEXP ptype = Rf_getAttrib(x, symbols::ptype);
    if (Rf_isNull(ptype)) {
      if (XLENGTH(x) == 0) {
        cpp11::stop(
            "Requires at least one element to infer the values' type of a list vector");
      }

      ptype = VECTOR_ELT(x, 0);
    }

    return arrow::list(InferArrowType(ptype));
  }
}

std::shared_ptr<arrow::DataType> InferArrowType(SEXP x) {
  if (arrow::r::altrep::is_arrow_altrep(x)) {
    return arrow::r::altrep::vec_to_arrow_altrep_bypass(x)->type();
  }

  switch (TYPEOF(x)) {
    case ENVSXP:
      return InferArrowTypeFromVector<ENVSXP>(x);
    case LGLSXP:
      return InferArrowTypeFromVector<LGLSXP>(x);
    case INTSXP:
      return InferArrowTypeFromVector<INTSXP>(x);
    case REALSXP:
      return InferArrowTypeFromVector<REALSXP>(x);
    case RAWSXP:
      return uint8();
    case STRSXP:
      return InferArrowTypeFromVector<STRSXP>(x);
    case VECSXP:
      return InferArrowTypeFromVector<VECSXP>(x);
    default:
      break;
  }

  cpp11::stop("Cannot infer type from vector");
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Array__infer_type(SEXP x) {
  return arrow::r::InferArrowType(x);
}

#endif

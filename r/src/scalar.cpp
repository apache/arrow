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
#include <arrow/scalar.h>

// [[arrow::export]]
std::shared_ptr<arrow::Scalar> Scalar__create(SEXP x) {
  switch (TYPEOF(x)) {
    case NILSXP:
      return std::make_shared<arrow::NullScalar>();

    case LGLSXP:
      return std::make_shared<arrow::BooleanScalar>(Rf_asLogical(x));

    case REALSXP:
      if (Rf_inherits(x, "Date")) {
        return std::make_shared<arrow::Date32Scalar>(REAL(x)[0]);
      }

      if (Rf_inherits(x, "POSIXct")) {
        return std::make_shared<arrow::TimestampScalar>(
            REAL(x)[0], arrow::timestamp(arrow::TimeUnit::SECOND));
      }

      if (Rf_inherits(x, "integer64")) {
        int64_t value = *reinterpret_cast<int64_t*>(REAL(x));
        return std::make_shared<arrow::Int64Scalar>(value);
      }

      if (Rf_inherits(x, "difftime")) {
        int multiplier = 0;
        // TODO: shared with TimeConverter<> in array_from_vector.cpp
        std::string unit(CHAR(STRING_ELT(Rf_getAttrib(x, arrow::r::symbols::units), 0)));
        if (unit == "secs") {
          multiplier = 1;
        } else if (unit == "mins") {
          multiplier = 60;
        } else if (unit == "hours") {
          multiplier = 3600;
        } else if (unit == "days") {
          multiplier = 86400;
        } else if (unit == "weeks") {
          multiplier = 604800;
        } else {
          Rcpp::stop("unknown difftime unit");
        }
        return std::make_shared<arrow::Time32Scalar>(
            static_cast<int>(REAL(x)[0] * multiplier),
            arrow::time32(arrow::TimeUnit::SECOND));
      }

      return std::make_shared<arrow::DoubleScalar>(Rf_asReal(x));

    case INTSXP:
      if (Rf_inherits(x, "factor")) {
        // TODO: This does not use the actual value, just the levels
        auto type = arrow::r::InferArrowTypeFromFactor(x);
        return std::make_shared<arrow::DictionaryScalar>(type);
      }

      return std::make_shared<arrow::Int32Scalar>(Rf_asInteger(x));

    case STRSXP:
      return std::make_shared<arrow::StringScalar>(CHAR(STRING_ELT(x, 0)));

    default:
      break;
  }

  Rcpp::stop(tfm::format("R object of type %s not supported", Rf_type2char(TYPEOF(x))));
}

// [[arrow::export]]
std::string Scalar__ToString(const std::shared_ptr<arrow::Scalar>& s) {
  return s->ToString();
}

// [[arrow::export]]
std::shared_ptr<arrow::Scalar> Scalar__CastTo(const std::shared_ptr<arrow::Scalar>& s,
                                              const std::shared_ptr<arrow::DataType>& t) {
  return ValueOrStop(s->CastTo(t));
}

// [[arrow::export]]
SEXP Scalar__as_vector(const std::shared_ptr<arrow::Scalar>& scalar) {
  auto array = ValueOrStop(arrow::MakeArrayFromScalar(*scalar, 1));

  // defined in array_to_vector.cpp
  SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array);
  return Array__as_vector(array);
}

// [[arrow::export]]
bool Scalar__is_valid(const std::shared_ptr<arrow::Scalar>& s) { return s->is_valid; }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Scalar__type(const std::shared_ptr<arrow::Scalar>& s) {
  return s->type;
}

#endif

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

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__field_ref(std::string name) {
  return ds::field_ref(std::move(name));
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__not_equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::not_equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__greater(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::greater(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__greater_equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::greater_equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__less(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::less(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__less_equal(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::less_equal(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__in(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->In(rhs).Copy();
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__and(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::and_(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__or(
    const std::shared_ptr<ds::Expression>& lhs,
    const std::shared_ptr<ds::Expression>& rhs) {
  return ds::or_(lhs, rhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__not(
    const std::shared_ptr<ds::Expression>& lhs) {
  return ds::not_(lhs);
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__is_valid(
    const std::shared_ptr<ds::Expression>& lhs) {
  return lhs->IsValid().Copy();
}

// [[arrow::export]]
std::shared_ptr<ds::Expression> dataset___expr__scalar(SEXP x) {
  switch (TYPEOF(x)) {
    case NILSXP:
      return ds::scalar(std::make_shared<arrow::NullScalar>());
    case LGLSXP:
      return ds::scalar(Rf_asLogical(x));
    case REALSXP:
      if (Rf_inherits(x, "Date")) {
        return ds::scalar(std::make_shared<arrow::Date32Scalar>(REAL(x)[0]));
      } else if (Rf_inherits(x, "POSIXct")) {
        return ds::scalar(std::make_shared<arrow::TimestampScalar>(
            REAL(x)[0], arrow::timestamp(arrow::TimeUnit::SECOND)));
      } else if (Rf_inherits(x, "integer64")) {
        int64_t value = *reinterpret_cast<int64_t*>(REAL(x));
        return ds::scalar(value);
      } else if (Rf_inherits(x, "difftime")) {
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
        return ds::scalar(std::make_shared<arrow::Time32Scalar>(
            static_cast<int>(REAL(x)[0] * multiplier),
            arrow::time32(arrow::TimeUnit::SECOND)));
      }
      return ds::scalar(Rf_asReal(x));
    case INTSXP:
      if (Rf_inherits(x, "factor")) {
        // TODO: This does not use the actual value, just the levels
        auto type = arrow::r::InferArrowTypeFromFactor(x);
        return ds::scalar(std::make_shared<arrow::DictionaryScalar>(type));
      }
      return ds::scalar(Rf_asInteger(x));
    case STRSXP:
      return ds::scalar(CHAR(STRING_ELT(x, 0)));
    default:
      Rcpp::stop(
          tfm::format("R object of type %s not supported", Rf_type2char(TYPEOF(x))));
  }

  return nullptr;
}

// [[arrow::export]]
std::string dataset___expr__ToString(const std::shared_ptr<ds::Expression>& x) {
  return x->ToString();
}

#endif

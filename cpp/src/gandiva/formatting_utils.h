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

#include "arrow/type.h"
#include "arrow/util/formatting.h"
#include "arrow/vendored/double-conversion/double-conversion.h"

#ifndef ARROW_SRC_GANDIVA_FORMATTING_UTILS_H_
#define ARROW_SRC_GANDIVA_FORMATTING_UTILS_H_

namespace gandiva {

/// \brief The entry point for conversion to strings.
template <typename ARROW_TYPE, typename Enable = void>
class GdvStringFormatter;

using double_conversion::DoubleToStringConverter;

template <typename ARROW_TYPE>
class FloatToStringGdvMixin
    : public arrow::internal::FloatToStringFormatterMixin<ARROW_TYPE> {
 public:
  using arrow::internal::FloatToStringFormatterMixin<
      ARROW_TYPE>::FloatToStringFormatterMixin;

  explicit FloatToStringGdvMixin(const std::shared_ptr<arrow::DataType>& = NULLPTR)
      : arrow::internal::FloatToStringFormatterMixin<ARROW_TYPE>(
            DoubleToStringConverter::EMIT_TRAILING_ZERO_AFTER_POINT |
                DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT,
            "inf", "nan", 'E', -3, 7, 3, 1) {}
};

template <>
class GdvStringFormatter<arrow::FloatType>
    : public FloatToStringGdvMixin<arrow::FloatType> {
 public:
  using FloatToStringGdvMixin::FloatToStringGdvMixin;
};

template <>
class GdvStringFormatter<arrow::DoubleType>
    : public FloatToStringGdvMixin<arrow::DoubleType> {
 public:
  using FloatToStringGdvMixin::FloatToStringGdvMixin;
};
}  // namespace gandiva
#endif  // ARROW_SRC_GANDIVA_FORMATTING_UTILS_H_

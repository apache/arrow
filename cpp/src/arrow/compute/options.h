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

#include <memory>
#include <utility>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;

namespace compute {

struct ARROW_EXPORT FunctionOptions {};

struct ARROW_EXPORT CastOptions : public FunctionOptions {
  CastOptions()
      : allow_int_overflow(false),
        allow_time_truncate(false),
        allow_time_overflow(false),
        allow_decimal_truncate(false),
        allow_float_truncate(false),
        allow_invalid_utf8(false) {}

  explicit CastOptions(bool safe)
      : allow_int_overflow(!safe),
        allow_time_truncate(!safe),
        allow_time_overflow(!safe),
        allow_decimal_truncate(!safe),
        allow_float_truncate(!safe),
        allow_invalid_utf8(!safe) {}

  static CastOptions Safe() { return CastOptions(true); }

  static CastOptions Unsafe() { return CastOptions(false); }

  // Type being casted to. May be passed separate to eager function
  // compute::Cast
  std::shared_ptr<DataType> to_type;

  bool allow_int_overflow;
  bool allow_time_truncate;
  bool allow_time_overflow;
  bool allow_decimal_truncate;
  bool allow_float_truncate;
  // Indicate if conversions from Binary/FixedSizeBinary to string must
  // validate the utf8 payload.
  bool allow_invalid_utf8;
};

enum CompareOperator {
  EQUAL,
  NOT_EQUAL,
  GREATER,
  GREATER_EQUAL,
  LESS,
  LESS_EQUAL,
};

struct CompareOptions : public FunctionOptions {
  explicit CompareOptions(CompareOperator op) : op(op) {}

  enum CompareOperator op;
};

/// \class CountOptions
///
/// The user control the Count kernel behavior with this class. By default, the
/// it will count all non-null values.
struct ARROW_EXPORT CountOptions : public FunctionOptions {
  enum mode {
    // Count all non-null values.
    COUNT_ALL = 0,
    // Count all null values.
    COUNT_NULL,
  };

  explicit CountOptions(enum mode count_mode) : count_mode(count_mode) {}

  static CountOptions Defaults() { return CountOptions(COUNT_ALL); }

  enum mode count_mode = COUNT_ALL;
};

/// For set lookup operations like IsIn, Match
struct ARROW_EXPORT SetLookupOptions : public FunctionOptions {
  explicit SetLookupOptions(std::shared_ptr<Array> value_set, bool skip_nulls)
      : value_set(std::move(value_set)), skip_nulls(skip_nulls) {}

  std::shared_ptr<Array> value_set;
  bool skip_nulls;
};

struct FilterOptions : public FunctionOptions {
  /// Configure the action taken when a slot of the selection mask is null
  enum NullSelectionBehavior {
    /// the corresponding filtered value will be removed in the output
    DROP,
    /// the corresponding filtered value will be null in the output
    EMIT_NULL,
  };

  static FilterOptions Defaults() { return FilterOptions{}; }

  NullSelectionBehavior null_selection_behavior = DROP;
};

struct ARROW_EXPORT TakeOptions : public FunctionOptions {
  static TakeOptions Defaults() { return TakeOptions{}; }
};

/// \class MinMaxOptions
///
/// The user can control the MinMax kernel behavior with this class. By default,
/// it will skip null if there is a null value present.
struct ARROW_EXPORT MinMaxOptions : public FunctionOptions {
  enum mode {
    /// skip null values
    SKIP = 0,
    /// any nulls will result in null output
    OUTPUT_NULL
  };

  explicit MinMaxOptions(enum mode null_handling = SKIP) : null_handling(null_handling) {}

  static MinMaxOptions Defaults() { return MinMaxOptions{}; }

  enum mode null_handling = SKIP;
};

struct PartitionOptions : public FunctionOptions {
  explicit PartitionOptions(int64_t pivot) : pivot(pivot) {}
  int64_t pivot;
};

}  // namespace compute
}  // namespace arrow

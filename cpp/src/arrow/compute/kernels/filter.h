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

#include "arrow/compute/kernel.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace compute {

class FunctionContext;

struct FilterOptions {
  /// Configure the action taken when a slot of the selection mask is null
  enum NullSelectionBehavior {
    /// the corresponding filtered value will be removed in the output
    DROP,
    /// the corresponding filtered value will be null in the output
    EMIT_NULL,
  };

  NullSelectionBehavior null_selection_behavior = DROP;
};

/// \brief Filter with a boolean selection filter
///
/// The output will be populated with values from the input at positions
/// where the selection filter is not 0. Nulls in the filter will be handled
/// based on options.null_selection_behavior.
///
/// For example given values = ["a", "b", "c", null, "e", "f"] and
/// filter = [0, 1, 1, 0, null, 1], the output will be
/// (null_selection_behavior == DROP)      = ["b", "c", "f"]
/// (null_selection_behavior == EMIT_NULL) = ["b", "c", null, "f"]
///
/// \param[in] ctx the FunctionContext
/// \param[in] values array to filter
/// \param[in] filter indicates which values should be filtered out
/// \param[in] options configures null_selection_behavior
/// \param[out] out resulting array
ARROW_EXPORT
Status Filter(FunctionContext* ctx, const Datum& values, const Datum& filter,
              FilterOptions options, Datum* out);

/// \brief BinaryKernel implementing Filter operation
class ARROW_EXPORT FilterKernel : public BinaryKernel {
 public:
  const FilterOptions& options() const { return options_; }

  /// \brief BinaryKernel interface
  ///
  /// delegates to subclasses via Filter()
  Status Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
              Datum* out) override;

  /// \brief output type of this kernel (identical to type of values filtered)
  std::shared_ptr<DataType> out_type() const override { return type_; }

  /// \brief factory for FilterKernels
  ///
  /// \param[in] value_type constructed FilterKernel will support filtering
  ///            values of this type
  /// \param[in] options configures null_selection_behavior
  /// \param[out] out created kernel
  static Status Make(std::shared_ptr<DataType> value_type, FilterOptions options,
                     std::unique_ptr<FilterKernel>* out);

  /// \brief single-array implementation
  virtual Status Filter(FunctionContext* ctx, const Array& values,
                        const BooleanArray& filter, int64_t out_length,
                        std::shared_ptr<Array>* out) = 0;

 protected:
  explicit FilterKernel(std::shared_ptr<DataType> type, FilterOptions options)
      : type_(std::move(type)), options_(options) {}

  std::shared_ptr<DataType> type_;
  FilterOptions options_;
};

}  // namespace compute
}  // namespace arrow

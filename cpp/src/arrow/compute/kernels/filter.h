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

#include "arrow/compute/kernel.h"

namespace arrow {

class Array;
struct Scalar;
class Status;

namespace compute {

class FunctionContext;
struct Datum;

/// FilterFunction is an interface for Filters
///
/// Filters takes an array and emits a selection vector. The selection vector
/// is given in the form of a bitmask as a BooleanArray result.
class ARROW_EXPORT FilterFunction {
 public:
  /// Filter an array with a scalar argument.
  virtual Status Filter(const ArrayData& input, const Scalar& scalar,
                        ArrayData* output) const = 0;

  /// By default, FilterFunction emits a result bitmap.
  virtual std::shared_ptr<DataType> out_type() const { return boolean(); }

  virtual ~FilterFunction() {}
};

/// \brief BinaryKernel bound to a filter function
class ARROW_EXPORT FilterBinaryKernel : public BinaryKernel {
 public:
  explicit FilterBinaryKernel(std::shared_ptr<FilterFunction>& filter)
      : filter_function_(filter) {}

  Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
              Datum* out) override;

  std::shared_ptr<DataType> out_type() const override;

 private:
  std::shared_ptr<FilterFunction> filter_function_;
};

}  // namespace compute
}  // namespace arrow

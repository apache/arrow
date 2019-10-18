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
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace compute {

class FunctionContext;

/// \brief Summarizes two arrays.
///
/// Summarizes two arrays with the same length.
/// The output is an array with same length and type as input.
/// Types of both input arrays should be equal
///
/// For example given lhs = [1, null, 3], rhs = [4, 5, 6], the output
/// will be [5, null, 7]
///
/// \param[in] ctx the FunctionContext
/// \param[in] lhs the first array
/// \param[in] rhs the second array
/// \param[out] result the sum of first and second arrays

ARROW_EXPORT
Status Add(FunctionContext* ctx, const Array& lhs, const Array& rhs,
           std::shared_ptr<Array>* result);

/// \brief BinaryKernel implementing Add operation
class ARROW_EXPORT AddKernel : public BinaryKernel {
 public:
  /// \brief BinaryKernel interface
  ///
  /// delegates to subclasses via Add()
  Status Call(FunctionContext* ctx, const Datum& lhs, const Datum& rhs,
              Datum* out) override = 0;

  /// \brief output type of this kernel
  std::shared_ptr<DataType> out_type() const override = 0;

  /// \brief single-array implementation
  virtual Status Add(FunctionContext* ctx, const std::shared_ptr<Array>& lhs,
                     const std::shared_ptr<Array>& rhs,
                     std::shared_ptr<Array>* result) = 0;

  /// \brief factory for Add
  ///
  /// \param[in] value_type constructed AddKernel
  /// \param[out] out created kernel
  static Status Make(const std::shared_ptr<DataType>& value_type,
                     std::unique_ptr<AddKernel>* out);
};
}  // namespace compute
}  // namespace arrow

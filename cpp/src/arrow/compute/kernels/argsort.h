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

/// \brief Returns the indices that would sort an array.
///
/// Perform an indirect sort of array. The output array will contain
/// indices that would sort an array, which would be the same length
/// as input. Nulls will be stably partitioned to the end of the output.
///
/// For example given values = [null, 1, 3.3, null, 2, 5.3], the output
/// will be [1, 4, 2, 5, 0, 3]
///
/// \param[in] ctx the FunctionContext
/// \param[in] values array to sort
/// \param[out] offsets indices that would sort an array
ARROW_EXPORT
Status Argsort(FunctionContext* ctx, const Array& values,
               std::shared_ptr<Array>* offsets);

/// \brief Returns the indices that would sort an array.
///
/// \param[in] ctx the FunctionContext
/// \param[in] values datum to sort
/// \param[out] offsets indices that would sort an array
ARROW_EXPORT
Status Argsort(FunctionContext* ctx, const Datum& values, Datum* offsets);

/// \brief UnaryKernel implementing Argsort operation
class ARROW_EXPORT ArgsortKernel : public UnaryKernel {
 protected:
  std::shared_ptr<DataType> type_;

 public:
  /// \brief UnaryKernel interface
  ///
  /// delegates to subclasses via Argsort()
  Status Call(FunctionContext* ctx, const Datum& values, Datum* offsets) override = 0;

  /// \brief output type of this kernel
  std::shared_ptr<DataType> out_type() const override { return uint64(); }

  /// \brief single-array implementation
  virtual Status Argsort(FunctionContext* ctx, const std::shared_ptr<Array>& values,
                         std::shared_ptr<Array>* offsets) = 0;

  /// \brief factory for ArgsortKernel
  ///
  /// \param[in] value_type constructed ArgsortKernel will support sorting
  ///            values of this type
  /// \param[out] out created kernel
  static Status Make(const std::shared_ptr<DataType>& value_type,
                     std::unique_ptr<ArgsortKernel>* out);
};

}  // namespace compute
}  // namespace arrow

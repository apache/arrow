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

struct ARROW_EXPORT TakeOptions {
  enum {
    // indices out of bounds will raise an error
    ERROR,
    // indices out of bounds will result in a null value
    TONULL,
    // indices out of bounds is undefined behavior
    UNSAFE
  } out_of_bounds = ERROR;
};

/// \brief Take from one array type to another
/// \param[in] context the FunctionContext
/// \param[in] values array from which to take
/// \param[in] indices which values to take
/// \param[in] options options
/// \param[out] out resulting array
ARROW_EXPORT
Status Take(FunctionContext* context, const Array& values, const Array& indices,
            const TakeOptions& options, std::shared_ptr<Array>* out);

/// \brief BinaryKernel implementing Take operation
class ARROW_EXPORT TakeKernel : public BinaryKernel {
 public:
  explicit TakeKernel(const std::shared_ptr<DataType>& type, TakeOptions options = {})
      : type_(type), options_(options) {}

  Status Call(FunctionContext* ctx, const Datum& values, const Datum& indices,
              Datum* out) override;

  std::shared_ptr<DataType> out_type() const override { return type_; }

 private:
  std::shared_ptr<DataType> type_;
  TakeOptions options_;
};
}  // namespace compute
}  // namespace arrow

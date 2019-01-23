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

#ifndef ARROW_COMPUTE_KERNELS_AGGREGATION_H
#define ARROW_COMPUTE_KERNELS_AGGREGATION_H

#include <memory>
#include <mutex>

#include "arrow/compute/kernel.h"

namespace arrow {

class Array;
class Status;

namespace compute {

struct Datum;
class FunctionContext;

/// \class AggregateState
/// \brief Interface for aggregate kernels.
///
/// An AggregateState separate the concerns of kernel computation and parallel
/// scheduling.
///
/// \code{.cpp}
/// // Loop can run in parallel.
/// for (array: input.chunks()) {
///   RETURN_NOT_OK(state->Consume(ctx, array));
/// }
///
/// return state->Finalize(ctx, out);
/// \endcode
class AggregateState {
 public:
  /// \brief Consume an array.
  ///
  /// \param[in] ctx Function context provided by the user.
  /// \param[in] input Array to consume.
  virtual Status Consume(FunctionContext* ctx, const Array& input) = 0;

  /// \brief Finalize the computation into a Datum.
  ///
  ///
  ///
  /// \param[in] ctx Function context provided by the user.
  /// \param[out] out The output of the function.
  virtual Status Finalize(FunctionContext* ctx, Datum* out) = 0;

  virtual ~AggregateState() {}
};

/// \brief UnaryKernel implemented by an AggregateState
class ARROW_EXPORT AggregateUnaryKernel : public UnaryKernel {
 public:
  explicit AggregateUnaryKernel(AggregateState* state) : state_(state) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override;

 private:
  std::unique_ptr<AggregateState> state_;
};

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_AGGREGATION_H

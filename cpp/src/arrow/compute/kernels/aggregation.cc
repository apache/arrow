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

#include "arrow/compute/kernels/aggregation.h"

#include "arrow/compute/kernels/monoid.h"
#include "arrow/status.h"

namespace arrow {
namespace compute {

Status AggregateUnaryKernel::Call(FunctionContext* ctx, const Datum& input, Datum* out) {
  switch (input.kind()) {
    case Datum::ARRAY:
      RETURN_NOT_OK(state_->Consume(ctx, *input.make_array()));
      break;
    case Datum::CHUNKED_ARRAY: {
      auto chunked = input.chunked_array();
      for (auto& array : chunked->chunks()) {
        RETURN_NOT_OK(state_->Consume(ctx, *array));
      }
    } break;
    default:
      return Status::Invalid(
          "Aggregation Kernel expects an array-like (Array or ChunkedArray) datum");
  }

  return state_->Finalize(ctx, out);
}

}  // namespace compute
}  // namespace arrow

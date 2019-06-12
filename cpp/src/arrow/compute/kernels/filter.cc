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

#include "arrow/compute/kernels/filter.h"

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace compute {

std::shared_ptr<DataType> FilterBinaryKernel::out_type() const {
  return filter_function_->out_type();
}

Status FilterBinaryKernel::Call(FunctionContext* ctx, const Datum& left,
                                const Datum& right, Datum* out) {
  DCHECK(left.type()->Equals(right.type()));

  auto lk = left.kind();
  auto rk = right.kind();
  auto out_array = out->array();

  if (lk == Datum::ARRAY && rk == Datum::SCALAR) {
    auto array = left.array();
    auto scalar = right.scalar();
    return filter_function_->Filter(*array, *scalar, &out_array);
  } else if (lk == Datum::SCALAR && rk == Datum::ARRAY) {
    auto scalar = left.scalar();
    auto array = right.array();
    auto out_array = out->array();
    return filter_function_->Filter(*scalar, *array, &out_array);
  } else if (lk == Datum::ARRAY && rk == Datum::ARRAY) {
    auto lhs = left.array();
    auto rhs = right.array();
    return filter_function_->Filter(*lhs, *rhs, &out_array);
  }

  return Status::Invalid("Invalid datum signature for FilterBinaryKernel");
}

}  // namespace compute
}  // namespace arrow

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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

#include "arrow/compute/kernels/sum.h"

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregation.h"
#include "arrow/compute/kernels/monoid.h"
#include "arrow/compute/kernels/monoid-impl.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

template <typename NumericType>
using SumMonoidType = SumMonoid<typename NumericType::c_type>;

template <typename NumericType>
using SumAggregateState = MonoidAggregateState<NumericType, SumMonoidType<NumericType>>;

DEFINE_GET_MONOID_KERNEL(SumAggregateState)

Status Sum(FunctionContext* ctx, const Datum& value, Datum* out) {
  std::unique_ptr<AggregateUnaryKernel> kernel;

  auto data_type = value.type();
  if (data_type == nullptr)
    return Status::Invalid("Datum must be array-like");
  else if (!is_integer(data_type->id()) && !is_floating(data_type->id()))
    return Status::Invalid("Datum must contain a NumericType");

  RETURN_NOT_OK(GetMonoidAggregateKernel(ctx, *data_type, kernel));

  return kernel->Call(ctx, value, out);
}

Status Sum(FunctionContext* ctx, const Array& array, Datum* out) {
  return Sum(ctx, Datum(array.data()), out);
}

}  // namespace compute
}  // namespace arrow

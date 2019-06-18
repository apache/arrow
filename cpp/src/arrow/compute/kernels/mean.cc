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

#include "arrow/compute/kernels/mean.h"

#include <algorithm>

#include "arrow/compute/kernels/sum-internal.h"

namespace arrow {
namespace compute {

template <typename ArrowType,
          typename SumType = typename FindAccumulatorType<ArrowType>::Type>
struct MeanState {
  using ThisType = MeanState<ArrowType, SumType>;

  ThisType operator+(const ThisType& rhs) const {
    return ThisType(this->count + rhs.count, this->sum + rhs.sum);
  }

  ThisType& operator+=(const ThisType& rhs) {
    this->count += rhs.count;
    this->sum += rhs.sum;

    return *this;
  }

  std::shared_ptr<Scalar> Finalize() const {
    using ScalarType = typename TypeTraits<DoubleType>::ScalarType;

    const bool is_valid = count > 0;
    const double divisor = static_cast<double>(is_valid ? count : 1UL);
    const double mean = static_cast<double>(sum) / divisor;

    return std::make_shared<ScalarType>(mean, is_valid);
  }

  static std::shared_ptr<DataType> out_type() {
    return TypeTraits<DoubleType>::type_singleton();
  }

  size_t count = 0;
  typename SumType::c_type sum = 0;
};

#define MEAN_AGG_FN_CASE(T)                             \
  case T::type_id:                                      \
    return std::static_pointer_cast<AggregateFunction>( \
        std::make_shared<SumAggregateFunction<T, MeanState<T>>>());

std::shared_ptr<AggregateFunction> MakeMeanAggregateFunction(const DataType& type,
                                                             FunctionContext* ctx) {
  switch (type.id()) {
    MEAN_AGG_FN_CASE(UInt8Type);
    MEAN_AGG_FN_CASE(Int8Type);
    MEAN_AGG_FN_CASE(UInt16Type);
    MEAN_AGG_FN_CASE(Int16Type);
    MEAN_AGG_FN_CASE(UInt32Type);
    MEAN_AGG_FN_CASE(Int32Type);
    MEAN_AGG_FN_CASE(UInt64Type);
    MEAN_AGG_FN_CASE(Int64Type);
    MEAN_AGG_FN_CASE(FloatType);
    MEAN_AGG_FN_CASE(DoubleType);
    default:
      return nullptr;
  }

#undef MEAN_AGG_FN_CASE
}

static Status GetMeanKernel(FunctionContext* ctx, const DataType& type,
                            std::shared_ptr<AggregateUnaryKernel>& kernel) {
  std::shared_ptr<AggregateFunction> aggregate = MakeMeanAggregateFunction(type, ctx);
  if (!aggregate) return Status::Invalid("No mean for type ", type);

  kernel = std::make_shared<AggregateUnaryKernel>(aggregate);

  return Status::OK();
}

Status Mean(FunctionContext* ctx, const Datum& value, Datum* out) {
  std::shared_ptr<AggregateUnaryKernel> kernel;

  auto data_type = value.type();
  if (data_type == nullptr)
    return Status::Invalid("Datum must be array-like");
  else if (!is_integer(data_type->id()) && !is_floating(data_type->id()))
    return Status::Invalid("Datum must contain a NumericType");

  RETURN_NOT_OK(GetMeanKernel(ctx, *data_type, kernel));

  return kernel->Call(ctx, value, out);
}

Status Mean(FunctionContext* ctx, const Array& array, Datum* out) {
  return Mean(ctx, array.data(), out);
}

}  // namespace compute
}  // namespace arrow

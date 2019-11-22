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

#include <algorithm>
#include <limits>
#include <utility>

#include "arrow/compute/kernels/aggregate.h"
#include "arrow/compute/kernels/minmax.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

template <typename ArrowType, typename Enable = void>
struct MinMaxState {};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_integer<ArrowType>> {
  using ThisType = MinMaxState<ArrowType>;
  using c_type = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->min = std::min(this->min, rhs.min);
    this->max = std::max(this->max, rhs.max);
    return *this;
  }

  void MergeOne(c_type value) {
    this->min = std::min(this->min, value);
    this->max = std::max(this->max, value);
  }

  c_type min = std::numeric_limits<c_type>::max();
  c_type max = std::numeric_limits<c_type>::min();
};

template <typename ArrowType>
struct MinMaxState<ArrowType, enable_if_floating_point<ArrowType>> {
  using ThisType = MinMaxState<ArrowType>;
  using c_type = typename ArrowType::c_type;

  ThisType& operator+=(const ThisType& rhs) {
    this->min = std::fmin(this->min, rhs.min);
    this->max = std::fmax(this->max, rhs.max);
    return *this;
  }

  void MergeOne(c_type value) {
    this->min = std::fmin(this->min, value);
    this->max = std::fmax(this->max, value);
  }

  c_type min = std::numeric_limits<c_type>::infinity();
  c_type max = -std::numeric_limits<c_type>::infinity();
};

template <typename ArrowType>
class MinMaxAggregateFunction final
    : public AggregateFunctionStaticState<MinMaxState<ArrowType>> {
 public:
  using StateType = MinMaxState<ArrowType>;

  explicit MinMaxAggregateFunction(const MinMaxOptions& options) : options_(options) {}

  Status Consume(const Array& array, StateType* state) const override {
    StateType local;

    internal::BitmapReader reader(array.null_bitmap_data(), array.offset(),
                                  array.length());
    const auto values =
        checked_cast<const typename TypeTraits<ArrowType>::ArrayType&>(array)
            .raw_values();
    for (int64_t i = 0; i < array.length(); i++) {
      if (reader.IsSet()) {
        local.MergeOne(values[i]);
      }
      reader.Next();
    }
    *state = local;

    return Status::OK();
  }

  Status Merge(const StateType& src, StateType* dst) const override {
    *dst += src;
    return Status::OK();
  }

  Status Finalize(const StateType& src, Datum* output) const override {
    *output = Datum({Datum(src.min), Datum(src.max)});
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override {
    return TypeTraits<ArrowType>::type_singleton();
  }

 private:
  MinMaxOptions options_;
};

#define MINMAX_AGG_FN_CASE(T)                           \
  case T::type_id:                                      \
    return std::static_pointer_cast<AggregateFunction>( \
        std::make_shared<MinMaxAggregateFunction<T>>(options));

std::shared_ptr<AggregateFunction> MakeMinMaxAggregateFunction(
    const DataType& type, FunctionContext* ctx, const MinMaxOptions& options) {
  switch (type.id()) {
    MINMAX_AGG_FN_CASE(UInt8Type);
    MINMAX_AGG_FN_CASE(Int8Type);
    MINMAX_AGG_FN_CASE(UInt16Type);
    MINMAX_AGG_FN_CASE(Int16Type);
    MINMAX_AGG_FN_CASE(UInt32Type);
    MINMAX_AGG_FN_CASE(Int32Type);
    MINMAX_AGG_FN_CASE(UInt64Type);
    MINMAX_AGG_FN_CASE(Int64Type);
    MINMAX_AGG_FN_CASE(FloatType);
    MINMAX_AGG_FN_CASE(DoubleType);
    default:
      return nullptr;
  }

#undef MINMAX_AGG_FN_CASE
}

static Status GetMinMaxKernel(FunctionContext* ctx, const DataType& type,
                              const MinMaxOptions& options,
                              std::shared_ptr<AggregateUnaryKernel>& kernel) {
  std::shared_ptr<AggregateFunction> aggregate =
      MakeMinMaxAggregateFunction(type, ctx, options);
  if (!aggregate) return Status::Invalid("No min/max for type ", type);

  kernel = std::make_shared<AggregateUnaryKernel>(aggregate);

  return Status::OK();
}

Status MinMax(FunctionContext* ctx, const MinMaxOptions& options, const Datum& value,
              Datum* out) {
  std::shared_ptr<AggregateUnaryKernel> kernel;

  auto data_type = value.type();
  if (data_type == nullptr) {
    return Status::Invalid("Datum must be array-like");
  } else if (!is_integer(data_type->id()) && !is_floating(data_type->id())) {
    return Status::Invalid("Datum must contain a NumericType");
  }

  RETURN_NOT_OK(GetMinMaxKernel(ctx, *data_type, options, kernel));

  return kernel->Call(ctx, value, out);
}

Status MinMax(FunctionContext* ctx, const MinMaxOptions& options, const Array& array,
              Datum* out) {
  return MinMax(ctx, options, array.data(), out);
}

}  // namespace compute
}  // namespace arrow

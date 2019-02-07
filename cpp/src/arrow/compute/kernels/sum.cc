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
#include "arrow/compute/kernels/aggregate.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

template <typename CType>
struct SumState {
  SumState<CType> operator+(const SumState<CType>& rhs) const {
    return SumState<CType>(this->count_ + rhs.count_, this->sum_ + rhs.sum_);
  }

  SumState<CType>& operator+=(const SumState<CType>& rhs) {
    this->count_ += rhs.count_;
    this->sum_ += rhs.sum_;

    return *this;
  }

  size_t count_ = 0;
  CType sum_ = 0;
};

// Generated with the following Python code

// output = 'static constexpr uint8_t kBytePopcount[] = {{{0}}};'
// popcounts = [str(bin(i).count('1')) for i in range(0, 256)]
// print(output.format(', '.join(popcounts)))

static constexpr uint8_t kBytePopcount[] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3,
    4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4,
    4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4,
    5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5,
    4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2,
    3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5,
    5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
    5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

template <typename ValueType, typename CType = typename ValueType::c_type>
class SumAggregateFunction final : public AggregateFunctionStaticState<SumState<CType>> {
  using State = SumState<CType>;
  using ArrayType = typename TypeTraits<ValueType>::ArrayType;

 public:
  Status Consume(const Array& input, State* state) const override {
    const ArrayType& array = static_cast<const ArrayType&>(input);

    if (input.null_count() > 0)
      *state = ConsumeSparse(array);
    else
      *state = ConsumeDense(array);

    return Status::OK();
  }

  Status Merge(const State& src, State* dst) const override {
    *dst += src;
    return Status::OK();
  }

  Status Finalize(const State& src, Datum* output) const override {
    *output = (src.count_ > 0) ? Datum(Scalar(src.sum_)) : Datum();
    return Status::OK();
  }

 private:
  State ConsumeDense(const ArrayType& array) const {
    State local;

    const auto values = array.raw_values();
    for (int64_t i = 0; i < array.length(); i++) {
      local.sum_ += values[i];
    }

    local.count_ = array.length();

    return local;
  }

  State ConsumeSparse(const ArrayType& array) const {
    State local;

    // TODO(fsaintjacques): This fails on slice not byte-aligned.
    DCHECK_EQ(array.offset() % 8, 0);

    const auto values = array.raw_values();
    const auto bitmap = array.null_bitmap_data() + BitUtil::RoundDown(array.offset(), 8);
    const auto length = array.length();
    const int64_t length_rounded = BitUtil::RoundDown(length, 8);

    for (int64_t i = 0; i < length_rounded; i += 8) {
      const uint8_t valid_byte = bitmap[i / 8];
      if (valid_byte < 0xFF) {
#define SUM_SHIFT(ITEM) (values[i + ITEM] * ((valid_byte >> ITEM) & 1))
        // Some nulls
        local.sum_ += SUM_SHIFT(0);
        local.sum_ += SUM_SHIFT(1);
        local.sum_ += SUM_SHIFT(2);
        local.sum_ += SUM_SHIFT(3);
        local.sum_ += SUM_SHIFT(4);
        local.sum_ += SUM_SHIFT(5);
        local.sum_ += SUM_SHIFT(6);
        local.sum_ += SUM_SHIFT(7);
        local.count_ += kBytePopcount[valid_byte];
#undef SUM_SHIFT
      } else {
        // No nulls
        local.sum_ += values[i + 0] + values[i + 1] + values[i + 2] + values[i + 3] +
                      values[i + 4] + values[i + 5] + values[i + 6] + values[i + 7];
        local.count_ += 8;
      }
    }

    for (int64_t i = length_rounded; i < length; ++i) {
      if (BitUtil::GetBit(bitmap, i)) {
        local.sum_ += values[i];
        local.count_++;
      }
    }

    return local;
  }
};

#define SUM_AGG_FN_CASE(T)                              \
  case T::type_id:                                      \
    return std::static_pointer_cast<AggregateFunction>( \
        std::make_shared<SumAggregateFunction<T>>());

std::shared_ptr<AggregateFunction> MakeSumAggregateFunction(const DataType& type,
                                                            FunctionContext* ctx) {
  switch (type.id()) {
    SUM_AGG_FN_CASE(UInt8Type);
    SUM_AGG_FN_CASE(Int8Type);
    SUM_AGG_FN_CASE(UInt16Type);
    SUM_AGG_FN_CASE(Int16Type);
    SUM_AGG_FN_CASE(UInt32Type);
    SUM_AGG_FN_CASE(Int32Type);
    SUM_AGG_FN_CASE(UInt64Type);
    SUM_AGG_FN_CASE(Int64Type);
    SUM_AGG_FN_CASE(FloatType);
    SUM_AGG_FN_CASE(DoubleType);
    default:
      return nullptr;
  }

#undef SUM_AGG_FN_CASE
}

static Status GetSumKernel(FunctionContext* ctx, const DataType& type,
                           std::shared_ptr<AggregateUnaryKernel>& kernel) {
  std::shared_ptr<AggregateFunction> aggregate = MakeSumAggregateFunction(type, ctx);
  if (!aggregate) return Status::Invalid("No sum for type ", type);

  kernel = std::make_shared<AggregateUnaryKernel>(aggregate);

  return Status::OK();
}

Status Sum(FunctionContext* ctx, const Datum& value, Datum* out) {
  std::shared_ptr<AggregateUnaryKernel> kernel;

  auto data_type = value.type();
  if (data_type == nullptr)
    return Status::Invalid("Datum must be array-like");
  else if (!is_integer(data_type->id()) && !is_floating(data_type->id()))
    return Status::Invalid("Datum must contain a NumericType");

  RETURN_NOT_OK(GetSumKernel(ctx, *data_type, kernel));

  return kernel->Call(ctx, value, out);
}

Status Sum(FunctionContext* ctx, const Array& array, Datum* out) {
  return Sum(ctx, Datum(array.data()), out);
}

}  // namespace compute
}  // namespace arrow

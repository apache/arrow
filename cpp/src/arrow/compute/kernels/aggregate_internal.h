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

#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {
namespace internal {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, enable_if_boolean<I>> {
  using Type = UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_signed_integer<I>> {
  using Type = Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_unsigned_integer<I>> {
  using Type = UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_floating_point<I>> {
  using Type = DoubleType;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_decimal128<I>> {
  using Type = Decimal128Type;
};

template <typename I>
struct FindAccumulatorType<I, enable_if_decimal256<I>> {
  using Type = Decimal256Type;
};

// Helpers for implementing aggregations on decimals

template <typename Type, typename Enable = void>
struct MultiplyTraits {
  using CType = typename TypeTraits<Type>::CType;

  constexpr static CType one(const DataType&) { return static_cast<CType>(1); }

  constexpr static CType Multiply(const DataType&, CType lhs, CType rhs) {
    return static_cast<CType>(internal::to_unsigned(lhs) * internal::to_unsigned(rhs));
  }
};

template <typename Type>
struct MultiplyTraits<Type, enable_if_decimal<Type>> {
  using CType = typename TypeTraits<Type>::CType;

  constexpr static CType one(const DataType& ty) {
    // Return 1 scaled to output type scale
    return CType(1).IncreaseScaleBy(static_cast<const Type&>(ty).scale());
  }

  constexpr static CType Multiply(const DataType& ty, CType lhs, CType rhs) {
    // Multiply then rescale down to output scale
    return (lhs * rhs).ReduceScaleBy(static_cast<const Type&>(ty).scale());
  }
};

struct ScalarAggregator : public KernelState {
  virtual Status Consume(KernelContext* ctx, const ExecSpan& batch) = 0;
  virtual Status MergeFrom(KernelContext* ctx, KernelState&& src) = 0;
  virtual Status Finalize(KernelContext* ctx, Datum* out) = 0;
};

// Helper to differentiate between var/std calculation so we can fold
// kernel implementations together
enum class VarOrStd : bool { Var, Std };

// Helper to differentiate between first/last calculation so we can fold
// kernel implementations together
enum class FirstOrLast : bool { First, Last };

// Helper to differentiate between min/max calculation so we can fold
// kernel implementations together
enum class MinOrMax : uint8_t { Min = 0, Max };

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func,
                  SimdLevel::type simd_level = SimdLevel::NONE, bool ordered = false);

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFinalize finalize, ScalarAggregateFunction* func,
                  SimdLevel::type simd_level = SimdLevel::NONE, bool ordered = false);

using arrow::internal::VisitSetBitRunsVoid;

template <typename T, typename Enable = void>
struct GetSumType;

template <typename T>
struct GetSumType<T, enable_if_floating_point<T>> {
  using SumType = double;
};

template <typename T>
struct GetSumType<T, enable_if_integer<T>> {
  using SumType = arrow::internal::int128_t;
};

template <typename T>
struct GetSumType<T, enable_if_decimal<T>> {
  using SumType = typename TypeTraits<T>::CType;
};

// SumArray must be parameterized with the SIMD level since it's called both from
// translation units with and without vectorization. Normally it gets inlined but
// if not, without the parameter, we'll have multiple definitions of the same
// symbol and we'll get unexpected results.

// non-recursive pairwise summation for floating points
// https://en.wikipedia.org/wiki/Pairwise_summation
template <typename ValueType, typename SumType, SimdLevel::type SimdLevel,
          typename ValueFunc>
enable_if_t<std::is_floating_point<SumType>::value, SumType> SumArray(
    const ArraySpan& data, ValueFunc&& func) {
  using arrow::internal::VisitSetBitRunsVoid;

  const int64_t data_size = data.length - data.GetNullCount();
  if (data_size == 0) {
    return 0;
  }

  // number of inputs to accumulate before merging with another block
  constexpr int kBlockSize = 16;  // same as numpy
  // levels (tree depth) = ceil(log2(len)) + 1, a bit larger than necessary
  const int levels = bit_util::Log2(static_cast<uint64_t>(data_size)) + 1;
  // temporary summation per level
  std::vector<SumType> sum(levels);
  // whether two summations are ready and should be reduced to upper level
  // one bit for each level, bit0 -> level0, ...
  uint64_t mask = 0;
  // level of root node holding the final summation
  int root_level = 0;

  // reduce summation of one block (may be smaller than kBlockSize) from leaf node
  // continue reducing to upper level if two summations are ready for non-leaf node
  // (capture `levels` by value because of ARROW-17567)
  auto reduce = [&, levels](SumType block_sum) {
    int cur_level = 0;
    uint64_t cur_level_mask = 1ULL;
    sum[cur_level] += block_sum;
    mask ^= cur_level_mask;
    while ((mask & cur_level_mask) == 0) {
      block_sum = sum[cur_level];
      sum[cur_level] = 0;
      ++cur_level;
      DCHECK_LT(cur_level, levels);
      cur_level_mask <<= 1;
      sum[cur_level] += block_sum;
      mask ^= cur_level_mask;
    }
    root_level = std::max(root_level, cur_level);
  };

  const ValueType* values = data.GetValues<ValueType>(1);
  VisitSetBitRunsVoid(data.buffers[0].data, data.offset, data.length,
                      [&](int64_t pos, int64_t len) {
                        const ValueType* v = &values[pos];
                        // unsigned division by constant is cheaper than signed one
                        const uint64_t blocks = static_cast<uint64_t>(len) / kBlockSize;
                        const uint64_t remains = static_cast<uint64_t>(len) % kBlockSize;

                        for (uint64_t i = 0; i < blocks; ++i) {
                          SumType block_sum = 0;
                          for (int j = 0; j < kBlockSize; ++j) {
                            block_sum += func(v[j]);
                          }
                          reduce(block_sum);
                          v += kBlockSize;
                        }

                        if (remains > 0) {
                          SumType block_sum = 0;
                          for (uint64_t i = 0; i < remains; ++i) {
                            block_sum += func(v[i]);
                          }
                          reduce(block_sum);
                        }
                      });

  // reduce intermediate summations from all non-leaf nodes
  for (int i = 1; i <= root_level; ++i) {
    sum[i] += sum[i - 1];
  }

  return sum[root_level];
}

// naive summation for integers and decimals
template <typename ValueType, typename SumType, SimdLevel::type SimdLevel,
          typename ValueFunc>
enable_if_t<!std::is_floating_point<SumType>::value, SumType> SumArray(
    const ArraySpan& data, ValueFunc&& func) {
  using arrow::internal::VisitSetBitRunsVoid;

  SumType sum = 0;
  const ValueType* values = data.GetValues<ValueType>(1);
  VisitSetBitRunsVoid(data.buffers[0].data, data.offset, data.length,
                      [&](int64_t pos, int64_t len) {
                        for (int64_t i = 0; i < len; ++i) {
                          sum += func(values[pos + i]);
                        }
                      });
  return sum;
}

template <typename ValueType, typename SumType, SimdLevel::type SimdLevel>
SumType SumArray(const ArraySpan& data) {
  return SumArray<ValueType, SumType, SimdLevel>(
      data, [](ValueType v) { return static_cast<SumType>(v); });
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

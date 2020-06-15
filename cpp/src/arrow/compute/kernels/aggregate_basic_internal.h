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
#include <utility>
#include <vector>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"

namespace arrow {
namespace compute {
namespace aggregate {

struct ScalarAggregator : public KernelState {
  virtual void Consume(KernelContext* ctx, const ExecBatch& batch) = 0;
  virtual void MergeFrom(KernelContext* ctx, const KernelState& src) = 0;
  virtual void Finalize(KernelContext* ctx, Datum* out) = 0;
};

template <template <typename> class KernelClass>
struct SumLikeInit {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& type;

  SumLikeInit(KernelContext* ctx, const DataType& type) : ctx(ctx), type(type) {}

  Status Visit(const DataType&) { return Status::NotImplemented("No sum implemented"); }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No sum implemented");
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    state.reset(new KernelClass<Type>());
    return Status::OK();
  }

  std::unique_ptr<KernelState> Create() {
    ctx->SetStatus(VisitTypeInline(type, this));
    return std::move(state);
  }
};

// _mm256_load_si256/_mm256_lddqu_si256 take __m256i* as input
#define LOAD_SI256(addr) _mm256_load_si256(reinterpret_cast<const __m256i*>(addr))
#define LOADU_SI256(addr) _mm256_lddqu_si256(reinterpret_cast<const __m256i*>(addr))
// _mm_load_si128/_mm_lddqu_si128 take __m128i* as input
#define LOAD_SI128(addr) _mm_load_si128(reinterpret_cast<const __m128i*>(addr))
#define LOADU_SI128(addr) _mm_lddqu_si128(reinterpret_cast<const __m128i*>(addr))

template <typename T>
struct SumResult {
  T sum = 0;
  size_t count = 0;
};

template <typename T, typename SumT>
inline SumResult<SumT> SumSparseBits(const uint8_t* valid_bits, int64_t valid_bits_offset,
                                     const T* values, int64_t num_values) {
  SumResult<SumT> sum_result;

  BitmapReader reader(valid_bits, valid_bits_offset, num_values);
  for (int64_t i = 0; i < num_values; i++) {
    if (reader.IsSet()) {
      sum_result.sum += values[i];
      sum_result.count++;
    }
    reader.Next();
  }

  return sum_result;
}

template <typename T>
inline T MaskedValue(bool valid, T value) {
  return valid ? value : 0;
}

template <typename T, typename SumT>
inline SumResult<SumT> SumSparseByte(uint8_t bits, const T* values) {
  SumResult<SumT> sum_result;

  if (bits < 0xFF) {
    // Some nulls
    for (size_t i = 0; i < 8; i++) {
      sum_result.sum += MaskedValue(bits & (1U << i), values[i]);
    }
    sum_result.count = BitUtil::kBytePopcount[bits];
  } else {
    // No nulls
    for (size_t i = 0; i < 8; i++) {
      sum_result.sum += values[i];
    }
    sum_result.count = 8;
  }

  return sum_result;
}

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func);

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func);

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func);

}  // namespace aggregate
}  // namespace compute
}  // namespace arrow

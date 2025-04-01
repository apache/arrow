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

#include "arrow/array/data.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"

namespace arrow::compute::internal {

/// C++ abstract base class for the HashAggregateKernel interface.
/// Implementations should be default constructible and perform initialization in
/// Init().
struct GroupedAggregator : KernelState {
  virtual Status Init(ExecContext*, const KernelInitArgs& args) = 0;

  virtual Status Resize(int64_t new_num_groups) = 0;

  virtual Status Consume(const ExecSpan& batch) = 0;

  virtual Status Merge(GroupedAggregator&& other, const ArrayData& group_id_mapping) = 0;

  virtual Result<Datum> Finalize() = 0;

  virtual std::shared_ptr<DataType> out_type() const = 0;
};

template <typename Impl>
Result<std::unique_ptr<KernelState>> HashAggregateInit(KernelContext* ctx,
                                                       const KernelInitArgs& args) {
  auto impl = std::make_unique<Impl>();
  RETURN_NOT_OK(impl->Init(ctx->exec_context(), args));
  // R build with openSUSE155 requires an explicit unique_ptr construction
  return std::unique_ptr<KernelState>(std::move(impl));
}

inline Status HashAggregateResize(KernelContext* ctx, int64_t num_groups) {
  return checked_cast<GroupedAggregator*>(ctx->state())->Resize(num_groups);
}

inline Status HashAggregateConsume(KernelContext* ctx, const ExecSpan& batch) {
  return checked_cast<GroupedAggregator*>(ctx->state())->Consume(batch);
}

inline Status HashAggregateMerge(KernelContext* ctx, KernelState&& other,
                                 const ArrayData& group_id_mapping) {
  return checked_cast<GroupedAggregator*>(ctx->state())
      ->Merge(checked_cast<GroupedAggregator&&>(other), group_id_mapping);
}

inline Status HashAggregateFinalize(KernelContext* ctx, Datum* out) {
  return checked_cast<GroupedAggregator*>(ctx->state())->Finalize().Value(out);
}

inline Result<TypeHolder> ResolveGroupOutputType(KernelContext* ctx,
                                                 const std::vector<TypeHolder>&) {
  return checked_cast<GroupedAggregator*>(ctx->state())->out_type();
}

inline HashAggregateKernel MakeKernel(std::shared_ptr<KernelSignature> signature,
                                      KernelInit init, const bool ordered = false) {
  HashAggregateKernel kernel(std::move(signature), std::move(init), HashAggregateResize,
                             HashAggregateConsume, HashAggregateMerge,
                             HashAggregateFinalize, ordered);
  return kernel;
}

inline HashAggregateKernel MakeKernel(InputType argument_type, KernelInit init,
                                      const bool ordered = false) {
  return MakeKernel(
      KernelSignature::Make({std::move(argument_type), InputType(Type::UINT32)},
                            OutputType(ResolveGroupOutputType)),
      std::move(init), ordered);
}

inline HashAggregateKernel MakeUnaryKernel(KernelInit init) {
  return MakeKernel(KernelSignature::Make({InputType(Type::UINT32)},
                                          OutputType(ResolveGroupOutputType)),
                    std::move(init));
}

using HashAggregateKernelFactory =
    std::function<Result<HashAggregateKernel>(const std::shared_ptr<DataType>&)>;

inline Status AddHashAggKernels(const std::vector<std::shared_ptr<DataType>>& types,
                                HashAggregateKernelFactory make_kernel,
                                HashAggregateFunction* function) {
  for (const auto& ty : types) {
    ARROW_ASSIGN_OR_RAISE(auto kernel, make_kernel(ty));
    RETURN_NOT_OK(function->AddKernel(std::move(kernel)));
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Helpers for more easily implementing hash aggregates

template <typename T>
struct GroupedValueTraits {
  using CType = typename TypeTraits<T>::CType;

  static CType Get(const CType* values, uint32_t g) { return values[g]; }
  static void Set(CType* values, uint32_t g, CType v) { values[g] = v; }
  static Status AppendBuffers(TypedBufferBuilder<CType>* destination,
                              const uint8_t* values, int64_t offset, int64_t num_values) {
    RETURN_NOT_OK(
        destination->Append(reinterpret_cast<const CType*>(values) + offset, num_values));
    return Status::OK();
  }
};
template <>
struct GroupedValueTraits<BooleanType> {
  static bool Get(const uint8_t* values, uint32_t g) {
    return bit_util::GetBit(values, g);
  }
  static void Set(uint8_t* values, uint32_t g, bool v) {
    bit_util::SetBitTo(values, g, v);
  }
  static Status AppendBuffers(TypedBufferBuilder<bool>* destination,
                              const uint8_t* values, int64_t offset, int64_t num_values) {
    RETURN_NOT_OK(destination->Reserve(num_values));
    destination->UnsafeAppend(values, offset, num_values);
    return Status::OK();
  }
};

template <typename Type, typename ConsumeValue, typename ConsumeNull>
typename arrow::internal::call_traits::enable_if_return<ConsumeValue, void>::type
VisitGroupedValues(const ExecSpan& batch, ConsumeValue&& valid_func,
                   ConsumeNull&& null_func) {
  auto g = batch[1].array.GetValues<uint32_t>(1);
  if (batch[0].is_array()) {
    VisitArrayValuesInline<Type>(
        batch[0].array,
        [&](typename TypeTraits<Type>::CType val) { valid_func(*g++, val); },
        [&]() { null_func(*g++); });
    return;
  }
  const Scalar& input = *batch[0].scalar;
  if (input.is_valid) {
    const auto val = UnboxScalar<Type>::Unbox(input);
    for (int64_t i = 0; i < batch.length; i++) {
      valid_func(*g++, val);
    }
  } else {
    for (int64_t i = 0; i < batch.length; i++) {
      null_func(*g++);
    }
  }
}

template <typename Type, typename ConsumeValue, typename ConsumeNull>
typename arrow::internal::call_traits::enable_if_return<ConsumeValue, Status>::type
VisitGroupedValues(const ExecSpan& batch, ConsumeValue&& valid_func,
                   ConsumeNull&& null_func) {
  auto g = batch[1].array.GetValues<uint32_t>(1);
  if (batch[0].is_array()) {
    return VisitArrayValuesInline<Type>(
        batch[0].array,
        [&](typename GetViewType<Type>::T val) { return valid_func(*g++, val); },
        [&]() { return null_func(*g++); });
  }
  const Scalar& input = *batch[0].scalar;
  if (input.is_valid) {
    const auto val = UnboxScalar<Type>::Unbox(input);
    for (int64_t i = 0; i < batch.length; i++) {
      RETURN_NOT_OK(valid_func(*g++, val));
    }
  } else {
    for (int64_t i = 0; i < batch.length; i++) {
      RETURN_NOT_OK(null_func(*g++));
    }
  }
  return Status::OK();
}

template <typename Type, typename ConsumeValue>
void VisitGroupedValuesNonNull(const ExecSpan& batch, ConsumeValue&& valid_func) {
  VisitGroupedValues<Type>(batch, std::forward<ConsumeValue>(valid_func),
                           [](uint32_t) {});
}

}  // namespace arrow::compute::internal

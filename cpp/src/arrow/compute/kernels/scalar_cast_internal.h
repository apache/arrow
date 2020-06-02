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

#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"           // IWYU pragma: export
#include "arrow/compute/cast_internal.h"  // IWYU pragma: export
#include "arrow/compute/kernels/common.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

// No-op functor for identity casts
template <typename O, typename I>
struct CastFunctor<
    O, I, enable_if_t<std::is_same<O, I>::value && is_parameter_free_type<I>::value>> {
  static void Exec(KernelContext*, const ExecBatch&, Datum*) {}
};

void CastFromExtension(KernelContext* ctx, const ExecBatch& batch, Datum* out);

// ----------------------------------------------------------------------
// Dictionary to other things

void UnpackDictionary(KernelContext* ctx, const ExecBatch& batch, Datum* out);

void OutputAllNull(KernelContext* ctx, const ExecBatch& batch, Datum* out);

template <typename T>
struct FromNullCast {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayData* output = out->mutable_array();
    std::shared_ptr<Array> nulls;
    Status s = MakeArrayOfNull(output->type, batch.length).Value(&nulls);
    KERNEL_RETURN_IF_ERROR(ctx, s);
    out->value = nulls->data();
  }
};

// Adds a cast function where the functor is defined and the input and output
// types have a type_singleton
template <typename InType, typename OutType>
void AddSimpleCast(InputType in_ty, OutputType out_ty, CastFunction* func) {
  DCHECK_OK(func->AddKernel(InType::type_id, {in_ty}, out_ty,
                            CastFunctor<OutType, InType>::Exec));
}

void ZeroCopyCastExec(KernelContext* ctx, const ExecBatch& batch, Datum* out);

void AddZeroCopyCast(Type::type in_type_id, InputType in_type, OutputType out_type,
                     CastFunction* func);

// OutputType::Resolver that returns a descr with the shape of the input
// argument and the type from CastOptions
Result<ValueDescr> ResolveOutputFromOptions(KernelContext* ctx,
                                            const std::vector<ValueDescr>& args);

ARROW_EXPORT extern OutputType kOutputTargetType;

template <typename T, typename Enable = void>
struct MaybeAddFromDictionary {
  static void Add(const OutputType& out_ty, CastFunction* func) {}
};

template <typename T>
struct MaybeAddFromDictionary<
    T, enable_if_t<!is_boolean_type<T>::value && !is_nested_type<T>::value &&
                   !is_null_type<T>::value && !std::is_same<DictionaryType, T>::value>> {
  static void Add(const OutputType& out_ty, CastFunction* func) {
    // Dictionary unpacking not implemented for boolean or nested types.
    //
    // XXX: Uses Take and does its own memory allocation for the moment. We can
    // fix this later.
    DCHECK_OK(func->AddKernel(
        Type::DICTIONARY, {InputType::Array(Type::DICTIONARY)}, out_ty, UnpackDictionary,
        NullHandling::COMPUTED_NO_PREALLOCATE, MemAllocation::NO_PREALLOCATE));
  }
};

template <typename OutType>
void AddCommonCasts(OutputType out_ty, CastFunction* func) {
  // From null to this type
  DCHECK_OK(func->AddKernel(Type::NA, {InputType::Array(null())}, out_ty,
                            FromNullCast<OutType>::Exec));

  // From dictionary to this type
  MaybeAddFromDictionary<OutType>::Add(out_ty, func);

  // From extension type to this type
  DCHECK_OK(func->AddKernel(Type::EXTENSION, {InputType::Array(Type::EXTENSION)}, out_ty,
                            CastFromExtension, NullHandling::COMPUTED_NO_PREALLOCATE,
                            MemAllocation::NO_PREALLOCATE));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

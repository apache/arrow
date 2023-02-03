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

#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/compute/cast_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/extension_type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;
using internal::PrimitiveScalarBase;

namespace compute {
namespace internal {

// ----------------------------------------------------------------------

namespace {

template <typename OutType, typename InType, typename Enable = void>
struct CastPrimitive {
  ARROW_DISABLE_UBSAN("float-cast-overflow")
  static void Exec(const ArraySpan& arr, ArraySpan* out) {
    using OutT = typename OutType::c_type;
    using InT = typename InType::c_type;
    const InT* in_values = arr.GetValues<InT>(1);
    OutT* out_values = out->GetValues<OutT>(1);
    for (int64_t i = 0; i < arr.length; ++i) {
      *out_values++ = static_cast<OutT>(*in_values++);
    }
  }
};

template <typename OutType, typename InType>
struct CastPrimitive<OutType, InType, enable_if_t<std::is_same<OutType, InType>::value>> {
  // memcpy output
  static void Exec(const ArraySpan& arr, ArraySpan* out) {
    using T = typename InType::c_type;
    std::memcpy(out->GetValues<T>(1), arr.GetValues<T>(1), arr.length * sizeof(T));
  }
};

template <typename InType>
void CastNumberImpl(Type::type out_type, const ArraySpan& input, ArraySpan* out) {
  switch (out_type) {
    case Type::INT8:
      return CastPrimitive<Int8Type, InType>::Exec(input, out);
    case Type::INT16:
      return CastPrimitive<Int16Type, InType>::Exec(input, out);
    case Type::INT32:
      return CastPrimitive<Int32Type, InType>::Exec(input, out);
    case Type::INT64:
      return CastPrimitive<Int64Type, InType>::Exec(input, out);
    case Type::UINT8:
      return CastPrimitive<UInt8Type, InType>::Exec(input, out);
    case Type::UINT16:
      return CastPrimitive<UInt16Type, InType>::Exec(input, out);
    case Type::UINT32:
      return CastPrimitive<UInt32Type, InType>::Exec(input, out);
    case Type::UINT64:
      return CastPrimitive<UInt64Type, InType>::Exec(input, out);
    case Type::FLOAT:
      return CastPrimitive<FloatType, InType>::Exec(input, out);
    case Type::DOUBLE:
      return CastPrimitive<DoubleType, InType>::Exec(input, out);
    default:
      break;
  }
}

}  // namespace

void CastNumberToNumberUnsafe(Type::type in_type, Type::type out_type,
                              const ArraySpan& input, ArraySpan* out) {
  switch (in_type) {
    case Type::INT8:
      return CastNumberImpl<Int8Type>(out_type, input, out);
    case Type::INT16:
      return CastNumberImpl<Int16Type>(out_type, input, out);
    case Type::INT32:
      return CastNumberImpl<Int32Type>(out_type, input, out);
    case Type::INT64:
      return CastNumberImpl<Int64Type>(out_type, input, out);
    case Type::UINT8:
      return CastNumberImpl<UInt8Type>(out_type, input, out);
    case Type::UINT16:
      return CastNumberImpl<UInt16Type>(out_type, input, out);
    case Type::UINT32:
      return CastNumberImpl<UInt32Type>(out_type, input, out);
    case Type::UINT64:
      return CastNumberImpl<UInt64Type>(out_type, input, out);
    case Type::FLOAT:
      return CastNumberImpl<FloatType>(out_type, input, out);
    case Type::DOUBLE:
      return CastNumberImpl<DoubleType>(out_type, input, out);
    default:
      DCHECK(false);
      break;
  }
}

// ----------------------------------------------------------------------

Status UnpackDictionary(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // TODO: is there an implementation more friendly to the "span" data structures?

  DictionaryArray dict_arr(batch[0].array.ToArrayData());
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;

  const auto& dict_type = *dict_arr.dictionary()->type();
  const DataType& to_type = *options.to_type;
  if (!to_type.Equals(dict_type) && !CanCast(dict_type, to_type)) {
    return Status::Invalid("Cast type ", to_type.ToString(),
                           " incompatible with dictionary type ", dict_type.ToString());
  }

  ARROW_ASSIGN_OR_RAISE(Datum unpacked,
                        Take(dict_arr.dictionary(), dict_arr.indices(),
                             TakeOptions::Defaults(), ctx->exec_context()));
  if (!dict_type.Equals(to_type)) {
    ARROW_ASSIGN_OR_RAISE(unpacked, Cast(unpacked, options));
  }
  out->value = std::move(unpacked.array());
  return Status::OK();
}

Status OutputAllNull(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // TODO(wesm): there is no good reason to have to use ArrayData here, so we
  // should clean this up later. This is used in the dict<null>->null cast
  ArrayData* output = out->array_data().get();
  output->buffers = {nullptr};
  output->null_count = batch.length;
  return Status::OK();
}

Status CastFromExtension(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const CastOptions& options = checked_cast<const CastState*>(ctx->state())->options;

  DCHECK(batch[0].is_array());
  ExtensionArray extension(batch[0].array.ToArrayData());
  std::shared_ptr<Array> result;
  RETURN_NOT_OK(Cast(*extension.storage(), out->type()->GetSharedPtr(), options,
                     ctx->exec_context())
                    .Value(&result));
  out->value = std::move(result->data());
  return Status::OK();
}

Status CastFromNull(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // TODO(wesm): handle this case more gracefully
  std::shared_ptr<Array> nulls;
  RETURN_NOT_OK(MakeArrayOfNull(out->type()->GetSharedPtr(), batch.length).Value(&nulls));
  out->value = nulls->data();
  return Status::OK();
}

Result<TypeHolder> ResolveOutputFromOptions(KernelContext* ctx,
                                            const std::vector<TypeHolder>&) {
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
  return options.to_type;
}

/// You will see some of kernels with
///
/// kOutputTargetType
///
/// for their output type resolution. This is somewhat of an eyesore but the
/// easiest initial way to get the requested cast type including the TimeUnit
/// to the kernel (which is needed to compute the output) was through
/// CastOptions

OutputType kOutputTargetType(ResolveOutputFromOptions);

Status ZeroCopyCastExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // TODO(wesm): alternative strategy for zero copy casts after ARROW-16576
  std::shared_ptr<ArrayData> input = batch[0].array.ToArrayData();
  ArrayData* output = out->array_data().get();
  output->length = input->length;
  output->offset = input->offset;
  output->SetNullCount(input->null_count);
  output->buffers = std::move(input->buffers);
  output->child_data = std::move(input->child_data);
  return Status::OK();
}

void AddZeroCopyCast(Type::type in_type_id, InputType in_type, OutputType out_type,
                     CastFunction* func) {
  auto sig = KernelSignature::Make({in_type}, out_type);
  ScalarKernel kernel;
  kernel.exec = ZeroCopyCastExec;
  kernel.signature = sig;
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(in_type_id, std::move(kernel)));
}

static bool CanCastFromDictionary(Type::type type_id) {
  return (is_primitive(type_id) || is_base_binary_like(type_id) ||
          is_fixed_size_binary(type_id));
}

void AddCommonCasts(Type::type out_type_id, OutputType out_ty, CastFunction* func) {
  // From null to this type
  ScalarKernel kernel;
  kernel.exec = CastFromNull;
  kernel.signature = KernelSignature::Make({null()}, out_ty);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(Type::NA, std::move(kernel)));

  // From dictionary to this type
  if (CanCastFromDictionary(out_type_id)) {
    // Dictionary unpacking not implemented for boolean or nested types.
    //
    // XXX: Uses Take and does its own memory allocation for the moment. We can
    // fix this later.
    DCHECK_OK(func->AddKernel(Type::DICTIONARY, {InputType(Type::DICTIONARY)}, out_ty,
                              UnpackDictionary, NullHandling::COMPUTED_NO_PREALLOCATE,
                              MemAllocation::NO_PREALLOCATE));
  }

  // From extension type to this type
  DCHECK_OK(func->AddKernel(Type::EXTENSION, {InputType(Type::EXTENSION)}, out_ty,
                            CastFromExtension, NullHandling::COMPUTED_NO_PREALLOCATE,
                            MemAllocation::NO_PREALLOCATE));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

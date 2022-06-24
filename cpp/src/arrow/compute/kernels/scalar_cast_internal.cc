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
#include "arrow/compute/kernels/common.h"
#include "arrow/extension_type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;
using internal::PrimitiveScalarBase;

namespace compute {
namespace internal {

// ----------------------------------------------------------------------

namespace {

template <typename OutT, typename InT>
ARROW_DISABLE_UBSAN("float-cast-overflow")
void DoStaticCast(const void* in_data, int64_t in_offset, int64_t length,
                  int64_t out_offset, void* out_data) {
  auto in = reinterpret_cast<const InT*>(in_data) + in_offset;
  auto out = reinterpret_cast<OutT*>(out_data) + out_offset;
  for (int64_t i = 0; i < length; ++i) {
    *out++ = static_cast<OutT>(*in++);
  }
}

using StaticCastFunc = std::function<void(const void*, int64_t, int64_t, int64_t, void*)>;

template <typename OutType, typename InType, typename Enable = void>
struct CastPrimitive {
  static void Exec(const ExecValue& input, ExecResult* out) {
    using OutT = typename OutType::c_type;
    using InT = typename InType::c_type;

    StaticCastFunc caster = DoStaticCast<OutT, InT>;
    if (input.is_array()) {
      const ArraySpan& arr = input.array;
      ArraySpan* out_span = out->array_span();
      caster(arr.buffers[1].data, arr.offset, arr.length, out_span->offset,
             out_span->buffers[1].data);
    } else {
      // Scalar path. Use the caster with length 1 to place the casted value into
      // the output
      const auto& in_scalar = input.scalar_as<PrimitiveScalarBase>();
      auto out_scalar = checked_cast<PrimitiveScalarBase*>(out->scalar().get());
      caster(reinterpret_cast<const void*>(in_scalar.view().data()), /*in_offset=*/0,
             /*length=*/1, /*out_offset=*/0, out_scalar->mutable_data());
    }
  }
};

template <typename OutType, typename InType>
struct CastPrimitive<OutType, InType, enable_if_t<std::is_same<OutType, InType>::value>> {
  // memcpy output
  static void Exec(const ExecValue& input, ExecResult* out) {
    using T = typename InType::c_type;

    if (input.is_array()) {
      const ArraySpan& arr = input.array;
      std::memcpy(out->array_span()->GetValues<T>(1), arr.GetValues<T>(1),
                  arr.length * sizeof(T));
    } else {
      // Scalar path. Use the caster with length 1 to place the casted value into
      // the output
      const auto& in_scalar = input.scalar_as<PrimitiveScalarBase>();
      auto out_scalar = checked_cast<PrimitiveScalarBase*>(out->scalar().get());
      *reinterpret_cast<T*>(out_scalar->mutable_data()) =
          *reinterpret_cast<const T*>(in_scalar.view().data());
    }
  }
};

template <typename InType>
void CastNumberImpl(Type::type out_type, const ExecValue& input, ExecResult* out) {
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
                              const ExecValue& input, ExecResult* out) {
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
  DCHECK(out->is_array_data());

  // TODO: is there an implementation more friendly to the "span" data structures?

  DictionaryArray dict_arr(batch[0].array.ToArrayData());
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;

  const auto& dict_type = *dict_arr.dictionary()->type();
  if (!dict_type.Equals(options.to_type) && !CanCast(dict_type, *options.to_type)) {
    return Status::Invalid("Cast type ", options.to_type->ToString(),
                           " incompatible with dictionary type ", dict_type.ToString());
  }

  Datum take_result;
  ARROW_ASSIGN_OR_RAISE(take_result,
                        Take(Datum(dict_arr.dictionary()), Datum(dict_arr.indices()),
                             TakeOptions::Defaults(), ctx->exec_context()));

  if (!dict_type.Equals(options.to_type)) {
    ARROW_ASSIGN_OR_RAISE(take_result, Cast(take_result, options));
  }
  out->value = std::move(take_result.array());
  return Status::OK();
}

Status OutputAllNull(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  if (out->is_scalar()) {
    out->scalar()->is_valid = false;
  } else {
    // TODO(wesm): there is no good reason to have to use ArrayData here, so we
    // should clean this up later. This is used in the dict<null>->null cast
    DCHECK(out->is_array_data());
    ArrayData* output = out->array_data().get();
    output->buffers = {nullptr};
    output->null_count = batch.length;
  }
  return Status::OK();
}

Status CastFromExtension(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const CastOptions& options = checked_cast<const CastState*>(ctx->state())->options;

  Datum result;
  if (batch[0].is_scalar()) {
    const auto& ext_scalar = checked_cast<const ExtensionScalar&>(*batch[0].scalar);
    if (ext_scalar.is_valid) {
      RETURN_NOT_OK(
          Cast(ext_scalar.value, out->type()->Copy(), options, ctx->exec_context())
              .Value(&result));
    } else {
      const auto& storage_type =
          checked_cast<const ExtensionType&>(*ext_scalar.type).storage_type();
      RETURN_NOT_OK(Cast(MakeNullScalar(storage_type), out->type()->Copy(), options,
                         ctx->exec_context())
                        .Value(&result));
    }
    out->value = std::move(result.scalar());
  } else {
    DCHECK(batch[0].is_array());
    ExtensionArray extension(batch[0].array.ToArrayData());
    std::shared_ptr<Array> result;
    RETURN_NOT_OK(
        Cast(*extension.storage(), out->type()->Copy(), options, ctx->exec_context())
            .Value(&result));
    out->value = std::move(result->data());
  }
  return Status::OK();
}

Status CastFromNull(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // TODO(wesm): handle this case more gracefully
  if (!batch[0].is_scalar()) {
    std::shared_ptr<Array> nulls;
    RETURN_NOT_OK(MakeArrayOfNull(out->type()->Copy(), batch.length).Value(&nulls));
    out->value = nulls->data();
  }
  return Status::OK();
}

Result<ValueDescr> ResolveOutputFromOptions(KernelContext* ctx,
                                            const std::vector<ValueDescr>& args) {
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
  return ValueDescr(options.to_type, args[0].shape);
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
  DCHECK(batch[0].is_array());
  DCHECK(out->is_array_data());
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
  kernel.exec = TrivialScalarUnaryAsArraysExec(ZeroCopyCastExec,
                                               /*use_array_span=*/false);
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
                              TrivialScalarUnaryAsArraysExec(UnpackDictionary,
                                                             /*use_array_span=*/false),
                              NullHandling::COMPUTED_NO_PREALLOCATE,
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

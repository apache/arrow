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
#include <vector>

#include "arrow/builder.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/cast_internal.h"
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

template <typename T, typename IndexType, typename Enable = void>
struct FromDictVisitor {};

// Visitor for Dict<FixedSizeBinaryType>
template <typename T, typename IndexType>
struct FromDictVisitor<T, IndexType, enable_if_fixed_size_binary<T>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  FromDictVisitor(KernelContext* ctx, const ArrayType& dictionary, ArrayData* output)
      : dictionary_(dictionary),
        byte_width_(dictionary.byte_width()),
        out_(output->buffers[1]->mutable_data() + byte_width_ * output->offset) {}

  Status Init() { return Status::OK(); }

  Status VisitNull() {
    memset(out_, 0, byte_width_);
    out_ += byte_width_;
    return Status::OK();
  }

  Status VisitValue(typename IndexType::c_type dict_index) {
    const uint8_t* value = dictionary_.Value(dict_index);
    memcpy(out_, value, byte_width_);
    out_ += byte_width_;
    return Status::OK();
  }

  Status Finish() { return Status::OK(); }

  const ArrayType& dictionary_;
  int32_t byte_width_;
  uint8_t* out_;
};

// Visitor for Dict<BinaryType>
template <typename T, typename IndexType>
struct FromDictVisitor<T, IndexType, enable_if_base_binary<T>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  FromDictVisitor(KernelContext* ctx, const ArrayType& dictionary, ArrayData* output)
      : ctx_(ctx), dictionary_(dictionary), output_(output) {}

  Status Init() {
    RETURN_NOT_OK(MakeBuilder(ctx_->memory_pool(), output_->type, &builder_));
    binary_builder_ = checked_cast<BinaryBuilder*>(builder_.get());
    return Status::OK();
  }

  Status VisitNull() { return binary_builder_->AppendNull(); }

  Status VisitValue(typename IndexType::c_type dict_index) {
    return binary_builder_->Append(dictionary_.GetView(dict_index));
  }

  Status Finish() {
    std::shared_ptr<Array> plain_array;
    RETURN_NOT_OK(binary_builder_->Finish(&plain_array));
    output_->buffers = plain_array->data()->buffers;
    return Status::OK();
  }

  KernelContext* ctx_;
  const ArrayType& dictionary_;
  ArrayData* output_;
  std::unique_ptr<ArrayBuilder> builder_;
  BinaryBuilder* binary_builder_;
};

// Visitor for Dict<NumericType | TemporalType>
template <typename T, typename IndexType>
struct FromDictVisitor<
    T, IndexType, enable_if_t<is_number_type<T>::value || is_temporal_type<T>::value>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  using value_type = typename T::c_type;

  FromDictVisitor(KernelContext* ctx, const ArrayType& dictionary, ArrayData* output)
      : dictionary_(dictionary), out_(output->GetMutableValues<value_type>(1)) {}

  Status Init() { return Status::OK(); }

  Status VisitNull() {
    *out_++ = value_type{};  // Zero-initialize
    return Status::OK();
  }

  Status VisitValue(typename IndexType::c_type dict_index) {
    *out_++ = dictionary_.Value(dict_index);
    return Status::OK();
  }

  Status Finish() { return Status::OK(); }

  const ArrayType& dictionary_;
  value_type* out_;
};

template <typename T>
struct FromDictUnpackHelper {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  template <typename IndexType>
  void Unpack(KernelContext* ctx, const ArrayData& indices, const ArrayType& dictionary,
              ArrayData* output) {
    FromDictVisitor<T, IndexType> visitor{ctx, dictionary, output};
    KERNEL_RETURN_IF_ERROR(ctx, visitor.Init());
    KERNEL_RETURN_IF_ERROR(ctx, ArrayDataVisitor<IndexType>::Visit(indices, &visitor));
    KERNEL_RETURN_IF_ERROR(ctx, visitor.Finish());
  }
};

// Dispatch dictionary casts to UnpackHelper
template <typename T>
struct FromDictionaryCast {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& input = *batch[0].array();
    ArrayData* output = out->mutable_array();

    const DictionaryType& type = checked_cast<const DictionaryType&>(*input.type);
    const Array& dictionary = *input.dictionary;
    const DataType& values_type = *dictionary.type();

    // ARROW-7077
    if (!values_type.Equals(*output->type)) {
      ctx->SetStatus(Status::Invalid("Cannot unpack dictionary of type ", type.ToString(),
                                     " to type ", output->type->ToString()));
      return;
    }

    FromDictUnpackHelper<T> unpack_helper;
    switch (type.index_type()->id()) {
      case Type::INT8:
        unpack_helper.template Unpack<Int8Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output);
        break;
      case Type::INT16:
        unpack_helper.template Unpack<Int16Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output);
        break;
      case Type::INT32:
        unpack_helper.template Unpack<Int32Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output);
        break;
      case Type::INT64:
        unpack_helper.template Unpack<Int64Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output);
        break;
      default:
        ctx->SetStatus(
            Status::TypeError("Invalid index type: ", type.index_type()->ToString()));
        break;
    }
  }
};

template <>
struct FromDictionaryCast<NullType> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayData* output = out->mutable_array();
    output->buffers = {nullptr};
    output->null_count = batch.length;
  }
};

template <>
struct FromDictionaryCast<BooleanType> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {}
};

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
                   !std::is_same<DictionaryType, T>::value>> {
  static void Add(const OutputType& out_ty, CastFunction* func) {
    // Dictionary unpacking not implemented for boolean or nested types
    DCHECK_OK(func->AddKernel(Type::DICTIONARY, {InputType::Array(Type::DICTIONARY)},
                              out_ty, FromDictionaryCast<T>::Exec));
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

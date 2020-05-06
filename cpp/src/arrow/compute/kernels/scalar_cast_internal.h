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

#ifdef ARROW_EXTRA_ERROR_CONTEXT

#define FUNC_RETURN_NOT_OK(expr)                     \
  do {                                               \
    Status _st = (expr);                             \
    if (ARROW_PREDICT_FALSE(!_st.ok())) {            \
      _st.AddContextLine(__FILE__, __LINE__, #expr); \
      ctx->SetStatus(_st);                           \
      return;                                        \
    }                                                \
  } while (0)

#else

#define FUNC_RETURN_NOT_OK(expr)          \
  do {                                    \
    Status _st = (expr);                  \
    if (ARROW_PREDICT_FALSE(!_st.ok())) { \
      ctx->SetStatus(_st);                \
      return;                             \
    }                                     \
  } while (0)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

namespace compute {

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

typedef std::function<void(FunctionContext*, const CastOptions& options, const ArrayData&,
                           ArrayData*)>
    CastFunction;

// ----------------------------------------------------------------------
// Dictionary to other things

template <typename T, typename IndexType, typename Enable = void>
struct FromDictVisitor {};

// Visitor for Dict<FixedSizeBinaryType>
template <typename T, typename IndexType>
struct FromDictVisitor<T, IndexType, enable_if_fixed_size_binary<T>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  FromDictVisitor(FunctionContext* ctx, const ArrayType& dictionary, ArrayData* output)
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

  FromDictVisitor(FunctionContext* ctx, const ArrayType& dictionary, ArrayData* output)
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
    // Copy all buffer except the valid bitmap
    DCHECK_EQ(output_->buffers.size(), 1);
    for (size_t i = 1; i < plain_array->data()->buffers.size(); i++) {
      output_->buffers.push_back(plain_array->data()->buffers[i]);
    }
    return Status::OK();
  }

  FunctionContext* ctx_;
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

  FromDictVisitor(FunctionContext* ctx, const ArrayType& dictionary, ArrayData* output)
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
  Status Unpack(FunctionContext* ctx, const ArrayData& indices,
                const ArrayType& dictionary, ArrayData* output) {
    FromDictVisitor<T, IndexType> visitor{ctx, dictionary, output};
    RETURN_NOT_OK(visitor.Init());
    RETURN_NOT_OK(ArrayDataVisitor<IndexType>::Visit(indices, &visitor));
    return visitor.Finish();
  }
};

// Dispatch dictionary casts to UnpackHelper
template <typename T>
struct CastFunctor<T, DictionaryType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using ArrayType = typename TypeTraits<T>::ArrayType;

    const DictionaryType& type = checked_cast<const DictionaryType&>(*input.type);
    const Array& dictionary = *input.dictionary;
    const DataType& values_type = *dictionary.type();

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    FromDictUnpackHelper<T> unpack_helper;
    switch (type.index_type()->id()) {
      case Type::INT8:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int8Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      case Type::INT16:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int16Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      case Type::INT32:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int32Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      case Type::INT64:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int64Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      default:
        ctx->SetStatus(
            Status::TypeError("Invalid index type: ", type.index_type()->ToString()));
        return;
    }
  }
};

}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/kernels/take.h"

#include <sstream>
#include <vector>

#include "arrow/builder.h"
#include "arrow/compare.h"
#include "arrow/util/logging.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/util-internal.h"

#ifdef ARROW_EXTRA_ERROR_CONTEXT

#define FUNC_RETURN_NOT_OK(s)                                                       \
  do {                                                                              \
    Status _s = (s);                                                                \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                                            \
      std::stringstream ss;                                                         \
      ss << __FILE__ << ":" << __LINE__ << " code: " << #s << "\n" << _s.message(); \
      ctx->SetStatus(Status(_s.code(), ss.str()));                                  \
      return;                                                                       \
    }                                                                               \
  } while (0)

#else

#define FUNC_RETURN_NOT_OK(s)            \
  do {                                   \
    Status _s = (s);                     \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      ctx->SetStatus(_s);                \
      return;                            \
    }                                    \
  } while (0)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

namespace arrow {
namespace compute {

template <typename T>
using enable_if_index_valid =
typename std::enable_if<std::is_base_of<Integer, T>::value>::type;

template <typename ValueType, typename IndexType, typename Enable = void>
struct TakeFunctor {};

template <typename V, typename I>
struct TakeFunctor<V, I, enable_if_index_valid<I>> {
  void operator()(FunctionContext* ctx, const TakeOptions& options,
                  const ArrayData& input, const ArrayData& indices, ArrayData* output) {
    using value_type = typename V::c_type;
    using index_type = typename I::c_type;
    auto in_data = GetValues<value_type>(input, 1);
    auto index_data = GetValues<index_type>(indices, 1);

    auto out_data = GetMutableValues<value_type>(output, 1);
    for (int64_t i = 0; i < indices.length; ++i) {
      *out_data++ = *(in_data + (*index_data++));
    }
  }
};

template <typename I>
struct TakeFunctor<BooleanType, I, enable_if_index_valid<I>> {
  void operator()(FunctionContext* ctx, const TakeOptions& options,
                  const ArrayData& input, const ArrayData& indices, ArrayData* output) {
    auto index_data = GetValues<int32_t>(indices, 1);
    const uint8_t* input_data = input.buffers[1]->data();
    uint8_t* out_data = output->buffers[1]->mutable_data();
    internal::BitmapWriter bit_writer(out_data, output->offset, indices.length);
    for (int64_t i = 0; i < indices.length; ++i) {
      if (BitUtil::GetBit(input_data, *index_data++)) {
        bit_writer.Set();
      } else {
        bit_writer.Clear();
      }
      bit_writer.Next();
    }
    bit_writer.Finish();
  }
};

template <typename I>
struct TakeFunctor<NullType, I, enable_if_index_valid<I>> {
  void operator()(FunctionContext* ctx, const TakeOptions& options,
                  const ArrayData& input, const ArrayData& indices, ArrayData* output) {}
};

// ----------------------------------------------------------------------

typedef std::function<void(FunctionContext*,
                           const TakeOptions& options,
                           const ArrayData&,
                           const ArrayData&,
                           ArrayData*)>
    TakeFunction;

static Status AllocateIfNotPreallocated(FunctionContext* ctx,
                                        const int64_t length,
                                        ArrayData* out) {
  if (out->buffers.size() == 2) { // pre-allocated
    return Status::OK();
  } else {
    DCHECK_EQ(0, out->buffers.size());
  }

  std::shared_ptr<Buffer> validity_bitmap;
  int64_t bitmap_size = BitUtil::BytesForBits(length);
  RETURN_NOT_OK(ctx->Allocate(bitmap_size, &validity_bitmap));
  if (out->type->id() == Type::NA) {
    out->null_count = length;
    memset(validity_bitmap->mutable_data(), 0, bitmap_size);
  } else {
    out->null_count = 0;
    memset(validity_bitmap->mutable_data(), 0xFF, bitmap_size); // preset all to valid
  }

  const Type::type type_id = out->type->id();
  if (!(is_primitive(type_id) || type_id == Type::FIXED_SIZE_BINARY ||
      type_id == Type::DECIMAL)) {
    std::stringstream ss;
    ss << "Cannot pre-allocate memory for type: " << out->type->ToString();
    return Status::NotImplemented(ss.str());
  }

  std::shared_ptr<Buffer> out_data;
  if (type_id != Type::NA) {
    const auto& fw_type = static_cast<const FixedWidthType&>(*out->type);
    int bit_width = fw_type.bit_width();
    int64_t buffer_size = 0;
    if (bit_width == 1) {
      buffer_size = BitUtil::BytesForBits(length);
    } else if (bit_width % 8 == 0) {
      buffer_size = length * fw_type.bit_width() / 8;
    } else {
      DCHECK(false);
    }
    RETURN_NOT_OK(ctx->Allocate(buffer_size, &out_data));
    memset(out_data->mutable_data(), 0, buffer_size);
  }

  out->buffers.push_back(validity_bitmap);
  out->buffers.push_back(out_data);

  return Status::OK();
}

template <typename IndexType>
class TakeKernel : public UnaryKernel {};

template <>
class TakeKernel<Int32Type> : public UnaryKernel {
 public:
  TakeKernel(const TakeOptions& options, const TakeFunction& func,
             const std::shared_ptr<DataType>& index_type)
      : options_(options),
        func_(func),
        index_type_(index_type) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::COLLECTION, input.kind());

    const std::vector<Datum>& input_datums = input.collection();
    auto input_array = input_datums[0].array();
    auto indies_array = input_datums[1].array();

    ArrayData* result;
    if (out->kind() == Datum::NONE) {
      out->value = ArrayData::Make(input_array->type, indies_array->length);
    }
    result = out->array().get();
    RETURN_NOT_OK(AllocateIfNotPreallocated(ctx, indies_array->length, result));

    func_(ctx, options_, *input_array, *indies_array, result);

    RETURN_IF_ERROR(ctx);
    return Status::OK();
  }

 private:
  TakeOptions options_;
  TakeFunction func_;
  std::shared_ptr<DataType> index_type_;
};

#define TAKE_CASE(ValueType, IndexType)                                                 \
  case IndexType::type_id:                                                              \
    func = [](FunctionContext* ctx, const TakeOptions& options, const ArrayData& input, \
              const ArrayData& indices, ArrayData* out) {                               \
      TakeFunctor<ValueType, IndexType> func;                                           \
      func(ctx, options, input, indices, out);                                          \
    };                                                                                  \
    break;

#define PRIMITIVE_CASES(FN, VALUE_TYPE) \
  FN(VALUE_TYPE, Int8Type);           \
  FN(VALUE_TYPE, UInt8Type);          \
  FN(VALUE_TYPE, Int16Type);          \
  FN(VALUE_TYPE, UInt16Type);         \
  FN(VALUE_TYPE, Int32Type);          \
  FN(VALUE_TYPE, UInt32Type);         \
  FN(VALUE_TYPE, Int64Type);          \
  FN(VALUE_TYPE, UInt64Type);

#define GET_TAKE_FUNCTION(CASE_GENERATOR, ValueType)                           \
  static std::unique_ptr<UnaryKernel> Get##ValueType##TakeFunc(                \
      const std::shared_ptr<DataType>& index_type,                             \
      const TakeOptions& options) {                                            \
    TakeFunction func;                                                         \
    switch (index_type->id()) {                                                \
      CASE_GENERATOR(TAKE_CASE, ValueType);                                    \
      default:                                                                 \
        break;                                                                 \
    }                                                                          \
    if (func != nullptr) {                                                     \
      return std::unique_ptr<UnaryKernel>(new TakeKernel<Int32Type>(           \
          options, func, index_type));                                         \
    }                                                                          \
    return nullptr;                                                            \
  }

GET_TAKE_FUNCTION(PRIMITIVE_CASES, NullType);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, BooleanType);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, UInt8Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, Int8Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, UInt16Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, Int16Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, Int32Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, UInt32Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, Int64Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, UInt64Type);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, FloatType);
GET_TAKE_FUNCTION(PRIMITIVE_CASES, DoubleType);

#define TAKE_FUNCTION_CASE(ValueType)                                    \
  case ValueType::type_id:                                               \
    *kernel = Get##ValueType##TakeFunc(index_type, options);             \
    break


Status GetTakeFunction(const std::shared_ptr<DataType>& value_type,
                       const std::shared_ptr<DataType>& index_type,
                       const TakeOptions& options,
                       std::unique_ptr<UnaryKernel>* kernel) {
  switch (value_type->id()) {
    TAKE_FUNCTION_CASE(NullType);
    TAKE_FUNCTION_CASE(BooleanType);
    TAKE_FUNCTION_CASE(UInt8Type);
    TAKE_FUNCTION_CASE(Int8Type);
    TAKE_FUNCTION_CASE(UInt16Type);
    TAKE_FUNCTION_CASE(Int16Type);
    TAKE_FUNCTION_CASE(UInt32Type);
    TAKE_FUNCTION_CASE(Int32Type);
    TAKE_FUNCTION_CASE(UInt64Type);
    TAKE_FUNCTION_CASE(Int64Type);
    TAKE_FUNCTION_CASE(FloatType);
    TAKE_FUNCTION_CASE(DoubleType);
    default:
      break;
  }
  if (*kernel == nullptr) {
    std::stringstream ss;
    ss << "No take implemented of value type (" << value_type->ToString()
       << ") with index type (" << index_type->ToString() << ")";
    return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

Status Take(FunctionContext* context,
            const Datum& in,
            const Datum& indices,
            const TakeOptions& options,
            Datum* out) {
  std::vector<Datum> input_datums;
  input_datums.emplace_back(in);
  input_datums.emplace_back(indices);

  std::unique_ptr<UnaryKernel> func;
  RETURN_NOT_OK(GetTakeFunction(in.type(), indices.type(), {}, &func));

  std::vector<Datum> result;
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(context, func.get(), Datum(input_datums),
                                               &result));

  *out = detail::WrapDatumsLike(in, result);
  return Status::OK();
}

Status Take(FunctionContext* context,
            const Array& in,
            const Array& indices,
            const TakeOptions& options,
            std::shared_ptr<Array>* out) {
  Datum datum_out;
  RETURN_NOT_OK(Take(context, Datum(in.data()), Datum(indices.data()), options,
                     &datum_out));
  DCHECK_EQ(Datum::ARRAY, datum_out.kind());
  *out = MakeArray(datum_out.array());
  return Status::OK();
}

Status Take(FunctionContext* context,
            const Datum& in,
            const int64_t index,
            const TakeOptions& options,
            Datum* out) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(context->memory_pool(), int64(), &builder));
  auto int64_builder = static_cast<Int64Builder*>(builder.get());

  int64_builder->Append(index);
  std::shared_ptr<Array> index_array;
  RETURN_NOT_OK(int64_builder->Finish(&index_array));

  return Take(context, in, Datum(index_array->data()), options, out);
}

Status Take(FunctionContext* context,
            const Array& in,
            const int64_t index,
            const TakeOptions& options,
            std::shared_ptr<Array>* out) {
  Datum datum_out;
  RETURN_NOT_OK(Take(context, Datum(in.data()), index, options, &datum_out));
  DCHECK_EQ(Datum::ARRAY, datum_out.kind());
  *out = MakeArray(datum_out.array());
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

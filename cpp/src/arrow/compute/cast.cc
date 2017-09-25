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

#include "arrow/compute/cast.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compare.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"

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

// ----------------------------------------------------------------------
// Zero copy casts

template <typename O, typename I, typename Enable = void>
struct is_zero_copy_cast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_zero_copy_cast<O, I, typename std::enable_if<std::is_same<I, O>::value>::type> {
  static constexpr bool value = true;
};

// From integers to date/time types with zero copy
template <typename O, typename I>
struct is_zero_copy_cast<
    O, I, typename std::enable_if<std::is_base_of<Integer, I>::value &&
                                  (std::is_base_of<TimeType, O>::value ||
                                   std::is_base_of<DateType, O>::value ||
                                   std::is_base_of<TimestampType, O>::value)>::type> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value = sizeof(O_T) == sizeof(I_T);
};

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

// Indicated no computation required
template <typename O, typename I>
struct CastFunctor<O, I, typename std::enable_if<is_zero_copy_cast<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    auto in_data = input.data();
    output->null_count = input.null_count();
    output->buffers = in_data->buffers;
    output->child_data = in_data->child_data;
  }
};

// ----------------------------------------------------------------------
// Null to other things

template <typename T>
struct CastFunctor<T, NullType, typename std::enable_if<
                                    std::is_base_of<FixedWidthType, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    // Simply initialize data to 0
    auto buf = output->buffers[1];
    memset(buf->mutable_data(), 0, buf->size());
  }
};

// ----------------------------------------------------------------------
// Boolean to other things

// Cast from Boolean to other numbers
template <typename T>
struct CastFunctor<T, BooleanType,
                   typename std::enable_if<std::is_base_of<Number, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    using c_type = typename T::c_type;
    const uint8_t* data = input.data()->buffers[1]->data();
    auto out = reinterpret_cast<c_type*>(output->buffers[1]->mutable_data());
    constexpr auto kOne = static_cast<c_type>(1);
    constexpr auto kZero = static_cast<c_type>(0);
    for (int64_t i = 0; i < input.length(); ++i) {
      *out++ = BitUtil::GetBit(data, i) ? kOne : kZero;
    }
  }
};

// ----------------------------------------------------------------------
// Integers and Floating Point

template <typename O, typename I>
struct is_numeric_cast {
  static constexpr bool value =
      (std::is_base_of<Number, O>::value && std::is_base_of<Number, I>::value) &&
      (!std::is_same<O, I>::value);
};

template <typename O, typename I, typename Enable = void>
struct is_integer_downcast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_integer_downcast<
    O, I, typename std::enable_if<std::is_base_of<Integer, O>::value &&
                                  std::is_base_of<Integer, I>::value>::type> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&

       // same size, but unsigned to signed
       ((sizeof(O_T) == sizeof(I_T) && std::is_signed<O_T>::value &&
         std::is_unsigned<I_T>::value) ||

        // Smaller output size
        (sizeof(O_T) < sizeof(I_T))));
};

template <typename O, typename I>
struct CastFunctor<O, I, typename std::enable_if<std::is_same<BooleanType, O>::value &&
                                                 std::is_base_of<Number, I>::value &&
                                                 !std::is_same<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    using in_type = typename I::c_type;
    auto in_data = reinterpret_cast<const in_type*>(input.data()->buffers[1]->data());
    uint8_t* out_data = reinterpret_cast<uint8_t*>(output->buffers[1]->mutable_data());
    for (int64_t i = 0; i < input.length(); ++i) {
      BitUtil::SetBitTo(out_data, i, (*in_data++) != 0);
    }
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<is_integer_downcast<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    auto in_offset = input.offset();

    const auto& input_buffers = input.data()->buffers;

    auto in_data = reinterpret_cast<const in_type*>(input_buffers[1]->data()) + in_offset;
    auto out_data = reinterpret_cast<out_type*>(output->buffers[1]->mutable_data());

    if (!options.allow_int_overflow) {
      constexpr in_type kMax = static_cast<in_type>(std::numeric_limits<out_type>::max());
      constexpr in_type kMin = static_cast<in_type>(std::numeric_limits<out_type>::min());

      if (input.null_count() > 0) {
        const uint8_t* is_valid = input_buffers[0]->data();
        int64_t is_valid_offset = in_offset;
        for (int64_t i = 0; i < input.length(); ++i) {
          if (ARROW_PREDICT_FALSE(BitUtil::GetBit(is_valid, is_valid_offset++) &&
                                  (*in_data > kMax || *in_data < kMin))) {
            ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
          }
          *out_data++ = static_cast<out_type>(*in_data++);
        }
      } else {
        for (int64_t i = 0; i < input.length(); ++i) {
          if (ARROW_PREDICT_FALSE(*in_data > kMax || *in_data < kMin)) {
            ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
          }
          *out_data++ = static_cast<out_type>(*in_data++);
        }
      }
    } else {
      for (int64_t i = 0; i < input.length(); ++i) {
        *out_data++ = static_cast<out_type>(*in_data++);
      }
    }
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<is_numeric_cast<O, I>::value &&
                                           !is_integer_downcast<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    auto in_data = reinterpret_cast<const in_type*>(input.data()->buffers[1]->data());
    auto out_data = reinterpret_cast<out_type*>(output->buffers[1]->mutable_data());
    for (int64_t i = 0; i < input.length(); ++i) {
      *out_data++ = static_cast<out_type>(*in_data++);
    }
  }
};

// ----------------------------------------------------------------------
// Dictionary to other things

template <typename IndexType>
void UnpackFixedSizeBinaryDictionary(FunctionContext* ctx, const Array& indices,
                                     const FixedSizeBinaryArray& dictionary,
                                     ArrayData* output) {
  using index_c_type = typename IndexType::c_type;

  internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(), indices.offset(),
                                           indices.length());

  const index_c_type* in =
      reinterpret_cast<const index_c_type*>(indices.data()->buffers[1]->data()) +
      indices.offset();
  uint8_t* out = output->buffers[1]->mutable_data();
  int32_t byte_width =
      static_cast<const FixedSizeBinaryType&>(*output->type).byte_width();
  for (int64_t i = 0; i < indices.length(); ++i) {
    if (valid_bits_reader.IsSet()) {
      const uint8_t* value = dictionary.Value(in[i]);
      memcpy(out + i * byte_width, value, byte_width);
    }
    valid_bits_reader.Next();
  }
}

template <typename T>
struct CastFunctor<
    T, DictionaryType,
    typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    const DictionaryArray& dict_array = static_cast<const DictionaryArray&>(input);
    const DictionaryType& type = static_cast<const DictionaryType&>(*input.type());
    const DataType& values_type = *type.dictionary()->type();
    const FixedSizeBinaryArray& dictionary =
        static_cast<const FixedSizeBinaryArray&>(*type.dictionary());

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    const Array& indices = *dict_array.indices();
    switch (indices.type()->id()) {
      case Type::INT8:
        UnpackFixedSizeBinaryDictionary<Int8Type>(ctx, indices, dictionary, output);
        break;
      case Type::INT16:
        UnpackFixedSizeBinaryDictionary<Int16Type>(ctx, indices, dictionary, output);
        break;
      case Type::INT32:
        UnpackFixedSizeBinaryDictionary<Int32Type>(ctx, indices, dictionary, output);
        break;
      case Type::INT64:
        UnpackFixedSizeBinaryDictionary<Int64Type>(ctx, indices, dictionary, output);
        break;
      default:
        std::stringstream ss;
        ss << "Invalid index type: " << indices.type()->ToString();
        ctx->SetStatus(Status::Invalid(ss.str()));
        return;
    }
  }
};

template <typename IndexType>
Status UnpackBinaryDictionary(FunctionContext* ctx, const Array& indices,
                              const BinaryArray& dictionary, ArrayData* output) {
  using index_c_type = typename IndexType::c_type;
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), output->type, &builder));
  BinaryBuilder* binary_builder = static_cast<BinaryBuilder*>(builder.get());

  internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(), indices.offset(),
                                           indices.length());

  const index_c_type* in =
      reinterpret_cast<const index_c_type*>(indices.data()->buffers[1]->data()) +
      indices.offset();
  for (int64_t i = 0; i < indices.length(); ++i) {
    if (valid_bits_reader.IsSet()) {
      int32_t length;
      const uint8_t* value = dictionary.GetValue(in[i], &length);
      RETURN_NOT_OK(binary_builder->Append(value, length));
    } else {
      RETURN_NOT_OK(binary_builder->AppendNull());
    }
    valid_bits_reader.Next();
  }

  std::shared_ptr<Array> plain_array;
  RETURN_NOT_OK(binary_builder->Finish(&plain_array));
  // Copy all buffer except the valid bitmap
  for (size_t i = 1; i < plain_array->data()->buffers.size(); i++) {
    output->buffers.push_back(plain_array->data()->buffers[i]);
  }

  return Status::OK();
}

template <typename T>
struct CastFunctor<T, DictionaryType,
                   typename std::enable_if<std::is_base_of<BinaryType, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    const DictionaryArray& dict_array = static_cast<const DictionaryArray&>(input);
    const DictionaryType& type = static_cast<const DictionaryType&>(*input.type());
    const DataType& values_type = *type.dictionary()->type();
    const BinaryArray& dictionary = static_cast<const BinaryArray&>(*type.dictionary());

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    const Array& indices = *dict_array.indices();
    switch (indices.type()->id()) {
      case Type::INT8:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int8Type>(ctx, indices, dictionary, output)));
        break;
      case Type::INT16:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int16Type>(ctx, indices, dictionary, output)));
        break;
      case Type::INT32:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int32Type>(ctx, indices, dictionary, output)));
        break;
      case Type::INT64:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int64Type>(ctx, indices, dictionary, output)));
        break;
      default:
        std::stringstream ss;
        ss << "Invalid index type: " << indices.type()->ToString();
        ctx->SetStatus(Status::Invalid(ss.str()));
        return;
    }
  }
};

template <typename IndexType, typename c_type>
void UnpackPrimitiveDictionary(const Array& indices, const c_type* dictionary,
                               c_type* out) {
  using index_c_type = typename IndexType::c_type;

  internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(), indices.offset(),
                                           indices.length());

  const index_c_type* in =
      reinterpret_cast<const index_c_type*>(indices.data()->buffers[1]->data()) +
      indices.offset();
  for (int64_t i = 0; i < indices.length(); ++i) {
    if (valid_bits_reader.IsSet()) {
      out[i] = dictionary[in[i]];
    }
    valid_bits_reader.Next();
  }
}

// Cast from dictionary to plain representation
template <typename T>
struct CastFunctor<T, DictionaryType,
                   typename std::enable_if<IsNumeric<T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options, const Array& input,
                  ArrayData* output) {
    using c_type = typename T::c_type;

    const DictionaryArray& dict_array = static_cast<const DictionaryArray&>(input);
    const DictionaryType& type = static_cast<const DictionaryType&>(*input.type());
    const DataType& values_type = *type.dictionary()->type();

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    auto dictionary =
        reinterpret_cast<const c_type*>(type.dictionary()->data()->buffers[1]->data()) +
        type.dictionary()->offset();
    auto out = reinterpret_cast<c_type*>(output->buffers[1]->mutable_data());
    const Array& indices = *dict_array.indices();
    switch (indices.type()->id()) {
      case Type::INT8:
        UnpackPrimitiveDictionary<Int8Type, c_type>(indices, dictionary, out);
        break;
      case Type::INT16:
        UnpackPrimitiveDictionary<Int16Type, c_type>(indices, dictionary, out);
        break;
      case Type::INT32:
        UnpackPrimitiveDictionary<Int32Type, c_type>(indices, dictionary, out);
        break;
      case Type::INT64:
        UnpackPrimitiveDictionary<Int64Type, c_type>(indices, dictionary, out);
        break;
      default:
        std::stringstream ss;
        ss << "Invalid index type: " << indices.type()->ToString();
        ctx->SetStatus(Status::Invalid(ss.str()));
        return;
    }
  }
};

// ----------------------------------------------------------------------

typedef std::function<void(FunctionContext*, const CastOptions& options, const Array&,
                           ArrayData*)>
    CastFunction;

static Status AllocateIfNotPreallocated(FunctionContext* ctx, const Array& input,
                                        bool can_pre_allocate_values, ArrayData* out) {
  const int64_t length = input.length();

  out->null_count = input.null_count();

  // Propagate bitmap unless we are null type
  std::shared_ptr<Buffer> validity_bitmap = input.data()->buffers[0];
  if (input.type_id() == Type::NA) {
    int64_t bitmap_size = BitUtil::BytesForBits(length);
    RETURN_NOT_OK(ctx->Allocate(bitmap_size, &validity_bitmap));
    memset(validity_bitmap->mutable_data(), 0, bitmap_size);
  }

  if (out->buffers.size() == 2) {
    // Assuming preallocated, propagage bitmap and move on
    out->buffers[0] = validity_bitmap;
    return Status::OK();
  } else {
    DCHECK_EQ(0, out->buffers.size());
  }

  out->buffers.push_back(validity_bitmap);

  if (can_pre_allocate_values) {
    std::shared_ptr<Buffer> out_data;

    if (!(is_primitive(out->type->id()) || out->type->id() == Type::FIXED_SIZE_BINARY)) {
      std::stringstream ss;
      ss << "Cannot pre-allocate memory for type: " << out->type->ToString();
      return Status::NotImplemented(ss.str());
    }

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

    out->buffers.push_back(out_data);
  }

  return Status::OK();
}

class CastKernel : public UnaryKernel {
 public:
  CastKernel(const CastOptions& options, const CastFunction& func, bool is_zero_copy,
             bool can_pre_allocate_values)
      : options_(options),
        func_(func),
        is_zero_copy_(is_zero_copy),
        can_pre_allocate_values_(can_pre_allocate_values) {}

  Status Call(FunctionContext* ctx, const Array& input, ArrayData* out) override {
    if (!is_zero_copy_) {
      RETURN_NOT_OK(AllocateIfNotPreallocated(ctx, input, can_pre_allocate_values_, out));
    }
    func_(ctx, options_, input, out);
    RETURN_IF_ERROR(ctx);
    return Status::OK();
  }

 private:
  CastOptions options_;
  CastFunction func_;
  bool is_zero_copy_;
  bool can_pre_allocate_values_;
};

#define CAST_CASE(InType, OutType)                                                  \
  case OutType::type_id:                                                            \
    is_zero_copy = is_zero_copy_cast<OutType, InType>::value;                       \
    can_pre_allocate_values =                                                       \
        !(!is_binary_like(InType::type_id) && is_binary_like(OutType::type_id));    \
    func = [](FunctionContext* ctx, const CastOptions& options, const Array& input, \
              ArrayData* out) {                                                     \
      CastFunctor<OutType, InType> func;                                            \
      func(ctx, options, input, out);                                               \
    };                                                                              \
    break;

#define NUMERIC_CASES(FN, IN_TYPE) \
  FN(IN_TYPE, BooleanType);        \
  FN(IN_TYPE, UInt8Type);          \
  FN(IN_TYPE, Int8Type);           \
  FN(IN_TYPE, UInt16Type);         \
  FN(IN_TYPE, Int16Type);          \
  FN(IN_TYPE, UInt32Type);         \
  FN(IN_TYPE, Int32Type);          \
  FN(IN_TYPE, UInt64Type);         \
  FN(IN_TYPE, Int64Type);          \
  FN(IN_TYPE, FloatType);          \
  FN(IN_TYPE, DoubleType);

#define NULL_CASES(FN, IN_TYPE) \
  NUMERIC_CASES(FN, IN_TYPE)    \
  FN(NullType, Time32Type);     \
  FN(NullType, Date32Type);     \
  FN(NullType, TimestampType);  \
  FN(NullType, Time64Type);     \
  FN(NullType, Date64Type);

#define INT32_CASES(FN, IN_TYPE) \
  NUMERIC_CASES(FN, IN_TYPE)     \
  FN(Int32Type, Time32Type);     \
  FN(Int32Type, Date32Type);

#define INT64_CASES(FN, IN_TYPE) \
  NUMERIC_CASES(FN, IN_TYPE)     \
  FN(Int64Type, TimestampType);  \
  FN(Int64Type, Time64Type);     \
  FN(Int64Type, Date64Type);

#define DATE32_CASES(FN, IN_TYPE) FN(Date32Type, Date32Type);

#define DATE64_CASES(FN, IN_TYPE) FN(Date64Type, Date64Type);

#define TIME32_CASES(FN, IN_TYPE) FN(Time32Type, Time32Type);

#define TIME64_CASES(FN, IN_TYPE) FN(Time64Type, Time64Type);

#define TIMESTAMP_CASES(FN, IN_TYPE) FN(TimestampType, TimestampType);

#define DICTIONARY_CASES(FN, IN_TYPE) \
  FN(IN_TYPE, Time32Type);            \
  FN(IN_TYPE, Date32Type);            \
  FN(IN_TYPE, TimestampType);         \
  FN(IN_TYPE, Time64Type);            \
  FN(IN_TYPE, Date64Type);            \
  FN(IN_TYPE, UInt8Type);             \
  FN(IN_TYPE, Int8Type);              \
  FN(IN_TYPE, UInt16Type);            \
  FN(IN_TYPE, Int16Type);             \
  FN(IN_TYPE, UInt32Type);            \
  FN(IN_TYPE, Int32Type);             \
  FN(IN_TYPE, UInt64Type);            \
  FN(IN_TYPE, Int64Type);             \
  FN(IN_TYPE, FloatType);             \
  FN(IN_TYPE, DoubleType);            \
  FN(IN_TYPE, FixedSizeBinaryType);   \
  FN(IN_TYPE, BinaryType);            \
  FN(IN_TYPE, StringType);

#define GET_CAST_FUNCTION(CASE_GENERATOR, InType)                                \
  static std::unique_ptr<UnaryKernel> Get##InType##CastFunc(                     \
      const std::shared_ptr<DataType>& out_type, const CastOptions& options) {   \
    CastFunction func;                                                           \
    bool is_zero_copy = false;                                                   \
    bool can_pre_allocate_values = true;                                         \
    switch (out_type->id()) {                                                    \
      CASE_GENERATOR(CAST_CASE, InType);                                         \
      default:                                                                   \
        break;                                                                   \
    }                                                                            \
    if (func != nullptr) {                                                       \
      return std::unique_ptr<UnaryKernel>(                                       \
          new CastKernel(options, func, is_zero_copy, can_pre_allocate_values)); \
    }                                                                            \
    return nullptr;                                                              \
  }

GET_CAST_FUNCTION(NULL_CASES, NullType);
GET_CAST_FUNCTION(NUMERIC_CASES, BooleanType);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt8Type);
GET_CAST_FUNCTION(NUMERIC_CASES, Int8Type);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt16Type);
GET_CAST_FUNCTION(NUMERIC_CASES, Int16Type);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt32Type);
GET_CAST_FUNCTION(INT32_CASES, Int32Type);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt64Type);
GET_CAST_FUNCTION(INT64_CASES, Int64Type);
GET_CAST_FUNCTION(NUMERIC_CASES, FloatType);
GET_CAST_FUNCTION(NUMERIC_CASES, DoubleType);
GET_CAST_FUNCTION(DATE32_CASES, Date32Type);
GET_CAST_FUNCTION(DATE64_CASES, Date64Type);
GET_CAST_FUNCTION(TIME32_CASES, Time32Type);
GET_CAST_FUNCTION(TIME64_CASES, Time64Type);
GET_CAST_FUNCTION(TIMESTAMP_CASES, TimestampType);

GET_CAST_FUNCTION(DICTIONARY_CASES, DictionaryType);

#define CAST_FUNCTION_CASE(InType)                      \
  case InType::type_id:                                 \
    *kernel = Get##InType##CastFunc(out_type, options); \
    break

Status GetCastFunction(const DataType& in_type, const std::shared_ptr<DataType>& out_type,
                       const CastOptions& options, std::unique_ptr<UnaryKernel>* kernel) {
  switch (in_type.id()) {
    CAST_FUNCTION_CASE(NullType);
    CAST_FUNCTION_CASE(BooleanType);
    CAST_FUNCTION_CASE(UInt8Type);
    CAST_FUNCTION_CASE(Int8Type);
    CAST_FUNCTION_CASE(UInt16Type);
    CAST_FUNCTION_CASE(Int16Type);
    CAST_FUNCTION_CASE(UInt32Type);
    CAST_FUNCTION_CASE(Int32Type);
    CAST_FUNCTION_CASE(UInt64Type);
    CAST_FUNCTION_CASE(Int64Type);
    CAST_FUNCTION_CASE(FloatType);
    CAST_FUNCTION_CASE(DoubleType);
    CAST_FUNCTION_CASE(Date32Type);
    CAST_FUNCTION_CASE(Date64Type);
    CAST_FUNCTION_CASE(Time32Type);
    CAST_FUNCTION_CASE(Time64Type);
    CAST_FUNCTION_CASE(TimestampType);
    CAST_FUNCTION_CASE(DictionaryType);
    default:
      break;
  }
  if (*kernel == nullptr) {
    std::stringstream ss;
    ss << "No cast implemented from " << in_type.ToString() << " to "
       << out_type->ToString();
    return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

Status Cast(FunctionContext* ctx, const Array& array,
            const std::shared_ptr<DataType>& out_type, const CastOptions& options,
            std::shared_ptr<Array>* out) {
  // Dynamic dispatch to obtain right cast function
  std::unique_ptr<UnaryKernel> func;
  RETURN_NOT_OK(GetCastFunction(*array.type(), out_type, options, &func));

  // Data structure for output
  auto out_data = std::make_shared<ArrayData>(out_type, array.length());

  RETURN_NOT_OK(func->Call(ctx, array, out_data.get()));
  return MakeArray(out_data, out);
}

}  // namespace compute
}  // namespace arrow

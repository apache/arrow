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

#include "arrow/compute/kernels/cast.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

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

constexpr int64_t kMillisecondsInDay = 86400000;

// ----------------------------------------------------------------------
// Zero copy casts

template <typename O, typename I, typename Enable = void>
struct is_zero_copy_cast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_zero_copy_cast<
    O, I, typename std::enable_if<std::is_same<I, O>::value &&
                                  !std::is_base_of<ParametricType, O>::value>::type> {
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
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    CopyData(input, output);
  }
};

// ----------------------------------------------------------------------
// Null to other things

template <typename T>
struct CastFunctor<T, NullType, typename std::enable_if<
                                    std::is_base_of<FixedWidthType, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    // Simply initialize data to 0
    auto buf = output->buffers[1];
    DCHECK_EQ(output->offset, 0);
    memset(buf->mutable_data(), 0, buf->size());
  }
};

template <>
struct CastFunctor<NullType, DictionaryType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {}
};

// ----------------------------------------------------------------------
// Boolean to other things

// Cast from Boolean to other numbers
template <typename T>
struct CastFunctor<T, BooleanType, enable_if_number<T>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using c_type = typename T::c_type;
    constexpr auto kOne = static_cast<c_type>(1);
    constexpr auto kZero = static_cast<c_type>(0);

    internal::BitmapReader bit_reader(input.buffers[1]->data(), input.offset,
                                      input.length);
    auto out = GetMutableValues<c_type>(output, 1);
    for (int64_t i = 0; i < input.length; ++i) {
      *out++ = bit_reader.IsSet() ? kOne : kZero;
      bit_reader.Next();
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
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    DCHECK_EQ(output->offset, 0);

    const in_type* in_data = GetValues<in_type>(input, 1);
    uint8_t* out_data = GetMutableValues<uint8_t>(output, 1);
    for (int64_t i = 0; i < input.length; ++i) {
      BitUtil::SetBitTo(out_data, i, (*in_data++) != 0);
    }
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<is_integer_downcast<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;
    DCHECK_EQ(output->offset, 0);

    auto in_offset = input.offset;

    const in_type* in_data = GetValues<in_type>(input, 1);
    auto out_data = GetMutableValues<out_type>(output, 1);

    if (!options.allow_int_overflow) {
      constexpr in_type kMax = static_cast<in_type>(std::numeric_limits<out_type>::max());
      constexpr in_type kMin = static_cast<in_type>(std::numeric_limits<out_type>::min());

      // Null count may be -1 if the input array had been sliced
      if (input.null_count != 0) {
        internal::BitmapReader is_valid_reader(input.buffers[0]->data(), in_offset,
                                               input.length);
        for (int64_t i = 0; i < input.length; ++i) {
          if (ARROW_PREDICT_FALSE(is_valid_reader.IsSet() &&
                                  (*in_data > kMax || *in_data < kMin))) {
            ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
          }
          *out_data++ = static_cast<out_type>(*in_data++);
          is_valid_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; ++i) {
          if (ARROW_PREDICT_FALSE(*in_data > kMax || *in_data < kMin)) {
            ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
          }
          *out_data++ = static_cast<out_type>(*in_data++);
        }
      }
    } else {
      for (int64_t i = 0; i < input.length; ++i) {
        *out_data++ = static_cast<out_type>(*in_data++);
      }
    }
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<is_numeric_cast<O, I>::value &&
                                           !is_integer_downcast<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    const in_type* in_data = GetValues<in_type>(input, 1);
    auto out_data = GetMutableValues<out_type>(output, 1);
    for (int64_t i = 0; i < input.length; ++i) {
      *out_data++ = static_cast<out_type>(*in_data++);
    }
  }
};

// ----------------------------------------------------------------------
// From one timestamp to another

template <typename in_type, typename out_type>
void ShiftTime(FunctionContext* ctx, const CastOptions& options, const bool is_multiply,
               const int64_t factor, const ArrayData& input, ArrayData* output) {
  const in_type* in_data = GetValues<in_type>(input, 1);
  auto out_data = GetMutableValues<out_type>(output, 1);

  if (factor == 1) {
    for (int64_t i = 0; i < input.length; i++) {
      out_data[i] = static_cast<out_type>(in_data[i]);
    }
  } else if (is_multiply) {
    for (int64_t i = 0; i < input.length; i++) {
      out_data[i] = static_cast<out_type>(in_data[i] * factor);
    }
  } else {
    if (options.allow_time_truncate) {
      for (int64_t i = 0; i < input.length; i++) {
        out_data[i] = static_cast<out_type>(in_data[i] / factor);
      }
    } else {
#define RAISE_INVALID_CAST(VAL)                                                         \
  std::stringstream ss;                                                                 \
  ss << "Casting from " << input.type->ToString() << " to " << output->type->ToString() \
     << " would lose data: " << VAL;                                                    \
  ctx->SetStatus(Status::Invalid(ss.str()));

      if (input.null_count != 0) {
        internal::BitmapReader bit_reader(input.buffers[0]->data(), input.offset,
                                          input.length);
        for (int64_t i = 0; i < input.length; i++) {
          out_data[i] = static_cast<out_type>(in_data[i] / factor);
          if (bit_reader.IsSet() && (out_data[i] * factor != in_data[i])) {
            RAISE_INVALID_CAST(in_data[i]);
            break;
          }
          bit_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; i++) {
          out_data[i] = static_cast<out_type>(in_data[i] / factor);
          if (out_data[i] * factor != in_data[i]) {
            RAISE_INVALID_CAST(in_data[i]);
            break;
          }
        }
      }

#undef RAISE_INVALID_CAST
    }
  }
}

namespace {

// {is_multiply, factor}
const std::pair<bool, int64_t> kTimeConversionTable[4][4] = {
    {{true, 1}, {true, 1000}, {true, 1000000}, {true, 1000000000L}},     // SECOND
    {{false, 1000}, {true, 1}, {true, 1000}, {true, 1000000}},           // MILLI
    {{false, 1000000}, {false, 1000}, {true, 1}, {true, 1000}},          // MICRO
    {{false, 1000000000L}, {false, 1000000}, {false, 1000}, {true, 1}},  // NANO
};

}  // namespace

template <>
struct CastFunctor<TimestampType, TimestampType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    // If units are the same, zero copy, otherwise convert
    const auto& in_type = static_cast<const TimestampType&>(*input.type);
    const auto& out_type = static_cast<const TimestampType&>(*output->type);

    if (in_type.unit() == out_type.unit()) {
      CopyData(input, output);
      return;
    }

    std::pair<bool, int64_t> conversion =
        kTimeConversionTable[static_cast<int>(in_type.unit())]
                            [static_cast<int>(out_type.unit())];

    ShiftTime<int64_t, int64_t>(ctx, options, conversion.first, conversion.second, input,
                                output);
  }
};

template <>
struct CastFunctor<Date32Type, TimestampType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    const auto& in_type = static_cast<const TimestampType&>(*input.type);

    static const int64_t kTimestampToDateFactors[4] = {
        86400LL,                             // SECOND
        86400LL * 1000LL,                    // MILLI
        86400LL * 1000LL * 1000LL,           // MICRO
        86400LL * 1000LL * 1000LL * 1000LL,  // NANO
    };

    const int64_t factor = kTimestampToDateFactors[static_cast<int>(in_type.unit())];
    ShiftTime<int64_t, int32_t>(ctx, options, false, factor, input, output);
  }
};

template <>
struct CastFunctor<Date64Type, TimestampType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    const auto& in_type = static_cast<const TimestampType&>(*input.type);

    std::pair<bool, int64_t> conversion =
        kTimeConversionTable[static_cast<int>(in_type.unit())]
                            [static_cast<int>(TimeUnit::MILLI)];

    ShiftTime<int64_t, int64_t>(ctx, options, conversion.first, conversion.second, input,
                                output);

    internal::BitmapReader bit_reader(input.buffers[0]->data(), input.offset,
                                      input.length);

    // Ensure that intraday milliseconds have been zeroed out
    auto out_data = GetMutableValues<int64_t>(output, 1);
    for (int64_t i = 0; i < input.length; ++i) {
      const int64_t remainder = out_data[i] % kMillisecondsInDay;
      if (ARROW_PREDICT_FALSE(!options.allow_time_truncate && bit_reader.IsSet() &&
                              remainder > 0)) {
        ctx->SetStatus(
            Status::Invalid("Timestamp value had non-zero intraday milliseconds"));
        break;
      }
      out_data[i] -= remainder;
      bit_reader.Next();
    }
  }
};

// ----------------------------------------------------------------------
// From one time32 or time64 to another

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<std::is_base_of<TimeType, I>::value &&
                                           std::is_base_of<TimeType, O>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_t = typename I::c_type;
    using out_t = typename O::c_type;

    // If units are the same, zero copy, otherwise convert
    const auto& in_type = static_cast<const I&>(*input.type);
    const auto& out_type = static_cast<const O&>(*output->type);

    if (in_type.unit() == out_type.unit()) {
      CopyData(input, output);
      return;
    }

    std::pair<bool, int64_t> conversion =
        kTimeConversionTable[static_cast<int>(in_type.unit())]
                            [static_cast<int>(out_type.unit())];

    ShiftTime<in_t, out_t>(ctx, options, conversion.first, conversion.second, input,
                           output);
  }
};

// ----------------------------------------------------------------------
// Between date32 and date64

template <>
struct CastFunctor<Date64Type, Date32Type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    ShiftTime<int32_t, int64_t>(ctx, options, true, kMillisecondsInDay, input, output);
  }
};

template <>
struct CastFunctor<Date32Type, Date64Type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    ShiftTime<int64_t, int32_t>(ctx, options, false, kMillisecondsInDay, input, output);
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

  const index_c_type* in = GetValues<index_c_type>(*indices.data(), 1);

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
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    DictionaryArray dict_array(input.ShallowCopy());

    const DictionaryType& type = static_cast<const DictionaryType&>(*input.type);
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

  const index_c_type* in = GetValues<index_c_type>(*indices.data(), 1);
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
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    DictionaryArray dict_array(input.ShallowCopy());

    const DictionaryType& type = static_cast<const DictionaryType&>(*input.type);
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

  const index_c_type* in = GetValues<index_c_type>(*indices.data(), 1);
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
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using c_type = typename T::c_type;

    DictionaryArray dict_array(input.ShallowCopy());

    const DictionaryType& type = static_cast<const DictionaryType&>(*input.type);
    const DataType& values_type = *type.dictionary()->type();

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    const c_type* dictionary = GetValues<c_type>(*type.dictionary()->data(), 1);

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

typedef std::function<void(FunctionContext*, const CastOptions& options, const ArrayData&,
                           ArrayData*)>
    CastFunction;

static Status AllocateIfNotPreallocated(FunctionContext* ctx, const ArrayData& input,
                                        bool can_pre_allocate_values, ArrayData* out) {
  const int64_t length = input.length;
  out->null_count = input.null_count;

  // Propagate bitmap unless we are null type
  std::shared_ptr<Buffer> validity_bitmap = input.buffers[0];
  if (input.type->id() == Type::NA) {
    int64_t bitmap_size = BitUtil::BytesForBits(length);
    RETURN_NOT_OK(ctx->Allocate(bitmap_size, &validity_bitmap));
    memset(validity_bitmap->mutable_data(), 0, bitmap_size);
  } else if (input.offset != 0) {
    RETURN_NOT_OK(CopyBitmap(ctx->memory_pool(), validity_bitmap->data(), input.offset,
                             length, &validity_bitmap));
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

    const Type::type type_id = out->type->id();

    if (!(is_primitive(type_id) || type_id == Type::FIXED_SIZE_BINARY ||
          type_id == Type::DECIMAL)) {
      std::stringstream ss;
      ss << "Cannot pre-allocate memory for type: " << out->type->ToString();
      return Status::NotImplemented(ss.str());
    }

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

      out->buffers.push_back(out_data);
    }
  }

  return Status::OK();
}

class CastKernel : public UnaryKernel {
 public:
  CastKernel(const CastOptions& options, const CastFunction& func, bool is_zero_copy,
             bool can_pre_allocate_values, const std::shared_ptr<DataType>& out_type)
      : options_(options),
        func_(func),
        is_zero_copy_(is_zero_copy),
        can_pre_allocate_values_(can_pre_allocate_values),
        out_type_(out_type) {}

  Status Call(FunctionContext* ctx, const ArrayData& input, Datum* out) override {
    ArrayData* result;

    if (out->kind() == Datum::NONE) {
      out->value = std::make_shared<ArrayData>(out_type_, input.length);
    }

    result = out->array().get();

    if (!is_zero_copy_) {
      RETURN_NOT_OK(
          AllocateIfNotPreallocated(ctx, input, can_pre_allocate_values_, result));
    }
    func_(ctx, options_, input, result);

    RETURN_IF_ERROR(ctx);
    return Status::OK();
  }

 private:
  CastOptions options_;
  CastFunction func_;
  bool is_zero_copy_;
  bool can_pre_allocate_values_;
  std::shared_ptr<DataType> out_type_;
};

#define CAST_CASE(InType, OutType)                                                      \
  case OutType::type_id:                                                                \
    is_zero_copy = is_zero_copy_cast<OutType, InType>::value;                           \
    can_pre_allocate_values =                                                           \
        !(!is_binary_like(InType::type_id) && is_binary_like(OutType::type_id));        \
    func = [](FunctionContext* ctx, const CastOptions& options, const ArrayData& input, \
              ArrayData* out) {                                                         \
      CastFunctor<OutType, InType> func;                                                \
      func(ctx, options, input, out);                                                   \
    };                                                                                  \
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

#define DATE32_CASES(FN, IN_TYPE) \
  FN(Date32Type, Date32Type);     \
  FN(Date32Type, Date64Type);

#define DATE64_CASES(FN, IN_TYPE) \
  FN(Date64Type, Date64Type);     \
  FN(Date64Type, Date32Type);

#define TIME32_CASES(FN, IN_TYPE) \
  FN(Time32Type, Time32Type);     \
  FN(Time32Type, Time64Type);

#define TIME64_CASES(FN, IN_TYPE) \
  FN(Time64Type, Time32Type);     \
  FN(Time64Type, Time64Type);

#define TIMESTAMP_CASES(FN, IN_TYPE) \
  FN(TimestampType, TimestampType);  \
  FN(TimestampType, Date32Type);     \
  FN(TimestampType, Date64Type);

#define DICTIONARY_CASES(FN, IN_TYPE) \
  FN(IN_TYPE, NullType);              \
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
  FN(IN_TYPE, Decimal128Type);        \
  FN(IN_TYPE, BinaryType);            \
  FN(IN_TYPE, StringType);

#define GET_CAST_FUNCTION(CASE_GENERATOR, InType)                              \
  static std::unique_ptr<UnaryKernel> Get##InType##CastFunc(                   \
      const std::shared_ptr<DataType>& out_type, const CastOptions& options) { \
    CastFunction func;                                                         \
    bool is_zero_copy = false;                                                 \
    bool can_pre_allocate_values = true;                                       \
    switch (out_type->id()) {                                                  \
      CASE_GENERATOR(CAST_CASE, InType);                                       \
      default:                                                                 \
        break;                                                                 \
    }                                                                          \
    if (func != nullptr) {                                                     \
      return std::unique_ptr<UnaryKernel>(new CastKernel(                      \
          options, func, is_zero_copy, can_pre_allocate_values, out_type));    \
    }                                                                          \
    return nullptr;                                                            \
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

Status Cast(FunctionContext* ctx, const Datum& value,
            const std::shared_ptr<DataType>& out_type, const CastOptions& options,
            Datum* out) {
  // Dynamic dispatch to obtain right cast function
  std::unique_ptr<UnaryKernel> func;
  RETURN_NOT_OK(GetCastFunction(*value.type(), out_type, options, &func));

  std::vector<Datum> result;
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func.get(), value, &result));

  *out = detail::WrapDatumsLike(value, result);
  return Status::OK();
}

Status Cast(FunctionContext* ctx, const Array& array,
            const std::shared_ptr<DataType>& out_type, const CastOptions& options,
            std::shared_ptr<Array>* out) {
  Datum datum_out;
  RETURN_NOT_OK(Cast(ctx, Datum(array.data()), out_type, options, &datum_out));
  DCHECK_EQ(Datum::ARRAY, datum_out.kind());
  *out = MakeArray(datum_out.array());
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

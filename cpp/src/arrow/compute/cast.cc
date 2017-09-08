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
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <type_traits>

#include "arrow/type_traits.h"
#include "arrow/util/logging.h"

#include "arrow/compute/context.h"

namespace arrow {
namespace compute {

struct CastContext {
  FunctionContext* func_ctx;
  CastOptions options;
};

typedef std::function<void(CastContext*, const ArrayData&, ArrayData*)> CastFunction;

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

// Type is the same, no computation required
template <typename O, typename I>
struct CastFunctor<O, I, typename std::enable_if<std::is_same<I, O>::value>::type> {
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    output->type = input.type;
    output->buffers = input.buffers;
    output->length = input.length;
    output->offset = input.offset;
    output->null_count = input.null_count;
    output->child_data = input.child_data;
  }
};

// ----------------------------------------------------------------------
// Null to other things

template <typename T>
struct CastFunctor<T, NullType,
                   typename std::enable_if<!std::is_same<T, NullType>::value>::type> {
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    ctx->func_ctx->SetStatus(Status::NotImplemented("NullType"));
  }
};

// ----------------------------------------------------------------------
// Boolean to other things

// Cast from Boolean to other numbers
template <typename T>
struct CastFunctor<T, BooleanType,
                   typename std::enable_if<std::is_base_of<Number, T>::value>::type> {
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    using c_type = typename T::c_type;
    const uint8_t* data = input.buffers[1]->data();
    auto out = reinterpret_cast<c_type*>(output->buffers[1]->mutable_data());
    constexpr auto kOne = static_cast<c_type>(1);
    constexpr auto kZero = static_cast<c_type>(0);
    for (int64_t i = 0; i < input.length; ++i) {
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
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    auto in_data = reinterpret_cast<const in_type*>(input.buffers[1]->data());
    uint8_t* out_data = reinterpret_cast<uint8_t*>(output->buffers[1]->mutable_data());
    for (int64_t i = 0; i < input.length; ++i) {
      BitUtil::SetBitTo(out_data, i, (*in_data++) != 0);
    }
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<is_integer_downcast<O, I>::value>::type> {
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    auto in_offset = input.offset;

    auto in_data = reinterpret_cast<const in_type*>(input.buffers[1]->data()) + in_offset;
    auto out_data = reinterpret_cast<out_type*>(output->buffers[1]->mutable_data());

    if (!ctx->options.allow_int_overflow) {
      constexpr in_type kMax = static_cast<in_type>(std::numeric_limits<out_type>::max());
      constexpr in_type kMin = static_cast<in_type>(std::numeric_limits<out_type>::min());

      if (input.null_count > 0) {
        const uint8_t* is_valid = input.buffers[0]->data();
        int64_t is_valid_offset = in_offset;
        for (int64_t i = 0; i < input.length; ++i) {
          if (ARROW_PREDICT_FALSE(BitUtil::GetBit(is_valid, is_valid_offset++) &&
                                  (*in_data > kMax || *in_data < kMin))) {
            ctx->func_ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
          }
          *out_data++ = static_cast<out_type>(*in_data++);
        }
      } else {
        for (int64_t i = 0; i < input.length; ++i) {
          if (ARROW_PREDICT_FALSE(*in_data > kMax || *in_data < kMin)) {
            ctx->func_ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
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
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    auto in_data = reinterpret_cast<const in_type*>(input.buffers[1]->data());
    auto out_data = reinterpret_cast<out_type*>(output->buffers[1]->mutable_data());
    for (int64_t i = 0; i < input.length; ++i) {
      *out_data++ = static_cast<out_type>(*in_data++);
    }
  }
};

// ----------------------------------------------------------------------

#define CAST_CASE(InType, OutType)                                            \
  case OutType::type_id:                                                      \
    return [type](CastContext* ctx, const ArrayData& input, ArrayData* out) { \
      CastFunctor<OutType, InType> func;                                      \
      func(ctx, input, out);                                                  \
    }

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

#define GET_CAST_FUNCTION(CapType)                                                    \
  static CastFunction Get##CapType##CastFunc(const std::shared_ptr<DataType>& type) { \
    switch (type->id()) {                                                             \
      NUMERIC_CASES(CAST_CASE, CapType);                                              \
      default:                                                                        \
        break;                                                                        \
    }                                                                                 \
    return nullptr;                                                                   \
  }

#define CAST_FUNCTION_CASE(CapType)          \
  case CapType::type_id:                     \
    *out = Get##CapType##CastFunc(out_type); \
    break

GET_CAST_FUNCTION(BooleanType);
GET_CAST_FUNCTION(UInt8Type);
GET_CAST_FUNCTION(Int8Type);
GET_CAST_FUNCTION(UInt16Type);
GET_CAST_FUNCTION(Int16Type);
GET_CAST_FUNCTION(UInt32Type);
GET_CAST_FUNCTION(Int32Type);
GET_CAST_FUNCTION(UInt64Type);
GET_CAST_FUNCTION(Int64Type);
GET_CAST_FUNCTION(FloatType);
GET_CAST_FUNCTION(DoubleType);

static Status GetCastFunction(const DataType& in_type,
                              const std::shared_ptr<DataType>& out_type,
                              CastFunction* out) {
  switch (in_type.id()) {
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
    default:
      break;
  }
  if (*out == nullptr) {
    std::stringstream ss;
    ss << "No cast implemented from " << in_type.ToString() << " to "
       << out_type->ToString();
    return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

static Status AllocateLike(FunctionContext* ctx, const Array& array,
                           const std::shared_ptr<DataType>& out_type,
                           std::shared_ptr<ArrayData>* out) {
  if (!is_primitive(out_type->id())) {
    return Status::NotImplemented(out_type->ToString());
  }

  const auto& fw_type = static_cast<const FixedWidthType&>(*out_type);

  auto result = std::make_shared<ArrayData>();
  result->type = out_type;
  result->length = array.length();
  result->offset = 0;
  result->null_count = array.null_count();

  // Propagate null bitmap
  // TODO(wesm): handling null bitmap when input type is NullType
  result->buffers.push_back(array.data()->buffers[0]);

  std::shared_ptr<Buffer> out_data;

  int bit_width = fw_type.bit_width();

  if (bit_width == 1) {
    RETURN_NOT_OK(ctx->Allocate(BitUtil::BytesForBits(array.length()), &out_data));
  } else if (bit_width % 8 == 0) {
    RETURN_NOT_OK(ctx->Allocate(array.length() * fw_type.bit_width() / 8, &out_data));
  } else {
    DCHECK(false);
  }
  result->buffers.push_back(out_data);

  *out = result;
  return Status::OK();
}

static Status Cast(CastContext* cast_ctx, const Array& array,
                   const std::shared_ptr<DataType>& out_type,
                   std::shared_ptr<Array>* out) {
  // Dynamic dispatch to obtain right cast function
  CastFunction func;
  RETURN_NOT_OK(GetCastFunction(*array.type(), out_type, &func));

  // Allocate memory for output
  std::shared_ptr<ArrayData> out_data;
  RETURN_NOT_OK(AllocateLike(cast_ctx->func_ctx, array, out_type, &out_data));

  func(cast_ctx, *array.data(), out_data.get());
  RETURN_IF_ERROR(cast_ctx->func_ctx);
  return internal::MakeArray(out_data, out);
}

Status Cast(FunctionContext* ctx, const Array& array,
            const std::shared_ptr<DataType>& out_type, const CastOptions& options,
            std::shared_ptr<Array>* out) {
  CastContext cast_ctx{ctx, options};
  return Cast(&cast_ctx, array, out_type, out);
}

}  // namespace compute
}  // namespace arrow

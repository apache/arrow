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

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/visit_data_inline.h"

namespace arrow {

using internal::BinaryBitBlockCounter;
using internal::BitBlockCount;
using internal::BitmapReader;
using internal::checked_cast;
using internal::FirstTimeBitmapWriter;
using internal::GenerateBitsUnrolled;
using internal::VisitBitBlocks;
using internal::VisitBitBlocksVoid;
using internal::VisitTwoBitBlocksVoid;

namespace compute {
namespace internal {

/// KernelState adapter for the common case of kernels whose only
/// state is an instance of a subclass of FunctionOptions.
/// Default FunctionOptions are *not* handled here.
template <typename OptionsType>
struct OptionsWrapper : public KernelState {
  explicit OptionsWrapper(OptionsType options) : options(std::move(options)) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    if (auto options = static_cast<const OptionsType*>(args.options)) {
      return std::make_unique<OptionsWrapper>(*options);
    }

    return Status::Invalid(
        "Attempted to initialize KernelState from null FunctionOptions");
  }

  static const OptionsType& Get(const KernelState& state) {
    return ::arrow::internal::checked_cast<const OptionsWrapper&>(state).options;
  }

  static const OptionsType& Get(KernelContext* ctx) { return Get(*ctx->state()); }

  OptionsType options;
};

/// KernelState adapter for when the state is an instance constructed with the
/// KernelContext and the FunctionOptions as argument
template <typename StateType, typename OptionsType>
struct KernelStateFromFunctionOptions : public KernelState {
  explicit KernelStateFromFunctionOptions(KernelContext* ctx, OptionsType options)
      : state(StateType(ctx, std::move(options))) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    if (auto options = static_cast<const OptionsType*>(args.options)) {
      return std::make_unique<KernelStateFromFunctionOptions>(ctx, *options);
    }

    return Status::Invalid(
        "Attempted to initialize KernelState from null FunctionOptions");
  }

  static const StateType& Get(const KernelState& state) {
    return ::arrow::internal::checked_cast<const KernelStateFromFunctionOptions&>(state)
        .state;
  }

  static const StateType& Get(KernelContext* ctx) { return Get(*ctx->state()); }

  StateType state;
};

// ----------------------------------------------------------------------
// Input and output value type definitions

template <typename Type, typename Enable = void>
struct GetViewType;

template <typename Type>
struct GetViewType<Type, enable_if_has_c_type<Type>> {
  using T = typename Type::c_type;
  using PhysicalType = T;

  static T LogicalValue(PhysicalType value) { return value; }
};

template <typename Type>
struct GetViewType<Type, enable_if_t<is_base_binary_type<Type>::value ||
                                     is_fixed_size_binary_type<Type>::value>> {
  using T = std::string_view;
  using PhysicalType = T;

  static T LogicalValue(PhysicalType value) { return value; }
};

template <>
struct GetViewType<Decimal128Type> {
  using T = Decimal128;
  using PhysicalType = std::string_view;

  static T LogicalValue(PhysicalType value) {
    return Decimal128(reinterpret_cast<const uint8_t*>(value.data()));
  }

  static T LogicalValue(T value) { return value; }
};

template <>
struct GetViewType<Decimal256Type> {
  using T = Decimal256;
  using PhysicalType = std::string_view;

  static T LogicalValue(PhysicalType value) {
    return Decimal256(reinterpret_cast<const uint8_t*>(value.data()));
  }

  static T LogicalValue(T value) { return value; }
};

template <typename Type, typename Enable = void>
struct GetOutputType;

template <typename Type>
struct GetOutputType<Type, enable_if_has_c_type<Type>> {
  using T = typename Type::c_type;
};

template <typename Type>
struct GetOutputType<Type, enable_if_t<is_string_like_type<Type>::value>> {
  using T = std::string;
};

template <>
struct GetOutputType<Decimal128Type> {
  using T = Decimal128;
};

template <>
struct GetOutputType<Decimal256Type> {
  using T = Decimal256;
};

// ----------------------------------------------------------------------
// enable_if helpers for C types

template <typename T>
using is_unsigned_integer_value =
    std::integral_constant<bool,
                           std::is_integral<T>::value && std::is_unsigned<T>::value>;

template <typename T>
using is_signed_integer_value =
    std::integral_constant<bool, std::is_integral<T>::value && std::is_signed<T>::value>;

template <typename T, typename R = T>
using enable_if_signed_integer_value = enable_if_t<is_signed_integer_value<T>::value, R>;

template <typename T, typename R = T>
using enable_if_unsigned_integer_value =
    enable_if_t<is_unsigned_integer_value<T>::value, R>;

template <typename T, typename R = T>
using enable_if_integer_value =
    enable_if_t<is_signed_integer_value<T>::value || is_unsigned_integer_value<T>::value,
                R>;

template <typename T, typename R = T>
using enable_if_floating_value = enable_if_t<std::is_floating_point<T>::value, R>;

template <typename T, typename R = T>
using enable_if_decimal_value =
    enable_if_t<std::is_same<Decimal128, T>::value || std::is_same<Decimal256, T>::value,
                R>;

// ----------------------------------------------------------------------
// Iteration / value access utilities

template <typename T, typename R = void>
using enable_if_c_number_or_decimal = enable_if_t<
    (has_c_type<T>::value && !is_boolean_type<T>::value) || is_decimal_type<T>::value, R>;

// Iterator over various input array types, yielding a GetViewType<Type>

template <typename Type, typename Enable = void>
struct ArrayIterator;

template <typename Type>
struct ArrayIterator<Type, enable_if_c_number_or_decimal<Type>> {
  using T = typename TypeTraits<Type>::ScalarType::ValueType;
  const T* values;

  explicit ArrayIterator(const ArraySpan& arr) : values(arr.GetValues<T>(1)) {}
  T operator()() { return *values++; }
};

template <typename Type>
struct ArrayIterator<Type, enable_if_boolean<Type>> {
  BitmapReader reader;

  explicit ArrayIterator(const ArraySpan& arr)
      : reader(arr.buffers[1].data, arr.offset, arr.length) {}
  bool operator()() {
    bool out = reader.IsSet();
    reader.Next();
    return out;
  }
};

template <typename Type>
struct ArrayIterator<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  const ArraySpan& arr;
  const offset_type* offsets;
  offset_type cur_offset;
  const char* data;
  int64_t position;

  explicit ArrayIterator(const ArraySpan& arr)
      : arr(arr),
        offsets(reinterpret_cast<const offset_type*>(arr.buffers[1].data) + arr.offset),
        cur_offset(offsets[0]),
        data(reinterpret_cast<const char*>(arr.buffers[2].data)),
        position(0) {}

  std::string_view operator()() {
    offset_type next_offset = offsets[++position];
    auto result = std::string_view(data + cur_offset, next_offset - cur_offset);
    cur_offset = next_offset;
    return result;
  }
};

template <>
struct ArrayIterator<FixedSizeBinaryType> {
  const ArraySpan& arr;
  const char* data;
  const int32_t width;
  int64_t position;

  explicit ArrayIterator(const ArraySpan& arr)
      : arr(arr),
        data(reinterpret_cast<const char*>(arr.buffers[1].data)),
        width(arr.type->byte_width()),
        position(arr.offset) {}

  std::string_view operator()() {
    auto result = std::string_view(data + position * width, width);
    position++;
    return result;
  }
};

// Iterator over various output array types, taking a GetOutputType<Type>

template <typename Type, typename Enable = void>
struct OutputArrayWriter;

template <typename Type>
struct OutputArrayWriter<Type, enable_if_c_number_or_decimal<Type>> {
  using T = typename TypeTraits<Type>::ScalarType::ValueType;
  T* values;

  explicit OutputArrayWriter(ArraySpan* data) : values(data->GetValues<T>(1)) {}

  void Write(T value) { *values++ = value; }

  // Note that this doesn't write the null bitmap, which should be consistent
  // with Write / WriteNull calls
  void WriteNull() { *values++ = T{}; }

  void WriteAllNull(int64_t length) {
    std::memset(static_cast<void*>(values), 0, sizeof(T) * length);
  }
};

// (Un)box Scalar to / from C++ value

template <typename Type, typename Enable = void>
struct UnboxScalar;

template <typename Type>
struct UnboxScalar<Type, enable_if_has_c_type<Type>> {
  using T = typename Type::c_type;
  static T Unbox(const Scalar& val) {
    std::string_view view =
        checked_cast<const ::arrow::internal::PrimitiveScalarBase&>(val).view();
    DCHECK_EQ(view.size(), sizeof(T));
    return *reinterpret_cast<const T*>(view.data());
  }
};

template <typename Type>
struct UnboxScalar<Type, enable_if_has_string_view<Type>> {
  using T = std::string_view;
  static T Unbox(const Scalar& val) {
    if (!val.is_valid) return std::string_view();
    return checked_cast<const ::arrow::internal::PrimitiveScalarBase&>(val).view();
  }
};

template <>
struct UnboxScalar<Decimal128Type> {
  using T = Decimal128;
  static const T& Unbox(const Scalar& val) {
    return checked_cast<const Decimal128Scalar&>(val).value;
  }
};

template <>
struct UnboxScalar<Decimal256Type> {
  using T = Decimal256;
  static const T& Unbox(const Scalar& val) {
    return checked_cast<const Decimal256Scalar&>(val).value;
  }
};

template <typename Type, typename Enable = void>
struct BoxScalar;

template <typename Type>
struct BoxScalar<Type, enable_if_has_c_type<Type>> {
  using T = typename GetOutputType<Type>::T;
  static void Box(T val, Scalar* out) {
    // Enables BoxScalar<Int64Type> to work on a (for example) Time64Scalar
    T* mutable_data = reinterpret_cast<T*>(
        checked_cast<::arrow::internal::PrimitiveScalarBase*>(out)->mutable_data());
    *mutable_data = val;
  }
};

template <typename Type>
struct BoxScalar<Type, enable_if_base_binary<Type>> {
  using T = typename GetOutputType<Type>::T;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static void Box(T val, Scalar* out) {
    checked_cast<ScalarType*>(out)->value = std::make_shared<Buffer>(val);
  }
};

template <>
struct BoxScalar<Decimal128Type> {
  using T = Decimal128;
  using ScalarType = Decimal128Scalar;
  static void Box(T val, Scalar* out) { checked_cast<ScalarType*>(out)->value = val; }
};

template <>
struct BoxScalar<Decimal256Type> {
  using T = Decimal256;
  using ScalarType = Decimal256Scalar;
  static void Box(T val, Scalar* out) { checked_cast<ScalarType*>(out)->value = val; }
};

// A VisitArraySpanInline variant that calls its visitor function with logical
// values, such as Decimal128 rather than std::string_view.

template <typename T, typename VisitFunc, typename NullFunc>
static typename ::arrow::internal::call_traits::enable_if_return<VisitFunc, void>::type
VisitArrayValuesInline(const ArraySpan& arr, VisitFunc&& valid_func,
                       NullFunc&& null_func) {
  VisitArraySpanInline<T>(
      arr,
      [&](typename GetViewType<T>::PhysicalType v) {
        valid_func(GetViewType<T>::LogicalValue(std::move(v)));
      },
      std::forward<NullFunc>(null_func));
}

template <typename T, typename VisitFunc, typename NullFunc>
static typename ::arrow::internal::call_traits::enable_if_return<VisitFunc, Status>::type
VisitArrayValuesInline(const ArraySpan& arr, VisitFunc&& valid_func,
                       NullFunc&& null_func) {
  return VisitArraySpanInline<T>(
      arr,
      [&](typename GetViewType<T>::PhysicalType v) {
        return valid_func(GetViewType<T>::LogicalValue(std::move(v)));
      },
      std::forward<NullFunc>(null_func));
}

// Like VisitArrayValuesInline, but for binary functions.

template <typename Arg0Type, typename Arg1Type, typename VisitFunc, typename NullFunc>
static void VisitTwoArrayValuesInline(const ArraySpan& arr0, const ArraySpan& arr1,
                                      VisitFunc&& valid_func, NullFunc&& null_func) {
  ArrayIterator<Arg0Type> arr0_it(arr0);
  ArrayIterator<Arg1Type> arr1_it(arr1);

  auto visit_valid = [&](int64_t i) {
    valid_func(GetViewType<Arg0Type>::LogicalValue(arr0_it()),
               GetViewType<Arg1Type>::LogicalValue(arr1_it()));
  };
  auto visit_null = [&]() {
    arr0_it();
    arr1_it();
    null_func();
  };
  VisitTwoBitBlocksVoid(arr0.buffers[0].data, arr0.offset, arr1.buffers[0].data,
                        arr1.offset, arr0.length, std::move(visit_valid),
                        std::move(visit_null));
}

// ----------------------------------------------------------------------
// Reusable type resolvers

Result<TypeHolder> FirstType(KernelContext*, const std::vector<TypeHolder>& types);
Result<TypeHolder> LastType(KernelContext*, const std::vector<TypeHolder>& types);
Result<TypeHolder> ListValuesType(KernelContext*, const std::vector<TypeHolder>& types);

// ----------------------------------------------------------------------
// Helpers for iterating over common DataType instances for adding kernels to
// functions

// Returns a vector of example instances of parametric types such as
//
// * Decimal
// * Timestamp (requiring unit)
// * Time32 (requiring unit)
// * Time64 (requiring unit)
// * Duration (requiring unit)
// * List, LargeList, FixedSizeList
// * Struct
// * Union
// * Dictionary
// * Map
//
// Generally kernels will use the "FirstType" OutputType::Resolver above for
// the OutputType of the kernel's signature and match::SameTypeId for the
// corresponding InputType
const std::vector<std::shared_ptr<DataType>>& ExampleParametricTypes();

// ----------------------------------------------------------------------
// "Applicators" take an operator definition and creates an ArrayKernelExec
// which can be used to add a kernel to a Function.

namespace applicator {

// Generate an ArrayKernelExec given a functor that handles all of its own
// iteration, etc.
//
// Operator must implement
//
// static Status Call(KernelContext*, const ArraySpan& arg0, const ArraySpan& arg1,
//                    * out)
// static Status Call(KernelContext*, const ArraySpan& arg0, const Scalar& arg1,
//                    ExecResult* out)
// static Status Call(KernelContext*, const Scalar& arg0, const ArraySpan& arg1,
//                    ExecResult* out)
template <typename Operator>
static Status SimpleBinary(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  if (batch.length == 0) return Status::OK();

  if (batch[0].is_array()) {
    if (batch[1].is_array()) {
      return Operator::Call(ctx, batch[0].array, batch[1].array, out);
    } else {
      return Operator::Call(ctx, batch[0].array, *batch[1].scalar, out);
    }
  } else {
    if (batch[1].is_array()) {
      return Operator::Call(ctx, *batch[0].scalar, batch[1].array, out);
    } else {
      DCHECK(false);
      return Status::Invalid("Should be unreachable");
    }
  }
}

// OutputAdapter allows passing an inlineable lambda that provides a sequence
// of output values to write into output memory. Boolean and primitive outputs
// are currently implemented, and the validity bitmap is presumed to be handled
// at a higher level, so this writes into every output slot, null or not.
template <typename Type, typename Enable = void>
struct OutputAdapter;

template <typename Type>
struct OutputAdapter<Type, enable_if_boolean<Type>> {
  template <typename Generator>
  static Status Write(KernelContext*, ArraySpan* out, Generator&& generator) {
    GenerateBitsUnrolled(out->buffers[1].data, out->offset, out->length,
                         std::forward<Generator>(generator));
    return Status::OK();
  }
};

template <typename Type>
struct OutputAdapter<Type, enable_if_c_number_or_decimal<Type>> {
  using T = typename TypeTraits<Type>::ScalarType::ValueType;

  template <typename Generator>
  static Status Write(KernelContext*, ArraySpan* out, Generator&& generator) {
    T* out_data = out->GetValues<T>(1);
    // TODO: Is this as fast as a more explicitly inlined function?
    for (int64_t i = 0; i < out->length; ++i) {
      *out_data++ = generator();
    }
    return Status::OK();
  }
};

template <typename Type>
struct OutputAdapter<Type, enable_if_base_binary<Type>> {
  template <typename Generator>
  static Status Write(KernelContext* ctx, ArraySpan* out, Generator&& generator) {
    return Status::NotImplemented("NYI");
  }
};

// A kernel exec generator for unary functions that addresses both array and
// scalar inputs and dispatches input iteration and output writing to other
// templates
//
// This template executes the operator even on the data behind null values,
// therefore it is generally only suitable for operators that are safe to apply
// even on the null slot values.
//
// The "Op" functor should have the form
//
// struct Op {
//   template <typename OutValue, typename Arg0Value>
//   static OutValue Call(KernelContext* ctx, Arg0Value val, Status* st) {
//     // implementation
//     // NOTE: "status" should only populated with errors,
//     //        leave it unmodified to indicate Status::OK()
//   }
// };
template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnary {
  using OutValue = typename GetOutputType<OutType>::T;
  using Arg0Value = typename GetViewType<Arg0Type>::T;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    DCHECK(batch[0].is_array());
    const ArraySpan& arg0 = batch[0].array;
    Status st = Status::OK();
    ArrayIterator<Arg0Type> arg0_it(arg0);
    RETURN_NOT_OK(
        OutputAdapter<OutType>::Write(ctx, out->array_span_mutable(), [&]() -> OutValue {
          return Op::template Call<OutValue, Arg0Value>(ctx, arg0_it(), &st);
        }));
    return st;
  }
};

// An alternative to ScalarUnary that Applies a scalar operation with state on
// only the not-null values of a single array
template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnaryNotNullStateful {
  using ThisType = ScalarUnaryNotNullStateful<OutType, Arg0Type, Op>;
  using OutValue = typename GetOutputType<OutType>::T;
  using Arg0Value = typename GetViewType<Arg0Type>::T;

  Op op;
  explicit ScalarUnaryNotNullStateful(Op op) : op(std::move(op)) {}

  // NOTE: In ArrayExec<Type>, Type is really OutputType

  template <typename Type, typename Enable = void>
  struct ArrayExec {
    static Status Exec(const ThisType& functor, KernelContext* ctx, const ExecSpan& batch,
                       ExecResult* out) {
      ARROW_LOG(FATAL) << "Missing ArrayExec specialization for output type "
                       << out->type();
      return Status::NotImplemented("NYI");
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_c_number_or_decimal<Type>> {
    static Status Exec(const ThisType& functor, KernelContext* ctx, const ArraySpan& arg0,
                       ExecResult* out) {
      Status st = Status::OK();
      auto out_data = out->array_span_mutable()->GetValues<OutValue>(1);
      VisitArrayValuesInline<Arg0Type>(
          arg0,
          [&](Arg0Value v) {
            *out_data++ = functor.op.template Call<OutValue, Arg0Value>(ctx, v, &st);
          },
          [&]() {
            // null
            *out_data++ = OutValue{};
          });
      return st;
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_base_binary<Type>> {
    static Status Exec(const ThisType& functor, KernelContext* ctx, const ArraySpan& arg0,
                       ExecResult* out) {
      // NOTE: This code is not currently used by any kernels and has
      // suboptimal performance because it's recomputing the validity bitmap
      // that is already computed by the kernel execution layer. Consider
      // writing a lower-level "output adapter" for base binary types.
      typename TypeTraits<Type>::BuilderType builder;
      Status st = Status::OK();
      RETURN_NOT_OK(VisitArrayValuesInline<Arg0Type>(
          arg0, [&](Arg0Value v) { return builder.Append(functor.op.Call(ctx, v, &st)); },
          [&]() { return builder.AppendNull(); }));
      if (st.ok()) {
        std::shared_ptr<ArrayData> result;
        RETURN_NOT_OK(builder.FinishInternal(&result));
        out->value = std::move(result);
      }
      return st;
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_t<is_boolean_type<Type>::value>> {
    static Status Exec(const ThisType& functor, KernelContext* ctx, const ArraySpan& arg0,
                       ExecResult* out) {
      Status st = Status::OK();
      ArraySpan* out_arr = out->array_span_mutable();
      FirstTimeBitmapWriter out_writer(out_arr->buffers[1].data, out_arr->offset,
                                       out_arr->length);
      VisitArrayValuesInline<Arg0Type>(
          arg0,
          [&](Arg0Value v) {
            if (functor.op.template Call<OutValue, Arg0Value>(ctx, v, &st)) {
              out_writer.Set();
            }
            out_writer.Next();
          },
          [&]() {
            // null
            out_writer.Clear();
            out_writer.Next();
          });
      out_writer.Finish();
      return st;
    }
  };

  Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    DCHECK(batch[0].is_array());
    return ArrayExec<OutType>::Exec(*this, ctx, batch[0].array, out);
  }
};

// An alternative to ScalarUnary that Applies a scalar operation on only the
// not-null values of a single array. The operator is not stateful; if the
// operator requires some initialization use ScalarUnaryNotNullStateful
template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnaryNotNull {
  using OutValue = typename GetOutputType<OutType>::T;
  using Arg0Value = typename GetViewType<Arg0Type>::T;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // Seed kernel with dummy state
    ScalarUnaryNotNullStateful<OutType, Arg0Type, Op> kernel({});
    return kernel.Exec(ctx, batch, out);
  }
};

// A kernel exec generator for binary functions that addresses both array and
// scalar inputs and dispatches input iteration and output writing to other
// templates
//
// This template executes the operator even on the data behind null values,
// therefore it is generally only suitable for operators that are safe to apply
// even on the null slot values.
//
// The "Op" functor should have the form
//
// struct Op {
//   template <typename OutValue, typename Arg0Value, typename Arg1Value>
//   static OutValue Call(KernelContext* ctx, Arg0Value arg0, Arg1Value arg1, Status* st)
//   {
//     // implementation
//     // NOTE: "status" should only populated with errors,
//     //       leave it unmodified to indicate Status::OK()
//   }
// };
template <typename OutType, typename Arg0Type, typename Arg1Type, typename Op>
struct ScalarBinary {
  using OutValue = typename GetOutputType<OutType>::T;
  using Arg0Value = typename GetViewType<Arg0Type>::T;
  using Arg1Value = typename GetViewType<Arg1Type>::T;

  static Status ArrayArray(KernelContext* ctx, const ArraySpan& arg0,
                           const ArraySpan& arg1, ExecResult* out) {
    Status st = Status::OK();
    ArrayIterator<Arg0Type> arg0_it(arg0);
    ArrayIterator<Arg1Type> arg1_it(arg1);
    RETURN_NOT_OK(
        OutputAdapter<OutType>::Write(ctx, out->array_span_mutable(), [&]() -> OutValue {
          return Op::template Call<OutValue, Arg0Value, Arg1Value>(ctx, arg0_it(),
                                                                   arg1_it(), &st);
        }));
    return st;
  }

  static Status ArrayScalar(KernelContext* ctx, const ArraySpan& arg0, const Scalar& arg1,
                            ExecResult* out) {
    Status st = Status::OK();
    ArrayIterator<Arg0Type> arg0_it(arg0);
    auto arg1_val = UnboxScalar<Arg1Type>::Unbox(arg1);
    RETURN_NOT_OK(
        OutputAdapter<OutType>::Write(ctx, out->array_span_mutable(), [&]() -> OutValue {
          return Op::template Call<OutValue, Arg0Value, Arg1Value>(ctx, arg0_it(),
                                                                   arg1_val, &st);
        }));
    return st;
  }

  static Status ScalarArray(KernelContext* ctx, const Scalar& arg0, const ArraySpan& arg1,
                            ExecResult* out) {
    Status st = Status::OK();
    auto arg0_val = UnboxScalar<Arg0Type>::Unbox(arg0);
    ArrayIterator<Arg1Type> arg1_it(arg1);
    RETURN_NOT_OK(
        OutputAdapter<OutType>::Write(ctx, out->array_span_mutable(), [&]() -> OutValue {
          return Op::template Call<OutValue, Arg0Value, Arg1Value>(ctx, arg0_val,
                                                                   arg1_it(), &st);
        }));
    return st;
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (batch[0].is_array()) {
      if (batch[1].is_array()) {
        return ArrayArray(ctx, batch[0].array, batch[1].array, out);
      } else {
        return ArrayScalar(ctx, batch[0].array, *batch[1].scalar, out);
      }
    } else {
      if (batch[1].is_array()) {
        return ScalarArray(ctx, *batch[0].scalar, batch[1].array, out);
      } else {
        DCHECK(false);
        return Status::Invalid("Should be unreachable");
      }
    }
  }
};

// An alternative to ScalarBinary that Applies a scalar operation with state on
// only the value pairs which are not-null in both arrays
template <typename OutType, typename Arg0Type, typename Arg1Type, typename Op>
struct ScalarBinaryNotNullStateful {
  using ThisType = ScalarBinaryNotNullStateful<OutType, Arg0Type, Arg1Type, Op>;
  using OutValue = typename GetOutputType<OutType>::T;
  using Arg0Value = typename GetViewType<Arg0Type>::T;
  using Arg1Value = typename GetViewType<Arg1Type>::T;

  Op op;
  explicit ScalarBinaryNotNullStateful(Op op) : op(std::move(op)) {}

  // NOTE: In ArrayExec<Type>, Type is really OutputType

  Status ArrayArray(KernelContext* ctx, const ArraySpan& arg0, const ArraySpan& arg1,
                    ExecResult* out) {
    Status st = Status::OK();
    OutputArrayWriter<OutType> writer(out->array_span_mutable());
    VisitTwoArrayValuesInline<Arg0Type, Arg1Type>(
        arg0, arg1,
        [&](Arg0Value u, Arg1Value v) {
          writer.Write(op.template Call<OutValue, Arg0Value, Arg1Value>(ctx, u, v, &st));
        },
        [&]() { writer.WriteNull(); });
    return st;
  }

  Status ArrayScalar(KernelContext* ctx, const ArraySpan& arg0, const Scalar& arg1,
                     ExecResult* out) {
    Status st = Status::OK();
    ArraySpan* out_span = out->array_span_mutable();
    OutputArrayWriter<OutType> writer(out_span);
    if (arg1.is_valid) {
      const auto arg1_val = UnboxScalar<Arg1Type>::Unbox(arg1);
      VisitArrayValuesInline<Arg0Type>(
          arg0,
          [&](Arg0Value u) {
            writer.Write(
                op.template Call<OutValue, Arg0Value, Arg1Value>(ctx, u, arg1_val, &st));
          },
          [&]() { writer.WriteNull(); });
    } else {
      writer.WriteAllNull(out_span->length);
    }
    return st;
  }

  Status ScalarArray(KernelContext* ctx, const Scalar& arg0, const ArraySpan& arg1,
                     ExecResult* out) {
    Status st = Status::OK();
    ArraySpan* out_span = out->array_span_mutable();
    OutputArrayWriter<OutType> writer(out_span);
    if (arg0.is_valid) {
      const auto arg0_val = UnboxScalar<Arg0Type>::Unbox(arg0);
      VisitArrayValuesInline<Arg1Type>(
          arg1,
          [&](Arg1Value v) {
            writer.Write(
                op.template Call<OutValue, Arg0Value, Arg1Value>(ctx, arg0_val, v, &st));
          },
          [&]() { writer.WriteNull(); });
    } else {
      writer.WriteAllNull(out_span->length);
    }
    return st;
  }

  Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (batch[0].is_array()) {
      if (batch[1].is_array()) {
        return ArrayArray(ctx, batch[0].array, batch[1].array, out);
      } else {
        return ArrayScalar(ctx, batch[0].array, *batch[1].scalar, out);
      }
    } else {
      if (batch[1].is_array()) {
        return ScalarArray(ctx, *batch[0].scalar, batch[1].array, out);
      } else {
        DCHECK(false);
        return Status::Invalid("Should be unreachable");
      }
    }
  }
};

// An alternative to ScalarBinary that Applies a scalar operation on only
// the value pairs which are not-null in both arrays.
// The operator is not stateful; if the operator requires some initialization
// use ScalarBinaryNotNullStateful.
template <typename OutType, typename Arg0Type, typename Arg1Type, typename Op>
struct ScalarBinaryNotNull {
  using OutValue = typename GetOutputType<OutType>::T;
  using Arg0Value = typename GetViewType<Arg0Type>::T;
  using Arg1Value = typename GetViewType<Arg1Type>::T;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // Seed kernel with dummy state
    ScalarBinaryNotNullStateful<OutType, Arg0Type, Arg1Type, Op> kernel({});
    return kernel.Exec(ctx, batch, out);
  }
};

// A kernel exec generator for binary kernels where both input types are the
// same
template <typename OutType, typename ArgType, typename Op>
using ScalarBinaryEqualTypes = ScalarBinary<OutType, ArgType, ArgType, Op>;

// A kernel exec generator for non-null binary kernels where both input types are the
// same
template <typename OutType, typename ArgType, typename Op>
using ScalarBinaryNotNullEqualTypes = ScalarBinaryNotNull<OutType, ArgType, ArgType, Op>;

template <typename OutType, typename ArgType, typename Op>
using ScalarBinaryNotNullStatefulEqualTypes =
    ScalarBinaryNotNullStateful<OutType, ArgType, ArgType, Op>;

}  // namespace applicator

// ----------------------------------------------------------------------
// BEGIN of kernel generator-dispatchers ("GD")
//
// These GD functions instantiate kernel functor templates and select one of
// the instantiated kernels dynamically based on the data type or Type::type id
// that is passed. This enables functions to be populated with kernels by
// looping over vectors of data types rather than using macros or other
// approaches.
//
// The kernel functor must be of the form:
//
// template <typename Type0, typename Type1, Args...>
// struct FUNCTOR {
//   static void Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
//     // IMPLEMENTATION
//   }
// };
//
// When you pass FUNCTOR to a GD function, you must pass at least one static
// type along with the functor -- this is often the fixed return type of the
// functor. This Type0 argument is passed as the first argument to the functor
// during instantiation. The 2nd type passed to the functor is the DataType
// subclass corresponding to the type passed as argument (not template type) to
// the function.
//
// For example, GenerateNumeric<FUNCTOR, Type0>(int32()) will select a kernel
// instantiated like FUNCTOR<Type0, Int32Type>. Any additional variadic
// template arguments will be passed as additional template arguments to the
// kernel template.

namespace detail {

// Convenience so we can pass DataType or Type::type for the GD's
struct GetTypeId {
  Type::type id;
  GetTypeId(const std::shared_ptr<DataType>& type)  // NOLINT implicit construction
      : id(type->id()) {}
  GetTypeId(const DataType& type)  // NOLINT implicit construction
      : id(type.id()) {}
  GetTypeId(Type::type id)  // NOLINT implicit construction
      : id(id) {}
};

}  // namespace detail

template <typename KernelType>
struct FailFunctor {};

template <>
struct FailFunctor<ArrayKernelExec> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return Status::NotImplemented("This kernel is malformed");
  }
};

template <>
struct FailFunctor<VectorKernel::ChunkedExec> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return Status::NotImplemented("This kernel is malformed");
  }
};

// GD for numeric types (integer and floating point)
template <template <typename...> class Generator, typename Type0,
          typename KernelType = ArrayKernelExec, typename... Args>
KernelType GenerateNumeric(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return Generator<Type0, Int8Type, Args...>::Exec;
    case Type::UINT8:
      return Generator<Type0, UInt8Type, Args...>::Exec;
    case Type::INT16:
      return Generator<Type0, Int16Type, Args...>::Exec;
    case Type::UINT16:
      return Generator<Type0, UInt16Type, Args...>::Exec;
    case Type::INT32:
      return Generator<Type0, Int32Type, Args...>::Exec;
    case Type::UINT32:
      return Generator<Type0, UInt32Type, Args...>::Exec;
    case Type::INT64:
      return Generator<Type0, Int64Type, Args...>::Exec;
    case Type::UINT64:
      return Generator<Type0, UInt64Type, Args...>::Exec;
    case Type::FLOAT:
      return Generator<Type0, FloatType, Args...>::Exec;
    case Type::DOUBLE:
      return Generator<Type0, DoubleType, Args...>::Exec;
    default:
      DCHECK(false);
      return FailFunctor<KernelType>::Exec;
  }
}

// Generate a kernel given a templated functor for floating point types
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateFloatingPoint(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::FLOAT:
      return Generator<Type0, FloatType, Args...>::Exec;
    case Type::DOUBLE:
      return Generator<Type0, DoubleType, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// Generate a kernel given a templated functor for integer types
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateInteger(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return Generator<Type0, Int8Type, Args...>::Exec;
    case Type::INT16:
      return Generator<Type0, Int16Type, Args...>::Exec;
    case Type::INT32:
      return Generator<Type0, Int32Type, Args...>::Exec;
    case Type::INT64:
      return Generator<Type0, Int64Type, Args...>::Exec;
    case Type::UINT8:
      return Generator<Type0, UInt8Type, Args...>::Exec;
    case Type::UINT16:
      return Generator<Type0, UInt16Type, Args...>::Exec;
    case Type::UINT32:
      return Generator<Type0, UInt32Type, Args...>::Exec;
    case Type::UINT64:
      return Generator<Type0, UInt64Type, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GeneratePhysicalInteger(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return Generator<Type0, Int8Type, Args...>::Exec;
    case Type::INT16:
      return Generator<Type0, Int16Type, Args...>::Exec;
    case Type::INT32:
    case Type::DATE32:
    case Type::TIME32:
      return Generator<Type0, Int32Type, Args...>::Exec;
    case Type::INT64:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
      return Generator<Type0, Int64Type, Args...>::Exec;
    case Type::UINT8:
      return Generator<Type0, UInt8Type, Args...>::Exec;
    case Type::UINT16:
      return Generator<Type0, UInt16Type, Args...>::Exec;
    case Type::UINT32:
      return Generator<Type0, UInt32Type, Args...>::Exec;
    case Type::UINT64:
      return Generator<Type0, UInt64Type, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

template <template <typename...> class KernelGenerator, typename Op,
          typename KernelType = ArrayKernelExec, typename... Args>
KernelType ArithmeticExecFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return KernelGenerator<Int8Type, Int8Type, Op, Args...>::Exec;
    case Type::UINT8:
      return KernelGenerator<UInt8Type, UInt8Type, Op, Args...>::Exec;
    case Type::INT16:
      return KernelGenerator<Int16Type, Int16Type, Op, Args...>::Exec;
    case Type::UINT16:
      return KernelGenerator<UInt16Type, UInt16Type, Op, Args...>::Exec;
    case Type::INT32:
      return KernelGenerator<Int32Type, Int32Type, Op, Args...>::Exec;
    case Type::UINT32:
      return KernelGenerator<UInt32Type, UInt32Type, Op, Args...>::Exec;
    case Type::DURATION:
    case Type::INT64:
    case Type::TIMESTAMP:
      return KernelGenerator<Int64Type, Int64Type, Op, Args...>::Exec;
    case Type::UINT64:
      return KernelGenerator<UInt64Type, UInt64Type, Op, Args...>::Exec;
    case Type::FLOAT:
      return KernelGenerator<FloatType, FloatType, Op, Args...>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, DoubleType, Op, Args...>::Exec;
    default:
      DCHECK(false);
      return FailFunctor<KernelType>::Exec;
  }
}

template <typename ReturnType, template <typename... Args> class Generator,
          typename... Args>
ReturnType GeneratePhysicalNumericGeneric(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return Generator<Int8Type, Args...>::Exec;
    case Type::INT16:
      return Generator<Int16Type, Args...>::Exec;
    case Type::INT32:
    case Type::DATE32:
    case Type::TIME32:
      return Generator<Int32Type, Args...>::Exec;
    case Type::INT64:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
      return Generator<Int64Type, Args...>::Exec;
    case Type::UINT8:
      return Generator<UInt8Type, Args...>::Exec;
    case Type::UINT16:
      return Generator<UInt16Type, Args...>::Exec;
    case Type::UINT32:
      return Generator<UInt32Type, Args...>::Exec;
    case Type::UINT64:
      return Generator<UInt64Type, Args...>::Exec;
    case Type::FLOAT:
      return Generator<FloatType, Args...>::Exec;
    case Type::DOUBLE:
      return Generator<DoubleType, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}
template <template <typename... Args> class Generator, typename... Args>
ArrayKernelExec GeneratePhysicalNumeric(detail::GetTypeId get_id) {
  return GeneratePhysicalNumericGeneric<ArrayKernelExec, Generator, Args...>(get_id);
}

// Generate a kernel given a templated functor for decimal types
template <template <typename... Args> class Generator, typename... Args>
ArrayKernelExec GenerateDecimalToDecimal(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::DECIMAL128:
      return Generator<Decimal128Type, Args...>::Exec;
    case Type::DECIMAL256:
      return Generator<Decimal256Type, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// Generate a kernel given a templated functor for integer types
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateSignedInteger(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return Generator<Type0, Int8Type, Args...>::Exec;
    case Type::INT16:
      return Generator<Type0, Int16Type, Args...>::Exec;
    case Type::INT32:
      return Generator<Type0, Int32Type, Args...>::Exec;
    case Type::INT64:
      return Generator<Type0, Int64Type, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// Generate a kernel given a templated functor. Only a single template is
// instantiated for each bit width, and the functor is expected to treat types
// of the same bit width the same without utilizing any type-specific behavior
// (e.g. int64 should be handled equivalent to uint64 or double -- all 64
// bits).
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator, typename KernelType = ArrayKernelExec,
          typename... Args>
KernelType GenerateTypeAgnosticPrimitive(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::NA:
      return Generator<NullType, Args...>::Exec;
    case Type::BOOL:
      return Generator<BooleanType, Args...>::Exec;
    case Type::UINT8:
    case Type::INT8:
      return Generator<UInt8Type, Args...>::Exec;
    case Type::UINT16:
    case Type::INT16:
      return Generator<UInt16Type, Args...>::Exec;
    case Type::UINT32:
    case Type::INT32:
    case Type::FLOAT:
    case Type::DATE32:
    case Type::TIME32:
    case Type::INTERVAL_MONTHS:
      return Generator<UInt32Type, Args...>::Exec;
    case Type::UINT64:
    case Type::INT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
    case Type::INTERVAL_DAY_TIME:
      return Generator<UInt64Type, Args...>::Exec;
    case Type::INTERVAL_MONTH_DAY_NANO:
      return Generator<MonthDayNanoIntervalType, Args...>::Exec;
    default:
      DCHECK(false);
      return FailFunctor<KernelType>::Exec;
  }
}

// similar to GenerateTypeAgnosticPrimitive, but for base variable binary types
template <template <typename...> class Generator, typename KernelType = ArrayKernelExec,
          typename... Args>
KernelType GenerateTypeAgnosticVarBinaryBase(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::BINARY:
    case Type::STRING:
      return Generator<BinaryType, Args...>::Exec;
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
      return Generator<LargeBinaryType, Args...>::Exec;
    default:
      DCHECK(false);
      return FailFunctor<KernelType>::Exec;
  }
}

// Generate a kernel given a templated functor for binary and string types
template <template <typename...> class Generator, typename... Args>
ArrayKernelExec GenerateVarBinaryToVarBinary(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::BINARY:
      return Generator<BinaryType, Args...>::Exec;
    case Type::STRING:
      return Generator<StringType, Args...>::Exec;
    case Type::LARGE_BINARY:
      return Generator<LargeBinaryType, Args...>::Exec;
    case Type::LARGE_STRING:
      return Generator<LargeStringType, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// Generate a kernel given a templated functor for base binary types. Generates
// a single kernel for binary/string and large binary/large string. If your kernel
// implementation needs access to the specific type at compile time, please use
// BaseBinarySpecific.
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateVarBinaryBase(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::BINARY:
    case Type::STRING:
      return Generator<Type0, BinaryType, Args...>::Exec;
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
      return Generator<Type0, LargeBinaryType, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// See BaseBinary documentation
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateVarBinary(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::BINARY:
      return Generator<Type0, BinaryType, Args...>::Exec;
    case Type::STRING:
      return Generator<Type0, StringType, Args...>::Exec;
    case Type::LARGE_BINARY:
      return Generator<Type0, LargeBinaryType, Args...>::Exec;
    case Type::LARGE_STRING:
      return Generator<Type0, LargeStringType, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// Generate a kernel given a templated functor for temporal types
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateTemporal(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::DATE32:
      return Generator<Type0, Date32Type, Args...>::Exec;
    case Type::DATE64:
      return Generator<Type0, Date64Type, Args...>::Exec;
    case Type::DURATION:
      return Generator<Type0, DurationType, Args...>::Exec;
    case Type::TIME32:
      return Generator<Type0, Time32Type, Args...>::Exec;
    case Type::TIME64:
      return Generator<Type0, Time64Type, Args...>::Exec;
    case Type::TIMESTAMP:
      return Generator<Type0, TimestampType, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// Generate a kernel given a templated functor for decimal types
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateDecimal(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::DECIMAL128:
      return Generator<Type0, Decimal128Type, Args...>::Exec;
    case Type::DECIMAL256:
      return Generator<Type0, Decimal256Type, Args...>::Exec;
    default:
      DCHECK(false);
      return nullptr;
  }
}

// END of kernel generator-dispatchers
// ----------------------------------------------------------------------

ARROW_EXPORT
void EnsureDictionaryDecoded(std::vector<TypeHolder>* types);

ARROW_EXPORT
void EnsureDictionaryDecoded(TypeHolder* begin, size_t count);

ARROW_EXPORT
void ReplaceNullWithOtherType(std::vector<TypeHolder>* types);

ARROW_EXPORT
void ReplaceNullWithOtherType(TypeHolder* begin, size_t count);

ARROW_EXPORT
void ReplaceTypes(const TypeHolder& replacement, std::vector<TypeHolder>* types);

ARROW_EXPORT
void ReplaceTypes(const TypeHolder& replacement, TypeHolder* types, size_t count);

ARROW_EXPORT
void ReplaceTemporalTypes(TimeUnit::type unit, std::vector<TypeHolder>* types);

ARROW_EXPORT
TypeHolder CommonNumeric(const std::vector<TypeHolder>& types);

ARROW_EXPORT
TypeHolder CommonNumeric(const TypeHolder* begin, size_t count);

ARROW_EXPORT
TypeHolder CommonTemporal(const TypeHolder* begin, size_t count);

ARROW_EXPORT
bool CommonTemporalResolution(const TypeHolder* begin, size_t count,
                              TimeUnit::type* finest_unit);

ARROW_EXPORT
TypeHolder CommonBinary(const TypeHolder* begin, size_t count);

/// How to promote decimal precision/scale in CastBinaryDecimalArgs.
enum class DecimalPromotion : uint8_t {
  kAdd,
  kMultiply,
  kDivide,
};

/// Given two arguments, at least one of which is decimal, promote all
/// to not necessarily identical types, but types which are compatible
/// for the given operator (add/multiply/divide).
ARROW_EXPORT
Status CastBinaryDecimalArgs(DecimalPromotion promotion, std::vector<TypeHolder>* types);

/// Given one or more arguments, at least one of which is decimal,
/// promote all to an identical type.
ARROW_EXPORT
Status CastDecimalArgs(TypeHolder* begin, size_t count);

ARROW_EXPORT
bool HasDecimal(const std::vector<TypeHolder>& types);

}  // namespace internal
}  // namespace compute
}  // namespace arrow

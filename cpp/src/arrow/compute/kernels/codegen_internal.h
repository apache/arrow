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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::BitmapReader;
using internal::checked_cast;
using internal::FirstTimeBitmapWriter;
using internal::GenerateBitsUnrolled;

namespace compute {
namespace internal {

#ifdef ARROW_EXTRA_ERROR_CONTEXT

#define KERNEL_RETURN_IF_ERROR(ctx, expr)            \
  do {                                               \
    Status _st = (expr);                             \
    if (ARROW_PREDICT_FALSE(!_st.ok())) {            \
      _st.AddContextLine(__FILE__, __LINE__, #expr); \
      ctx->SetStatus(_st);                           \
      return;                                        \
    }                                                \
  } while (0)

#else

#define KERNEL_RETURN_IF_ERROR(ctx, expr) \
  do {                                    \
    Status _st = (expr);                  \
    if (ARROW_PREDICT_FALSE(!_st.ok())) { \
      ctx->SetStatus(_st);                \
      return;                             \
    }                                     \
  } while (0)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

#define KERNEL_ASSIGN_OR_RAISE_IMPL(result_name, lhs, ctx, rexpr) \
  auto result_name = (rexpr);                                     \
  KERNEL_RETURN_IF_ERROR(ctx, (result_name).status());            \
  lhs = std::move(result_name).MoveValueUnsafe();

#define KERNEL_ASSIGN_OR_RAISE_NAME(x, y) ARROW_CONCAT(x, y)

#define KERNEL_ASSIGN_OR_RAISE(lhs, ctx, rexpr)                                          \
  KERNEL_ASSIGN_OR_RAISE_IMPL(KERNEL_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                              lhs, ctx, rexpr);

/// KernelState adapter for the common case of kernels whose only
/// state is an instance of a subclass of FunctionOptions.
/// Default FunctionOptions are *not* handled here.
template <typename OptionsType>
struct OptionsWrapper : public KernelState {
  explicit OptionsWrapper(OptionsType options) : options(std::move(options)) {}

  static std::unique_ptr<KernelState> Init(KernelContext* ctx,
                                           const KernelInitArgs& args) {
    if (auto options = static_cast<const OptionsType*>(args.options)) {
      return ::arrow::internal::make_unique<OptionsWrapper>(*options);
    }

    ctx->SetStatus(
        Status::Invalid("Attempted to initialize KernelState from null FunctionOptions"));
    return NULLPTR;
  }

  static const OptionsType& Get(const KernelState& state) {
    return ::arrow::internal::checked_cast<const OptionsWrapper&>(state).options;
  }

  static const OptionsType& Get(KernelContext* ctx) { return Get(*ctx->state()); }

  OptionsType options;
};

// ----------------------------------------------------------------------
// Iteration / value access utilities

template <typename T, typename R = void>
using enable_if_has_c_type_not_boolean =
    enable_if_t<has_c_type<T>::value && !is_boolean_type<T>::value, R>;

template <typename Type, typename Enable = void>
struct ArrayIterator;

template <typename Type>
struct ArrayIterator<Type, enable_if_has_c_type_not_boolean<Type>> {
  using T = typename Type::c_type;
  const T* values;
  explicit ArrayIterator(const ArrayData& data) : values(data.GetValues<T>(1)) {}
  T operator()() { return *values++; }
};

template <typename Type>
struct ArrayIterator<Type, enable_if_boolean<Type>> {
  BitmapReader reader;
  explicit ArrayIterator(const ArrayData& data)
      : reader(data.buffers[1]->data(), data.offset, data.length) {}
  bool operator()() {
    bool out = reader.IsSet();
    reader.Next();
    return out;
  }
};

template <typename Type>
struct ArrayIterator<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  const ArrayData& arr;
  const offset_type* offsets;
  offset_type cur_offset;
  const char* data;
  int64_t position;
  explicit ArrayIterator(const ArrayData& arr)
      : arr(arr),
        offsets(reinterpret_cast<const offset_type*>(arr.buffers[1]->data()) +
                arr.offset),
        cur_offset(offsets[0]),
        data(reinterpret_cast<const char*>(arr.buffers[2]->data())),
        position(0) {}

  util::string_view operator()() {
    offset_type next_offset = offsets[position++ + 1];
    auto result = util::string_view(data + cur_offset, next_offset - cur_offset);
    cur_offset = next_offset;
    return result;
  }
};

template <typename Type, typename Enable = void>
struct UnboxScalar;

template <typename Type>
struct UnboxScalar<Type, enable_if_has_c_type<Type>> {
  using T = typename Type::c_type;
  static T Unbox(const Scalar& val) {
    return *reinterpret_cast<const T*>(
        checked_cast<const ::arrow::internal::PrimitiveScalarBase&>(val).data());
  }
};

template <typename Type>
struct UnboxScalar<Type, enable_if_base_binary<Type>> {
  static util::string_view Unbox(const Scalar& val) {
    return util::string_view(*checked_cast<const BaseBinaryScalar&>(val).value);
  }
};

template <>
struct UnboxScalar<Decimal128Type> {
  static Decimal128 Unbox(const Scalar& val) {
    return checked_cast<const Decimal128Scalar&>(val).value;
  }
};

template <typename Type, typename Enable = void>
struct GetViewType;

template <typename Type>
struct GetViewType<Type, enable_if_has_c_type<Type>> {
  using T = typename Type::c_type;
};

template <typename Type>
struct GetViewType<Type, enable_if_t<is_base_binary_type<Type>::value ||
                                     is_fixed_size_binary_type<Type>::value>> {
  using T = util::string_view;
};

template <>
struct GetViewType<Decimal128Type> {
  using T = Decimal128;
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

template <typename Type, typename Enable = void>
struct BoxScalar;

template <typename Type>
struct BoxScalar<Type, enable_if_has_c_type<Type>> {
  using T = typename GetOutputType<Type>::T;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static void Box(T val, Scalar* out) { checked_cast<ScalarType*>(out)->value = val; }
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

// ----------------------------------------------------------------------
// Reusable type resolvers

Result<ValueDescr> FirstType(KernelContext*, const std::vector<ValueDescr>& descrs);

// ----------------------------------------------------------------------
// Generate an array kernel given template classes

void ExecFail(KernelContext* ctx, const ExecBatch& batch, Datum* out);

ArrayKernelExec MakeFlippedBinaryExec(ArrayKernelExec exec);

// ----------------------------------------------------------------------
// Helpers for iterating over common DataType instances for adding kernels to
// functions

const std::vector<std::shared_ptr<DataType>>& BaseBinaryTypes();
const std::vector<std::shared_ptr<DataType>>& StringTypes();
const std::vector<std::shared_ptr<DataType>>& SignedIntTypes();
const std::vector<std::shared_ptr<DataType>>& UnsignedIntTypes();
const std::vector<std::shared_ptr<DataType>>& IntTypes();
const std::vector<std::shared_ptr<DataType>>& FloatingPointTypes();

ARROW_EXPORT
const std::vector<TimeUnit::type>& AllTimeUnits();

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

// Number types without boolean
const std::vector<std::shared_ptr<DataType>>& NumericTypes();

// Temporal types including time and timestamps for each unit
const std::vector<std::shared_ptr<DataType>>& TemporalTypes();

// Integer, floating point, base binary, and temporal
const std::vector<std::shared_ptr<DataType>>& PrimitiveTypes();

// ----------------------------------------------------------------------
// "Applicators" take an operator definition (which may be scalar-valued or
// array-valued) and creates an ArrayKernelExec which can be used to add an
// ArrayKernel to a Function.

namespace applicator {

// Generate an ArrayKernelExec given a functor that handles all of its own
// iteration, etc.
//
// Operator must implement
//
// static void Call(KernelContext*, const ArrayData& in, ArrayData* out)
// static void Call(KernelContext*, const Scalar& in, Scalar* out)
template <typename Operator>
void SimpleUnary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  if (batch[0].kind() == Datum::SCALAR) {
    Operator::Call(ctx, *batch[0].scalar(), out->scalar().get());
  } else if (batch.length > 0) {
    Operator::Call(ctx, *batch[0].array(), out->mutable_array());
  }
}

// Generate an ArrayKernelExec given a functor that handles all of its own
// iteration, etc.
//
// Operator must implement
//
// static void Call(KernelContext*, const ArrayData& arg0, const ArrayData& arg1,
//                  ArrayData* out)
template <typename Operator>
void SimpleBinary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  if (batch[0].kind() == Datum::SCALAR || batch[1].kind() == Datum::SCALAR) {
    ctx->SetStatus(Status::NotImplemented("NYI"));
  } else if (batch.length > 0) {
    Operator::Call(ctx, *batch[0].array(), *batch[1].array(), out->mutable_array());
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
  static void Write(KernelContext*, Datum* out, Generator&& generator) {
    ArrayData* out_arr = out->mutable_array();
    auto out_bitmap = out_arr->buffers[1]->mutable_data();
    GenerateBitsUnrolled(out_bitmap, out_arr->offset, out_arr->length,
                         std::forward<Generator>(generator));
  }
};

template <typename Type>
struct OutputAdapter<Type, enable_if_has_c_type_not_boolean<Type>> {
  template <typename Generator>
  static void Write(KernelContext*, Datum* out, Generator&& generator) {
    ArrayData* out_arr = out->mutable_array();
    auto out_data = out_arr->GetMutableValues<typename Type::c_type>(1);
    // TODO: Is this as fast as a more explicitly inlined function?
    for (int64_t i = 0; i < out_arr->length; ++i) {
      *out_data++ = generator();
    }
  }
};

template <typename Type>
struct OutputAdapter<Type, enable_if_base_binary<Type>> {
  template <typename Generator>
  static void Write(KernelContext* ctx, Datum* out, Generator&& generator) {
    ctx->SetStatus(Status::NotImplemented("NYI"));
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
//   template <typename OUT, typename ARG0>
//   static OUT Call(KernelContext* ctx, ARG0 val) {
//     // implementation
//   }
// };
template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnary {
  using OUT = typename GetOutputType<OutType>::T;
  using ARG0 = typename GetViewType<Arg0Type>::T;

  static void Array(KernelContext* ctx, const ArrayData& arg0, Datum* out) {
    ArrayIterator<Arg0Type> arg0_it(arg0);
    OutputAdapter<OutType>::Write(
        ctx, out, [&]() -> OUT { return Op::template Call<OUT, ARG0>(ctx, arg0_it()); });
  }

  static void Scalar(KernelContext* ctx, const Scalar& arg0, Datum* out) {
    if (arg0.is_valid) {
      ARG0 arg0_val = UnboxScalar<Arg0Type>::Unbox(arg0);
      BoxScalar<OutType>::Box(Op::template Call<OUT, ARG0>(ctx, arg0_val),
                              out->scalar().get());
    } else {
      out->value = MakeNullScalar(arg0.type);
    }
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      return Array(ctx, *batch[0].array(), out);
    } else {
      return Scalar(ctx, *batch[0].scalar(), out);
    }
  }
};

// A VisitArrayDataInline variant that passes a Decimal128 value,
// not util::string_view, for decimal128 arrays,

template <typename T, typename VisitFunc, typename NullFunc>
static typename std::enable_if<!std::is_same<T, Decimal128Type>::value, void>::type
VisitArrayValuesInline(const ArrayData& arr, VisitFunc&& valid_func,
                       NullFunc&& null_func) {
  VisitArrayDataInline<T>(arr, std::forward<VisitFunc>(valid_func),
                          std::forward<NullFunc>(null_func));
}

template <typename T, typename VisitFunc, typename NullFunc>
static typename std::enable_if<std::is_same<T, Decimal128Type>::value, void>::type
VisitArrayValuesInline(const ArrayData& arr, VisitFunc&& valid_func,
                       NullFunc&& null_func) {
  VisitArrayDataInline<T>(
      arr,
      [&](util::string_view v) {
        const auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v.data()));
        valid_func(dec_value);
      },
      std::forward<NullFunc>(null_func));
}

// An alternative to ScalarUnary that Applies a scalar operation with state on
// only the not-null values of a single array
template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnaryNotNullStateful {
  using ThisType = ScalarUnaryNotNullStateful<OutType, Arg0Type, Op>;
  using OUT = typename GetOutputType<OutType>::T;
  using ARG0 = typename GetViewType<Arg0Type>::T;

  Op op;
  explicit ScalarUnaryNotNullStateful(Op op) : op(std::move(op)) {}

  // NOTE: In ArrayExec<Type>, Type is really OutputType

  template <typename Type, typename Enable = void>
  struct ArrayExec {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ExecBatch& batch,
                     Datum* out) {
      ARROW_LOG(FATAL) << "Missing ArrayExec specialization for output type "
                       << out->type();
    }
  };

  template <typename Type>
  struct ArrayExec<
      Type, enable_if_t<has_c_type<Type>::value && !is_boolean_type<Type>::value>> {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ArrayData& arg0,
                     Datum* out) {
      ArrayData* out_arr = out->mutable_array();
      auto out_data = out_arr->GetMutableValues<OUT>(1);
      VisitArrayValuesInline<Arg0Type>(
          arg0,
          [&](ARG0 v) { *out_data++ = functor.op.template Call<OUT, ARG0>(ctx, v); },
          [&]() {
            // null
            ++out_data;
          });
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_base_binary<Type>> {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ArrayData& arg0,
                     Datum* out) {
      // NOTE: This code is not currently used by any kernels and has
      // suboptimal performance because it's recomputing the validity bitmap
      // that is already computed by the kernel execution layer. Consider
      // writing a lower-level "output adapter" for base binary types.
      typename TypeTraits<Type>::BuilderType builder;
      VisitArrayValuesInline<Arg0Type>(
          arg0,
          [&](ARG0 v) {
            KERNEL_RETURN_IF_ERROR(ctx, builder.Append(functor.op.Call(ctx, v)));
          },
          [&]() { KERNEL_RETURN_IF_ERROR(ctx, builder.AppendNull()); });
      if (!ctx->HasError()) {
        std::shared_ptr<ArrayData> result;
        ctx->SetStatus(builder.FinishInternal(&result));
        out->value = std::move(result);
      }
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_t<is_boolean_type<Type>::value>> {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ArrayData& arg0,
                     Datum* out) {
      ArrayData* out_arr = out->mutable_array();
      FirstTimeBitmapWriter out_writer(out_arr->buffers[1]->mutable_data(),
                                       out_arr->offset, out_arr->length);
      VisitArrayValuesInline<Arg0Type>(
          arg0,
          [&](ARG0 v) {
            if (functor.op.template Call<OUT, ARG0>(ctx, v)) {
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
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_t<std::is_same<Type, Decimal128Type>::value>> {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ArrayData& arg0,
                     Datum* out) {
      ArrayData* out_arr = out->mutable_array();
      auto out_data = out_arr->GetMutableValues<uint8_t>(1);
      VisitArrayValuesInline<Arg0Type>(
          arg0,
          [&](ARG0 v) {
            functor.op.template Call<OUT, ARG0>(ctx, v).ToBytes(out_data);
            out_data += 16;
          },
          [&]() { out_data += 16; });
    }
  };

  void Scalar(KernelContext* ctx, const Scalar& arg0, Datum* out) {
    if (arg0.is_valid) {
      ARG0 arg0_val = UnboxScalar<Arg0Type>::Unbox(arg0);
      BoxScalar<OutType>::Box(this->op.template Call<OUT, ARG0>(ctx, arg0_val),
                              out->scalar().get());
    } else {
      out->value = MakeNullScalar(arg0.type);
    }
  }

  void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      ArrayExec<OutType>::Exec(*this, ctx, *batch[0].array(), out);
    } else {
      return Scalar(ctx, *batch[0].scalar(), out);
    }
  }
};

// An alternative to ScalarUnary that Applies a scalar operation on only the
// not-null values of a single array. The operator is not stateful; if the
// operator requires some initialization use ScalarUnaryNotNullStateful
template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnaryNotNull {
  using OUT = typename GetOutputType<OutType>::T;
  using ARG0 = typename GetViewType<Arg0Type>::T;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
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
//   template <typename OUT, typename ARG0, typename ARG1>
//   static OUT Call(KernelContext* ctx, ARG0 arg0, ARG1 arg1) {
//     // implementation
//   }
// };
template <typename OutType, typename Arg0Type, typename Arg1Type, typename Op>
struct ScalarBinary {
  using OUT = typename GetOutputType<OutType>::T;
  using ARG0 = typename GetViewType<Arg0Type>::T;
  using ARG1 = typename GetViewType<Arg1Type>::T;

  static void ArrayArray(KernelContext* ctx, const ArrayData& arg0, const ArrayData& arg1,
                         Datum* out) {
    ArrayIterator<Arg0Type> arg0_it(arg0);
    ArrayIterator<Arg1Type> arg1_it(arg1);
    OutputAdapter<OutType>::Write(
        ctx, out, [&]() -> OUT { return Op::template Call(ctx, arg0_it(), arg1_it()); });
  }

  static void ArrayScalar(KernelContext* ctx, const ArrayData& arg0, const Scalar& arg1,
                          Datum* out) {
    ArrayIterator<Arg0Type> arg0_it(arg0);
    auto arg1_val = UnboxScalar<Arg1Type>::Unbox(arg1);
    OutputAdapter<OutType>::Write(
        ctx, out, [&]() -> OUT { return Op::template Call(ctx, arg0_it(), arg1_val); });
  }

  static void ScalarArray(KernelContext* ctx, const Scalar& arg0, const ArrayData& arg1,
                          Datum* out) {
    auto arg0_val = UnboxScalar<Arg0Type>::Unbox(arg0);
    ArrayIterator<Arg1Type> arg1_it(arg1);
    OutputAdapter<OutType>::Write(
        ctx, out, [&]() -> OUT { return Op::template Call(ctx, arg0_val, arg1_it()); });
  }

  static void ScalarScalar(KernelContext* ctx, const Scalar& arg0, const Scalar& arg1,
                           Datum* out) {
    if (out->scalar()->is_valid) {
      auto arg0_val = UnboxScalar<Arg0Type>::Unbox(arg0);
      auto arg1_val = UnboxScalar<Arg1Type>::Unbox(arg1);
      BoxScalar<OutType>::Box(Op::template Call(ctx, arg0_val, arg1_val),
                              out->scalar().get());
    }
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      if (batch[1].kind() == Datum::ARRAY) {
        return ArrayArray(ctx, *batch[0].array(), *batch[1].array(), out);
      } else {
        return ArrayScalar(ctx, *batch[0].array(), *batch[1].scalar(), out);
      }
    } else {
      if (batch[1].kind() == Datum::ARRAY) {
        // e.g. if we were doing scalar < array, we flip and do array >= scalar
        return ScalarArray(ctx, *batch[0].scalar(), *batch[1].array(), out);
      } else {
        return ScalarScalar(ctx, *batch[0].scalar(), *batch[1].scalar(), out);
      }
    }
  }
};

// A kernel exec generator for binary kernels where both input types are the
// same
template <typename OutType, typename ArgType, typename Op>
using ScalarBinaryEqualTypes = ScalarBinary<OutType, ArgType, ArgType, Op>;

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
//   static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
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

// GD for numeric types (integer and floating point)
template <template <typename...> class Generator, typename Type0, typename... Args>
ArrayKernelExec GenerateNumeric(detail::GetTypeId get_id) {
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
      return ExecFail;
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
      return ExecFail;
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
      return ExecFail;
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
      return ExecFail;
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
      return ExecFail;
  }
}

// Generate a kernel given a templated functor. Only a single template is
// instantiated for each bit width, and the functor is expected to treat types
// of the same bit width the same without utilizing any type-specific behavior
// (e.g. int64 should be handled equivalent to uint64 or double -- all 64
// bits).
//
// See "Numeric" above for description of the generator functor
template <template <typename...> class Generator>
ArrayKernelExec GenerateTypeAgnosticPrimitive(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::NA:
      return Generator<NullType>::Exec;
    case Type::BOOL:
      return Generator<BooleanType>::Exec;
    case Type::UINT8:
    case Type::INT8:
      return Generator<UInt8Type>::Exec;
    case Type::UINT16:
    case Type::INT16:
      return Generator<UInt16Type>::Exec;
    case Type::UINT32:
    case Type::INT32:
    case Type::FLOAT:
    case Type::DATE32:
    case Type::TIME32:
      return Generator<UInt32Type>::Exec;
    case Type::UINT64:
    case Type::INT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
      return Generator<UInt64Type>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

// Generate a kernel given a templated functor for base binary types. Generates
// a single kernel for binary/string and large binary / large string. If your
// kernel implementation needs access to the specific type at compile time,
// please use BaseBinarySpecific.
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
      return ExecFail;
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
      return ExecFail;
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
      return ExecFail;
  }
}

// END of kernel generator-dispatchers
// ----------------------------------------------------------------------

}  // namespace internal
}  // namespace compute
}  // namespace arrow

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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/scalar.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::BitmapReader;
using internal::FirstTimeBitmapWriter;
using internal::GenerateBitsUnrolled;

namespace compute {

#ifdef ARROW_EXTRA_ERROR_CONTEXT

#define KERNEL_ABORT_IF_ERROR(ctx, expr)                \
  do {                                                  \
    Status _st = (expr);                                \
    if (ARROW_PREDICT_FALSE(!_st.ok())) {               \
      _st.AddContextLine(__FILE__, __LINE__, #expr);    \
      ctx->SetStatus(_st);                              \
      return;                                           \
    }                                                   \
  } while (0)

#else

#define KERNEL_ABORT_IF_ERROR(ctx, expr)        \
  do {                                          \
    Status _st = (expr);                        \
    if (ARROW_PREDICT_FALSE(!_st.ok())) {       \
      ctx->SetStatus(_st);                      \
      return;                                   \
    }                                           \
  } while (0)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

// A kernel that exposes Call methods that handles iteration over ArrayData
// inputs itself
//

constexpr int kValidity = 0;
constexpr int kBinaryOffsets = 1;
constexpr int kPrimitiveData = 1;
constexpr int kBinaryData = 2;

// ----------------------------------------------------------------------
// Iteration / value access utilities

template <typename T, typename R = void>
using enable_if_has_c_type_not_boolean = enable_if_t<has_c_type<T>::value &&
                                                     !is_boolean_type<T>::value, R>;

template <typename T, typename Enable = void>
struct CodegenTraits;

template <typename T>
struct CodegenTraits<T, enable_if_has_c_type<T>> {
  using value_type = typename T::c_type;
};

template <typename T>
struct CodegenTraits<T, enable_if_base_binary<T>> {
  using value_type = util::string_view;
};

template <typename Type, typename Enable = void>
struct ArrayIterator;

template <typename Type>
struct ArrayIterator<Type, enable_if_has_c_type_not_boolean<Type>> {
  using T = typename Type::c_type;
  const T* values;
  ArrayIterator(const ArrayData& data) : values(data.GetValues<T>(1)) {}
  T operator()() { return *values++; }
};

template <typename Type>
struct ArrayIterator<Type, enable_if_boolean<Type>> {
  BitmapReader reader;
  ArrayIterator(const ArrayData& data)
      : reader(data.buffers[1]->data(), data.offset, data.length) {}
  bool operator()() {
    bool out = reader.IsSet();
    reader.Next();
    return out;
  }
};

template <typename Type>
struct ArrayIterator<Type, enable_if_base_binary<Type>> {
  int64_t position = 0;
  typename TypeTraits<Type>::ArrayType arr;
  ArrayIterator(const ArrayData& data)
      : arr(data.Copy()) {}
  util::string_view operator()() { return arr.GetView(position++); }
};

template <typename Type, typename Enable = void>
struct UnboxScalar;

template <typename Type>
struct UnboxScalar<Type, enable_if_has_c_type<Type>> {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static typename Type::c_type Unbox(const Datum& datum) {
    return datum.scalar_as<ScalarType>().value;
  }
};

template <typename Type>
struct UnboxScalar<Type, enable_if_base_binary<Type>> {
  static util::string_view Unbox(const Datum& datum) {
    return util::string_view(*datum.scalar_as<BaseBinaryScalar>().value);
  }
};

template <typename Type, typename Enable = void>
struct GetValueType;

template <typename Type>
struct GetValueType<Type, enable_if_has_c_type<Type>> {
  using T = typename Type::c_type;
};

template <typename Type>
struct GetValueType<
    Type, enable_if_t<is_base_binary_type<Type>::value || is_decimal_type<Type>::value ||
                      is_fixed_size_binary_type<Type>::value>> {
  using T = util::string_view;
};

// ----------------------------------------------------------------------
// Generate an array kernel given template classes

void ExecFail(KernelContext* ctx, const ExecBatch& batch, Datum* out);

void BinaryExecFlipped(KernelContext* ctx, ArrayKernelExec exec,
                       const ExecBatch& batch, Datum* out);

// ----------------------------------------------------------------------
// Boolean data utilities

// ----------------------------------------------------------------------
// Template kernel exec function generators

template <typename T>
void Extend(const std::vector<T>& values, std::vector<T>* out) {
  for (const auto& t : values) {
    out->push_back(t);
  }
}

const std::vector<std::shared_ptr<DataType>>& BaseBinaryTypes();
const std::vector<std::shared_ptr<DataType>>& SignedIntTypes();
const std::vector<std::shared_ptr<DataType>>& UnsignedIntTypes();
const std::vector<std::shared_ptr<DataType>>& IntTypes();
const std::vector<std::shared_ptr<DataType>>& FloatingPointTypes();

// Number types without boolean
const std::vector<std::shared_ptr<DataType>>& NumericTypes();

// Temporal types including time and timestamps for each unit
const std::vector<std::shared_ptr<DataType>>& TemporalTypes();

// Integer, floating point, base binary, and temporal
const std::vector<std::shared_ptr<DataType>>& PrimitiveTypes();

namespace codegen {

struct SimpleExec {
  // Operator must implement
  //
  // static void Call(KernelContext*, const ArrayData& in, ArrayData* out)
  template <typename Operator>
  static void Unary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::SCALAR) {
      ctx->SetStatus(Status::NotImplemented("NYI"));
    } else if (batch.length > 0) {
      Operator::Call(ctx, *batch[0].array(), out->mutable_array());
    }
  }

  // Operator must implement
  //
  // static void Call(KernelContext*, const ArrayData& arg0, const ArrayData& arg1,
  //                  ArrayData* out)
  template <typename Operator>
  static void Binary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::SCALAR || batch[1].kind() == Datum::SCALAR) {
      ctx->SetStatus(Status::NotImplemented("NYI"));
    } else if (batch.length > 0) {
      Operator::Call(ctx, *batch[0].array(), *batch[1].array(), out->mutable_array());
    }
  }
};

// TODO: Run benchmarks to determine if OutputAdapter is a zero-cost abstraction
struct ScalarPrimitiveExec {
  template <typename Op, typename OutType, typename Arg0Type>
  static void Unary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    using OUT = typename OutType::c_type;
    using ARG0 = typename Arg0Type::c_type;

    if (batch[0].kind() == Datum::SCALAR) {
      ctx->SetStatus(Status::NotImplemented("NYI"));
    } else {
      ArrayData* out_arr = out->mutable_array();
      auto out_data = out_arr->GetMutableValues<OUT>(kPrimitiveData);
      auto arg0_data = batch[0].array()->GetValues<ARG0>(kPrimitiveData);
      for (int64_t i = 0; i < batch.length; ++i) {
        *out_data++ = Op::template Call<OUT, ARG0>(ctx, *arg0_data++);
      }
    }
  }

  template <typename Op, typename OutType, typename Arg0Type, typename Arg1Type>
  static void Binary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    using OUT = typename OutType::c_type;
    using ARG0 = typename Arg0Type::c_type;
    using ARG1 = typename Arg1Type::c_type;

    if (batch[0].kind() == Datum::SCALAR || batch[1].kind() == Datum::SCALAR) {
      ctx->SetStatus(Status::NotImplemented("NYI"));
    } else {
      ArrayData* out_arr = out->mutable_array();
      auto out_data = out_arr->GetMutableValues<OUT>(kPrimitiveData);
      auto arg0_data = batch[0].array()->GetValues<ARG0>(kPrimitiveData);
      auto arg1_data = batch[1].array()->GetValues<ARG1>(kPrimitiveData);
      for (int64_t i = 0; i < batch.length; ++i) {
        *out_data++ = Op::template Call<OUT, ARG0, ARG1>(ctx, *arg0_data++, *arg1_data++);
      }
    }
  }
};

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
    auto out_data = out_arr->GetMutableValues<typename Type::c_type>(kPrimitiveData);
    // TODO: Is this as fast as a more explicitly inlined function?
    for (int64_t i = 0 ; i < out_arr->length; ++i) {
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

template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnary {
  using OutScalar = typename TypeTraits<OutType>::ScalarType;

  using OUT = typename CodegenTraits<OutType>::value_type;
  using ARG0 = typename CodegenTraits<Arg0Type>::value_type;

  static void Array(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayIterator<Arg0Type> arg0(*batch[0].array());
    OutputAdapter<OutType>::Write(ctx, out, [&]() -> OUT {
        return Op::template Call<OUT, ARG0>(ctx, arg0());
    });
  }

  static void Scalar(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].scalar()->is_valid) {
      ARG0 arg0 = UnboxScalar<Arg0Type>::Unbox(batch[0]);
      out->value = std::make_shared<OutScalar>(Op::template Call<OUT, ARG0>(ctx, arg0));
    } else {
      out->value = MakeNullScalar(batch[0].type());
    }
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      return Array(ctx, batch, out);
    } else {
      return Scalar(ctx, batch, out);
    }
  }
};

// Applies a scalar operation with state on the null-null values of a single
// array
template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnaryNotNullStateful {
  using ThisType = ScalarUnaryNotNullStateful<OutType, Arg0Type, Op>;
  using OutScalar = typename TypeTraits<OutType>::ScalarType;
  using OUT = typename CodegenTraits<OutType>::value_type;
  using ARG0 = typename CodegenTraits<Arg0Type>::value_type;

  Op op;
  ScalarUnaryNotNullStateful(Op op) : op(std::move(op)) {}

  template <typename Type, typename Enable = void>
  struct ArrayExec {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ExecBatch& batch,
                     Datum* out) {
      DCHECK(false);
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_t<has_c_type<Type>::value &&
                                     !is_boolean_type<Type>::value>> {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ExecBatch& batch,
                     Datum* out) {
      ArrayData* out_arr = out->mutable_array();
      auto out_data = out_arr->GetMutableValues<OUT>(kPrimitiveData);
      VisitArrayDataInline<Arg0Type>(*batch[0].array(), [&](util::optional<ARG0> v) {
          if (v.has_value()) {
            *out_data = functor.op.template Call<OUT, ARG0>(ctx, *v);
          }
          ++out_data;
        });
    }
  };

  template <typename Type>
  struct ArrayExec<Type, enable_if_t<is_boolean_type<Type>::value>> {
    static void Exec(const ThisType& functor, KernelContext* ctx, const ExecBatch& batch,
                     Datum* out) {
      ArrayData* out_arr = out->mutable_array();
      FirstTimeBitmapWriter out_writer(out_arr->buffers[1]->mutable_data(),
                                       out_arr->offset, out_arr->length);
      VisitArrayDataInline<Arg0Type>(*batch[0].array(), [&](util::optional<ARG0> v) {
          if (v.has_value()) {
            if (functor.op.template Call<OUT, ARG0>(ctx, *v)) {
              out_writer.Set();
            }
          }
          out_writer.Next();
        });
      out_writer.Finish();
    }
  };

  void Scalar(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].scalar()->is_valid) {
      ARG0 arg0 = UnboxScalar<Arg0Type>::Unbox(batch[0]);
      out->value = std::make_shared<OutScalar>(
          this->op.template Call<OUT, ARG0>(ctx, arg0));
    } else {
      out->value = MakeNullScalar(batch[0].type());
    }
  }

  void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      ArrayExec<OutType>::Exec(*this, ctx, batch, out);
    } else {
      return Scalar(ctx, batch, out);
    }
  }
};

template <typename OutType, typename Arg0Type, typename Op>
struct ScalarUnaryNotNull {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Seed kernel with dummy state
    ScalarUnaryNotNullStateful<OutType, Arg0Type, Op> kernel({});
    return kernel.Exec(ctx, batch, out);
  }
};

template <typename OutType, typename Arg0Type, typename Arg1Type, typename Op,
          typename FlippedOp = Op>
struct ScalarBinary {
  using OutScalarType = typename TypeTraits<OutType>::ScalarType;

  using OUT = typename CodegenTraits<OutType>::value_type;
  using ARG0 = typename CodegenTraits<Arg0Type>::value_type;
  using ARG1 = typename CodegenTraits<Arg1Type>::value_type;

  template <typename ChosenOp>
  static void ArrayArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayIterator<Arg0Type> arg0(*batch[0].array());
    ArrayIterator<Arg1Type> arg1(*batch[1].array());
    OutputAdapter<OutType>::Write(ctx, out, [&]() -> OUT {
        return ChosenOp::template Call(ctx, arg0(), arg1());
    });
  }

  template <typename ChosenOp>
  static void ArrayScalar(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayIterator<Arg0Type> arg0(*batch[0].array());
    auto arg1 = UnboxScalar<Arg1Type>::Unbox(batch[1]);
    OutputAdapter<OutType>::Write(ctx, out, [&]() -> OUT {
        return ChosenOp::template Call(ctx, arg0(), arg1);
    });
  }

  template <typename ChosenOp>
  static void ScalarScalar(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    auto arg0 = UnboxScalar<Arg0Type>::Unbox(batch[0]);
    auto arg1 = UnboxScalar<Arg1Type>::Unbox(batch[1]);
    out->value = std::make_shared<OutScalarType>(ChosenOp::template Call(ctx, arg0, arg1));
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {

    if (batch[0].kind() == Datum::ARRAY) {
      if (batch[1].kind() == Datum::ARRAY) {
        return ArrayArray<Op>(ctx, batch, out);
      } else {
        return ArrayScalar<Op>(ctx, batch, out);
      }
    } else {
      if (batch[1].kind() == Datum::ARRAY) {
        // e.g. if we were doing scalar < array, we flip and do array >= scalar
        return BinaryExecFlipped(ctx, ArrayScalar<FlippedOp>, batch, out);
      } else {
        return ScalarScalar<Op>(ctx, batch, out);
      }
    }
  }
};

template <typename OutType, typename ArgType, typename Op,
          typename FlippedOp = Op>
using ScalarBinaryEqualTypes = ScalarBinary<OutType, ArgType, ArgType, Op, FlippedOp>;

struct ScalarNumericEqualTypes {
  template <typename Op>
  static ArrayKernelExec Unary(const DataType& type) {
    switch (type.id()) {
      case Type::INT8:
        return ScalarPrimitiveExec::Unary<Op, Int8Type, Int8Type>;
      case Type::UINT8:
        return ScalarPrimitiveExec::Unary<Op, UInt8Type, UInt8Type>;
      case Type::INT16:
        return ScalarPrimitiveExec::Unary<Op, Int16Type, Int16Type>;
      case Type::UINT16:
        return ScalarPrimitiveExec::Unary<Op, UInt16Type, UInt16Type>;
      case Type::INT32:
        return ScalarPrimitiveExec::Unary<Op, Int32Type, Int32Type>;
      case Type::UINT32:
        return ScalarPrimitiveExec::Unary<Op, UInt32Type, UInt32Type>;
      case Type::INT64:
        return ScalarPrimitiveExec::Unary<Op, Int64Type, Int64Type>;
      case Type::UINT64:
        return ScalarPrimitiveExec::Unary<Op, UInt64Type, UInt64Type>;
      case Type::FLOAT:
        return ScalarPrimitiveExec::Unary<Op, FloatType, FloatType>;
      case Type::DOUBLE:
        return ScalarPrimitiveExec::Unary<Op, DoubleType, DoubleType>;
      default:
        DCHECK(false);
        return ExecFail;
    }
  }

  template <typename Op>
  static ArrayKernelExec Binary(const DataType& type) {
    switch (type.id()) {
      case Type::INT8:
        return ScalarPrimitiveExec::Binary<Op, Int8Type, Int8Type, Int8Type>;
      case Type::UINT8:
        return ScalarPrimitiveExec::Binary<Op, UInt8Type, UInt8Type, UInt8Type>;
      case Type::INT16:
        return ScalarPrimitiveExec::Binary<Op, Int16Type, Int16Type, Int16Type>;
      case Type::UINT16:
        return ScalarPrimitiveExec::Binary<Op, UInt16Type, UInt16Type, UInt16Type>;
      case Type::INT32:
        return ScalarPrimitiveExec::Binary<Op, Int32Type, Int32Type, Int32Type>;
      case Type::UINT32:
        return ScalarPrimitiveExec::Binary<Op, UInt32Type, UInt32Type, UInt32Type>;
      case Type::INT64:
        return ScalarPrimitiveExec::Binary<Op, Int64Type, Int64Type, Int64Type>;
      case Type::UINT64:
        return ScalarPrimitiveExec::Binary<Op, UInt64Type, UInt64Type, UInt64Type>;
      case Type::FLOAT:
        return ScalarPrimitiveExec::Binary<Op, FloatType, FloatType, FloatType>;
      case Type::DOUBLE:
        return ScalarPrimitiveExec::Binary<Op, DoubleType, DoubleType, DoubleType>;
      default:
        DCHECK(false);
        return ExecFail;
    }
  }
};

template <template <typename...> class Generator,
          typename Type0, typename... Args>
ArrayKernelExec Numeric(const DataType& type) {
  switch (type.id()) {
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

template <template <typename...> class Generator,
          typename Type0, typename... Args>
ArrayKernelExec FloatingPoint(const DataType& type) {
  switch (type.id()) {
    case Type::FLOAT:
      return Generator<Type0, FloatType, Args...>::Exec;
    case Type::DOUBLE:
      return Generator<Type0, DoubleType, Args...>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <template <typename...> class Generator,
          typename Type0, typename... Args>
ArrayKernelExec Integer(const DataType& type) {
  switch (type.id()) {
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

template <template <typename...> class Generator,
          typename Type0, typename... Args>
ArrayKernelExec BaseBinary(const DataType& type) {
  switch (type.id()) {
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

template <template <typename...> class Generator,
          typename Type0, typename... Args>
ArrayKernelExec Temporal(const DataType& type) {
  switch (type.id()) {
    case Type::DATE32:
      return Generator<Type0, Date32Type, Args...>::Exec;
    case Type::DATE64:
      return Generator<Type0, Date64Type, Args...>::Exec;
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

}  // namespace codegen

// ----------------------------------------------------------------------
// Reusable type resolvers

Result<ValueDescr> FirstType(KernelContext*, const std::vector<ValueDescr>& descrs);

}  // namespace compute
}  // namespace arrow

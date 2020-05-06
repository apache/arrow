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
#include "arrow/util/string_view.h"

namespace arrow {
namespace compute {

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
  internal::BitmapReader reader;
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

    // No support for selection vectors yet implemented
    DCHECK_EQ(nullptr, batch.selection_vector);
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

    // No support for selection vectors yet implemented
    DCHECK_EQ(nullptr, batch.selection_vector);

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

// ----------------------------------------------------------------------
// Generate an array kernel given template classes

void ExecFail(KernelContext* ctx, const ExecBatch& batch, Datum* out);

// ----------------------------------------------------------------------
// Boolean data utilities

// ----------------------------------------------------------------------
// Code generator for numeric-type kernels where the input and output types
// are all the same

namespace codegen {

const std::vector<std::shared_ptr<DataType>>& BaseBinaryTypes();
const std::vector<std::shared_ptr<DataType>>& SignedIntTypes();
const std::vector<std::shared_ptr<DataType>>& UnsignedIntTypes();
const std::vector<std::shared_ptr<DataType>>& FloatingPointTypes();
const std::vector<std::shared_ptr<DataType>>& NumericTypes();
const std::vector<std::shared_ptr<DataType>>& TemporalTypes();

template <typename Type, typename Enable = void>
struct OutputAdapter;

template <typename Type>
struct OutputAdapter<Type, enable_if_boolean<Type>> {
  template <typename Generator>
  static void Write(KernelContext*, Datum* out, Generator&& generator) {
    ArrayData* out_arr = out->mutable_array();
    auto out_bitmap = out_arr->buffers[1]->mutable_data();
    internal::GenerateBitsUnrolled(out_bitmap, out_arr->offset, out_arr->length,
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

void BinaryExecFlipped(KernelContext* ctx, ArrayKernelExec exec,
                       const ExecBatch& batch, Datum* out);

// A binary kernel that outputs boolean values.
template <typename OutType, typename Arg0Type, typename Arg1Type, typename Op,
          typename FlippedOp = Op>
struct ScalarBinary {
  using OutScalarType = typename TypeTraits<OutType>::ScalarType;
  template <typename ChosenOp>
  static void ArrayArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayIterator<Arg0Type> arg0(*batch[0].array());
    ArrayIterator<Arg1Type> arg1(*batch[1].array());
    OutputAdapter<OutType>::Write(ctx, out, [&]() -> bool {
        return ChosenOp::template Call(ctx, arg0(), arg1());
    });
  }

  template <typename ChosenOp>
  static void ArrayScalar(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayIterator<Arg0Type> arg0(*batch[0].array());
    auto arg1 = UnboxScalar<Arg1Type>::Unbox(batch[1]);
    OutputAdapter<OutType>::Write(ctx, out, [&]() -> bool {
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

struct NumericEqualTypes {
  template <typename Op>
  static ArrayKernelExec MakeUnary(const DataType& in_type) {
    switch (in_type.id()) {
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
  static ArrayKernelExec MakeBinary(const DataType& in_type) {
    switch (in_type.id()) {
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
          typename OutType, typename... Args>
ArrayKernelExec NumericSetReturn(const DataType& in_type) {
  switch (in_type.id()) {
    case Type::INT8:
      return Generator<OutType, Int8Type, Args...>::Exec;
    case Type::UINT8:
      return Generator<OutType, UInt8Type, Args...>::Exec;
    case Type::INT16:
      return Generator<OutType, Int16Type, Args...>::Exec;
    case Type::UINT16:
      return Generator<OutType, UInt16Type, Args...>::Exec;
    case Type::INT32:
      return Generator<OutType, Int32Type, Args...>::Exec;
    case Type::UINT32:
      return Generator<OutType, UInt32Type, Args...>::Exec;
    case Type::INT64:
      return Generator<OutType, Int64Type, Args...>::Exec;
    case Type::UINT64:
      return Generator<OutType, UInt64Type, Args...>::Exec;
    case Type::FLOAT:
      return Generator<OutType, FloatType, Args...>::Exec;
    case Type::DOUBLE:
      return Generator<OutType, DoubleType, Args...>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <template <typename...> class Generator,
          typename OutType, typename... Args>
ArrayKernelExec BaseBinarySetReturn(const DataType& in_type) {
  switch (in_type.id()) {
    case Type::BINARY:
      return Generator<OutType, BinaryType, Args...>::Exec;
    case Type::STRING:
      return Generator<OutType, StringType, Args...>::Exec;
    case Type::LARGE_BINARY:
      return Generator<OutType, LargeBinaryType, Args...>::Exec;
    case Type::LARGE_STRING:
      return Generator<OutType, LargeStringType, Args...>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <template <typename...> class Generator,
          typename OutType, typename... Args>
ArrayKernelExec TemporalSetReturn(const DataType& in_type) {
  switch (in_type.id()) {
    case Type::DATE32:
      return Generator<OutType, Date32Type, Args...>::Exec;
    case Type::DATE64:
      return Generator<OutType, Date64Type, Args...>::Exec;
    case Type::TIME32:
      return Generator<OutType, Time32Type, Args...>::Exec;
    case Type::TIME64:
      return Generator<OutType, Time64Type, Args...>::Exec;
    case Type::TIMESTAMP:
      return Generator<OutType, TimestampType, Args...>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

}  // namespace codegen
}  // namespace compute
}  // namespace arrow

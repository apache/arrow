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

#include "arrow/compute/kernels/compare.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/filter.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace compute {

class FunctionContext;
struct Datum;

template <typename ArrowType, CompareOperator Op,
          typename ScalarType = typename TypeTraits<ArrowType>::ScalarType,
          typename T = typename TypeTraits<ArrowType>::CType>
static Status CompareArrayScalar(const ArrayData& array, const ScalarType& scalar,
                                 uint8_t* output_bitmap) {
  const T* left = array.GetValues<T>(1);
  const T right = scalar.value;

  internal::GenerateBitsUnrolled(
      output_bitmap, 0, array.length,
      [&left, right]() -> bool { return Comparator<T, Op>::Compare(*left++, right); });

  return Status::OK();
}

template <typename ArrowType, CompareOperator Op,
          typename ScalarType = typename TypeTraits<ArrowType>::ScalarType,
          typename T = typename TypeTraits<ArrowType>::CType>
static Status CompareScalarArray(const ScalarType& scalar, const ArrayData& array,
                                 uint8_t* output_bitmap) {
  const T left = scalar.value;
  const T* right = array.GetValues<T>(1);

  internal::GenerateBitsUnrolled(
      output_bitmap, 0, array.length,
      [left, &right]() -> bool { return Comparator<T, Op>::Compare(left, *right++); });

  return Status::OK();
}

template <typename ArrowType, CompareOperator Op,
          typename T = typename TypeTraits<ArrowType>::CType>
static Status CompareArrayArray(const ArrayData& lhs, const ArrayData& rhs,
                                uint8_t* output_bitmap) {
  const T* left = lhs.GetValues<T>(1);
  const T* right = rhs.GetValues<T>(1);

  internal::GenerateBitsUnrolled(output_bitmap, 0, lhs.length, [&left, &right]() -> bool {
    return Comparator<T, Op>::Compare(*left++, *right++);
  });

  return Status::OK();
}

template <typename ArrowType, CompareOperator Op>
class CompareFunction final : public FilterFunction {
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

 public:
  explicit CompareFunction(FunctionContext* ctx) : ctx_(ctx) {}

  Status Filter(const ArrayData& array, const Scalar& scalar, ArrayData* output) const {
    // Caller must cast
    DCHECK(array.type->Equals(scalar.type));
    // Output must be a boolean array
    DCHECK(output->type->Equals(boolean()));
    // Output must be of same length
    DCHECK_EQ(output->length, array.length);

    // Scalar is null, all comparisons are null.
    if (!scalar.is_valid) {
      return detail::SetAllNulls(ctx_, array, output);
    }

    // Copy null_bitmap
    RETURN_NOT_OK(detail::PropagateNulls(ctx_, array, output));

    uint8_t* bitmap_result = output->buffers[1]->mutable_data();
    return CompareArrayScalar<ArrowType, Op>(
        array, static_cast<const ScalarType&>(scalar), bitmap_result);
  }

  Status Filter(const Scalar& scalar, const ArrayData& array, ArrayData* output) const {
    // Caller must cast
    DCHECK(array.type->Equals(scalar.type));
    // Output must be a boolean array
    DCHECK(output->type->Equals(boolean()));
    // Output must be of same length
    DCHECK_EQ(output->length, array.length);

    // Scalar is null, all comparisons are null.
    if (!scalar.is_valid) {
      return detail::SetAllNulls(ctx_, array, output);
    }

    // Copy null_bitmap
    RETURN_NOT_OK(detail::PropagateNulls(ctx_, array, output));

    uint8_t* bitmap_result = output->buffers[1]->mutable_data();
    return CompareScalarArray<ArrowType, Op>(static_cast<const ScalarType&>(scalar),
                                             array, bitmap_result);
  }

  Status Filter(const ArrayData& lhs, const ArrayData& rhs, ArrayData* output) const {
    // Caller must cast
    DCHECK(lhs.type->Equals(rhs.type));
    // Output must be a boolean array
    DCHECK(output->type->Equals(boolean()));
    // Inputs must be of same length
    DCHECK_EQ(lhs.length, rhs.length);
    // Output must be of same length as inputs
    DCHECK_EQ(output->length, lhs.length);

    // Copy null_bitmap
    RETURN_NOT_OK(detail::AssignNullIntersection(ctx_, lhs, rhs, output));

    uint8_t* bitmap_result = output->buffers[1]->mutable_data();
    return CompareArrayArray<ArrowType, Op>(lhs, rhs, bitmap_result);
  }

 private:
  FunctionContext* ctx_;
};

template <typename ArrowType, CompareOperator Op>
static inline std::shared_ptr<FilterFunction> MakeCompareFunctionTypeOp(
    FunctionContext* ctx) {
  return std::make_shared<CompareFunction<ArrowType, Op>>(ctx);
}

template <typename ArrowType>
static inline std::shared_ptr<FilterFunction> MakeCompareFilterFunctionType(
    FunctionContext* ctx, struct CompareOptions options) {
  switch (options.op) {
    case CompareOperator::EQUAL:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::EQUAL>(ctx);
    case CompareOperator::NOT_EQUAL:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::NOT_EQUAL>(ctx);
    case CompareOperator::GREATER:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::GREATER>(ctx);
    case CompareOperator::GREATER_EQUAL:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::GREATER_EQUAL>(ctx);
    case CompareOperator::LESS:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::LESS>(ctx);
    case CompareOperator::LESS_EQUAL:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::LESS_EQUAL>(ctx);
  }

  return nullptr;
}

std::shared_ptr<FilterFunction> MakeCompareFilterFunction(FunctionContext* ctx,
                                                          const DataType& type,
                                                          struct CompareOptions options) {
  switch (type.id()) {
    case UInt8Type::type_id:
      return MakeCompareFilterFunctionType<UInt8Type>(ctx, options);
    case Int8Type::type_id:
      return MakeCompareFilterFunctionType<Int8Type>(ctx, options);
    case UInt16Type::type_id:
      return MakeCompareFilterFunctionType<UInt16Type>(ctx, options);
    case Int16Type::type_id:
      return MakeCompareFilterFunctionType<Int16Type>(ctx, options);
    case UInt32Type::type_id:
      return MakeCompareFilterFunctionType<UInt32Type>(ctx, options);
    case Int32Type::type_id:
      return MakeCompareFilterFunctionType<Int32Type>(ctx, options);
    case UInt64Type::type_id:
      return MakeCompareFilterFunctionType<UInt64Type>(ctx, options);
    case Int64Type::type_id:
      return MakeCompareFilterFunctionType<Int64Type>(ctx, options);
    case FloatType::type_id:
      return MakeCompareFilterFunctionType<FloatType>(ctx, options);
    case DoubleType::type_id:
      return MakeCompareFilterFunctionType<DoubleType>(ctx, options);
    case Date32Type::type_id:
      return MakeCompareFilterFunctionType<Date32Type>(ctx, options);
    case Date64Type::type_id:
      return MakeCompareFilterFunctionType<Date64Type>(ctx, options);
    case TimestampType::type_id:
      return MakeCompareFilterFunctionType<TimestampType>(ctx, options);
    case Time32Type::type_id:
      return MakeCompareFilterFunctionType<Time32Type>(ctx, options);
    case Time64Type::type_id:
      return MakeCompareFilterFunctionType<Time64Type>(ctx, options);
    default:
      return nullptr;
  }
}

ARROW_EXPORT
Status Compare(FunctionContext* context, const Datum& left, const Datum& right,
               struct CompareOptions options, Datum* out) {
  DCHECK(out);

  auto type = left.type();
  DCHECK(type->Equals(right.type()));
  // Requires that both types are equal.
  auto fn = MakeCompareFilterFunction(context, *type, options);
  if (fn == nullptr) {
    return Status::NotImplemented("Compare not implemented for type ", type->ToString());
  }

  FilterBinaryKernel filter_kernel(fn);
  detail::PrimitiveAllocatingBinaryKernel kernel(&filter_kernel);

  const int64_t length = FilterBinaryKernel::out_length(left, right);
  out->value = ArrayData::Make(filter_kernel.out_type(), length);

  return kernel.Call(context, left, right, out);
}

}  // namespace compute
}  // namespace arrow

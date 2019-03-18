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

#include "arrow/compute/kernels/comparator.h"

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

static inline Status CopyOrTransferBitmap(const Array& array, MemoryPool* pool,
                                          std::shared_ptr<Buffer>* bitmap) {
  auto offset = array.offset();
  auto length = array.length();
  auto null_bitmap = array.null_bitmap();

  if (null_bitmap == nullptr || offset == 0 || length == 0) {
    *bitmap = null_bitmap;
    return Status::OK();
  }

  return internal::CopyBitmap(pool, null_bitmap->data(), offset, length, bitmap);
}

template <typename ArrowType, CompareOperator Operator,
          typename ArrayType = typename TypeTraits<ArrowType>::ArrayType,
          typename ScalarType = typename TypeTraits<ArrowType>::ScalarType,
          typename T = typename TypeTraits<ArrowType>::CType>
static Status CompareArrayScalar(const ArrayType& input, const ScalarType& scalar,
                                 uint8_t* bitmap) {
  const T right = scalar.value;
  const T* values = input.raw_values();

  // Bitmap is not sliced like the original array, thus offset=0
  auto writer = internal::BitmapWriter(bitmap, 0, input.length());
  for (int64_t i = 0; i < input.length(); i++, writer.Next()) {
    if (Comparator<T, Operator>::Compare(values[i], right)) {
      writer.Set();
    } else {
      writer.Clear();
    }
  }

  writer.Finish();

  return Status::OK();
}

template <typename ArrowType, CompareOperator Op>
class CompareFunction final : public FilterFunction {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

 public:
  explicit CompareFunction(FunctionContext* ctx) : ctx_(ctx) {}

  Status Filter(const Array& input, const Scalar& scalar, ArrayData* output) const {
    // Caller must cast
    DCHECK(input.type()->Equals(scalar.type));
    // Caller must not pass null scalar, use IsNull, IsNotNull
    DCHECK(scalar.is_valid);
    // Output must be a boolean array
    DCHECK(output->type->Equals(boolean()));
    // Output must be of same length
    DCHECK_EQ(output->length, input.length());

    // Copy null_bitmap
    RETURN_NOT_OK(CopyOrTransferBitmap(input, ctx_->memory_pool(), &output->buffers[0]));

    DCHECK(output->buffers[1]);
    uint8_t* bitmap_result = output->buffers[1]->mutable_data();
    return CompareArrayScalar<ArrowType, Op>(static_cast<const ArrayType&>(input),
                                             static_cast<const ScalarType&>(scalar),
                                             bitmap_result);
  }

 private:
  FunctionContext* ctx_;
};

template <typename ArrowType, CompareOperator Op>
static inline std::shared_ptr<FilterFunction> MakeCompareFunctionTypeOp(
    FunctionContext* ctx) {
  return std::static_pointer_cast<FilterFunction>(
      std::make_shared<CompareFunction<ArrowType, Op>>(ctx));
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
    case CompareOperator::LOWER:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::LOWER>(ctx);
    case CompareOperator::LOWER_EQUAL:
      return MakeCompareFunctionTypeOp<ArrowType, CompareOperator::LOWER_EQUAL>(ctx);
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

  if (!left.is_array())
    return Status::Invalid("Compare expects an Array as left operand");
  if (!right.is_scalar())
    return Status::Invalid("Compare expects an Scalar as right operand");
  if (!left.type()->Equals(right.type()))
    return Status::Invalid("Compare expects left and right of same type");

  auto array = left.make_array();
  auto scalar = right.scalar();
  auto type = array->type();
  auto fn = MakeCompareFilterFunction(context, *type, options);

  if (!fn) return Status::Invalid("Compare not implemented for type ", type->ToString());

  FilterBinaryKernel filter_kernel(fn);
  detail::PrimitiveAllocatingBinaryKernel kernel(&filter_kernel);
  out->value = ArrayData::Make(filter_kernel.out_type(), array->length());
  return kernel.Call(context, left, right, out);
}

}  // namespace compute
}  // namespace arrow

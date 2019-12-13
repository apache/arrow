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

#include <utility>

#include "boost/range.hpp"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

namespace compute {

template <typename T, CompareOperator Op>
struct Comparator;

template <typename T>
struct Comparator<T, CompareOperator::EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs == rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::NOT_EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs != rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::GREATER> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs > rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::GREATER_EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs >= rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::LESS> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs < rhs; }
};

template <typename T>
struct Comparator<T, CompareOperator::LESS_EQUAL> {
  constexpr static bool Compare(const T& lhs, const T& rhs) { return lhs <= rhs; }
};

template <typename Value>
struct RepeatedValue {
  Value operator()() { return value_; }
  Value value_;
};

struct RepeatedBufferAsStringView {
  explicit RepeatedBufferAsStringView(const Buffer& buffer) : value_(buffer) {}
  util::string_view operator()() { return value_; }
  util::string_view value_;
};

struct ReadFromBitmap : internal::BitmapReader {
  using internal::BitmapReader::BitmapReader;

  bool operator()() {
    bool out = IsSet();
    Next();
    return out;
  }
};

template <typename T>
struct DereferenceIncrementPointer {
  T operator()() { return *ptr_++; }
  const T* ptr_;
};

template <typename ArrayType>
struct GetViewFromStringLikeArray {
  explicit GetViewFromStringLikeArray(const ArrayType* array) : array_(array) {}

  string_view operator()() { return array_->GetView(i_++); }

  const ArrayType* array_;
  int64_t i_ = 0;
};

template <typename T, typename RangeType = RepeatedValue<typename T::c_type>>
RangeType MakeRange(const TemporalScalar<T>& scalar) {
  return RangeType{scalar.value};
}

template <typename T, typename RangeType = RepeatedValue<typename T::c_type>>
RangeType MakeRange(const internal::PrimitiveScalar<T>& scalar) {
  return RangeType{scalar.value};
}

RepeatedBufferAsStringView MakeRange(const BaseBinaryScalar& scalar) {
  return RepeatedBufferAsStringView{*scalar.value};
}

ReadFromBitmap MakeRange(const BooleanArray& array) {
  return ReadFromBitmap(array.data()->GetValues<uint8_t>(1), array.offset(),
                        array.length());
}

template <typename T,
          typename RangeType = DereferenceIncrementPointer<typename T::c_type>>
RangeType MakeRange(const NumericArray<T>& array) {
  return RangeType{array.raw_values()};
}

template <typename T, typename RangeType = GetViewFromStringLikeArray<BaseBinaryArray<T>>>
RangeType MakeRange(const BaseBinaryArray<T>& array) {
  return RangeType{&array};
}

inline Status AssignNulls(FunctionContext* ctx, const Scalar& scalar, const Array& array,
                          ArrayData* out) {
  return scalar.is_valid ? detail::PropagateNulls(ctx, *array.data(), out)
                         : detail::SetAllNulls(ctx, *array.data(), out);
}

inline Status AssignNulls(FunctionContext* ctx, const Array& left, const Array& right,
                          ArrayData* out) {
  return detail::AssignNullIntersection(ctx, *left.data(), *right.data(), out);
}

template <CompareOperator Op, typename L, typename R>
Status Compare(L&& get_left, R&& get_right, ArrayData* out) {
  auto out_bitmap = out->buffers[1]->mutable_data();
  internal::GenerateBitsUnrolled(out_bitmap, 0, out->length, [&]() -> bool {
    return Comparator<decltype(get_left()), Op>::Compare(get_left(), get_right());
  });
  return Status::OK();
}

template <typename ArrowType, CompareOperator Op>
class CompareKernel final : public BinaryKernel {
 public:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  std::shared_ptr<DataType> out_type() const override { return boolean(); }

  Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
              Datum* out_datum) override {
    auto out = out_datum->array();

    auto left_array = AsArray(left);
    auto left_scalar = AsScalar(left);

    auto right_array = AsArray(right);
    auto right_scalar = AsScalar(right);

    if (left_array && right_array) {
      RETURN_NOT_OK(AssignNulls(ctx, *left_array, *right_array, out.get()));
      return Compare<Op>(MakeRange(*left_array), MakeRange(*right_array), out.get());
    }

    if (left_array && right_scalar) {
      RETURN_NOT_OK(AssignNulls(ctx, *right_scalar, *left_array, out.get()));
      return Compare<Op>(MakeRange(*left_array), MakeRange(*right_scalar), out.get());
    }

    if (left_scalar && right_array) {
      RETURN_NOT_OK(AssignNulls(ctx, *left_scalar, *right_array, out.get()));
      return Compare<Op>(MakeRange(*left_scalar), MakeRange(*right_array), out.get());
    }

    return Status::Invalid("Invalid datum signature for CompareBinaryKernel::Call");
  }

 private:
  static std::shared_ptr<ArrayType> AsArray(const Datum& datum) {
    if (datum.kind() != Datum::ARRAY) return nullptr;
    return checked_pointer_cast<ArrayType>(datum.make_array());
  }

  static std::shared_ptr<ScalarType> AsScalar(const Datum& datum) {
    if (datum.kind() != Datum::SCALAR) return nullptr;
    return checked_pointer_cast<ScalarType>(datum.scalar());
  }
};

template <typename ArrowType>
std::shared_ptr<BinaryKernel> UnpackOperator(CompareOperator op) {
  switch (op) {
    case CompareOperator::EQUAL:
      return std::make_shared<CompareKernel<ArrowType, CompareOperator::EQUAL>>();

    case CompareOperator::NOT_EQUAL:
      return std::make_shared<CompareKernel<ArrowType, CompareOperator::NOT_EQUAL>>();

    case CompareOperator::GREATER:
      return std::make_shared<CompareKernel<ArrowType, CompareOperator::GREATER>>();

    case CompareOperator::GREATER_EQUAL:
      return std::make_shared<CompareKernel<ArrowType, CompareOperator::GREATER_EQUAL>>();

    case CompareOperator::LESS:
      return std::make_shared<CompareKernel<ArrowType, CompareOperator::LESS>>();

    case CompareOperator::LESS_EQUAL:
      return std::make_shared<CompareKernel<ArrowType, CompareOperator::LESS_EQUAL>>();
  }

  return nullptr;
}

struct UnpackType {
  Status Visit(const NullType& unreachable) { return Status::OK(); }

  Status Visit(const BooleanType& t) {
    *out_ = UnpackOperator<BooleanType>(options_.op);
    return Status::OK();
  }

  template <typename Numeric>
  enable_if_number<Numeric, Status> Visit(const Numeric& t) {
    *out_ = UnpackOperator<Numeric>(options_.op);
    return Status::OK();
  }

  template <typename Temporal>
  enable_if_temporal<Temporal, Status> Visit(const Temporal& t) {
    *out_ = UnpackOperator<Temporal>(options_.op);
    return Status::OK();
  }

  template <typename StringLike>
  enable_if_base_binary<StringLike, Status> Visit(const StringLike& t) {
    *out_ = UnpackOperator<StringLike>(options_.op);
    return Status::OK();
  }

  Status Visit(const DictionaryType& t) { return NotImplemented(t); }
  Status Visit(const DayTimeIntervalType& t) { return NotImplemented(t); }
  Status Visit(const MonthIntervalType& t) { return NotImplemented(t); }
  Status Visit(const FixedSizeBinaryType& t) { return NotImplemented(t); }
  Status Visit(const DurationType& t) { return NotImplemented(t); }
  Status Visit(const Decimal128Type& t) { return NotImplemented(t); }
  Status Visit(const ListType& t) { return NotImplemented(t); }
  Status Visit(const LargeListType& t) { return NotImplemented(t); }
  Status Visit(const MapType& t) { return NotImplemented(t); }
  Status Visit(const FixedSizeListType& t) { return NotImplemented(t); }
  Status Visit(const UnionType& t) { return NotImplemented(t); }
  Status Visit(const ExtensionType& t) { return NotImplemented(t); }
  Status Visit(const StructType& t) { return NotImplemented(t); }

  Status NotImplemented(const DataType& t) {
    return Status::NotImplemented("Compare not implemented for type ", t);
  }

  std::shared_ptr<BinaryKernel>* out_;
  CompareOptions options_;
};

Status MakeCompareKernel(const DataType& type, CompareOptions options,
                         std::shared_ptr<BinaryKernel>* out) {
  UnpackType visitor{out, options};
  return VisitTypeInline(type, &visitor);
}

inline int64_t OutLength(const Datum& left, const Datum& right) {
  if (left.kind() == Datum::ARRAY) return left.length();
  if (right.kind() == Datum::ARRAY) return right.length();
  return 0;
}

Status Compare(FunctionContext* context, const Datum& left, const Datum& right,
               struct CompareOptions options, Datum* out) {
  auto type = left.type();
  if (!type->Equals(right.type())) {
    return Status::TypeError("Cannot compare data of differing type ", *type, " vs ",
                             *right.type());
  }

  std::shared_ptr<BinaryKernel> kernel;
  RETURN_NOT_OK(MakeCompareKernel(*type, options, &kernel));

  out->value = ArrayData::Make(kernel->out_type(), OutLength(left, right));

  return detail::PrimitiveAllocatingBinaryKernel(kernel.get())
      .Call(context, left, right, out);
}

}  // namespace compute
}  // namespace arrow

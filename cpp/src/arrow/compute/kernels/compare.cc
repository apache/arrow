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

using DatumKind = decltype(Datum::SCALAR);

// A function object which either accesses successive elements of an Array or repeats
// the value of a Scalar.
template <typename ArrowType, DatumKind Kind, typename Enable = void>
struct Get;

template <typename Numeric>
struct Get<Numeric, Datum::ARRAY, enable_if_number<Numeric>> {
  using T = typename Numeric::c_type;

  explicit Get(const Datum& datum) : ptr_(datum.array()->GetMutableValues<T>(1)) {}

  T operator()() { return *ptr_++; }

  T* ptr_;
};

template <>
struct Get<BooleanType, Datum::ARRAY> {
  explicit Get(const Datum& datum)
      : reader_(datum.array()->GetValues<uint8_t>(1), datum.array()->offset,
                datum.array()->length) {}

  bool operator()() {
    bool out = reader_.IsSet();
    reader_.Next();
    return out;
  }

  internal::BitmapReader reader_;
};

template <typename StringLike>
struct Get<StringLike, Datum::ARRAY, enable_if_base_binary<StringLike>> {
  using ArrayType = typename TypeTraits<StringLike>::ArrayType;

  explicit Get(const Datum& datum)
      : array_(checked_pointer_cast<ArrayType>(datum.make_array())) {}

  string_view operator()() { return array_->GetView(i_++); }

  std::shared_ptr<ArrayType> array_;
  int64_t i_ = 0;
};

template <typename Numeric>
struct Get<Numeric, Datum::SCALAR, enable_if_number<Numeric>> {
  using T = typename Numeric::c_type;
  using ScalarType = typename TypeTraits<Numeric>::ScalarType;

  explicit Get(const Datum& datum)
      : value_(checked_cast<const ScalarType&>(*datum.scalar()).value) {}

  T operator()() { return value_; }

  T value_;
};

template <typename StringLike>
struct Get<StringLike, Datum::SCALAR, enable_if_base_binary<StringLike>> {
  using ScalarType = typename TypeTraits<StringLike>::ScalarType;

  explicit Get(const Datum& datum)
      : value_(*checked_cast<const ScalarType&>(*datum.scalar()).value) {}

  string_view operator()() { return value_; }

  string_view value_;
};

template <>
struct Get<BooleanType, Datum::SCALAR> {
  explicit Get(const Datum& datum)
      : value_(checked_cast<const BooleanScalar&>(*datum.scalar()).value) {}

  bool operator()() { return value_; }

  bool value_;
};

template <typename Cmp, typename ArrowType>
class CompareBinaryKernel final : public BinaryKernel {
 public:
  explicit CompareBinaryKernel(Cmp cmp) : cmp_(std::move(cmp)) {}

  std::shared_ptr<DataType> out_type() const override { return boolean(); }

  Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
              Datum* out) override {
    if (left.kind() == Datum::ARRAY && right.kind() == Datum::SCALAR) {
      return DoCall<Datum::ARRAY, Datum::SCALAR>(ctx, left, right, out);
    } else if (left.kind() == Datum::SCALAR && right.kind() == Datum::ARRAY) {
      return DoCall<Datum::SCALAR, Datum::ARRAY>(ctx, left, right, out);
    } else if (left.kind() == Datum::ARRAY && right.kind() == Datum::ARRAY) {
      return DoCall<Datum::ARRAY, Datum::ARRAY>(ctx, left, right, out);
    }
    return Status::Invalid("Invalid datum signature for CompareBinaryKernel");
  }

 private:
  template <DatumKind LeftKind, DatumKind RightKind>
  Status DoCall(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
    auto out_data = out->array();

    if (LeftKind == Datum::SCALAR && RightKind == Datum::ARRAY) {
      if (!left.scalar()->is_valid) {
        return detail::SetAllNulls(ctx, *right.array(), out_data.get());
      }
      RETURN_NOT_OK(detail::PropagateNulls(ctx, *right.array(), out_data.get()));
    }

    if (LeftKind == Datum::ARRAY && RightKind == Datum::SCALAR) {
      if (!right.scalar()->is_valid) {
        return detail::SetAllNulls(ctx, *left.array(), out_data.get());
      }
      RETURN_NOT_OK(detail::PropagateNulls(ctx, *left.array(), out_data.get()));
    }

    if (LeftKind == Datum::ARRAY && RightKind == Datum::ARRAY) {
      RETURN_NOT_OK(detail::AssignNullIntersection(ctx, *left.array(), *right.array(),
                                                   out_data.get()));
    }

    Get<ArrowType, LeftKind> get_left(left);
    Get<ArrowType, RightKind> get_right(right);

    auto out_bitmap = out_data->buffers[1]->mutable_data();
    internal::GenerateBitsUnrolled(out_bitmap, 0, out_data->length,
                                   [&] { return cmp_(get_left(), get_right()); });
    return Status::OK();
  }

  Cmp cmp_;
};

template <typename ArrowType, typename Cmp>
std::shared_ptr<BinaryKernel> MakeCompareKernel(Cmp cmp) {
  return std::make_shared<CompareBinaryKernel<Cmp, ArrowType>>(std::move(cmp));
}

template <typename ArrowType>
std::shared_ptr<BinaryKernel> UnpackOperator(CompareOperator op) {
  using T = decltype(Get<ArrowType, Datum::SCALAR>{Datum{}}());

  switch (op) {
    case CompareOperator::EQUAL:
      return MakeCompareKernel<ArrowType>([](T l, T r) { return l == r; });

    case CompareOperator::NOT_EQUAL:
      return MakeCompareKernel<ArrowType>([](T l, T r) { return l != r; });

    case CompareOperator::GREATER:
      return MakeCompareKernel<ArrowType>([](T l, T r) { return l > r; });

    case CompareOperator::GREATER_EQUAL:
      return MakeCompareKernel<ArrowType>([](T l, T r) { return l >= r; });

    case CompareOperator::LESS:
      return MakeCompareKernel<ArrowType>([](T l, T r) { return l < r; });

    case CompareOperator::LESS_EQUAL:
      return MakeCompareKernel<ArrowType>([](T l, T r) { return l <= r; });
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

  template <typename StringLike>
  enable_if_base_binary<StringLike, Status> Visit(const StringLike& t) {
    *out_ = UnpackOperator<StringLike>(options_.op);
    return Status::OK();
  }

  Status Visit(const DictionaryType& t) { return NotImplemented(t); }
  Status Visit(const IntervalType& t) { return NotImplemented(t); }
  Status Visit(const FixedSizeBinaryType& t) { return NotImplemented(t); }
  Status Visit(const DurationType& t) { return NotImplemented(t); }
  Status Visit(const Date32Type& t) { return NotImplemented(t); }
  Status Visit(const Date64Type& t) { return NotImplemented(t); }
  Status Visit(const TimestampType& t) { return NotImplemented(t); }
  Status Visit(const Time32Type& t) { return NotImplemented(t); }
  Status Visit(const Time64Type& t) { return NotImplemented(t); }
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

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

#include "arrow/array/array_base.h"

#include <cstdint>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <type_traits>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/util.h"
#include "arrow/array/validate.h"
#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/pretty_print.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"
#include "arrow/visit_array_inline.h"
#include "arrow/visitor.h"

namespace arrow {

class ExtensionArray;

// ----------------------------------------------------------------------
// Base array class

int64_t Array::null_count() const { return data_->GetNullCount(); }

namespace internal {

struct ScalarFromArraySlotImpl {
  template <typename T>
  using ScalarType = typename TypeTraits<T>::ScalarType;

  Status Visit(const NullArray& a) {
    out_ = std::make_shared<NullScalar>();
    return Status::OK();
  }

  Status Visit(const BooleanArray& a) { return Finish(a.Value(index_)); }

  template <typename T>
  Status Visit(const NumericArray<T>& a) {
    return Finish(a.Value(index_));
  }

  Status Visit(const Decimal128Array& a) {
    return Finish(Decimal128(a.GetValue(index_)));
  }

  Status Visit(const Decimal256Array& a) {
    return Finish(Decimal256(a.GetValue(index_)));
  }

  template <typename T>
  Status Visit(const BaseBinaryArray<T>& a) {
    return Finish(a.GetString(index_));
  }

  Status Visit(const FixedSizeBinaryArray& a) { return Finish(a.GetString(index_)); }

  Status Visit(const DayTimeIntervalArray& a) { return Finish(a.Value(index_)); }
  Status Visit(const MonthDayNanoIntervalArray& a) { return Finish(a.Value(index_)); }

  template <typename T>
  Status Visit(const BaseListArray<T>& a) {
    return Finish(a.value_slice(index_));
  }

  Status Visit(const FixedSizeListArray& a) { return Finish(a.value_slice(index_)); }

  Status Visit(const StructArray& a) {
    ScalarVector children;
    for (const auto& child : a.fields()) {
      children.emplace_back();
      ARROW_ASSIGN_OR_RAISE(children.back(), child->GetScalar(index_));
    }
    return Finish(std::move(children));
  }

  Status Visit(const SparseUnionArray& a) {
    int8_t type_code = a.type_code(index_);

    ScalarVector children;
    for (int i = 0; i < a.type()->num_fields(); ++i) {
      children.emplace_back();
      ARROW_ASSIGN_OR_RAISE(children.back(), a.field(i)->GetScalar(index_));
    }

    out_ = std::make_shared<SparseUnionScalar>(std::move(children), type_code, a.type());
    return Status::OK();
  }

  Status Visit(const DenseUnionArray& a) {
    const auto type_code = a.type_code(index_);
    // child array which stores the actual value
    auto arr = a.field(a.child_id(index_));
    // need to look up the value based on offsets
    auto offset = a.value_offset(index_);
    ARROW_ASSIGN_OR_RAISE(auto value, arr->GetScalar(offset));
    out_ = std::make_shared<DenseUnionScalar>(value, type_code, a.type());
    return Status::OK();
  }

  Status Visit(const DictionaryArray& a) {
    auto ty = a.type();

    ARROW_ASSIGN_OR_RAISE(
        auto index, MakeScalar(checked_cast<const DictionaryType&>(*ty).index_type(),
                               a.GetValueIndex(index_)));

    auto scalar = DictionaryScalar(ty);
    scalar.is_valid = a.IsValid(index_);
    scalar.value.index = index;
    scalar.value.dictionary = a.dictionary();

    out_ = std::make_shared<DictionaryScalar>(std::move(scalar));
    return Status::OK();
  }

  Status Visit(const RunEndEncodedArray& a) {
    ArraySpan span{*a.data()};
    const int64_t physical_index = ree_util::FindPhysicalIndex(span, index_, span.offset);
    ScalarFromArraySlotImpl scalar_from_values(*a.values(), physical_index);
    ARROW_ASSIGN_OR_RAISE(auto value, std::move(scalar_from_values).Finish());
    out_ = std::make_shared<RunEndEncodedScalar>(std::move(value), a.type());
    return Status::OK();
  }

  Status Visit(const ExtensionArray& a) {
    ARROW_ASSIGN_OR_RAISE(auto storage, a.storage()->GetScalar(index_));
    out_ = std::make_shared<ExtensionScalar>(std::move(storage), a.type());
    return Status::OK();
  }

  template <typename Arg>
  Status Finish(Arg&& arg) {
    return MakeScalar(array_.type(), std::forward<Arg>(arg)).Value(&out_);
  }

  Status Finish(std::string arg) {
    return MakeScalar(array_.type(), Buffer::FromString(std::move(arg))).Value(&out_);
  }

  Result<std::shared_ptr<Scalar>> Finish() && {
    if (index_ >= array_.length()) {
      return Status::IndexError("index with value of ", index_,
                                " is out-of-bounds for array of length ",
                                array_.length());
    }

    if (array_.type()->id() != Type::RUN_END_ENCODED && array_.IsNull(index_)) {
      auto null = MakeNullScalar(array_.type());
      if (is_dictionary(array_.type()->id())) {
        auto& dict_null = checked_cast<DictionaryScalar&>(*null);
        const auto& dict_array = checked_cast<const DictionaryArray&>(array_);
        dict_null.value.dictionary = dict_array.dictionary();
      }
      return null;
    }

    RETURN_NOT_OK(VisitArrayInline(array_, this));
    return std::move(out_);
  }

  ScalarFromArraySlotImpl(const Array& array, int64_t index)
      : array_(array), index_(index) {}

  const Array& array_;
  int64_t index_;
  std::shared_ptr<Scalar> out_;
};

}  // namespace internal

Result<std::shared_ptr<Scalar>> Array::GetScalar(int64_t i) const {
  return internal::ScalarFromArraySlotImpl{*this, i}.Finish();
}

std::string Array::Diff(const Array& other) const {
  std::stringstream diff;
  ARROW_IGNORE_EXPR(Equals(other, EqualOptions().diff_sink(&diff)));
  return diff.str();
}

bool Array::Equals(const Array& arr, const EqualOptions& opts) const {
  return ArrayEquals(*this, arr, opts);
}

bool Array::Equals(const std::shared_ptr<Array>& arr, const EqualOptions& opts) const {
  if (!arr) {
    return false;
  }
  return Equals(*arr, opts);
}

bool Array::ApproxEquals(const Array& arr, const EqualOptions& opts) const {
  return ArrayApproxEquals(*this, arr, opts);
}

bool Array::ApproxEquals(const std::shared_ptr<Array>& arr,
                         const EqualOptions& opts) const {
  if (!arr) {
    return false;
  }
  return ApproxEquals(*arr, opts);
}

bool Array::RangeEquals(const Array& other, int64_t start_idx, int64_t end_idx,
                        int64_t other_start_idx, const EqualOptions& opts) const {
  return ArrayRangeEquals(*this, other, start_idx, end_idx, other_start_idx, opts);
}

bool Array::RangeEquals(const std::shared_ptr<Array>& other, int64_t start_idx,
                        int64_t end_idx, int64_t other_start_idx,
                        const EqualOptions& opts) const {
  if (!other) {
    return false;
  }
  return ArrayRangeEquals(*this, *other, start_idx, end_idx, other_start_idx, opts);
}

bool Array::RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
                        const Array& other, const EqualOptions& opts) const {
  return ArrayRangeEquals(*this, other, start_idx, end_idx, other_start_idx, opts);
}

bool Array::RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
                        const std::shared_ptr<Array>& other,
                        const EqualOptions& opts) const {
  if (!other) {
    return false;
  }
  return ArrayRangeEquals(*this, *other, start_idx, end_idx, other_start_idx, opts);
}

std::shared_ptr<Array> Array::Slice(int64_t offset, int64_t length) const {
  return MakeArray(data_->Slice(offset, length));
}

std::shared_ptr<Array> Array::Slice(int64_t offset) const {
  int64_t slice_length = data_->length - offset;
  return Slice(offset, slice_length);
}

Result<std::shared_ptr<Array>> Array::SliceSafe(int64_t offset, int64_t length) const {
  ARROW_ASSIGN_OR_RAISE(auto sliced_data, data_->SliceSafe(offset, length));
  return MakeArray(std::move(sliced_data));
}

Result<std::shared_ptr<Array>> Array::SliceSafe(int64_t offset) const {
  if (offset < 0) {
    // Avoid UBSAN in subtraction below
    return Status::IndexError("Negative array slice offset");
  }
  return SliceSafe(offset, data_->length - offset);
}

std::string Array::ToString() const {
  std::stringstream ss;
  ARROW_CHECK_OK(PrettyPrint(*this, 0, &ss));
  return ss.str();
}

void PrintTo(const Array& x, std::ostream* os) { *os << x.ToString(); }

Result<std::shared_ptr<Array>> Array::View(
    const std::shared_ptr<DataType>& out_type) const {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> result,
                        internal::GetArrayView(data_, out_type));
  return MakeArray(result);
}

// ----------------------------------------------------------------------
// NullArray

NullArray::NullArray(int64_t length) {
  SetData(ArrayData::Make(null(), length, {nullptr}, length));
}

// ----------------------------------------------------------------------
// Implement Array::Accept as inline visitor

Status Array::Accept(ArrayVisitor* visitor) const {
  return VisitArrayInline(*this, visitor);
}

Status Array::Validate() const { return internal::ValidateArray(*this); }

Status Array::ValidateFull() const { return internal::ValidateArrayFull(*this); }

}  // namespace arrow

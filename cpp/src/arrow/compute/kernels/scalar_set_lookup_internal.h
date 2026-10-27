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

// Shared between the set lookup kernels (libarrow_compute) and expression
// simplification (libarrow). Everything here must stay header-only.

#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

/// Value sets up to this length are simplified against a guarantee by filtering
/// the value set and rebinding the `is_in` call (see SimplifyIsIn in expression.cc).
/// Longer value sets are instead binary searched through SetLookupStateBase::
/// sorted_value_set, which only ever simplifies the call to `false`, see
/// https://github.com/apache/arrow/issues/46777
constexpr int64_t kIsInSimplificationMaxValueSet = 50;

/// Whether a (valid) scalar of a floating point type is NaN.
inline bool IsNaNScalar(const Scalar& value) {
  if (value.type->id() == Type::FLOAT) {
    return std::isnan(::arrow::internal::checked_cast<const FloatScalar&>(value).value);
  }
  if (value.type->id() == Type::DOUBLE) {
    return std::isnan(::arrow::internal::checked_cast<const DoubleScalar&>(value).value);
  }
  return false;
}

inline Result<bool> IsSortedValueSet(const Array& values);

/// Common state for the is_in and index_in kernels.
struct SetLookupStateBase : public KernelState {
  std::shared_ptr<DataType> value_set_type;

  /// Whether the value set contains a null (regardless of null matching behavior).
  bool value_set_has_null = false;

  /// Make the value set available to GetSortedValueSet().
  ///
  /// Called once by the `is_in` kernel init, before the state is shared. If the
  /// value set is sorted already it is used as is; otherwise sorting is deferred
  /// to the first call of GetSortedValueSet() so that only expressions which are
  /// simplified against guarantees pay for it.
  Status SetSearchableValueSet(std::shared_ptr<Array> values) {
    if (values->null_count() == 0) {
      ARROW_ASSIGN_OR_RAISE(bool sorted, IsSortedValueSet(*values));
      if (sorted) {
        sorted_value_set_ = std::move(values);
        sorted_value_set_ready_.store(true, std::memory_order_release);
        return Status::OK();
      }
    }
    unsorted_value_set_ = std::move(values);
    return Status::OK();
  }

  /// A copy of the value set sorted ascending in the order of the comparison
  /// kernels, with nulls removed, or nullptr if there is none.
  ///
  /// This is only available for `is_in` when the value set is longer than
  /// kIsInSimplificationMaxValueSet, has a type for which IsSearchableValueSetType()
  /// holds and contains no NaN. It is computed at most once; concurrent calls are
  /// safe.
  std::shared_ptr<Array> GetSortedValueSet() const {
    if (!sorted_value_set_ready_.load(std::memory_order_acquire)) {
      std::lock_guard<std::mutex> lock(sorted_value_set_mutex_);
      if (!sorted_value_set_ready_.load(std::memory_order_relaxed)) {
        if (unsorted_value_set_) {
          // Best effort: a failure only disables the simplification of `is_in`
          // against guarantees.
          sorted_value_set_ = SortValueSet(*unsorted_value_set_).ValueOr(nullptr);
          unsorted_value_set_.reset();
        }
        sorted_value_set_ready_.store(true, std::memory_order_release);
      }
    }
    return sorted_value_set_;
  }

 private:
  /// \return `values` sorted with nulls removed, or nullptr if it contains NaN
  static Result<std::shared_ptr<Array>> SortValueSet(const Array& values) {
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          SortIndices(values, ArraySortOptions(SortOrder::Ascending,
                                                               NullPlacement::AtEnd)));
    ARROW_ASSIGN_OR_RAISE(Datum taken,
                          Take(values, indices, TakeOptions::NoBoundsCheck()));
    auto sorted = taken.make_array();
    sorted = sorted->Slice(0, sorted->length() - sorted->null_count());
    // NaN sorts after every other value and before nulls. Statistics based
    // guarantees cannot bound NaN, so such a value set is not searchable.
    if (sorted->length() > 0 && is_floating(sorted->type_id())) {
      ARROW_ASSIGN_OR_RAISE(auto last, sorted->GetScalar(sorted->length() - 1));
      if (IsNaNScalar(*last)) return std::shared_ptr<Array>(nullptr);
    }
    return sorted;
  }

  mutable std::mutex sorted_value_set_mutex_;
  mutable std::atomic<bool> sorted_value_set_ready_{false};
  mutable std::shared_ptr<Array> unsorted_value_set_;
  mutable std::shared_ptr<Array> sorted_value_set_;
};

/// Whether value sets of this type can be binary searched with a comparator that
/// agrees with both the sorting kernels and the comparison kernels.
inline bool IsSearchableValueSetType(const DataType& type) {
  switch (type.id()) {
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
    case Type::UINT8:
    case Type::UINT16:
    case Type::UINT32:
    case Type::UINT64:
    case Type::FLOAT:
    case Type::DOUBLE:
    case Type::DATE32:
    case Type::DATE64:
    case Type::TIME32:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::DURATION:
    case Type::DECIMAL32:
    case Type::DECIMAL64:
    case Type::DECIMAL128:
    case Type::DECIMAL256:
    case Type::STRING:
    case Type::LARGE_STRING:
    case Type::BINARY:
    case Type::LARGE_BINARY:
    case Type::FIXED_SIZE_BINARY:
      return true;
    default:
      return false;
  }
}

namespace set_lookup_detail {

template <typename T, typename Enable = void>
struct ValueSetAccessor;

template <typename T>
struct ValueSetAccessor<
    T, enable_if_t<has_c_type<T>::value && !is_boolean_type<T>::value &&
                   !is_half_float_type<T>::value && !is_interval_type<T>::value>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;
  using ScalarType = typename TypeTraits<T>::ScalarType;
  using ValueType = typename T::c_type;

  static ValueType Get(const Array& values, int64_t i) {
    return ::arrow::internal::checked_cast<const ArrayType&>(values).Value(i);
  }
  static ValueType FromScalar(const Scalar& scalar) {
    return ::arrow::internal::checked_cast<const ScalarType&>(scalar).value;
  }
};

template <typename T>
struct ValueSetAccessor<T, enable_if_decimal<T>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;
  using ScalarType = typename TypeTraits<T>::ScalarType;
  using ValueType = typename ScalarType::ValueType;

  static ValueType Get(const Array& values, int64_t i) {
    return ValueType(
        ::arrow::internal::checked_cast<const ArrayType&>(values).GetValue(i));
  }
  static ValueType FromScalar(const Scalar& scalar) {
    return ::arrow::internal::checked_cast<const ScalarType&>(scalar).value;
  }
};

template <typename T>
struct ValueSetAccessor<T, enable_if_t<is_base_binary_type<T>::value ||
                                       std::is_same<T, FixedSizeBinaryType>::value>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;
  using ValueType = std::string_view;

  static ValueType Get(const Array& values, int64_t i) {
    return ::arrow::internal::checked_cast<const ArrayType&>(values).GetView(i);
  }
  static ValueType FromScalar(const Scalar& scalar) {
    return ::arrow::internal::checked_cast<const BaseBinaryScalar&>(scalar).view();
  }
};

template <typename T, typename Enable = void>
struct is_searchable_value_set_type : std::false_type {};

template <typename T>
struct is_searchable_value_set_type<
    T, std::void_t<decltype(ValueSetAccessor<T>::Get(std::declval<const Array&>(), 0))>>
    : std::true_type {};

template <typename ValueType>
bool IsNaNValue(const ValueType&) {
  return false;
}
inline bool IsNaNValue(float value) { return std::isnan(value); }
inline bool IsNaNValue(double value) { return std::isnan(value); }

struct IsSortedVisitor {
  const Array& values;
  bool result = false;

  template <typename T>
  enable_if_t<is_searchable_value_set_type<T>::value, Status> Visit(const T&) {
    using Accessor = ValueSetAccessor<T>;
    if (values.length() == 0) {
      result = true;
      return Status::OK();
    }
    auto previous = Accessor::Get(values, 0);
    if (IsNaNValue(previous)) return Status::OK();
    for (int64_t i = 1; i < values.length(); ++i) {
      auto current = Accessor::Get(values, i);
      if (IsNaNValue(current) || current < previous) return Status::OK();
      previous = current;
    }
    result = true;
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Searching value sets of type ", type);
  }
};

struct SearchSortedVisitor {
  const Array& sorted;
  const Scalar& bound;
  bool upper;
  int64_t result = 0;

  template <typename T>
  enable_if_t<is_searchable_value_set_type<T>::value, Status> Visit(const T&) {
    using Accessor = ValueSetAccessor<T>;
    const auto target = Accessor::FromScalar(bound);
    int64_t lo = 0, hi = sorted.length();
    while (lo < hi) {
      const int64_t mid = lo + (hi - lo) / 2;
      const auto value = Accessor::Get(sorted, mid);
      const bool go_right = upper ? !(target < value) : (value < target);
      if (go_right) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    result = lo;
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Searching value sets of type ", type);
  }
};

}  // namespace set_lookup_detail

/// Whether `values` (which must contain no nulls) is sorted ascending in the order
/// of the comparison kernels and contains no NaN.
inline Result<bool> IsSortedValueSet(const Array& values) {
  ARROW_DCHECK_EQ(values.null_count(), 0);
  set_lookup_detail::IsSortedVisitor visitor{values, /*result=*/false};
  RETURN_NOT_OK(VisitTypeInline(*values.type(), &visitor));
  return visitor.result;
}

/// Binary search a sorted value set for `bound`.
///
/// \pre `sorted` satisfies IsSortedValueSet()
/// \pre `bound` is valid, not NaN, and has the same type as `sorted`
/// \return the index of the first element not less than `bound` (`upper == false`),
///   or of the first element greater than `bound` (`upper == true`)
inline Result<int64_t> SearchSortedValueSet(const Array& sorted, const Scalar& bound,
                                            bool upper) {
  ARROW_DCHECK(bound.is_valid);
  ARROW_DCHECK(sorted.type()->Equals(*bound.type));
  set_lookup_detail::SearchSortedVisitor visitor{sorted, bound, upper, /*result=*/0};
  RETURN_NOT_OK(VisitTypeInline(*sorted.type(), &visitor));
  return visitor.result;
}

}  // namespace arrow::compute::internal

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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
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
/// GetSortedValueSet, which only ever simplifies the call to `false`, see
/// https://github.com/apache/arrow/issues/46777
constexpr int64_t kIsInSimplificationMaxValueSet = 50;

template <typename ValueType>
bool IsNaNValue(const ValueType&) {
  return false;
}
inline bool IsNaNValue(float value) { return std::isnan(value); }
inline bool IsNaNValue(double value) { return std::isnan(value); }

/// Whether a (valid) scalar of a floating point type is NaN.
inline bool IsNaNScalar(const Scalar& value) {
  switch (value.type->id()) {
    case Type::FLOAT:
      return IsNaNValue(::arrow::internal::checked_cast<const FloatScalar&>(value).value);
    case Type::DOUBLE:
      return IsNaNValue(
          ::arrow::internal::checked_cast<const DoubleScalar&>(value).value);
    default:
      return false;
  }
}

inline Result<bool> IsSortedValueSet(const Array& values);
inline Result<std::shared_ptr<Array>> SortPrimitiveValueSet(const Array& values);

/// Common state for the is_in and index_in kernels.
struct SetLookupStateBase : public KernelState {
  std::shared_ptr<DataType> value_set_type;

  /// Whether the value set contains a null (regardless of null matching behavior).
  bool value_set_has_null = false;

  /// Make the value set available to GetSortedValueSet().
  ///
  /// Called once by the `is_in` kernel init, before the state is shared. Nothing
  /// is computed until the first call of GetSortedValueSet(), so that only
  /// expressions which are simplified against guarantees pay for it.
  void SetSearchableValueSet(Datum value_set, MemoryPool* pool) {
    searchable_value_set_ = std::move(value_set);
    searchable_value_set_pool_ = pool;
  }

  /// A copy of the value set sorted ascending in the order of the comparison
  /// kernels, with nulls removed, or nullptr if there is none.
  ///
  /// This is only available for `is_in` when the value set is longer than
  /// kIsInSimplificationMaxValueSet, has a type for which IsSearchableValueSetType()
  /// holds and contains no NaN. It is computed at most once; concurrent calls are
  /// safe.
  std::shared_ptr<Array> GetSortedValueSet() const {
    std::call_once(sorted_value_set_once_, [this] {
      if (searchable_value_set_.is_arraylike()) {
        // Best effort: a failure only disables the simplification of `is_in`
        // against guarantees.
        sorted_value_set_ =
            SortValueSet(searchable_value_set_, searchable_value_set_pool_)
                .ValueOr(nullptr);
        searchable_value_set_ = Datum();
      }
    });
    return sorted_value_set_;
  }

 private:
  /// \return `value_set` sorted with nulls removed, or nullptr if it contains NaN
  static Result<std::shared_ptr<Array>> SortValueSet(const Datum& value_set,
                                                     MemoryPool* pool) {
    std::shared_ptr<Array> values;
    if (value_set.is_array()) {
      values = value_set.make_array();
    } else {
      ARROW_ASSIGN_OR_RAISE(values,
                            Concatenate(value_set.chunked_array()->chunks(), pool));
    }
    // An already sorted value set is used as is.
    if (values->null_count() == 0) {
      ARROW_ASSIGN_OR_RAISE(bool sorted, IsSortedValueSet(*values));
      if (sorted) return values;
    }

    // Sorting a copy of primitive values directly is several times faster than
    // the generic sort_indices + take path.
    auto maybe_sorted = SortPrimitiveValueSet(*values);
    if (!maybe_sorted.status().IsNotImplemented()) return maybe_sorted;

    // Only decimal and binary types get here, neither of which has NaN.
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          SortIndices(*values, ArraySortOptions(SortOrder::Ascending,
                                                                NullPlacement::AtEnd)));
    ARROW_ASSIGN_OR_RAISE(Datum taken,
                          Take(values, indices, TakeOptions::NoBoundsCheck()));
    auto sorted = taken.make_array();
    return sorted->Slice(0, sorted->length() - sorted->null_count());
  }

  mutable std::once_flag sorted_value_set_once_;
  // Released once sorted_value_set_ is computed.
  mutable Datum searchable_value_set_;
  MemoryPool* searchable_value_set_pool_ = default_memory_pool();
  mutable std::shared_ptr<Array> sorted_value_set_;
};

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

struct IsSearchableTypeVisitor {
  bool result = false;

  template <typename T>
  Status Visit(const T&) {
    result = is_searchable_value_set_type<T>::value;
    return Status::OK();
  }
};

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

struct SortPrimitiveVisitor {
  const Array& values;
  std::shared_ptr<Array> result;

  template <typename T>
  enable_if_t<has_c_type<T>::value && is_searchable_value_set_type<T>::value, Status>
  Visit(const T&) {
    using c_type = typename T::c_type;
    const auto* data = values.data()->template GetValues<c_type>(1);
    std::vector<c_type> sorted;
    sorted.reserve(values.length() - values.null_count());
    if (values.null_count() == 0) {
      sorted.assign(data, data + values.length());
    } else {
      for (int64_t i = 0; i < values.length(); ++i) {
        if (values.IsValid(i)) sorted.push_back(data[i]);
      }
    }
    // NaN would break the strict weak ordering required by std::sort. Statistics
    // based guarantees cannot bound NaN anyway, so such a value set is not
    // searchable (nullptr).
    for (const auto& value : sorted) {
      if (IsNaNValue(value)) return Status::OK();
    }
    std::sort(sorted.begin(), sorted.end());
    const auto length = static_cast<int64_t>(sorted.size());
    result = MakeArray(ArrayData::Make(values.type(), length,
                                       {nullptr, Buffer::FromVector(std::move(sorted))},
                                       /*null_count=*/0));
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Sorting value sets of type ", type);
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

/// Whether value sets of this type can be binary searched with a comparator that
/// agrees with both the sorting kernels and the comparison kernels.
inline bool IsSearchableValueSetType(const DataType& type) {
  set_lookup_detail::IsSearchableTypeVisitor visitor;
  ARROW_UNUSED(VisitTypeInline(type, &visitor));
  return visitor.result;
}

/// Whether `values` (which must contain no nulls) is sorted ascending in the order
/// of the comparison kernels and contains no NaN.
inline Result<bool> IsSortedValueSet(const Array& values) {
  ARROW_DCHECK_EQ(values.null_count(), 0);
  set_lookup_detail::IsSortedVisitor visitor{values, /*result=*/false};
  RETURN_NOT_OK(VisitTypeInline(*values.type(), &visitor));
  return visitor.result;
}

/// Sort a value set of a primitive type, dropping nulls.
///
/// \return the sorted value set, or nullptr if it contains NaN
/// \return NotImplemented for non-primitive types
inline Result<std::shared_ptr<Array>> SortPrimitiveValueSet(const Array& values) {
  set_lookup_detail::SortPrimitiveVisitor visitor{values, /*result=*/nullptr};
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

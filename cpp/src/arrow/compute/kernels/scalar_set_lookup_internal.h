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

#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

namespace arrow::compute::internal {

/// Value sets up to this length are simplified against a guarantee by filtering
/// the value set and rebinding the `is_in` call (see SimplifyIsIn in expression.cc).
/// Longer value sets are instead searched with `search_sorted` in
/// SetLookupStateBase::GetSortedValueSet, which only ever simplifies the call to a
/// result without matches, see https://github.com/apache/arrow/issues/46777
constexpr int64_t kIsInSimplificationMaxValueSet = 50;

/// Value sets up to this length are sorted for SetLookupStateBase::GetSortedValueSet.
/// Sorting costs more than building the lookup table of the kernel, so longer value
/// sets are only bounded by their smallest and largest value, which is cheap to find
/// and prunes as well when the values are clustered.
constexpr int64_t kIsInMaxSortedValueSet = 1 << 17;

/// Whether a (valid) scalar of a floating point type is NaN.
inline bool IsNaNScalar(const Scalar& value) {
  switch (value.type->id()) {
    case Type::FLOAT:
      return std::isnan(::arrow::internal::checked_cast<const FloatScalar&>(value).value);
    case Type::DOUBLE:
      return std::isnan(
          ::arrow::internal::checked_cast<const DoubleScalar&>(value).value);
    default:
      return false;
  }
}

/// Whether value sets of this type are sorted by `sort_indices` and searched by
/// `search_sorted` in the order of the comparison kernels.
inline bool IsSearchableValueSetType(const DataType& type) {
  const auto id = type.id();
  return is_integer(id) || id == Type::FLOAT || id == Type::DOUBLE || is_temporal(id) ||
         id == Type::DURATION || is_base_binary_like(id);
}

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

  struct SortedValueSet {
    /// Ascending values without nulls, or nullptr if the value set cannot be searched
    std::shared_ptr<Array> values;
    /// Whether `values` holds every value of the value set, or only its smallest and
    /// largest value (for value sets longer than kIsInMaxSortedValueSet)
    bool exact = true;
  };

  /// The value set sorted for searching with `search_sorted`.
  ///
  /// This is only available for `is_in` when the value set is longer than
  /// kIsInSimplificationMaxValueSet, has a type for which IsSearchableValueSetType()
  /// holds and contains no NaN. It is computed at most once; concurrent calls are
  /// safe.
  SortedValueSet GetSortedValueSet() const {
    std::call_once(sorted_value_set_once_, [this] {
      if (searchable_value_set_.is_arraylike()) {
        // Best effort: a failure only disables the simplification of `is_in`
        // against guarantees.
        sorted_value_set_ =
            SortValueSet(searchable_value_set_, searchable_value_set_pool_)
                .ValueOr(SortedValueSet{});
        searchable_value_set_ = Datum();
      }
    });
    return sorted_value_set_;
  }

 private:
  static Result<SortedValueSet> SortValueSet(const Datum& value_set, MemoryPool* pool) {
    std::shared_ptr<Array> values;
    if (value_set.is_array()) {
      values = value_set.make_array();
    } else {
      ARROW_ASSIGN_OR_RAISE(values,
                            Concatenate(value_set.chunked_array()->chunks(), pool));
    }
    if (values->length() > kIsInMaxSortedValueSet) {
      ARROW_ASSIGN_OR_RAISE(auto bounds, ValueSetBounds(values));
      return SortedValueSet{std::move(bounds), /*exact=*/false};
    }
    ARROW_ASSIGN_OR_RAISE(auto sorted, SortValues(values));
    return SortedValueSet{std::move(sorted), /*exact=*/true};
  }

  /// \return `values` sorted with nulls removed, or nullptr if it contains NaN
  static Result<std::shared_ptr<Array>> SortValues(const std::shared_ptr<Array>& values) {
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          SortIndices(*values, ArraySortOptions(SortOrder::Ascending,
                                                                NullPlacement::AtEnd)));
    ARROW_ASSIGN_OR_RAISE(Datum taken,
                          Take(values, indices, TakeOptions::NoBoundsCheck()));
    auto sorted = taken.make_array();
    sorted = sorted->Slice(0, sorted->length() - sorted->null_count());
    // NaN sorts after every other value and before nulls. Statistics based
    // guarantees cannot bound NaN, so such a value set is not searchable.
    if (sorted->length() > 0) {
      ARROW_ASSIGN_OR_RAISE(auto last, sorted->GetScalar(sorted->length() - 1));
      if (IsNaNScalar(*last)) return nullptr;
    }
    return sorted;
  }

  /// \return the smallest and largest value of `values`, or nullptr if it contains NaN
  ///   or only nulls
  static Result<std::shared_ptr<Array>> ValueSetBounds(
      const std::shared_ptr<Array>& values) {
    if (is_floating(values->type_id())) {
      ARROW_ASSIGN_OR_RAISE(Datum is_nan, IsNan(values));
      ARROW_ASSIGN_OR_RAISE(Datum any_nan, Any(is_nan));
      if (any_nan.scalar_as<BooleanScalar>().value) return nullptr;
    }
    ARROW_ASSIGN_OR_RAISE(Datum min_max, MinMax(values));
    const auto& bounds = min_max.scalar_as<StructScalar>().value;
    if (!bounds[0]->is_valid) return nullptr;
    ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(values->type()));
    RETURN_NOT_OK(builder->AppendScalar(*bounds[0]));
    RETURN_NOT_OK(builder->AppendScalar(*bounds[1]));
    return builder->Finish();
  }

  mutable std::once_flag sorted_value_set_once_;
  // Released once sorted_value_set_ is computed.
  mutable Datum searchable_value_set_;
  MemoryPool* searchable_value_set_pool_ = default_memory_pool();
  mutable SortedValueSet sorted_value_set_;
};

}  // namespace arrow::compute::internal

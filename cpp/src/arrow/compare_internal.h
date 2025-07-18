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

#include <cmath>

#include "arrow/array/data.h"
#include "arrow/scalar.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/float16.h"
#include "arrow/util/memory_internal.h"
#include "arrow/util/ree_util.h"

namespace arrow {

using internal::checked_cast;
using util::Float16;

// TODO also handle HALF_FLOAT NaNs

template <bool Approximate, bool NansEqual, bool SignedZerosEqual>
struct FloatingEqualityFlags {
  static constexpr bool approximate = Approximate;
  static constexpr bool nans_equal = NansEqual;
  static constexpr bool signed_zeros_equal = SignedZerosEqual;
};

template <typename T, typename Flags>
struct FloatingEquality {
  explicit FloatingEquality(const EqualOptions& options)
      : epsilon(static_cast<T>(options.atol())) {}

  bool operator()(T x, T y) const {
    if (x == y) {
      return Flags::signed_zeros_equal || (std::signbit(x) == std::signbit(y));
    }
    if (Flags::nans_equal && std::isnan(x) && std::isnan(y)) {
      return true;
    }
    if (Flags::approximate && (fabs(x - y) <= epsilon)) {
      return true;
    }
    return false;
  }

  const T epsilon;
};

// For half-float equality.
template <typename Flags>
struct FloatingEquality<uint16_t, Flags> {
  explicit FloatingEquality(const EqualOptions& options)
      : epsilon(static_cast<float>(options.atol())) {}

  bool operator()(uint16_t x, uint16_t y) const {
    Float16 f_x = Float16::FromBits(x);
    Float16 f_y = Float16::FromBits(y);
    if (x == y) {
      return Flags::signed_zeros_equal || (f_x.signbit() == f_y.signbit());
    }
    if (Flags::nans_equal && f_x.is_nan() && f_y.is_nan()) {
      return true;
    }
    if (Flags::approximate && (fabs(f_x.ToFloat() - f_y.ToFloat()) <= epsilon)) {
      return true;
    }
    return false;
  }

  const float epsilon;
};

template <typename T, typename Visitor>
struct FloatingEqualityDispatcher {
  const EqualOptions& options;
  bool floating_approximate;
  Visitor&& visit;

  template <bool Approximate, bool NansEqual>
  void DispatchL3() {
    if (options.signed_zeros_equal()) {
      visit(FloatingEquality<T, FloatingEqualityFlags<Approximate, NansEqual, true>>{
          options});
    } else {
      visit(FloatingEquality<T, FloatingEqualityFlags<Approximate, NansEqual, false>>{
          options});
    }
  }

  template <bool Approximate>
  void DispatchL2() {
    if (options.nans_equal()) {
      DispatchL3<Approximate, true>();
    } else {
      DispatchL3<Approximate, false>();
    }
  }

  void Dispatch() {
    if (floating_approximate) {
      DispatchL2<true>();
    } else {
      DispatchL2<false>();
    }
  }
};

// Call `visit(equality_func)` where `equality_func` has the signature `bool(T, T)`
// and returns true if the two values compare equal.
template <typename T, typename Visitor>
void VisitFloatingEquality(const EqualOptions& options, bool floating_approximate,
                           Visitor&& visit) {
  FloatingEqualityDispatcher<T, Visitor>{options, floating_approximate,
                                         std::forward<Visitor>(visit)}
      .Dispatch();
}

bool CompareArrayRanges(const ArrayData& left, const ArrayData& right,
                        int64_t left_start_idx, int64_t left_end_idx,
                        int64_t right_start_idx, const EqualOptions& options,
                        bool floating_approximate);

class ARROW_EXPORT RangeDataEqualsImpl {
 public:
  // PRE-CONDITIONS:
  // - the types are equal
  // - the ranges are in bounds
  // - the ArrayData arguments have the same length
  RangeDataEqualsImpl(const EqualOptions& options, bool floating_approximate,
                      const ArraySpan& left, const ArraySpan& right,
                      int64_t left_start_idx, int64_t right_start_idx,
                      int64_t range_length)
      : options_(options),
        floating_approximate_(floating_approximate),
        left_(left),
        right_(right),
        left_start_idx_(left_start_idx),
        right_start_idx_(right_start_idx),
        range_length_(range_length),
        result_(false) {}

  bool Compare();
  bool CompareWithType(const DataType& type);
  Status Visit(const NullType&);

  template <typename TypeClass>
  enable_if_primitive_ctype<TypeClass, Status> Visit(const TypeClass& type) {
    return ComparePrimitive(type);
  }

  template <typename TypeClass>
  enable_if_t<is_temporal_type<TypeClass>::value, Status> Visit(const TypeClass& type) {
    return ComparePrimitive(type);
  }

  Status Visit(const BooleanType&);
  Status Visit(const FloatType& type);
  Status Visit(const DoubleType& type);
  Status Visit(const HalfFloatType& type);
  // Also matches StringType
  Status Visit(const BinaryType& type);
  // Also matches StringViewType
  Status Visit(const BinaryViewType& type);
  // Also matches LargeStringType
  Status Visit(const LargeBinaryType& type);
  Status Visit(const FixedSizeBinaryType& type);
  // Also matches MapType
  Status Visit(const ListType& type);
  Status Visit(const LargeListType& type);
  Status Visit(const ListViewType& type);
  Status Visit(const LargeListViewType& type);
  Status Visit(const FixedSizeListType& type);
  Status Visit(const StructType& type);
  Status Visit(const SparseUnionType& type);
  Status Visit(const DenseUnionType& type);
  Status Visit(const DictionaryType& type);
  Status Visit(const RunEndEncodedType& type);
  Status Visit(const ExtensionType& type);

 protected:
  template <typename TypeClass, typename CType = typename TypeClass::c_type>
  Status ComparePrimitive(const TypeClass&) {
    const CType* left_values = left_.GetValues<CType>(1);
    const CType* right_values = right_.GetValues<CType>(1);
    VisitValidRuns([&](int64_t i, int64_t length) {
      return memcmp(left_values + left_start_idx_ + i,
                    right_values + right_start_idx_ + i, length * sizeof(CType)) == 0;
    });
    return Status::OK();
  }

  template <typename TypeClass>
  Status CompareFloating(const TypeClass&) {
    using CType = typename TypeClass::c_type;
    const CType* left_values = left_.GetValues<CType>(1);
    const CType* right_values = right_.GetValues<CType>(1);

    auto visitor = [&](auto&& compare_func) {
      VisitValues([&](int64_t i) {
        const CType x = left_values[i + left_start_idx_];
        const CType y = right_values[i + right_start_idx_];
        return compare_func(x, y);
      });
    };
    VisitFloatingEquality<CType>(options_, floating_approximate_, std::move(visitor));
    return Status::OK();
  }

  template <typename TypeClass>
  Status CompareBinary(const TypeClass&) {
    const uint8_t* left_data = left_.GetValues<uint8_t>(2, 0);
    const uint8_t* right_data = right_.GetValues<uint8_t>(2, 0);

    if (left_data != nullptr && right_data != nullptr) {
      const auto compare_ranges = [&](int64_t left_offset, int64_t right_offset,
                                      int64_t length) -> bool {
        return memcmp(left_data + left_offset, right_data + right_offset, length) == 0;
      };
      CompareWithOffsets<typename TypeClass::offset_type>(1, compare_ranges);
    } else {
      // One of the arrays is an array of empty strings and nulls.
      // We just need to compare the offsets.
      // (note we must not call memcmp() with null data pointers)
      CompareWithOffsets<typename TypeClass::offset_type>(1, [](...) { return true; });
    }
    return Status::OK();
  }

  template <typename TypeClass>
  Status CompareList(const TypeClass&) {
    const auto left_data = left_.child_data[0];
    const auto right_data = right_.child_data[0];

    const auto compare_ranges = [&](int64_t left_offset, int64_t right_offset,
                                    int64_t length) -> bool {
      RangeDataEqualsImpl impl(options_, floating_approximate_, left_data, right_data,
                               left_offset, right_offset, length);
      return impl.Compare();
    };

    CompareWithOffsets<typename TypeClass::offset_type>(1, compare_ranges);
    return Status::OK();
  }

  template <typename TypeClass>
  Status CompareListView(const TypeClass& type) {
    const auto left_values = left_.child_data[0];
    const auto right_values = right_.child_data[0];

    using offset_type = typename TypeClass::offset_type;
    const auto* left_offsets = left_.GetValues<offset_type>(1) + left_start_idx_;
    const auto* right_offsets = right_.GetValues<offset_type>(1) + right_start_idx_;
    const auto* left_sizes = left_.GetValues<offset_type>(2) + left_start_idx_;
    const auto* right_sizes = right_.GetValues<offset_type>(2) + right_start_idx_;

    auto compare_view = [&](int64_t i, int64_t length) -> bool {
      for (int64_t j = i; j < i + length; ++j) {
        if (left_sizes[j] != right_sizes[j]) {
          return false;
        }
        const offset_type size = left_sizes[j];
        if (size == 0) {
          continue;
        }
        RangeDataEqualsImpl impl(options_, floating_approximate_, left_values,
                                 right_values, left_offsets[j], right_offsets[j], size);
        if (!impl.Compare()) {
          return false;
        }
      }
      return true;
    };
    VisitValidRuns(std::move(compare_view));
    return Status::OK();
  }

  template <typename RunEndCType>
  Status CompareRunEndEncoded() {
    auto left_span = ArraySpan(left_);
    auto right_span = ArraySpan(right_);
    left_span.SetSlice(left_.offset + left_start_idx_, range_length_);
    right_span.SetSlice(right_.offset + right_start_idx_, range_length_);
    const ree_util::RunEndEncodedArraySpan<RunEndCType> left(left_span);
    const ree_util::RunEndEncodedArraySpan<RunEndCType> right(right_span);

    const auto left_values = left_.child_data[1];
    const auto right_values = right_.child_data[1];

    auto it = ree_util::MergedRunsIterator(left, right);
    for (; !it.is_end(); ++it) {
      RangeDataEqualsImpl impl(options_, floating_approximate_, left_values, right_values,
                               it.index_into_left_array(), it.index_into_right_array(),
                               /*range_length=*/1);
      if (!impl.Compare()) {
        result_ = false;
        return Status::OK();
      }
    }
    return Status::OK();
  }

  template <typename offset_type, typename CompareRanges>
  void CompareWithOffsets(int offsets_buffer_index, CompareRanges&& compare_ranges) {
    const offset_type* left_offsets =
        left_.GetValues<offset_type>(offsets_buffer_index) + left_start_idx_;
    const offset_type* right_offsets =
        right_.GetValues<offset_type>(offsets_buffer_index) + right_start_idx_;

    const auto compare_runs = [&](int64_t i, int64_t length) {
      for (int64_t j = i; j < i + length; ++j) {
        if (left_offsets[j + 1] - left_offsets[j] !=
            right_offsets[j + 1] - right_offsets[j]) {
          return false;
        }
      }
      if (!compare_ranges(left_offsets[i], right_offsets[i],
                          left_offsets[i + length] - left_offsets[i])) {
        return false;
      }
      return true;
    };

    VisitValidRuns(compare_runs);
  }

  template <typename CompareValues>
  void VisitValues(CompareValues&& compare_values) {
    internal::VisitSetBitRunsVoid(left_.buffers[0].data, left_.offset + left_start_idx_,
                                  range_length_, [&](int64_t position, int64_t length) {
                                    for (int64_t i = 0; i < length; ++i) {
                                      result_ &= compare_values(position + i);
                                    }
                                  });
  }

  // Visit and compare runs of non-null values
  template <typename CompareRuns>
  void VisitValidRuns(CompareRuns&& compare_runs) {
    const uint8_t* left_null_bitmap = left_.GetValues<uint8_t>(0, 0);
    if (left_null_bitmap == nullptr) {
      result_ = compare_runs(0, range_length_);
      return;
    }
    internal::SetBitRunReader reader(left_null_bitmap, left_.offset + left_start_idx_,
                                     range_length_);
    while (true) {
      const auto run = reader.NextRun();
      if (run.length == 0) {
        return;
      }
      if (!compare_runs(run.position, run.length)) {
        result_ = false;
        return;
      }
    }
  }

  const EqualOptions& options_;
  const bool floating_approximate_;
  const ArraySpan& left_;
  const ArraySpan& right_;
  const int64_t left_start_idx_;
  const int64_t right_start_idx_;
  const int64_t range_length_;

  bool result_;
};

bool ArrayEquals(const Array& left, const Array& right, const EqualOptions& opts,
                 bool floating_approximate);
bool ScalarEquals(const Scalar& left, const Scalar& right, const EqualOptions& options,
                  bool floating_approximate);

class ScalarEqualsVisitor {
 public:
  // PRE-CONDITIONS:
  // - the types are equal
  // - the scalars are non-null
  explicit ScalarEqualsVisitor(const Scalar& right, const EqualOptions& opts,
                               bool floating_approximate)
      : right_(right),
        options_(opts),
        floating_approximate_(floating_approximate),
        result_(false) {}

  Status Visit(const NullScalar& left);
  Status Visit(const BooleanScalar& left);

  template <typename T>
  typename std::enable_if<(is_primitive_ctype<typename T::TypeClass>::value ||
                           is_temporal_type<typename T::TypeClass>::value),
                          Status>::type
  Visit(const T& left_) {
    const auto& right = checked_cast<const T&>(right_);
    result_ = right.value == left_.value;
    return Status::OK();
  }

  Status Visit(const FloatScalar& left);
  Status Visit(const DoubleScalar& left);
  Status Visit(const HalfFloatScalar& left);

  template <typename T>
  enable_if_t<std::is_base_of<BaseBinaryScalar, T>::value, Status> Visit(const T& left) {
    const auto& right = checked_cast<const BaseBinaryScalar&>(right_);
    result_ = internal::SharedPtrEquals(left.value, right.value);
    return Status::OK();
  }

  Status Visit(const Decimal32Scalar& left);
  Status Visit(const Decimal64Scalar& left);
  Status Visit(const Decimal128Scalar& left);
  Status Visit(const Decimal256Scalar& left);
  Status Visit(const ListScalar& left);
  Status Visit(const LargeListScalar& left);
  Status Visit(const ListViewScalar& left);
  Status Visit(const LargeListViewScalar& left);
  Status Visit(const MapScalar& left);
  Status Visit(const FixedSizeListScalar& left);
  Status Visit(const StructScalar& left);
  Status Visit(const DenseUnionScalar& left);
  Status Visit(const SparseUnionScalar& left);
  Status Visit(const DictionaryScalar& left);
  Status Visit(const RunEndEncodedScalar& left);
  Status Visit(const ExtensionScalar& left);

  bool result() const;

 protected:
  template <typename ScalarType>
  Status CompareFloating(const ScalarType& left) {
    using CType = decltype(left.value);
    const auto& right = checked_cast<const ScalarType&>(right_);

    auto visitor = [&](auto&& compare_func) {
      result_ = compare_func(left.value, right.value);
    };
    VisitFloatingEquality<CType>(options_, floating_approximate_, std::move(visitor));
    return Status::OK();
  }

  const Scalar& right_;
  const EqualOptions options_;
  const bool floating_approximate_;
  bool result_;
};

Status PrintDiff(const Array& left, const Array& right, std::ostream* os);
Status PrintDiff(const Array& left, const Array& right, int64_t left_offset,
                 int64_t left_length, int64_t right_offset, int64_t right_length,
                 std::ostream* os);

bool ArrayRangeEquals(const Array& left, const Array& right, int64_t left_start_idx,
                      int64_t left_end_idx, int64_t right_start_idx,
                      const EqualOptions& options, bool floating_approximate);
}  // namespace arrow

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

// Functions for comparing Arrow data structures

#include "arrow/compare.h"

#include <climits>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/diff.h"
#include "arrow/buffer.h"
#include "arrow/scalar.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/memory.h"
#include "arrow/util/ree_util.h"
#include "arrow/visit_scalar_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::BitmapEquals;
using internal::BitmapReader;
using internal::BitmapUInt64Reader;
using internal::checked_cast;
using internal::OptionalBitmapEquals;

// ----------------------------------------------------------------------
// Public method implementations

namespace {

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

inline bool IdentityImpliesEqualityNansNotEqual(const DataType& type) {
  if (type.id() == Type::FLOAT || type.id() == Type::DOUBLE) {
    return false;
  }
  for (const auto& child : type.fields()) {
    if (!IdentityImpliesEqualityNansNotEqual(*child->type())) {
      return false;
    }
  }
  return true;
}

inline bool IdentityImpliesEquality(const DataType& type, const EqualOptions& options) {
  if (options.nans_equal()) {
    return true;
  }
  return IdentityImpliesEqualityNansNotEqual(type);
}

bool CompareArrayRanges(const ArrayData& left, const ArrayData& right,
                        int64_t left_start_idx, int64_t left_end_idx,
                        int64_t right_start_idx, const EqualOptions& options,
                        bool floating_approximate);

class RangeDataEqualsImpl {
 public:
  // PRE-CONDITIONS:
  // - the types are equal
  // - the ranges are in bounds
  RangeDataEqualsImpl(const EqualOptions& options, bool floating_approximate,
                      const ArrayData& left, const ArrayData& right,
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

  bool Compare() {
    // Compare null bitmaps
    if (left_start_idx_ == 0 && right_start_idx_ == 0 && range_length_ == left_.length &&
        range_length_ == right_.length) {
      // If we're comparing entire arrays, we can first compare the cached null counts
      if (left_.GetNullCount() != right_.GetNullCount()) {
        return false;
      }
    }
    if (!OptionalBitmapEquals(left_.buffers[0], left_.offset + left_start_idx_,
                              right_.buffers[0], right_.offset + right_start_idx_,
                              range_length_)) {
      return false;
    }
    // Compare values
    return CompareWithType(*left_.type);
  }

  bool CompareWithType(const DataType& type) {
    result_ = true;
    if (range_length_ != 0) {
      ARROW_CHECK_OK(VisitTypeInline(type, this));
    }
    return result_;
  }

  Status Visit(const NullType&) { return Status::OK(); }

  template <typename TypeClass>
  enable_if_primitive_ctype<TypeClass, Status> Visit(const TypeClass& type) {
    return ComparePrimitive(type);
  }

  template <typename TypeClass>
  enable_if_t<is_temporal_type<TypeClass>::value, Status> Visit(const TypeClass& type) {
    return ComparePrimitive(type);
  }

  Status Visit(const BooleanType&) {
    const uint8_t* left_bits = left_.GetValues<uint8_t>(1, 0);
    const uint8_t* right_bits = right_.GetValues<uint8_t>(1, 0);
    auto compare_runs = [&](int64_t i, int64_t length) -> bool {
      if (length <= 8) {
        // Avoid the BitmapUInt64Reader overhead for very small runs
        for (int64_t j = i; j < i + length; ++j) {
          if (bit_util::GetBit(left_bits, left_start_idx_ + left_.offset + j) !=
              bit_util::GetBit(right_bits, right_start_idx_ + right_.offset + j)) {
            return false;
          }
        }
        return true;
      } else if (length <= 1024) {
        BitmapUInt64Reader left_reader(left_bits, left_start_idx_ + left_.offset + i,
                                       length);
        BitmapUInt64Reader right_reader(right_bits, right_start_idx_ + right_.offset + i,
                                        length);
        while (left_reader.position() < length) {
          if (left_reader.NextWord() != right_reader.NextWord()) {
            return false;
          }
        }
        DCHECK_EQ(right_reader.position(), length);
      } else {
        // BitmapEquals is the fastest method on large runs
        return BitmapEquals(left_bits, left_start_idx_ + left_.offset + i, right_bits,
                            right_start_idx_ + right_.offset + i, length);
      }
      return true;
    };
    VisitValidRuns(compare_runs);
    return Status::OK();
  }

  Status Visit(const FloatType& type) { return CompareFloating(type); }

  Status Visit(const DoubleType& type) { return CompareFloating(type); }

  // Also matches StringType
  Status Visit(const BinaryType& type) { return CompareBinary(type); }

  // Also matches LargeStringType
  Status Visit(const LargeBinaryType& type) { return CompareBinary(type); }

  Status Visit(const FixedSizeBinaryType& type) {
    const auto byte_width = type.byte_width();
    const uint8_t* left_data = left_.GetValues<uint8_t>(1, 0);
    const uint8_t* right_data = right_.GetValues<uint8_t>(1, 0);

    if (left_data != nullptr && right_data != nullptr) {
      auto compare_runs = [&](int64_t i, int64_t length) -> bool {
        return memcmp(left_data + (left_start_idx_ + left_.offset + i) * byte_width,
                      right_data + (right_start_idx_ + right_.offset + i) * byte_width,
                      length * byte_width) == 0;
      };
      VisitValidRuns(compare_runs);
    } else {
      auto compare_runs = [&](int64_t i, int64_t length) -> bool { return true; };
      VisitValidRuns(compare_runs);
    }
    return Status::OK();
  }

  // Also matches MapType
  Status Visit(const ListType& type) { return CompareList(type); }

  Status Visit(const LargeListType& type) { return CompareList(type); }

  Status Visit(const FixedSizeListType& type) {
    const auto list_size = type.list_size();
    const ArrayData& left_data = *left_.child_data[0];
    const ArrayData& right_data = *right_.child_data[0];

    auto compare_runs = [&](int64_t i, int64_t length) -> bool {
      RangeDataEqualsImpl impl(options_, floating_approximate_, left_data, right_data,
                               (left_start_idx_ + left_.offset + i) * list_size,
                               (right_start_idx_ + right_.offset + i) * list_size,
                               length * list_size);
      return impl.Compare();
    };
    VisitValidRuns(compare_runs);
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    const int32_t num_fields = type.num_fields();

    auto compare_runs = [&](int64_t i, int64_t length) -> bool {
      for (int32_t f = 0; f < num_fields; ++f) {
        RangeDataEqualsImpl impl(options_, floating_approximate_, *left_.child_data[f],
                                 *right_.child_data[f],
                                 left_start_idx_ + left_.offset + i,
                                 right_start_idx_ + right_.offset + i, length);
        if (!impl.Compare()) {
          return false;
        }
      }
      return true;
    };
    VisitValidRuns(compare_runs);
    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) {
    const auto& child_ids = type.child_ids();
    const int8_t* left_codes = left_.GetValues<int8_t>(1);
    const int8_t* right_codes = right_.GetValues<int8_t>(1);

    // Unions don't have a null bitmap
    for (int64_t i = 0; i < range_length_; ++i) {
      const auto type_id = left_codes[left_start_idx_ + i];
      if (type_id != right_codes[right_start_idx_ + i]) {
        result_ = false;
        break;
      }
      const auto child_num = child_ids[type_id];
      // XXX can we instead detect runs of same-child union values?
      RangeDataEqualsImpl impl(
          options_, floating_approximate_, *left_.child_data[child_num],
          *right_.child_data[child_num], left_start_idx_ + left_.offset + i,
          right_start_idx_ + right_.offset + i, 1);
      if (!impl.Compare()) {
        result_ = false;
        break;
      }
    }
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    const auto& child_ids = type.child_ids();
    const int8_t* left_codes = left_.GetValues<int8_t>(1);
    const int8_t* right_codes = right_.GetValues<int8_t>(1);
    const int32_t* left_offsets = left_.GetValues<int32_t>(2);
    const int32_t* right_offsets = right_.GetValues<int32_t>(2);

    for (int64_t i = 0; i < range_length_; ++i) {
      const auto type_id = left_codes[left_start_idx_ + i];
      if (type_id != right_codes[right_start_idx_ + i]) {
        result_ = false;
        break;
      }
      const auto child_num = child_ids[type_id];
      RangeDataEqualsImpl impl(
          options_, floating_approximate_, *left_.child_data[child_num],
          *right_.child_data[child_num], left_offsets[left_start_idx_ + i],
          right_offsets[right_start_idx_ + i], 1);
      if (!impl.Compare()) {
        result_ = false;
        break;
      }
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    // Compare dictionaries
    result_ &= CompareArrayRanges(
        *left_.dictionary, *right_.dictionary,
        /*left_start_idx=*/0,
        /*left_end_idx=*/std::max(left_.dictionary->length, right_.dictionary->length),
        /*right_start_idx=*/0, options_, floating_approximate_);
    if (result_) {
      // Compare indices
      result_ &= CompareWithType(*type.index_type());
    }
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    switch (type.run_end_type()->id()) {
      case Type::INT16:
        return CompareRunEndEncoded<int16_t>();
      case Type::INT32:
        return CompareRunEndEncoded<int32_t>();
      case Type::INT64:
        return CompareRunEndEncoded<int64_t>();
      default:
        return Status::Invalid("invalid run ends type: ", *type.run_end_type());
    }
  }

  Status Visit(const ExtensionType& type) {
    // Compare storages
    result_ &= CompareWithType(*type.storage_type());
    return Status::OK();
  }

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
    const ArrayData& left_data = *left_.child_data[0];
    const ArrayData& right_data = *right_.child_data[0];

    const auto compare_ranges = [&](int64_t left_offset, int64_t right_offset,
                                    int64_t length) -> bool {
      RangeDataEqualsImpl impl(options_, floating_approximate_, left_data, right_data,
                               left_offset, right_offset, length);
      return impl.Compare();
    };

    CompareWithOffsets<typename TypeClass::offset_type>(1, compare_ranges);
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

    const auto& left_values = *left_.child_data[1];
    const auto& right_values = *right_.child_data[1];

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
    internal::VisitSetBitRunsVoid(left_.buffers[0], left_.offset + left_start_idx_,
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
  const ArrayData& left_;
  const ArrayData& right_;
  const int64_t left_start_idx_;
  const int64_t right_start_idx_;
  const int64_t range_length_;

  bool result_;
};

bool CompareArrayRanges(const ArrayData& left, const ArrayData& right,
                        int64_t left_start_idx, int64_t left_end_idx,
                        int64_t right_start_idx, const EqualOptions& options,
                        bool floating_approximate) {
  if (left.type->id() != right.type->id() ||
      !TypeEquals(*left.type, *right.type, false /* check_metadata */)) {
    return false;
  }

  const int64_t range_length = left_end_idx - left_start_idx;
  DCHECK_GE(range_length, 0);
  if (left_start_idx + range_length > left.length) {
    // Left range too small
    return false;
  }
  if (right_start_idx + range_length > right.length) {
    // Right range too small
    return false;
  }
  if (&left == &right && left_start_idx == right_start_idx &&
      IdentityImpliesEquality(*left.type, options)) {
    return true;
  }
  // Compare values
  RangeDataEqualsImpl impl(options, floating_approximate, left, right, left_start_idx,
                           right_start_idx, range_length);
  return impl.Compare();
}

class TypeEqualsVisitor {
 public:
  explicit TypeEqualsVisitor(const DataType& right, bool check_metadata)
      : right_(right), check_metadata_(check_metadata), result_(false) {}

  bool MetadataEqual(const Field& left, const Field& right) {
    if (left.HasMetadata() && right.HasMetadata()) {
      return left.metadata()->Equals(*right.metadata());
    } else {
      return !left.HasMetadata() && !right.HasMetadata();
    }
  }

  Status VisitChildren(const DataType& left) {
    if (left.num_fields() != right_.num_fields()) {
      result_ = false;
      return Status::OK();
    }

    for (int i = 0; i < left.num_fields(); ++i) {
      if (!left.field(i)->Equals(right_.field(i), check_metadata_)) {
        result_ = false;
        return Status::OK();
      }
    }
    result_ = true;
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_null_type<T>::value || is_primitive_ctype<T>::value ||
                  is_base_binary_type<T>::value,
              Status>
  Visit(const T&) {
    result_ = true;
    return Status::OK();
  }

  template <typename T>
  enable_if_interval<T, Status> Visit(const T& left) {
    const auto& right = checked_cast<const IntervalType&>(right_);
    result_ = right.interval_type() == left.interval_type();
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_time_type<T>::value || is_date_type<T>::value ||
                  is_duration_type<T>::value,
              Status>
  Visit(const T& left) {
    const auto& right = checked_cast<const T&>(right_);
    result_ = left.unit() == right.unit();
    return Status::OK();
  }

  Status Visit(const TimestampType& left) {
    const auto& right = checked_cast<const TimestampType&>(right_);
    result_ = left.unit() == right.unit() && left.timezone() == right.timezone();
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType& left) {
    const auto& right = checked_cast<const FixedSizeBinaryType&>(right_);
    result_ = left.byte_width() == right.byte_width();
    return Status::OK();
  }

  Status Visit(const Decimal128Type& left) {
    const auto& right = checked_cast<const Decimal128Type&>(right_);
    result_ = left.precision() == right.precision() && left.scale() == right.scale();
    return Status::OK();
  }

  Status Visit(const Decimal256Type& left) {
    const auto& right = checked_cast<const Decimal256Type&>(right_);
    result_ = left.precision() == right.precision() && left.scale() == right.scale();
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_list_like_type<T>::value, Status> Visit(const T& left) {
    std::shared_ptr<Field> left_field = left.field(0);
    std::shared_ptr<Field> right_field = checked_cast<const T&>(right_).field(0);
    bool equal_names = !check_metadata_ || (left_field->name() == right_field->name());
    bool equal_metadata = !check_metadata_ || MetadataEqual(*left_field, *right_field);

    result_ = equal_names && equal_metadata &&
              (left_field->nullable() == right_field->nullable()) &&
              left_field->type()->Equals(*right_field->type(), check_metadata_);

    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_struct_type<T>::value, Status> Visit(const T& left) {
    return VisitChildren(left);
  }

  Status Visit(const MapType& left) {
    const auto& right = checked_cast<const MapType&>(right_);
    if (left.keys_sorted() != right.keys_sorted()) {
      result_ = false;
      return Status::OK();
    }
    if (check_metadata_ && (left.item_field()->name() != right.item_field()->name() ||
                            left.key_field()->name() != right.key_field()->name() ||
                            left.value_field()->name() != right.value_field()->name())) {
      result_ = false;
      return Status::OK();
    }
    if (check_metadata_ && !(MetadataEqual(*left.item_field(), *right.item_field()) &&
                             MetadataEqual(*left.key_field(), *right.key_field()) &&
                             MetadataEqual(*left.value_field(), *right.value_field()))) {
      result_ = false;
      return Status::OK();
    }
    result_ = left.key_type()->Equals(*right.key_type(), check_metadata_) &&
              left.item_type()->Equals(*right.item_type(), check_metadata_);
    return Status::OK();
  }

  Status Visit(const UnionType& left) {
    const auto& right = checked_cast<const UnionType&>(right_);

    if (left.mode() != right.mode() || left.type_codes() != right.type_codes()) {
      result_ = false;
      return Status::OK();
    }

    result_ = std::equal(
        left.fields().begin(), left.fields().end(), right.fields().begin(),
        [this](const std::shared_ptr<Field>& l, const std::shared_ptr<Field>& r) {
          return l->Equals(r, check_metadata_);
        });
    return Status::OK();
  }

  Status Visit(const DictionaryType& left) {
    const auto& right = checked_cast<const DictionaryType&>(right_);
    result_ = left.index_type()->Equals(right.index_type()) &&
              left.value_type()->Equals(right.value_type()) &&
              (left.ordered() == right.ordered());
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& left) {
    const auto& right = checked_cast<const RunEndEncodedType&>(right_);
    result_ = left.value_type()->Equals(right.value_type()) &&
              left.run_end_type()->Equals(right.run_end_type());
    return Status::OK();
  }

  Status Visit(const ExtensionType& left) {
    result_ = left.ExtensionEquals(static_cast<const ExtensionType&>(right_));
    return Status::OK();
  }

  bool result() const { return result_; }

 protected:
  const DataType& right_;
  bool check_metadata_;
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

  Status Visit(const NullScalar& left) {
    result_ = true;
    return Status::OK();
  }

  Status Visit(const BooleanScalar& left) {
    const auto& right = checked_cast<const BooleanScalar&>(right_);
    result_ = left.value == right.value;
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<(is_primitive_ctype<typename T::TypeClass>::value ||
                           is_temporal_type<typename T::TypeClass>::value),
                          Status>::type
  Visit(const T& left_) {
    const auto& right = checked_cast<const T&>(right_);
    result_ = right.value == left_.value;
    return Status::OK();
  }

  Status Visit(const FloatScalar& left) { return CompareFloating(left); }

  Status Visit(const DoubleScalar& left) { return CompareFloating(left); }

  template <typename T>
  typename std::enable_if<std::is_base_of<BaseBinaryScalar, T>::value, Status>::type
  Visit(const T& left) {
    const auto& right = checked_cast<const BaseBinaryScalar&>(right_);
    result_ = internal::SharedPtrEquals(left.value, right.value);
    return Status::OK();
  }

  Status Visit(const Decimal128Scalar& left) {
    const auto& right = checked_cast<const Decimal128Scalar&>(right_);
    result_ = left.value == right.value;
    return Status::OK();
  }

  Status Visit(const Decimal256Scalar& left) {
    const auto& right = checked_cast<const Decimal256Scalar&>(right_);
    result_ = left.value == right.value;
    return Status::OK();
  }

  Status Visit(const ListScalar& left) {
    const auto& right = checked_cast<const ListScalar&>(right_);
    result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
    return Status::OK();
  }

  Status Visit(const LargeListScalar& left) {
    const auto& right = checked_cast<const LargeListScalar&>(right_);
    result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
    return Status::OK();
  }

  Status Visit(const MapScalar& left) {
    const auto& right = checked_cast<const MapScalar&>(right_);
    result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
    return Status::OK();
  }

  Status Visit(const FixedSizeListScalar& left) {
    const auto& right = checked_cast<const FixedSizeListScalar&>(right_);
    result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
    return Status::OK();
  }

  Status Visit(const StructScalar& left) {
    const auto& right = checked_cast<const StructScalar&>(right_);

    if (right.value.size() != left.value.size()) {
      result_ = false;
    } else {
      bool all_equals = true;
      for (size_t i = 0; i < left.value.size() && all_equals; i++) {
        all_equals &= ScalarEquals(*left.value[i], *right.value[i], options_,
                                   floating_approximate_);
      }
      result_ = all_equals;
    }

    return Status::OK();
  }

  Status Visit(const DenseUnionScalar& left) {
    const auto& right = checked_cast<const DenseUnionScalar&>(right_);
    result_ = ScalarEquals(*left.value, *right.value, options_, floating_approximate_);
    return Status::OK();
  }

  Status Visit(const SparseUnionScalar& left) {
    const auto& right = checked_cast<const SparseUnionScalar&>(right_);
    result_ = ScalarEquals(*left.value[left.child_id], *right.value[right.child_id],
                           options_, floating_approximate_);
    return Status::OK();
  }

  Status Visit(const DictionaryScalar& left) {
    const auto& right = checked_cast<const DictionaryScalar&>(right_);
    result_ = ScalarEquals(*left.value.index, *right.value.index, options_,
                           floating_approximate_) &&
              ArrayEquals(*left.value.dictionary, *right.value.dictionary, options_,
                          floating_approximate_);
    return Status::OK();
  }

  Status Visit(const RunEndEncodedScalar& left) {
    const auto& right = checked_cast<const RunEndEncodedScalar&>(right_);
    result_ = ScalarEquals(*left.value, *right.value, options_, floating_approximate_);
    return Status::OK();
  }

  Status Visit(const ExtensionScalar& left) {
    const auto& right = checked_cast<const ExtensionScalar&>(right_);
    result_ = ScalarEquals(*left.value, *right.value, options_, floating_approximate_);
    return Status::OK();
  }

  bool result() const { return result_; }

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
                 std::ostream* os) {
  if (os == nullptr) {
    return Status::OK();
  }

  if (!left.type()->Equals(right.type())) {
    *os << "# Array types differed: " << *left.type() << " vs " << *right.type()
        << std::endl;
    return Status::OK();
  }

  if (left.type()->id() == Type::DICTIONARY) {
    *os << "# Dictionary arrays differed" << std::endl;

    const auto& left_dict = checked_cast<const DictionaryArray&>(left);
    const auto& right_dict = checked_cast<const DictionaryArray&>(right);

    *os << "## dictionary diff";
    auto pos = os->tellp();
    RETURN_NOT_OK(PrintDiff(*left_dict.dictionary(), *right_dict.dictionary(), os));
    if (os->tellp() == pos) {
      *os << std::endl;
    }

    *os << "## indices diff";
    pos = os->tellp();
    RETURN_NOT_OK(PrintDiff(*left_dict.indices(), *right_dict.indices(), os));
    if (os->tellp() == pos) {
      *os << std::endl;
    }
    return Status::OK();
  }

  const auto left_slice = left.Slice(left_offset, left_length);
  const auto right_slice = right.Slice(right_offset, right_length);
  ARROW_ASSIGN_OR_RAISE(auto edits,
                        Diff(*left_slice, *right_slice, default_memory_pool()));
  ARROW_ASSIGN_OR_RAISE(auto formatter, MakeUnifiedDiffFormatter(*left.type(), os));
  return formatter(*edits, *left_slice, *right_slice);
}

Status PrintDiff(const Array& left, const Array& right, std::ostream* os) {
  return PrintDiff(left, right, 0, left.length(), 0, right.length(), os);
}

bool ArrayRangeEquals(const Array& left, const Array& right, int64_t left_start_idx,
                      int64_t left_end_idx, int64_t right_start_idx,
                      const EqualOptions& options, bool floating_approximate) {
  bool are_equal =
      CompareArrayRanges(*left.data(), *right.data(), left_start_idx, left_end_idx,
                         right_start_idx, options, floating_approximate);
  if (!are_equal) {
    ARROW_IGNORE_EXPR(PrintDiff(
        left, right, left_start_idx, left_end_idx, right_start_idx,
        right_start_idx + (left_end_idx - left_start_idx), options.diff_sink()));
  }
  return are_equal;
}

bool ArrayEquals(const Array& left, const Array& right, const EqualOptions& opts,
                 bool floating_approximate) {
  if (left.length() != right.length()) {
    ARROW_IGNORE_EXPR(PrintDiff(left, right, opts.diff_sink()));
    return false;
  }
  return ArrayRangeEquals(left, right, 0, left.length(), 0, opts, floating_approximate);
}

bool ScalarEquals(const Scalar& left, const Scalar& right, const EqualOptions& options,
                  bool floating_approximate) {
  if (&left == &right && IdentityImpliesEquality(*left.type, options)) {
    return true;
  }
  if (!left.type->Equals(right.type)) {
    return false;
  }
  if (left.is_valid != right.is_valid) {
    return false;
  }
  if (!left.is_valid) {
    return true;
  }
  ScalarEqualsVisitor visitor(right, options, floating_approximate);
  auto error = VisitScalarInline(left, &visitor);
  DCHECK_OK(error);
  return visitor.result();
}

}  // namespace

bool ArrayRangeEquals(const Array& left, const Array& right, int64_t left_start_idx,
                      int64_t left_end_idx, int64_t right_start_idx,
                      const EqualOptions& options) {
  const bool floating_approximate = false;
  return ArrayRangeEquals(left, right, left_start_idx, left_end_idx, right_start_idx,
                          options, floating_approximate);
}

bool ArrayRangeApproxEquals(const Array& left, const Array& right, int64_t left_start_idx,
                            int64_t left_end_idx, int64_t right_start_idx,
                            const EqualOptions& options) {
  const bool floating_approximate = true;
  return ArrayRangeEquals(left, right, left_start_idx, left_end_idx, right_start_idx,
                          options, floating_approximate);
}

bool ArrayEquals(const Array& left, const Array& right, const EqualOptions& opts) {
  const bool floating_approximate = false;
  return ArrayEquals(left, right, opts, floating_approximate);
}

bool ArrayApproxEquals(const Array& left, const Array& right, const EqualOptions& opts) {
  const bool floating_approximate = true;
  return ArrayEquals(left, right, opts, floating_approximate);
}

bool ScalarEquals(const Scalar& left, const Scalar& right, const EqualOptions& options) {
  const bool floating_approximate = false;
  return ScalarEquals(left, right, options, floating_approximate);
}

bool ScalarApproxEquals(const Scalar& left, const Scalar& right,
                        const EqualOptions& options) {
  const bool floating_approximate = true;
  return ScalarEquals(left, right, options, floating_approximate);
}

namespace {

bool StridedIntegerTensorContentEquals(const int dim_index, int64_t left_offset,
                                       int64_t right_offset, int elem_size,
                                       const Tensor& left, const Tensor& right) {
  const auto n = left.shape()[dim_index];
  const auto left_stride = left.strides()[dim_index];
  const auto right_stride = right.strides()[dim_index];
  if (dim_index == left.ndim() - 1) {
    for (int64_t i = 0; i < n; ++i) {
      if (memcmp(left.raw_data() + left_offset + i * left_stride,
                 right.raw_data() + right_offset + i * right_stride, elem_size) != 0) {
        return false;
      }
    }
    return true;
  }
  for (int64_t i = 0; i < n; ++i) {
    if (!StridedIntegerTensorContentEquals(dim_index + 1, left_offset, right_offset,
                                           elem_size, left, right)) {
      return false;
    }
    left_offset += left_stride;
    right_offset += right_stride;
  }
  return true;
}

bool IntegerTensorEquals(const Tensor& left, const Tensor& right) {
  bool are_equal;
  // The arrays are the same object
  if (&left == &right) {
    are_equal = true;
  } else {
    const bool left_row_major_p = left.is_row_major();
    const bool left_column_major_p = left.is_column_major();
    const bool right_row_major_p = right.is_row_major();
    const bool right_column_major_p = right.is_column_major();

    if (!(left_row_major_p && right_row_major_p) &&
        !(left_column_major_p && right_column_major_p)) {
      const auto& type = checked_cast<const FixedWidthType&>(*left.type());
      are_equal =
          StridedIntegerTensorContentEquals(0, 0, 0, type.byte_width(), left, right);
    } else {
      const int byte_width = left.type()->byte_width();
      DCHECK_GT(byte_width, 0);

      const uint8_t* left_data = left.data()->data();
      const uint8_t* right_data = right.data()->data();

      are_equal = memcmp(left_data, right_data,
                         static_cast<size_t>(byte_width * left.size())) == 0;
    }
  }
  return are_equal;
}

template <typename DataType>
bool StridedFloatTensorContentEquals(const int dim_index, int64_t left_offset,
                                     int64_t right_offset, const Tensor& left,
                                     const Tensor& right, const EqualOptions& opts) {
  using c_type = typename DataType::c_type;
  static_assert(std::is_floating_point<c_type>::value,
                "DataType must be a floating point type");

  const auto n = left.shape()[dim_index];
  const auto left_stride = left.strides()[dim_index];
  const auto right_stride = right.strides()[dim_index];
  if (dim_index == left.ndim() - 1) {
    // Leaf dimension, compare values
    auto left_data = left.raw_data();
    auto right_data = right.raw_data();
    bool result = true;

    auto visitor = [&](auto&& compare_func) {
      for (int64_t i = 0; i < n; ++i) {
        c_type left_value =
            *reinterpret_cast<const c_type*>(left_data + left_offset + i * left_stride);
        c_type right_value = *reinterpret_cast<const c_type*>(right_data + right_offset +
                                                              i * right_stride);
        if (!compare_func(left_value, right_value)) {
          result = false;
          return;
        }
      }
    };

    VisitFloatingEquality<c_type>(opts, /*floating_approximate=*/false,
                                  std::move(visitor));
    return result;
  }

  // Outer dimension, recurse into inner
  for (int64_t i = 0; i < n; ++i) {
    if (!StridedFloatTensorContentEquals<DataType>(dim_index + 1, left_offset,
                                                   right_offset, left, right, opts)) {
      return false;
    }
    left_offset += left_stride;
    right_offset += right_stride;
  }
  return true;
}

template <typename DataType>
bool FloatTensorEquals(const Tensor& left, const Tensor& right,
                       const EqualOptions& opts) {
  return StridedFloatTensorContentEquals<DataType>(0, 0, 0, left, right, opts);
}

}  // namespace

bool TensorEquals(const Tensor& left, const Tensor& right, const EqualOptions& opts) {
  if (left.type_id() != right.type_id()) {
    return false;
  } else if (left.size() == 0 && right.size() == 0) {
    return true;
  } else if (left.shape() != right.shape()) {
    return false;
  }

  switch (left.type_id()) {
    // TODO: Support half-float tensors
    // case Type::HALF_FLOAT:
    case Type::FLOAT:
      return FloatTensorEquals<FloatType>(left, right, opts);

    case Type::DOUBLE:
      return FloatTensorEquals<DoubleType>(left, right, opts);

    default:
      return IntegerTensorEquals(left, right);
  }
}

namespace {

template <typename LeftSparseIndexType, typename RightSparseIndexType>
struct SparseTensorEqualsImpl {
  static bool Compare(const SparseTensorImpl<LeftSparseIndexType>& left,
                      const SparseTensorImpl<RightSparseIndexType>& right,
                      const EqualOptions&) {
    // TODO(mrkn): should we support the equality among different formats?
    return false;
  }
};

bool IntegerSparseTensorDataEquals(const uint8_t* left_data, const uint8_t* right_data,
                                   const int byte_width, const int64_t length) {
  if (left_data == right_data) {
    return true;
  }
  return memcmp(left_data, right_data, static_cast<size_t>(byte_width * length)) == 0;
}

template <typename DataType>
bool FloatSparseTensorDataEquals(const typename DataType::c_type* left_data,
                                 const typename DataType::c_type* right_data,
                                 const int64_t length, const EqualOptions& opts) {
  using c_type = typename DataType::c_type;
  static_assert(std::is_floating_point<c_type>::value,
                "DataType must be a floating point type");
  if (opts.nans_equal()) {
    if (left_data == right_data) {
      return true;
    }

    for (int64_t i = 0; i < length; ++i) {
      const auto left = left_data[i];
      const auto right = right_data[i];
      if (left != right && !(std::isnan(left) && std::isnan(right))) {
        return false;
      }
    }
  } else {
    for (int64_t i = 0; i < length; ++i) {
      if (left_data[i] != right_data[i]) {
        return false;
      }
    }
  }
  return true;
}

template <typename SparseIndexType>
struct SparseTensorEqualsImpl<SparseIndexType, SparseIndexType> {
  static bool Compare(const SparseTensorImpl<SparseIndexType>& left,
                      const SparseTensorImpl<SparseIndexType>& right,
                      const EqualOptions& opts) {
    DCHECK(left.type()->id() == right.type()->id());
    DCHECK(left.shape() == right.shape());

    const auto length = left.non_zero_length();
    DCHECK(length == right.non_zero_length());

    const auto& left_index = checked_cast<const SparseIndexType&>(*left.sparse_index());
    const auto& right_index = checked_cast<const SparseIndexType&>(*right.sparse_index());

    if (!left_index.Equals(right_index)) {
      return false;
    }

    const int byte_width = left.type()->byte_width();
    DCHECK_GT(byte_width, 0);

    const uint8_t* left_data = left.data()->data();
    const uint8_t* right_data = right.data()->data();
    switch (left.type()->id()) {
      // TODO: Support half-float tensors
      // case Type::HALF_FLOAT:
      case Type::FLOAT:
        return FloatSparseTensorDataEquals<FloatType>(
            reinterpret_cast<const float*>(left_data),
            reinterpret_cast<const float*>(right_data), length, opts);

      case Type::DOUBLE:
        return FloatSparseTensorDataEquals<DoubleType>(
            reinterpret_cast<const double*>(left_data),
            reinterpret_cast<const double*>(right_data), length, opts);

      default:  // Integer cases
        return IntegerSparseTensorDataEquals(left_data, right_data, byte_width, length);
    }
  }
};

template <typename SparseIndexType>
inline bool SparseTensorEqualsImplDispatch(const SparseTensorImpl<SparseIndexType>& left,
                                           const SparseTensor& right,
                                           const EqualOptions& opts) {
  switch (right.format_id()) {
    case SparseTensorFormat::COO: {
      const auto& right_coo =
          checked_cast<const SparseTensorImpl<SparseCOOIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCOOIndex>::Compare(
          left, right_coo, opts);
    }

    case SparseTensorFormat::CSR: {
      const auto& right_csr =
          checked_cast<const SparseTensorImpl<SparseCSRIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCSRIndex>::Compare(
          left, right_csr, opts);
    }

    case SparseTensorFormat::CSC: {
      const auto& right_csc =
          checked_cast<const SparseTensorImpl<SparseCSCIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCSCIndex>::Compare(
          left, right_csc, opts);
    }

    case SparseTensorFormat::CSF: {
      const auto& right_csf =
          checked_cast<const SparseTensorImpl<SparseCSFIndex>&>(right);
      return SparseTensorEqualsImpl<SparseIndexType, SparseCSFIndex>::Compare(
          left, right_csf, opts);
    }

    default:
      return false;
  }
}

}  // namespace

bool SparseTensorEquals(const SparseTensor& left, const SparseTensor& right,
                        const EqualOptions& opts) {
  if (left.type()->id() != right.type()->id()) {
    return false;
  } else if (left.size() == 0 && right.size() == 0) {
    return true;
  } else if (left.shape() != right.shape()) {
    return false;
  } else if (left.non_zero_length() != right.non_zero_length()) {
    return false;
  }

  switch (left.format_id()) {
    case SparseTensorFormat::COO: {
      const auto& left_coo = checked_cast<const SparseTensorImpl<SparseCOOIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_coo, right, opts);
    }

    case SparseTensorFormat::CSR: {
      const auto& left_csr = checked_cast<const SparseTensorImpl<SparseCSRIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_csr, right, opts);
    }

    case SparseTensorFormat::CSC: {
      const auto& left_csc = checked_cast<const SparseTensorImpl<SparseCSCIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_csc, right, opts);
    }

    case SparseTensorFormat::CSF: {
      const auto& left_csf = checked_cast<const SparseTensorImpl<SparseCSFIndex>&>(left);
      return SparseTensorEqualsImplDispatch(left_csf, right, opts);
    }

    default:
      return false;
  }
}

bool TypeEquals(const DataType& left, const DataType& right, bool check_metadata) {
  // The arrays are the same object
  if (&left == &right) {
    return true;
  } else if (left.id() != right.id()) {
    return false;
  } else {
    // First try to compute fingerprints
    if (check_metadata) {
      const auto& left_metadata_fp = left.metadata_fingerprint();
      const auto& right_metadata_fp = right.metadata_fingerprint();
      if (left_metadata_fp != right_metadata_fp) {
        return false;
      }
    }

    const auto& left_fp = left.fingerprint();
    const auto& right_fp = right.fingerprint();
    if (!left_fp.empty() && !right_fp.empty()) {
      return left_fp == right_fp;
    }

    // TODO remove check_metadata here?
    TypeEqualsVisitor visitor(right, check_metadata);
    auto error = VisitTypeInline(left, &visitor);
    if (!error.ok()) {
      DCHECK(false) << "Types are not comparable: " << error.ToString();
    }
    return visitor.result();
  }
}

}  // namespace arrow

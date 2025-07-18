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

#include "arrow/compare_internal.h"

#include "arrow/array/array_dict.h"
#include "arrow/array/diff.h"
#include "arrow/compare.h"
#include "arrow/util/binary_view_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/logging_internal.h"
#include "arrow/visit_scalar_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::BitmapEquals;
using internal::BitmapReader;
using internal::BitmapUInt64Reader;
using internal::OptionalBitmapEquals;

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

bool RangeDataEqualsImpl::Compare() {
  // Compare null bitmaps
  if (left_start_idx_ == 0 && right_start_idx_ == 0 && range_length_ == left_.length &&
      range_length_ == right_.length) {
    // If we're comparing entire arrays, we can first compare the cached null counts
    if (left_.GetNullCount() != right_.GetNullCount()) {
      return false;
    }
  }
  if (!OptionalBitmapEquals(left_.buffers[0].data, left_.offset + left_start_idx_,
                            right_.buffers[0].data, right_.offset + right_start_idx_,
                            range_length_)) {
    return false;
  }
  // Compare values
  return CompareWithType(*left_.type);
}

bool RangeDataEqualsImpl::CompareWithType(const DataType& type) {
  result_ = true;
  if (range_length_ != 0) {
    ARROW_CHECK_OK(VisitTypeInline(type, this));
  }
  return result_;
}

Status RangeDataEqualsImpl::Visit(const NullType&) { return Status::OK(); }

Status RangeDataEqualsImpl::Visit(const BooleanType&) {
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

Status RangeDataEqualsImpl::Visit(const FloatType& type) { return CompareFloating(type); }

Status RangeDataEqualsImpl::Visit(const DoubleType& type) {
  return CompareFloating(type);
}

Status RangeDataEqualsImpl::Visit(const HalfFloatType& type) {
  return CompareFloating(type);
}

Status RangeDataEqualsImpl::Visit(const BinaryType& type) { return CompareBinary(type); }

Status RangeDataEqualsImpl::Visit(const BinaryViewType& type) {
  auto* left_values = left_.GetValues<BinaryViewType::c_type>(1) + left_start_idx_;
  auto* right_values = right_.GetValues<BinaryViewType::c_type>(1) + right_start_idx_;

  const auto left_buffers = left_.GetVariadicBuffers().data();
  const auto right_buffers = right_.GetVariadicBuffers().data();
  VisitValidRuns([&](int64_t i, int64_t length) {
    for (auto end_i = i + length; i < end_i; ++i) {
      if (!util::EqualBinaryView(left_values[i], right_values[i], left_buffers,
                                 right_buffers)) {
        return false;
      }
    }
    return true;
  });
  return Status::OK();
}

Status RangeDataEqualsImpl::Visit(const LargeBinaryType& type) {
  return CompareBinary(type);
}

Status RangeDataEqualsImpl::Visit(const FixedSizeBinaryType& type) {
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

Status RangeDataEqualsImpl::Visit(const ListType& type) { return CompareList(type); }

Status RangeDataEqualsImpl::Visit(const LargeListType& type) { return CompareList(type); }

Status RangeDataEqualsImpl::Visit(const ListViewType& type) {
  return CompareListView(type);
}

Status RangeDataEqualsImpl::Visit(const LargeListViewType& type) {
  return CompareListView(type);
}

Status RangeDataEqualsImpl::Visit(const FixedSizeListType& type) {
  const auto list_size = type.list_size();
  const auto left_data = left_.child_data[0];
  const auto right_data = right_.child_data[0];

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

Status RangeDataEqualsImpl::Visit(const StructType& type) {
  const int32_t num_fields = type.num_fields();

  auto compare_runs = [&](int64_t i, int64_t length) -> bool {
    for (int32_t f = 0; f < num_fields; ++f) {
      RangeDataEqualsImpl impl(options_, floating_approximate_, left_.child_data[f],
                               right_.child_data[f], left_start_idx_ + left_.offset + i,
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

Status RangeDataEqualsImpl::Visit(const SparseUnionType& type) {
  const auto& child_ids = type.child_ids();
  const int8_t* left_codes = left_.GetValues<int8_t>(1);
  const int8_t* right_codes = right_.GetValues<int8_t>(1);

  // Unions don't have a null bitmap
  int64_t run_start = 0;  // Start index of the current run

  for (int64_t i = 0; i < range_length_; ++i) {
    const auto current_type_id = left_codes[left_start_idx_ + i];

    if (current_type_id != right_codes[right_start_idx_ + i]) {
      result_ = false;
      break;
    }
    // Check if the current element breaks the run
    if (i > 0 && current_type_id != left_codes[left_start_idx_ + i - 1]) {
      // Compare the previous run
      const auto previous_child_num = child_ids[left_codes[left_start_idx_ + i - 1]];
      int64_t run_length = i - run_start;

      RangeDataEqualsImpl impl(options_, floating_approximate_,
                               left_.child_data[previous_child_num],
                               right_.child_data[previous_child_num],
                               left_start_idx_ + left_.offset + run_start,
                               right_start_idx_ + right_.offset + run_start, run_length);

      if (!impl.Compare()) {
        result_ = false;
        break;
      }

      // Start a new run
      run_start = i;
    }
  }

  // Handle the final run
  if (result_) {
    const auto final_child_num = child_ids[left_codes[left_start_idx_ + run_start]];
    int64_t final_run_length = range_length_ - run_start;

    RangeDataEqualsImpl impl(
        options_, floating_approximate_, left_.child_data[final_child_num],
        right_.child_data[final_child_num], left_start_idx_ + left_.offset + run_start,
        right_start_idx_ + right_.offset + run_start, final_run_length);

    if (!impl.Compare()) {
      result_ = false;
    }
  }
  return Status::OK();
}

Status RangeDataEqualsImpl::Visit(const DenseUnionType& type) {
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
    RangeDataEqualsImpl impl(options_, floating_approximate_, left_.child_data[child_num],
                             right_.child_data[child_num],
                             left_offsets[left_start_idx_ + i],
                             right_offsets[right_start_idx_ + i], 1);
    if (!impl.Compare()) {
      result_ = false;
      break;
    }
  }
  return Status::OK();
}

Status RangeDataEqualsImpl::Visit(const DictionaryType& type) {
  // Compare dictionaries
  result_ &= CompareArrayRanges(
      *left_.dictionary().ToArrayData(), *right_.dictionary().ToArrayData(),
      /*left_start_idx=*/0,
      /*left_end_idx=*/std::max(left_.dictionary().length, right_.dictionary().length),
      /*right_start_idx=*/0, options_, floating_approximate_);
  if (result_) {
    // Compare indices
    result_ &= CompareWithType(*type.index_type());
  }
  return Status::OK();
}

Status RangeDataEqualsImpl::Visit(const RunEndEncodedType& type) {
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

Status RangeDataEqualsImpl::Visit(const ExtensionType& type) {
  // Compare storages
  result_ &= CompareWithType(*type.storage_type());
  return Status::OK();
}

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
  ArraySpan left_span{left};
  ArraySpan right_span{right};
  RangeDataEqualsImpl impl(options, floating_approximate, left_span, right_span,
                           left_start_idx, right_start_idx, range_length);
  return impl.Compare();
}

bool ArrayEquals(const Array& left, const Array& right, const EqualOptions& opts,
                 bool floating_approximate) {
  if (left.length() != right.length()) {
    ARROW_IGNORE_EXPR(PrintDiff(left, right, opts.diff_sink()));
    return false;
  }
  return ArrayRangeEquals(left, right, 0, left.length(), 0, opts, floating_approximate);
}

Status ScalarEqualsVisitor::Visit(const NullScalar& left) {
  result_ = true;
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const BooleanScalar& left) {
  const auto& right = checked_cast<const BooleanScalar&>(right_);
  result_ = left.value == right.value;
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const FloatScalar& left) {
  return CompareFloating(left);
}

Status ScalarEqualsVisitor::Visit(const DoubleScalar& left) {
  return CompareFloating(left);
}

Status ScalarEqualsVisitor::Visit(const HalfFloatScalar& left) {
  return CompareFloating(left);
}

Status ScalarEqualsVisitor::Visit(const Decimal32Scalar& left) {
  const auto& right = checked_cast<const Decimal32Scalar&>(right_);
  result_ = left.value == right.value;
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const Decimal64Scalar& left) {
  const auto& right = checked_cast<const Decimal64Scalar&>(right_);
  result_ = left.value == right.value;
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const Decimal128Scalar& left) {
  const auto& right = checked_cast<const Decimal128Scalar&>(right_);
  result_ = left.value == right.value;
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const Decimal256Scalar& left) {
  const auto& right = checked_cast<const Decimal256Scalar&>(right_);
  result_ = left.value == right.value;
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const ListScalar& left) {
  const auto& right = checked_cast<const ListScalar&>(right_);
  result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const LargeListScalar& left) {
  const auto& right = checked_cast<const LargeListScalar&>(right_);
  result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const ListViewScalar& left) {
  const auto& right = checked_cast<const ListViewScalar&>(right_);
  result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const LargeListViewScalar& left) {
  const auto& right = checked_cast<const LargeListViewScalar&>(right_);
  result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const MapScalar& left) {
  const auto& right = checked_cast<const MapScalar&>(right_);
  result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const FixedSizeListScalar& left) {
  const auto& right = checked_cast<const FixedSizeListScalar&>(right_);
  result_ = ArrayEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const StructScalar& left) {
  const auto& right = checked_cast<const StructScalar&>(right_);

  if (right.value.size() != left.value.size()) {
    result_ = false;
  } else {
    bool all_equals = true;
    for (size_t i = 0; i < left.value.size() && all_equals; i++) {
      all_equals &=
          ScalarEquals(*left.value[i], *right.value[i], options_, floating_approximate_);
    }
    result_ = all_equals;
  }

  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const DenseUnionScalar& left) {
  const auto& right = checked_cast<const DenseUnionScalar&>(right_);
  result_ = ScalarEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const SparseUnionScalar& left) {
  const auto& right = checked_cast<const SparseUnionScalar&>(right_);
  result_ = ScalarEquals(*left.value[left.child_id], *right.value[right.child_id],
                         options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const DictionaryScalar& left) {
  const auto& right = checked_cast<const DictionaryScalar&>(right_);
  result_ = ScalarEquals(*left.value.index, *right.value.index, options_,
                         floating_approximate_) &&
            ArrayEquals(*left.value.dictionary, *right.value.dictionary, options_,
                        floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const RunEndEncodedScalar& left) {
  const auto& right = checked_cast<const RunEndEncodedScalar&>(right_);
  result_ = ScalarEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

Status ScalarEqualsVisitor::Visit(const ExtensionScalar& left) {
  const auto& right = checked_cast<const ExtensionScalar&>(right_);
  result_ = ScalarEquals(*left.value, *right.value, options_, floating_approximate_);
  return Status::OK();
}

bool ScalarEqualsVisitor::result() const { return result_; }

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
}  // namespace arrow

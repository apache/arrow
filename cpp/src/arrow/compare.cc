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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// Public method implementations

class RangeEqualsVisitor : public ArrayVisitor {
 public:
  RangeEqualsVisitor(const Array& right, int32_t left_start_idx, int32_t left_end_idx,
      int32_t right_start_idx)
      : right_(right),
        left_start_idx_(left_start_idx),
        left_end_idx_(left_end_idx),
        right_start_idx_(right_start_idx),
        result_(false) {}

  Status Visit(const NullArray& left) override {
    UNUSED(left);
    result_ = true;
    return Status::OK();
  }

  template <typename ArrayType>
  inline Status CompareValues(const ArrayType& left) {
    const auto& right = static_cast<const ArrayType&>(right_);

    for (int32_t i = left_start_idx_, o_i = right_start_idx_; i < left_end_idx_;
         ++i, ++o_i) {
      const bool is_null = left.IsNull(i);
      if (is_null != right.IsNull(o_i) ||
          (!is_null && left.Value(i) != right.Value(o_i))) {
        result_ = false;
        return Status::OK();
      }
    }
    result_ = true;
    return Status::OK();
  }

  bool CompareBinaryRange(const BinaryArray& left) const {
    const auto& right = static_cast<const BinaryArray&>(right_);

    for (int32_t i = left_start_idx_, o_i = right_start_idx_; i < left_end_idx_;
         ++i, ++o_i) {
      const bool is_null = left.IsNull(i);
      if (is_null != right.IsNull(o_i)) { return false; }
      if (is_null) continue;
      const int32_t begin_offset = left.value_offset(i);
      const int32_t end_offset = left.value_offset(i + 1);
      const int32_t right_begin_offset = right.value_offset(o_i);
      const int32_t right_end_offset = right.value_offset(o_i + 1);
      // Underlying can't be equal if the size isn't equal
      if (end_offset - begin_offset != right_end_offset - right_begin_offset) {
        return false;
      }

      if (end_offset - begin_offset > 0 &&
          std::memcmp(left.data()->data() + begin_offset,
              right.data()->data() + right_begin_offset, end_offset - begin_offset)) {
        return false;
      }
    }
    return true;
  }

  Status Visit(const BooleanArray& left) override {
    return CompareValues<BooleanArray>(left);
  }

  Status Visit(const Int8Array& left) override { return CompareValues<Int8Array>(left); }

  Status Visit(const Int16Array& left) override {
    return CompareValues<Int16Array>(left);
  }
  Status Visit(const Int32Array& left) override {
    return CompareValues<Int32Array>(left);
  }
  Status Visit(const Int64Array& left) override {
    return CompareValues<Int64Array>(left);
  }
  Status Visit(const UInt8Array& left) override {
    return CompareValues<UInt8Array>(left);
  }
  Status Visit(const UInt16Array& left) override {
    return CompareValues<UInt16Array>(left);
  }
  Status Visit(const UInt32Array& left) override {
    return CompareValues<UInt32Array>(left);
  }
  Status Visit(const UInt64Array& left) override {
    return CompareValues<UInt64Array>(left);
  }
  Status Visit(const FloatArray& left) override {
    return CompareValues<FloatArray>(left);
  }
  Status Visit(const DoubleArray& left) override {
    return CompareValues<DoubleArray>(left);
  }

  Status Visit(const HalfFloatArray& left) override {
    return Status::NotImplemented("Half float type");
  }

  Status Visit(const StringArray& left) override {
    result_ = CompareBinaryRange(left);
    return Status::OK();
  }

  Status Visit(const BinaryArray& left) override {
    result_ = CompareBinaryRange(left);
    return Status::OK();
  }

  Status Visit(const DateArray& left) override { return CompareValues<DateArray>(left); }

  Status Visit(const TimeArray& left) override { return CompareValues<TimeArray>(left); }

  Status Visit(const TimestampArray& left) override {
    return CompareValues<TimestampArray>(left);
  }

  Status Visit(const IntervalArray& left) override {
    return CompareValues<IntervalArray>(left);
  }

  Status Visit(const DecimalArray& left) override {
    return Status::NotImplemented("Decimal type");
  }

  bool CompareLists(const ListArray& left) {
    const auto& right = static_cast<const ListArray&>(right_);

    const std::shared_ptr<Array>& left_values = left.values();
    const std::shared_ptr<Array>& right_values = right.values();

    for (int32_t i = left_start_idx_, o_i = right_start_idx_; i < left_end_idx_;
         ++i, ++o_i) {
      const bool is_null = left.IsNull(i);
      if (is_null != right.IsNull(o_i)) { return false; }
      if (is_null) continue;
      const int32_t begin_offset = left.value_offset(i);
      const int32_t end_offset = left.value_offset(i + 1);
      const int32_t right_begin_offset = right.value_offset(o_i);
      const int32_t right_end_offset = right.value_offset(o_i + 1);
      // Underlying can't be equal if the size isn't equal
      if (end_offset - begin_offset != right_end_offset - right_begin_offset) {
        return false;
      }
      if (!left_values->RangeEquals(
              begin_offset, end_offset, right_begin_offset, right_values)) {
        return false;
      }
    }
    return true;
  }

  Status Visit(const ListArray& left) override {
    result_ = CompareLists(left);
    return Status::OK();
  }

  bool CompareStructs(const StructArray& left) {
    const auto& right = static_cast<const StructArray&>(right_);
    bool equal_fields = true;
    for (int32_t i = left_start_idx_, o_i = right_start_idx_; i < left_end_idx_;
         ++i, ++o_i) {
      if (left.IsNull(i) != right.IsNull(o_i)) { return false; }
      if (left.IsNull(i)) continue;
      for (size_t j = 0; j < left.fields().size(); ++j) {
        // TODO: really we should be comparing stretches of non-null data rather
        // than looking at one value at a time.
        const int left_abs_index = i + left.offset();
        const int right_abs_index = o_i + right.offset();

        equal_fields = left.field(j)->RangeEquals(
            left_abs_index, left_abs_index + 1, right_abs_index, right.field(j));
        if (!equal_fields) { return false; }
      }
    }
    return true;
  }

  Status Visit(const StructArray& left) override {
    result_ = CompareStructs(left);
    return Status::OK();
  }

  bool CompareUnions(const UnionArray& left) const {
    const auto& right = static_cast<const UnionArray&>(right_);

    const UnionMode union_mode = left.mode();
    if (union_mode != right.mode()) { return false; }

    const auto& left_type = static_cast<const UnionType&>(*left.type());

    // Define a mapping from the type id to child number
    uint8_t max_code = 0;

    const std::vector<uint8_t> type_codes = left_type.type_codes;
    for (size_t i = 0; i < type_codes.size(); ++i) {
      const uint8_t code = type_codes[i];
      if (code > max_code) { max_code = code; }
    }

    // Store mapping in a vector for constant time lookups
    std::vector<uint8_t> type_id_to_child_num(max_code + 1);
    for (uint8_t i = 0; i < static_cast<uint8_t>(type_codes.size()); ++i) {
      type_id_to_child_num[type_codes[i]] = i;
    }

    const uint8_t* left_ids = left.raw_type_ids();
    const uint8_t* right_ids = right.raw_type_ids();

    uint8_t id, child_num;
    for (int32_t i = left_start_idx_, o_i = right_start_idx_; i < left_end_idx_;
         ++i, ++o_i) {
      if (left.IsNull(i) != right.IsNull(o_i)) { return false; }
      if (left.IsNull(i)) continue;
      if (left_ids[i] != right_ids[o_i]) { return false; }

      id = left_ids[i];
      child_num = type_id_to_child_num[id];

      const int left_abs_index = i + left.offset();
      const int right_abs_index = o_i + right.offset();

      // TODO(wesm): really we should be comparing stretches of non-null data
      // rather than looking at one value at a time.
      if (union_mode == UnionMode::SPARSE) {
        if (!left.child(child_num)->RangeEquals(left_abs_index, left_abs_index + 1,
                right_abs_index, right.child(child_num))) {
          return false;
        }
      } else {
        const int32_t offset = left.raw_value_offsets()[i];
        const int32_t o_offset = right.raw_value_offsets()[i];
        if (!left.child(child_num)->RangeEquals(
                offset, offset + 1, o_offset, right.child(child_num))) {
          return false;
        }
      }
    }
    return true;
  }

  Status Visit(const UnionArray& left) override {
    result_ = CompareUnions(left);
    return Status::OK();
  }

  Status Visit(const DictionaryArray& left) override {
    const auto& right = static_cast<const DictionaryArray&>(right_);
    if (!left.dictionary()->Equals(right.dictionary())) {
      result_ = false;
      return Status::OK();
    }
    result_ = left.indices()->RangeEquals(
        left_start_idx_, left_end_idx_, right_start_idx_, right.indices());
    return Status::OK();
  }

  bool result() const { return result_; }

 protected:
  const Array& right_;
  int32_t left_start_idx_;
  int32_t left_end_idx_;
  int32_t right_start_idx_;

  bool result_;
};

class EqualsVisitor : public RangeEqualsVisitor {
 public:
  explicit EqualsVisitor(const Array& right)
      : RangeEqualsVisitor(right, 0, right.length(), 0) {}

  Status Visit(const NullArray& left) override { return Status::OK(); }

  Status Visit(const BooleanArray& left) override {
    const auto& right = static_cast<const BooleanArray&>(right_);
    if (left.null_count() > 0) {
      const uint8_t* left_data = left.data()->data();
      const uint8_t* right_data = right.data()->data();

      for (int i = 0; i < left.length(); ++i) {
        if (!left.IsNull(i) &&
            BitUtil::GetBit(left_data, i) != BitUtil::GetBit(right_data, i)) {
          result_ = false;
          return Status::OK();
        }
      }
      result_ = true;
    } else {
      result_ = BitmapEquals(left.data()->data(), left.offset(), right.data()->data(),
          right.offset(), left.length());
    }
    return Status::OK();
  }

  bool IsEqualPrimitive(const PrimitiveArray& left) {
    const auto& right = static_cast<const PrimitiveArray&>(right_);
    const auto& size_meta = dynamic_cast<const FixedWidthType&>(*left.type());
    const int value_byte_size = size_meta.bit_width() / 8;
    DCHECK_GT(value_byte_size, 0);

    const uint8_t* left_data = nullptr;
    if (left.length() > 0) {
      left_data = left.data()->data() + left.offset() * value_byte_size;
    }

    const uint8_t* right_data = nullptr;
    if (right.length() > 0) {
      right_data = right.data()->data() + right.offset() * value_byte_size;
    }

    if (left.null_count() > 0) {
      for (int i = 0; i < left.length(); ++i) {
        if (!left.IsNull(i) && memcmp(left_data, right_data, value_byte_size)) {
          return false;
        }
        left_data += value_byte_size;
        right_data += value_byte_size;
      }
      return true;
    } else {
      if (left.length() == 0) { return true; }
      return memcmp(left_data, right_data, value_byte_size * left.length()) == 0;
    }
  }

  Status ComparePrimitive(const PrimitiveArray& left) {
    result_ = IsEqualPrimitive(left);
    return Status::OK();
  }

  Status Visit(const Int8Array& left) override { return ComparePrimitive(left); }

  Status Visit(const Int16Array& left) override { return ComparePrimitive(left); }

  Status Visit(const Int32Array& left) override { return ComparePrimitive(left); }

  Status Visit(const Int64Array& left) override { return ComparePrimitive(left); }

  Status Visit(const UInt8Array& left) override { return ComparePrimitive(left); }

  Status Visit(const UInt16Array& left) override { return ComparePrimitive(left); }

  Status Visit(const UInt32Array& left) override { return ComparePrimitive(left); }

  Status Visit(const UInt64Array& left) override { return ComparePrimitive(left); }

  Status Visit(const FloatArray& left) override { return ComparePrimitive(left); }

  Status Visit(const DoubleArray& left) override { return ComparePrimitive(left); }

  Status Visit(const DateArray& left) override { return ComparePrimitive(left); }

  Status Visit(const TimeArray& left) override { return ComparePrimitive(left); }

  Status Visit(const TimestampArray& left) override { return ComparePrimitive(left); }

  Status Visit(const IntervalArray& left) override { return ComparePrimitive(left); }

  template <typename ArrayType>
  bool ValueOffsetsEqual(const ArrayType& left) {
    const auto& right = static_cast<const ArrayType&>(right_);

    if (left.offset() == 0 && right.offset() == 0) {
      return left.value_offsets()->Equals(
          *right.value_offsets(), (left.length() + 1) * sizeof(int32_t));
    } else {
      // One of the arrays is sliced; logic is more complicated because the
      // value offsets are not both 0-based
      auto left_offsets =
          reinterpret_cast<const int32_t*>(left.value_offsets()->data()) + left.offset();
      auto right_offsets =
          reinterpret_cast<const int32_t*>(right.value_offsets()->data()) +
          right.offset();

      for (int32_t i = 0; i < left.length() + 1; ++i) {
        if (left_offsets[i] - left_offsets[0] != right_offsets[i] - right_offsets[0]) {
          return false;
        }
      }
      return true;
    }
  }

  bool CompareBinary(const BinaryArray& left) {
    const auto& right = static_cast<const BinaryArray&>(right_);

    bool equal_offsets = ValueOffsetsEqual<BinaryArray>(left);
    if (!equal_offsets) { return false; }

    if (left.offset() == 0 && right.offset() == 0) {
      if (!left.data() && !(right.data())) { return true; }
      return left.data()->Equals(*right.data(), left.raw_value_offsets()[left.length()]);
    } else {
      // Compare the corresponding data range
      const int64_t total_bytes = left.value_offset(left.length()) - left.value_offset(0);
      return std::memcmp(left.data()->data() + left.value_offset(0),
                 right.data()->data() + right.value_offset(0), total_bytes) == 0;
    }
  }

  Status Visit(const StringArray& left) override {
    result_ = CompareBinary(left);
    return Status::OK();
  }

  Status Visit(const BinaryArray& left) override {
    result_ = CompareBinary(left);
    return Status::OK();
  }

  Status Visit(const ListArray& left) override {
    const auto& right = static_cast<const ListArray&>(right_);
    bool equal_offsets = ValueOffsetsEqual<ListArray>(left);
    if (!equal_offsets) {
      result_ = false;
      return Status::OK();
    }

    if (left.offset() == 0 && right.offset() == 0) {
      result_ = left.values()->Equals(right.values());
    } else {
      // One of the arrays is sliced
      result_ = left.values()->RangeEquals(left.value_offset(0),
          left.value_offset(left.length()), right.value_offset(0), right.values());
    }

    return Status::OK();
  }

  Status Visit(const DictionaryArray& left) override {
    const auto& right = static_cast<const DictionaryArray&>(right_);
    if (!left.dictionary()->Equals(right.dictionary())) {
      result_ = false;
    } else {
      result_ = left.indices()->Equals(right.indices());
    }
    return Status::OK();
  }
};

template <typename TYPE>
inline bool FloatingApproxEquals(
    const NumericArray<TYPE>& left, const NumericArray<TYPE>& right) {
  using T = typename TYPE::c_type;

  const T* left_data = left.raw_data();
  const T* right_data = right.raw_data();

  static constexpr T EPSILON = 1E-5;

  if (left.length() == 0 && right.length() == 0) { return true; }

  if (left.null_count() > 0) {
    for (int32_t i = 0; i < left.length(); ++i) {
      if (left.IsNull(i)) continue;
      if (fabs(left_data[i] - right_data[i]) > EPSILON) { return false; }
    }
  } else {
    for (int32_t i = 0; i < left.length(); ++i) {
      if (fabs(left_data[i] - right_data[i]) > EPSILON) { return false; }
    }
  }
  return true;
}

class ApproxEqualsVisitor : public EqualsVisitor {
 public:
  using EqualsVisitor::EqualsVisitor;

  Status Visit(const FloatArray& left) override {
    result_ =
        FloatingApproxEquals<FloatType>(left, static_cast<const FloatArray&>(right_));
    return Status::OK();
  }

  Status Visit(const DoubleArray& left) override {
    result_ =
        FloatingApproxEquals<DoubleType>(left, static_cast<const DoubleArray&>(right_));
    return Status::OK();
  }
};

static bool BaseDataEquals(const Array& left, const Array& right) {
  if (left.length() != right.length() || left.null_count() != right.null_count() ||
      left.type_enum() != right.type_enum()) {
    return false;
  }
  if (left.null_count() > 0) {
    return BitmapEquals(left.null_bitmap()->data(), left.offset(),
        right.null_bitmap()->data(), right.offset(), left.length());
  }
  return true;
}

Status ArrayEquals(const Array& left, const Array& right, bool* are_equal) {
  // The arrays are the same object
  if (&left == &right) {
    *are_equal = true;
  } else if (!BaseDataEquals(left, right)) {
    *are_equal = false;
  } else {
    EqualsVisitor visitor(right);
    RETURN_NOT_OK(left.Accept(&visitor));
    *are_equal = visitor.result();
  }
  return Status::OK();
}

Status ArrayRangeEquals(const Array& left, const Array& right, int32_t left_start_idx,
    int32_t left_end_idx, int32_t right_start_idx, bool* are_equal) {
  if (&left == &right) {
    *are_equal = true;
  } else if (left.type_enum() != right.type_enum()) {
    *are_equal = false;
  } else {
    RangeEqualsVisitor visitor(right, left_start_idx, left_end_idx, right_start_idx);
    RETURN_NOT_OK(left.Accept(&visitor));
    *are_equal = visitor.result();
  }
  return Status::OK();
}

Status ArrayApproxEquals(const Array& left, const Array& right, bool* are_equal) {
  // The arrays are the same object
  if (&left == &right) {
    *are_equal = true;
  } else if (!BaseDataEquals(left, right)) {
    *are_equal = false;
  } else {
    ApproxEqualsVisitor visitor(right);
    RETURN_NOT_OK(left.Accept(&visitor));
    *are_equal = visitor.result();
  }
  return Status::OK();
}

}  // namespace arrow

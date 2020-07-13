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

#include "arrow/array/validate.h"

#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/buffer.h"
#include "arrow/extension_type.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace internal {

///////////////////////////////////////////////////////////////////////////
// ValidateArray: cheap validation checks

namespace {

struct ValidateArrayVisitor {
  Status Visit(const NullArray& array) {
    ARROW_RETURN_IF(array.null_count() != array.length(),
                    Status::Invalid("null_count is invalid"));
    return Status::OK();
  }

  Status Visit(const PrimitiveArray& array) {
    ARROW_RETURN_IF(array.data()->buffers.size() != 2,
                    Status::Invalid("number of buffers is != 2"));

    if (array.length() > 0) {
      if (array.data()->buffers[1] == nullptr) {
        return Status::Invalid("values buffer is null");
      }
      if (array.values() == nullptr) {
        return Status::Invalid("values is null");
      }
    }
    return Status::OK();
  }

  Status Visit(const Decimal128Array& array) {
    if (array.data()->buffers.size() != 2) {
      return Status::Invalid("number of buffers is != 2");
    }
    if (array.length() > 0 && array.values() == nullptr) {
      return Status::Invalid("values is null");
    }
    return Status::OK();
  }

  Status Visit(const StringArray& array) { return ValidateBinaryArray(array); }

  Status Visit(const BinaryArray& array) { return ValidateBinaryArray(array); }

  Status Visit(const LargeStringArray& array) { return ValidateBinaryArray(array); }

  Status Visit(const LargeBinaryArray& array) { return ValidateBinaryArray(array); }

  Status Visit(const ListArray& array) { return ValidateListArray(array); }

  Status Visit(const LargeListArray& array) { return ValidateListArray(array); }

  Status Visit(const MapArray& array) {
    if (!array.keys()) {
      return Status::Invalid("keys is null");
    }
    return ValidateListArray(array);
  }

  Status Visit(const FixedSizeListArray& array) {
    const int64_t len = array.length();
    const int64_t value_size = array.value_length();
    if (len > 0 && !array.values()) {
      return Status::Invalid("values is null");
    }
    if (value_size < 0) {
      return Status::Invalid("FixedSizeListArray has negative value size ", value_size);
    }
    if (HasPositiveMultiplyOverflow(len, value_size) ||
        array.values()->length() != len * value_size) {
      return Status::Invalid("Values Length (", array.values()->length(),
                             ") is not equal to the length (", len,
                             ") multiplied by the value size (", value_size, ")");
    }

    return Status::OK();
  }

  Status Visit(const StructArray& array) {
    const auto& struct_type = checked_cast<const StructType&>(*array.type());
    // Validate fields
    for (int i = 0; i < array.num_fields(); ++i) {
      // array.field() may crash due to an assertion in ArrayData::Slice(),
      // so check invariants before
      const auto& field_data = *array.data()->child_data[i];
      if (field_data.length < array.offset()) {
        return Status::Invalid("Struct child array #", i,
                               " has length smaller than struct array offset (",
                               field_data.length, " < ", array.offset(), ")");
      }

      auto it = array.field(i);
      if (it->length() != array.length()) {
        return Status::Invalid("Struct child array #", i,
                               " has length different from struct array (", it->length(),
                               " != ", array.length(), ")");
      }

      auto it_type = struct_type.field(i)->type();
      if (!it->type()->Equals(it_type)) {
        return Status::Invalid("Struct child array #", i,
                               " does not match type field: ", it->type()->ToString(),
                               " vs ", it_type->ToString());
      }

      const Status child_valid = ValidateArray(*it);
      if (!child_valid.ok()) {
        return Status::Invalid("Struct child array #", i,
                               " invalid: ", child_valid.ToString());
      }
    }
    return Status::OK();
  }

  Status Visit(const UnionArray& array) {
    const auto& union_type = *array.union_type();
    // Validate fields
    for (int i = 0; i < array.num_fields(); ++i) {
      if (union_type.mode() == UnionMode::SPARSE) {
        // array.field() may crash due to an assertion in ArrayData::Slice(),
        // so check invariants before
        const auto& child_data = *array.data()->child_data[i];
        if (child_data.length < array.offset()) {
          return Status::Invalid("Sparse union child array #", i,
                                 " has length smaller than union array offset (",
                                 child_data.length, " < ", array.offset(), ")");
        }
      }

      auto it = array.field(i);
      if (union_type.mode() == UnionMode::SPARSE && it->length() != array.length()) {
        return Status::Invalid("Sparse union child array #", i,
                               " has length different from union array (", it->length(),
                               " != ", array.length(), ")");
      }

      auto it_type = union_type.field(i)->type();
      if (!it->type()->Equals(it_type)) {
        return Status::Invalid("Union child array #", i,
                               " does not match type field: ", it->type()->ToString(),
                               " vs ", it_type->ToString());
      }

      const Status child_valid = ValidateArray(*it);
      if (!child_valid.ok()) {
        return Status::Invalid("Union child array #", i,
                               " invalid: ", child_valid.ToString());
      }
    }
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) {
    Type::type index_type_id = array.indices()->type()->id();
    if (!is_integer(index_type_id)) {
      return Status::Invalid("Dictionary indices must be integer type");
    }
    if (!array.data()->dictionary) {
      return Status::Invalid("Dictionary values must be non-null");
    }
    const Status dict_valid = ValidateArray(*MakeArray(array.data()->dictionary));
    if (!dict_valid.ok()) {
      return Status::Invalid("Dictionary array invalid: ", dict_valid.ToString());
    }
    return Status::OK();
  }

  Status Visit(const ExtensionArray& array) {
    const auto& ext_type = checked_cast<const ExtensionType&>(*array.type());

    if (!array.storage()->type()->Equals(*ext_type.storage_type())) {
      return Status::Invalid("Extension array of type '", array.type()->ToString(),
                             "' has storage array of incompatible type '",
                             array.storage()->type()->ToString(), "'");
    }
    return ValidateArray(*array.storage());
  }

 protected:
  template <typename BinaryArrayType>
  Status ValidateBinaryArray(const BinaryArrayType& array) {
    if (array.data()->buffers.size() != 3) {
      return Status::Invalid("number of buffers is != 3");
    }
    if (array.value_data() == nullptr) {
      return Status::Invalid("value data buffer is null");
    }
    RETURN_NOT_OK(ValidateOffsets(array));

    if (array.length() > 0) {
      const auto first_offset = array.value_offset(0);
      const auto last_offset = array.value_offset(array.length());
      // This early test avoids undefined behaviour when computing `data_extent`
      if (first_offset < 0 || last_offset < 0) {
        return Status::Invalid("Negative offsets in binary array");
      }
      const auto data_extent = last_offset - first_offset;
      const auto values_length = array.value_data()->size();
      if (values_length < data_extent) {
        return Status::Invalid("Length spanned by binary offsets (", data_extent,
                               ") larger than values array (size ", values_length, ")");
      }
      // These tests ensure that array concatenation is safe if Validate() succeeds
      // (for delta dictionaries)
      if (first_offset > values_length || last_offset > values_length) {
        return Status::Invalid("First or last binary offset out of bounds");
      }
      if (first_offset > last_offset) {
        return Status::Invalid("First offset larger than last offset in binary array");
      }
    }
    return Status::OK();
  }

  template <typename ListArrayType>
  Status ValidateListArray(const ListArrayType& array) {
    // First validate offsets, to make sure the accesses below are valid
    RETURN_NOT_OK(ValidateOffsets(array));

    // An empty list array can have 0 offsets
    if (array.length() > 0) {
      const auto first_offset = array.value_offset(0);
      const auto last_offset = array.value_offset(array.length());
      // This early test avoids undefined behaviour when computing `data_extent`
      if (first_offset < 0 || last_offset < 0) {
        return Status::Invalid("Negative offsets in list array");
      }
      const auto data_extent = last_offset - first_offset;
      if (data_extent > 0 && !array.values()) {
        return Status::Invalid("values is null");
      }
      const auto values_length = array.values()->length();
      if (values_length < data_extent) {
        return Status::Invalid("Length spanned by list offsets (", data_extent,
                               ") larger than values array (length ", values_length, ")");
      }
      // These tests ensure that array concatenation is safe if Validate() succeeds
      // (for delta dictionaries)
      if (first_offset > values_length || last_offset > values_length) {
        return Status::Invalid("First or last list offset out of bounds");
      }
      if (first_offset > last_offset) {
        return Status::Invalid("First offset larger than last offset in list array");
      }
    }

    const Status child_valid = ValidateArray(*array.values());
    if (!child_valid.ok()) {
      return Status::Invalid("List child array invalid: ", child_valid.ToString());
    }
    return Status::OK();
  }

  template <typename ArrayType>
  Status ValidateOffsets(const ArrayType& array) {
    using offset_type = typename ArrayType::offset_type;

    auto value_offsets = array.value_offsets();
    if (value_offsets == nullptr) {
      // For length 0, an empty offsets array seems accepted as a special case (ARROW-544)
      if (array.length() > 0) {
        return Status::Invalid("non-empty array but value_offsets_ is null");
      }
      return Status::OK();
    }

    // An empty list array can have 0 offsets
    auto required_offsets =
        (array.length() > 0) ? array.length() + array.offset() + 1 : 0;
    if (value_offsets->size() / static_cast<int>(sizeof(offset_type)) <
        required_offsets) {
      return Status::Invalid("offset buffer size (bytes): ", value_offsets->size(),
                             " isn't large enough for length: ", array.length());
    }

    return Status::OK();
  }
};

}  // namespace

ARROW_EXPORT
Status ValidateArray(const Array& array) {
  // First check the array layout conforms to the spec
  const DataType& type = *array.type();
  const auto layout = type.layout();
  const ArrayData& data = *array.data();

  if (array.length() < 0) {
    return Status::Invalid("Array length is negative");
  }

  if (data.buffers.size() != layout.buffers.size()) {
    return Status::Invalid("Expected ", layout.buffers.size(),
                           " buffers in array "
                           "of type ",
                           type.ToString(), ", got ", data.buffers.size());
  }
  // This check is required to avoid addition overflow below
  if (HasPositiveAdditionOverflow(array.length(), array.offset())) {
    return Status::Invalid("Array of type ", type.ToString(),
                           " has impossibly large length and offset");
  }
  for (int i = 0; i < static_cast<int>(data.buffers.size()); ++i) {
    const auto& buffer = data.buffers[i];
    const auto& spec = layout.buffers[i];

    if (buffer == nullptr) {
      continue;
    }
    int64_t min_buffer_size = -1;
    switch (spec.kind) {
      case DataTypeLayout::BITMAP:
        min_buffer_size = BitUtil::BytesForBits(array.length() + array.offset());
        break;
      case DataTypeLayout::FIXED_WIDTH:
        if (HasPositiveMultiplyOverflow(array.length() + array.offset(),
                                        spec.byte_width)) {
          return Status::Invalid("Array of type ", type.ToString(),
                                 " has impossibly large length and offset");
        }
        min_buffer_size = spec.byte_width * (array.length() + array.offset());
        break;
      case DataTypeLayout::ALWAYS_NULL:
        // XXX Should we raise on non-null buffer?
        continue;
      default:
        continue;
    }
    if (buffer->size() < min_buffer_size) {
      return Status::Invalid("Buffer #", i, " too small in array of type ",
                             type.ToString(), " and length ", array.length(),
                             ": expected at least ", min_buffer_size, " byte(s), got ",
                             buffer->size());
    }
  }
  if (type.id() != Type::NA && data.null_count > 0 && data.buffers[0] == nullptr) {
    return Status::Invalid("Array of type ", type.ToString(), " has ", data.null_count,
                           " nulls but no null bitmap");
  }

  // Check null_count() *after* validating the buffer sizes, to avoid
  // reading out of bounds.
  if (array.null_count() > array.length()) {
    return Status::Invalid("Null count exceeds array length");
  }

  if (type.id() != Type::EXTENSION) {
    if (data.child_data.size() != static_cast<size_t>(type.num_fields())) {
      return Status::Invalid("Expected ", type.num_fields(),
                             " child arrays in array "
                             "of type ",
                             type.ToString(), ", got ", data.child_data.size());
    }
  }
  if (layout.has_dictionary && !data.dictionary) {
    return Status::Invalid("Array of type ", type.ToString(),
                           " must have dictionary values");
  }
  if (!layout.has_dictionary && data.dictionary) {
    return Status::Invalid("Unexpected dictionary values in array of type ",
                           type.ToString());
  }

  ValidateArrayVisitor visitor;
  return VisitArrayInline(array, &visitor);
}

///////////////////////////////////////////////////////////////////////////
// ValidateArrayData: expensive validation checks

namespace {

struct BoundsCheckVisitor {
  int64_t min_value_;
  int64_t max_value_;

  Status Visit(const Array& array) {
    // Default, should be unreachable
    return Status::NotImplemented("");
  }

  template <typename T>
  Status Visit(const NumericArray<T>& array) {
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        const auto v = static_cast<int64_t>(array.Value(i));
        if (v < min_value_ || v > max_value_) {
          return Status::Invalid("Value at position ", i, " out of bounds: ", v,
                                 " (should be in [", min_value_, ", ", max_value_, "])");
        }
      }
    }
    return Status::OK();
  }
};

struct ValidateArrayDataVisitor {
  // Fallback
  Status Visit(const Array& array) { return Status::OK(); }

  Status Visit(const StringArray& array) {
    RETURN_NOT_OK(ValidateBinaryArray(array));
    return array.ValidateUTF8();
  }

  Status Visit(const LargeStringArray& array) {
    RETURN_NOT_OK(ValidateBinaryArray(array));
    return array.ValidateUTF8();
  }

  Status Visit(const BinaryArray& array) { return ValidateBinaryArray(array); }

  Status Visit(const LargeBinaryArray& array) { return ValidateBinaryArray(array); }

  Status Visit(const ListArray& array) { return ValidateListArray(array); }

  Status Visit(const LargeListArray& array) { return ValidateListArray(array); }

  Status Visit(const MapArray& array) {
    // TODO check keys and items individually?
    return ValidateListArray(array);
  }

  Status Visit(const UnionArray& array) {
    const auto& child_ids = array.union_type()->child_ids();

    const int8_t* type_codes = array.raw_type_codes();
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        continue;
      }
      const int32_t code = type_codes[i];
      if (code < 0 || child_ids[code] == UnionType::kInvalidChildId) {
        return Status::Invalid("Union value at position ", i, " has invalid type id ",
                               code);
      }
    }

    if (array.mode() == UnionMode::DENSE) {
      // Map logical type id to child length
      std::vector<int64_t> child_lengths(256);
      const auto& type_codes_map = array.union_type()->type_codes();
      for (int child_id = 0; child_id < array.type()->num_fields(); ++child_id) {
        child_lengths[type_codes_map[child_id]] = array.field(child_id)->length();
      }

      // Check offsets
      const int32_t* offsets =
          checked_cast<const DenseUnionArray&>(array).raw_value_offsets();
      for (int64_t i = 0; i < array.length(); ++i) {
        if (array.IsNull(i)) {
          continue;
        }
        const int32_t code = type_codes[i];
        const int32_t offset = offsets[i];
        if (offset < 0) {
          return Status::Invalid("Union value at position ", i, " has negative offset ",
                                 offset);
        }
        if (offset >= child_lengths[code]) {
          return Status::Invalid("Union value at position ", i,
                                 " has offset larger "
                                 "than child length (",
                                 offset, " >= ", child_lengths[code], ")");
        }
      }
    }
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) {
    const Status indices_status =
        CheckBounds(*array.indices(), 0, array.dictionary()->length() - 1);
    if (!indices_status.ok()) {
      return Status::Invalid("Dictionary indices invalid: ", indices_status.ToString());
    }
    return ValidateArrayData(*array.dictionary());
  }

  Status Visit(const ExtensionArray& array) {
    return ValidateArrayData(*array.storage());
  }

 protected:
  template <typename BinaryArrayType>
  Status ValidateBinaryArray(const BinaryArrayType& array) {
    if (array.value_data() == nullptr) {
      return Status::Invalid("value data buffer is null");
    }
    return ValidateOffsets(array, array.value_data()->size());
  }

  template <typename ListArrayType>
  Status ValidateListArray(const ListArrayType& array) {
    const auto& child_array = array.values();
    const Status child_valid = ValidateArrayData(*child_array);
    if (!child_valid.ok()) {
      return Status::Invalid("List child array invalid: ", child_valid.ToString());
    }
    return ValidateOffsets(array, child_array->offset() + child_array->length());
  }

  template <typename ArrayType>
  Status ValidateOffsets(const ArrayType& array, int64_t offset_limit) {
    if (array.length() == 0) {
      return Status::OK();
    }
    if (array.value_offsets() == nullptr) {
      return Status::Invalid("non-empty array but value_offsets_ is null");
    }

    auto prev_offset = array.value_offset(0);
    if (prev_offset < 0) {
      return Status::Invalid(
          "Offset invariant failure: array starts at negative "
          "offset ",
          prev_offset);
    }
    for (int64_t i = 1; i <= array.length(); ++i) {
      auto current_offset = array.value_offset(i);
      if (current_offset < prev_offset) {
        return Status::Invalid("Offset invariant failure: non-monotonic offset at slot ",
                               i, ": ", current_offset, " < ", prev_offset);
      }
      if (current_offset > offset_limit) {
        return Status::Invalid("Offset invariant failure: offset for slot ", i,
                               " out of bounds: ", current_offset, " > ", offset_limit);
      }
      prev_offset = current_offset;
    }
    return Status::OK();
  }

  Status CheckBounds(const Array& array, int64_t min_value, int64_t max_value) {
    BoundsCheckVisitor visitor{min_value, max_value};
    return VisitArrayInline(array, &visitor);
  }
};

}  // namespace

ARROW_EXPORT
Status ValidateArrayData(const Array& array) {
  ValidateArrayDataVisitor visitor;
  return VisitArrayInline(array, &visitor);
}

}  // namespace internal
}  // namespace arrow

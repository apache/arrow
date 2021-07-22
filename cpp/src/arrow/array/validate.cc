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
#include "arrow/extension_type.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/utf8.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace internal {

///////////////////////////////////////////////////////////////////////////
// ValidateArray: cheap validation checks

namespace {

struct ValidateArrayImpl {
  const ArrayData& data;

  Status Validate() { return ValidateWithType(*data.type); }

  Status ValidateWithType(const DataType& type) { return VisitTypeInline(type, this); }

  Status Visit(const NullType&) {
    if (data.null_count != data.length) {
      return Status::Invalid("Null array null_count unequal to its length");
    }
    return Status::OK();
  }

  Status Visit(const FixedWidthType&) {
    if (data.length > 0) {
      if (!IsBufferValid(1)) {
        return Status::Invalid("Missing values buffer in non-empty array");
      }
    }
    return Status::OK();
  }

  Status Visit(const StringType& type) { return ValidateBinaryLike(type); }

  Status Visit(const BinaryType& type) { return ValidateBinaryLike(type); }

  Status Visit(const LargeStringType& type) { return ValidateBinaryLike(type); }

  Status Visit(const LargeBinaryType& type) { return ValidateBinaryLike(type); }

  Status Visit(const ListType& type) { return ValidateListLike(type); }

  Status Visit(const LargeListType& type) { return ValidateListLike(type); }

  Status Visit(const MapType& type) { return ValidateListLike(type); }

  Status Visit(const FixedSizeListType& type) {
    const ArrayData& values = *data.child_data[0];
    const int64_t list_size = type.list_size();
    if (list_size < 0) {
      return Status::Invalid("Fixed size list has negative list size");
    }

    int64_t expected_values_length = -1;
    if (MultiplyWithOverflow(data.length, list_size, &expected_values_length) ||
        values.length != expected_values_length) {
      return Status::Invalid("Values length (", values.length,
                             ") is not equal to the length (", data.length,
                             ") multiplied by the value size (", list_size, ")");
    }

    const Status child_valid = ValidateArray(values);
    if (!child_valid.ok()) {
      return Status::Invalid("Fixed size list child array invalid: ",
                             child_valid.ToString());
    }

    return Status::OK();
  }

  Status Visit(const StructType& type) {
    for (int i = 0; i < type.num_fields(); ++i) {
      const auto& field_data = *data.child_data[i];

      // Validate child first, to catch nonsensical length / offset etc.
      const Status field_valid = ValidateArray(field_data);
      if (!field_valid.ok()) {
        return Status::Invalid("Struct child array #", i,
                               " invalid: ", field_valid.ToString());
      }

      if (field_data.length < data.length + data.offset) {
        return Status::Invalid("Struct child array #", i,
                               " has length smaller than expected for struct array (",
                               field_data.length, " < ", data.length + data.offset, ")");
      }

      const auto& field_type = type.field(i)->type();
      if (!field_data.type->Equals(*field_type)) {
        return Status::Invalid("Struct child array #", i, " does not match type field: ",
                               field_data.type->ToString(), " vs ",
                               field_type->ToString());
      }
    }
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    for (int i = 0; i < type.num_fields(); ++i) {
      const auto& field_data = *data.child_data[i];

      // Validate child first, to catch nonsensical length / offset etc.
      const Status field_valid = ValidateArray(field_data);
      if (!field_valid.ok()) {
        return Status::Invalid("Union child array #", i,
                               " invalid: ", field_valid.ToString());
      }

      if (type.mode() == UnionMode::SPARSE &&
          field_data.length < data.length + data.offset) {
        return Status::Invalid("Sparse union child array #", i,
                               " has length smaller than expected for union array (",
                               field_data.length, " < ", data.length + data.offset, ")");
      }

      const auto& field_type = type.field(i)->type();
      if (!field_data.type->Equals(*field_type)) {
        return Status::Invalid("Union child array #", i, " does not match type field: ",
                               field_data.type->ToString(), " vs ",
                               field_type->ToString());
      }
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    Type::type index_type_id = type.index_type()->id();
    if (!is_integer(index_type_id)) {
      return Status::Invalid("Dictionary indices must be integer type");
    }
    if (!data.dictionary) {
      return Status::Invalid("Dictionary values must be non-null");
    }
    const Status dict_valid = ValidateArray(*data.dictionary);
    if (!dict_valid.ok()) {
      return Status::Invalid("Dictionary array invalid: ", dict_valid.ToString());
    }
    // Visit indices
    return ValidateWithType(*type.index_type());
  }

  Status Visit(const ExtensionType& type) {
    // Visit storage
    return ValidateWithType(*type.storage_type());
  }

 private:
  bool IsBufferValid(int index) { return IsBufferValid(data, index); }

  static bool IsBufferValid(const ArrayData& data, int index) {
    return data.buffers[index] != nullptr && data.buffers[index]->address() != 0;
  }

  template <typename BinaryType>
  Status ValidateBinaryLike(const BinaryType& type) {
    if (!IsBufferValid(2)) {
      return Status::Invalid("Value data buffer is null");
    }
    // First validate offsets, to make sure the accesses below are valid
    RETURN_NOT_OK(ValidateOffsets(type));

    if (data.length > 0 && data.buffers[1]->is_cpu()) {
      using offset_type = typename BinaryType::offset_type;

      const auto offsets = data.GetValues<offset_type>(1);
      const Buffer& values = *data.buffers[2];

      const auto first_offset = offsets[0];
      const auto last_offset = offsets[data.length];
      // This early test avoids undefined behaviour when computing `data_extent`
      if (first_offset < 0 || last_offset < 0) {
        return Status::Invalid("Negative offsets in binary array");
      }
      const auto data_extent = last_offset - first_offset;
      const auto values_length = values.size();
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

  template <typename ListType>
  Status ValidateListLike(const ListType& type) {
    // First validate offsets, to make sure the accesses below are valid
    RETURN_NOT_OK(ValidateOffsets(type));

    const ArrayData& values = *data.child_data[0];

    // An empty list array can have 0 offsets
    if (data.length > 0 && data.buffers[1]->is_cpu()) {
      using offset_type = typename ListType::offset_type;

      const auto offsets = data.GetValues<offset_type>(1);

      const auto first_offset = offsets[0];
      const auto last_offset = offsets[data.length];
      // This early test avoids undefined behaviour when computing `data_extent`
      if (first_offset < 0 || last_offset < 0) {
        return Status::Invalid("Negative offsets in list array");
      }
      const auto data_extent = last_offset - first_offset;
      const auto values_length = values.length;
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

    const Status child_valid = ValidateArray(values);
    if (!child_valid.ok()) {
      return Status::Invalid("List child array invalid: ", child_valid.ToString());
    }
    return Status::OK();
  }

  template <typename TypeClass>
  Status ValidateOffsets(const TypeClass& type) {
    using offset_type = typename TypeClass::offset_type;

    const Buffer* offsets = data.buffers[1].get();
    if (offsets == nullptr) {
      // For length 0, an empty offsets buffer seems accepted as a special case
      // (ARROW-544)
      if (data.length > 0) {
        return Status::Invalid("Non-empty array but offsets are null");
      }
      return Status::OK();
    }

    // An empty list array can have 0 offsets
    auto required_offsets = (data.length > 0) ? data.length + data.offset + 1 : 0;
    if (offsets->size() / static_cast<int32_t>(sizeof(offset_type)) < required_offsets) {
      return Status::Invalid("Offsets buffer size (bytes): ", offsets->size(),
                             " isn't large enough for length: ", data.length);
    }

    return Status::OK();
  }
};

}  // namespace

ARROW_EXPORT
Status ValidateArray(const ArrayData& data) {
  // First check the data layout conforms to the spec
  const DataType& type = *data.type;
  const auto layout = type.layout();

  if (data.length < 0) {
    return Status::Invalid("Array length is negative");
  }

  if (data.buffers.size() != layout.buffers.size()) {
    return Status::Invalid("Expected ", layout.buffers.size(),
                           " buffers in array "
                           "of type ",
                           type.ToString(), ", got ", data.buffers.size());
  }

  // This check is required to avoid addition overflow below
  int64_t length_plus_offset = -1;
  if (AddWithOverflow(data.length, data.offset, &length_plus_offset)) {
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
        min_buffer_size = BitUtil::BytesForBits(length_plus_offset);
        break;
      case DataTypeLayout::FIXED_WIDTH:
        if (MultiplyWithOverflow(length_plus_offset, spec.byte_width, &min_buffer_size)) {
          return Status::Invalid("Array of type ", type.ToString(),
                                 " has impossibly large length and offset");
        }
        break;
      case DataTypeLayout::ALWAYS_NULL:
        // XXX Should we raise on non-null buffer?
        continue;
      default:
        continue;
    }
    if (buffer->size() < min_buffer_size) {
      return Status::Invalid("Buffer #", i, " too small in array of type ",
                             type.ToString(), " and length ", data.length,
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
  if (data.null_count > data.length) {
    return Status::Invalid("Null count exceeds array length");
  }
  if (data.null_count < 0 && data.null_count != kUnknownNullCount) {
    return Status::Invalid("Negative null count");
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

  ValidateArrayImpl validator{data};
  return validator.Validate();
}

ARROW_EXPORT
Status ValidateArray(const Array& array) { return ValidateArray(*array.data()); }

///////////////////////////////////////////////////////////////////////////
// ValidateArrayFull: expensive validation checks

namespace {

struct UTF8DataValidator {
  const ArrayData& data;

  Status Visit(const DataType&) {
    // Default, should be unreachable
    return Status::NotImplemented("");
  }

  template <typename StringType>
  enable_if_string<StringType, Status> Visit(const StringType&) {
    util::InitializeUTF8();

    int64_t i = 0;
    return VisitArrayDataInline<StringType>(
        data,
        [&](util::string_view v) {
          if (ARROW_PREDICT_FALSE(!util::ValidateUTF8(v))) {
            return Status::Invalid("Invalid UTF8 sequence at string index ", i);
          }
          ++i;
          return Status::OK();
        },
        [&]() {
          ++i;
          return Status::OK();
        });
  }
};

struct BoundsChecker {
  const ArrayData& data;
  int64_t min_value;
  int64_t max_value;

  Status Visit(const DataType&) {
    // Default, should be unreachable
    return Status::NotImplemented("");
  }

  template <typename IntegerType>
  enable_if_integer<IntegerType, Status> Visit(const IntegerType&) {
    using c_type = typename IntegerType::c_type;

    int64_t i = 0;
    return VisitArrayDataInline<IntegerType>(
        data,
        [&](c_type value) {
          const auto v = static_cast<int64_t>(value);
          if (ARROW_PREDICT_FALSE(v < min_value || v > max_value)) {
            return Status::Invalid("Value at position ", i, " out of bounds: ", v,
                                   " (should be in [", min_value, ", ", max_value, "])");
          }
          ++i;
          return Status::OK();
        },
        [&]() {
          ++i;
          return Status::OK();
        });
  }
};

struct ValidateArrayFullImpl {
  const ArrayData& data;

  Status Validate() { return ValidateWithType(*data.type); }

  Status ValidateWithType(const DataType& type) { return VisitTypeInline(type, this); }

  Status Visit(const NullType& type) { return Status::OK(); }

  Status Visit(const FixedWidthType& type) { return Status::OK(); }

  Status Visit(const StringType& type) {
    RETURN_NOT_OK(ValidateBinaryLike(type));
    return ValidateUTF8(data);
  }

  Status Visit(const LargeStringType& type) {
    RETURN_NOT_OK(ValidateBinaryLike(type));
    return ValidateUTF8(data);
  }

  Status Visit(const BinaryType& type) { return ValidateBinaryLike(type); }

  Status Visit(const LargeBinaryType& type) { return ValidateBinaryLike(type); }

  Status Visit(const ListType& type) { return ValidateListLike(type); }

  Status Visit(const LargeListType& type) { return ValidateListLike(type); }

  Status Visit(const MapType& type) { return ValidateListLike(type); }

  Status Visit(const FixedSizeListType& type) {
    const ArrayData& child = *data.child_data[0];
    const Status child_valid = ValidateArrayFull(child);
    if (!child_valid.ok()) {
      return Status::Invalid("Fixed size list child array invalid: ",
                             child_valid.ToString());
    }
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    // Validate children
    for (int64_t i = 0; i < type.num_fields(); ++i) {
      const ArrayData& field = *data.child_data[i];
      const Status field_valid = ValidateArrayFull(field);
      if (!field_valid.ok()) {
        return Status::Invalid("Struct child array #", i,
                               " invalid: ", field_valid.ToString());
      }
    }
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    const auto& child_ids = type.child_ids();
    const auto& type_codes_map = type.type_codes();

    const int8_t* type_codes = data.GetValues<int8_t>(1);

    for (int64_t i = 0; i < data.length; ++i) {
      // Note that union arrays never have top-level nulls
      const int32_t code = type_codes[i];
      if (code < 0 || child_ids[code] == UnionType::kInvalidChildId) {
        return Status::Invalid("Union value at position ", i, " has invalid type id ",
                               code);
      }
    }

    if (type.mode() == UnionMode::DENSE) {
      // Map logical type id to child length
      std::vector<int64_t> child_lengths(256);
      for (int child_id = 0; child_id < type.num_fields(); ++child_id) {
        child_lengths[type_codes_map[child_id]] = data.child_data[child_id]->length;
      }

      // Check offsets are in bounds
      std::vector<int64_t> last_child_offsets(256, 0);
      const int32_t* offsets = data.GetValues<int32_t>(2);
      for (int64_t i = 0; i < data.length; ++i) {
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
        if (offset < last_child_offsets[code]) {
          return Status::Invalid("Union value at position ", i,
                                 " has non-monotonic offset ", offset);
        }
        last_child_offsets[code] = offset;
      }
    }

    // Validate children
    for (int64_t i = 0; i < type.num_fields(); ++i) {
      const ArrayData& field = *data.child_data[i];
      const Status field_valid = ValidateArrayFull(field);
      if (!field_valid.ok()) {
        return Status::Invalid("Union child array #", i,
                               " invalid: ", field_valid.ToString());
      }
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    const Status indices_status =
        CheckBounds(*type.index_type(), 0, data.dictionary->length - 1);
    if (!indices_status.ok()) {
      return Status::Invalid("Dictionary indices invalid: ", indices_status.ToString());
    }
    return ValidateArrayFull(*data.dictionary);
  }

  Status Visit(const ExtensionType& type) {
    return ValidateWithType(*type.storage_type());
  }

 protected:
  template <typename BinaryType>
  Status ValidateBinaryLike(const BinaryType& type) {
    const auto& data_buffer = data.buffers[2];
    if (data_buffer == nullptr) {
      return Status::Invalid("Binary data buffer is null");
    }
    return ValidateOffsets(type, data_buffer->size());
  }

  template <typename ListType>
  Status ValidateListLike(const ListType& type) {
    const ArrayData& child = *data.child_data[0];
    const Status child_valid = ValidateArrayFull(child);
    if (!child_valid.ok()) {
      return Status::Invalid("List child array invalid: ", child_valid.ToString());
    }
    return ValidateOffsets(type, child.offset + child.length);
  }

  template <typename TypeClass>
  Status ValidateOffsets(const TypeClass& type, int64_t offset_limit) {
    using offset_type = typename TypeClass::offset_type;
    if (data.length == 0) {
      return Status::OK();
    }

    const offset_type* offsets = data.GetValues<offset_type>(1);
    if (offsets == nullptr) {
      return Status::Invalid("Non-empty array but offsets are null");
    }

    auto prev_offset = offsets[0];
    if (prev_offset < 0) {
      return Status::Invalid("Offset invariant failure: array starts at negative offset ",
                             prev_offset);
    }
    for (int64_t i = 1; i <= data.length; ++i) {
      const auto current_offset = offsets[i];
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

  Status CheckBounds(const DataType& type, int64_t min_value, int64_t max_value) {
    BoundsChecker checker{data, min_value, max_value};
    return VisitTypeInline(type, &checker);
  }
};

}  // namespace

ARROW_EXPORT
Status ValidateArrayFull(const ArrayData& data) {
  return ValidateArrayFullImpl{data}.Validate();
}

ARROW_EXPORT
Status ValidateArrayFull(const Array& array) { return ValidateArrayFull(*array.data()); }

ARROW_EXPORT
Status ValidateUTF8(const ArrayData& data) {
  DCHECK(data.type->id() == Type::STRING || data.type->id() == Type::LARGE_STRING);
  UTF8DataValidator validator{data};
  return VisitTypeInline(*data.type, &validator);
}

ARROW_EXPORT
Status ValidateUTF8(const Array& array) { return ValidateUTF8(*array.data()); }

}  // namespace internal
}  // namespace arrow

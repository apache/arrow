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
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/utf8.h"
#include "arrow/visit_data_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace internal {

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
    return VisitArraySpanInline<StringType>(
        data,
        [&](std::string_view v) {
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
    return VisitArraySpanInline<IntegerType>(
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

struct ValidateArrayImpl {
  const ArrayData& data;
  const bool full_validation;

  Status Validate() {
    if (data.type == nullptr) {
      return Status::Invalid("Array type is absent");
    }

    // XXX should we unpack extension types here?

    RETURN_NOT_OK(ValidateLayout(*data.type));
    // Check nulls *after* validating the buffer sizes, to avoid
    // reading out of bounds.
    RETURN_NOT_OK(ValidateNulls(*data.type));

    // Run type-specific validations
    return ValidateWithType(*data.type);
  }

  Status ValidateWithType(const DataType& type) {
    if (type.id() != Type::EXTENSION) {
      if (data.child_data.size() != static_cast<size_t>(type.num_fields())) {
        return Status::Invalid("Expected ", type.num_fields(),
                               " child arrays in array "
                               "of type ",
                               type.ToString(), ", got ", data.child_data.size());
      }
    }
    return VisitTypeInline(type, this);
  }

  Status Visit(const NullType&) {
    if (data.null_count != data.length) {
      return Status::Invalid("Null array null_count unequal to its length");
    }
    return Status::OK();
  }

  Status Visit(const FixedWidthType&) { return ValidateFixedWidthBuffers(); }

  Status Visit(const Decimal128Type& type) {
    RETURN_NOT_OK(ValidateFixedWidthBuffers());
    return ValidateDecimals(type);
  }

  Status Visit(const Decimal256Type& type) {
    RETURN_NOT_OK(ValidateFixedWidthBuffers());
    return ValidateDecimals(type);
  }

  Status Visit(const StringType& type) {
    RETURN_NOT_OK(ValidateBinaryLike(type));
    if (full_validation) {
      RETURN_NOT_OK(ValidateUTF8(data));
    }
    return Status::OK();
  }

  Status Visit(const LargeStringType& type) {
    RETURN_NOT_OK(ValidateBinaryLike(type));
    if (full_validation) {
      RETURN_NOT_OK(ValidateUTF8(data));
    }
    return Status::OK();
  }

  Status Visit(const Date64Type& type) {
    RETURN_NOT_OK(ValidateFixedWidthBuffers());

    if (full_validation) {
      using c_type = typename Date64Type::c_type;
      return VisitArraySpanInline<Date64Type>(
          data,
          [&](c_type date) {
            constexpr c_type kFullDayMillis = 1000 * 60 * 60 * 24;
            if (date % kFullDayMillis != 0) {
              return Status::Invalid(type, " ", date,
                                     " does not represent a whole number of days");
            }
            return Status::OK();
          },
          []() { return Status::OK(); });
    }
    return Status::OK();
  }

  Status Visit(const Time32Type& type) {
    RETURN_NOT_OK(ValidateFixedWidthBuffers());

    if (full_validation) {
      using c_type = typename Time32Type::c_type;
      return VisitArraySpanInline<Time32Type>(
          data,
          [&](c_type time) {
            constexpr c_type kFullDaySeconds = 60 * 60 * 24;
            constexpr c_type kFullDayMillis = kFullDaySeconds * 1000;
            if (type.unit() == TimeUnit::SECOND &&
                (time < 0 || time >= kFullDaySeconds)) {
              return Status::Invalid(type, " ", time,
                                     " is not within the acceptable range of ", "[0, ",
                                     kFullDaySeconds, ") s");
            }
            if (type.unit() == TimeUnit::MILLI && (time < 0 || time >= kFullDayMillis)) {
              return Status::Invalid(type, " ", time,
                                     " is not within the acceptable range of ", "[0, ",
                                     kFullDayMillis, ") ms");
            }
            return Status::OK();
          },
          []() { return Status::OK(); });
    }
    return Status::OK();
  }

  Status Visit(const Time64Type& type) {
    RETURN_NOT_OK(ValidateFixedWidthBuffers());

    if (full_validation) {
      using c_type = typename Time64Type::c_type;
      return VisitArraySpanInline<Time64Type>(
          data,
          [&](c_type time) {
            constexpr c_type kFullDayMicro = 1000000LL * 60 * 60 * 24;
            constexpr c_type kFullDayNano = kFullDayMicro * 1000;
            if (type.unit() == TimeUnit::MICRO && (time < 0 || time >= kFullDayMicro)) {
              return Status::Invalid(type, " ", time,
                                     " is not within the acceptable range of ", "[0, ",
                                     kFullDayMicro, ") us");
            }
            if (type.unit() == TimeUnit::NANO && (time < 0 || time >= kFullDayNano)) {
              return Status::Invalid(type, " ", time,
                                     " is not within the acceptable range of ", "[0, ",
                                     kFullDayNano, ") ns");
            }
            return Status::OK();
          },
          []() { return Status::OK(); });
    }
    return Status::OK();
  }

  Status Visit(const BinaryType& type) { return ValidateBinaryLike(type); }

  Status Visit(const LargeBinaryType& type) { return ValidateBinaryLike(type); }

  Status Visit(const ListType& type) { return ValidateListLike(type); }

  Status Visit(const LargeListType& type) { return ValidateListLike(type); }

  Status Visit(const MapType& type) {
    RETURN_NOT_OK(ValidateListLike(type));
    return MapArray::ValidateChildData(data.child_data);
  }

  Status Visit(const FixedSizeListType& type) {
    const ArrayData& values = *data.child_data[0];
    const int64_t list_size = type.list_size();
    if (list_size < 0) {
      return Status::Invalid("Fixed size list has negative list size");
    }

    int64_t expected_values_length = -1;
    if (MultiplyWithOverflow(data.length, list_size, &expected_values_length) ||
        values.length < expected_values_length) {
      return Status::Invalid("Values length (", values.length,
                             ") is less than the length (", data.length,
                             ") multiplied by the value size (", list_size, ")");
    }

    const Status child_valid = RecurseInto(values);
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
      const Status field_valid = RecurseInto(field_data);
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

      // Validate children first, to catch nonsensical length / offset etc.
      const Status field_valid = RecurseInto(field_data);
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

    if (full_validation) {
      // Validate all type codes
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
        // Validate all offsets

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
    // Validate dictionary
    const Status dict_valid = RecurseInto(*data.dictionary);
    if (!dict_valid.ok()) {
      return Status::Invalid("Dictionary array invalid: ", dict_valid.ToString());
    }
    // Validate indices
    RETURN_NOT_OK(ValidateWithType(*type.index_type()));

    if (full_validation) {
      // Check indices within dictionary bounds
      const Status indices_status =
          CheckBounds(*type.index_type(), 0, data.dictionary->length - 1);
      if (!indices_status.ok()) {
        return Status::Invalid("Dictionary indices invalid: ", indices_status.ToString());
      }
    }
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    switch (type.run_end_type()->id()) {
      case Type::INT16:
        return ValidateRunEndEncoded<int16_t>(type);
      case Type::INT32:
        return ValidateRunEndEncoded<int32_t>(type);
      case Type::INT64:
        return ValidateRunEndEncoded<int64_t>(type);
      default:
        return Status::Invalid("Run end type must be int16, int32 or int64, but got: ",
                               type.run_end_type()->ToString());
    }
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

  Status RecurseInto(const ArrayData& related_data) {
    ValidateArrayImpl impl{related_data, full_validation};
    return impl.Validate();
  }

  Status ValidateLayout(const DataType& type) {
    // Check the data layout conforms to the spec
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
      int64_t min_buffer_size = 0;
      switch (spec.kind) {
        case DataTypeLayout::BITMAP:
          // If length == 0, buffer size can be 0 regardless of offset
          if (data.length > 0) {
            min_buffer_size = bit_util::BytesForBits(length_plus_offset);
          }
          break;
        case DataTypeLayout::FIXED_WIDTH:
          if (data.length > 0 && MultiplyWithOverflow(length_plus_offset, spec.byte_width,
                                                      &min_buffer_size)) {
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
    if (layout.has_dictionary && !data.dictionary) {
      return Status::Invalid("Array of type ", type.ToString(),
                             " must have dictionary values");
    }
    if (!layout.has_dictionary && data.dictionary) {
      return Status::Invalid("Unexpected dictionary values in array of type ",
                             type.ToString());
    }
    return Status::OK();
  }

  Status ValidateNulls(const DataType& type) {
    if (type.storage_id() != Type::NA && data.null_count > 0 &&
        data.buffers[0] == nullptr) {
      return Status::Invalid("Array of type ", type.ToString(), " has ", data.null_count,
                             " nulls but no null bitmap");
    }
    if (data.null_count > data.length) {
      return Status::Invalid("Null count exceeds array length");
    }
    if (data.null_count < 0 && data.null_count != kUnknownNullCount) {
      return Status::Invalid("Negative null count");
    }

    if (full_validation) {
      if (data.null_count != kUnknownNullCount) {
        int64_t actual_null_count;
        if (HasValidityBitmap(data.type->id()) && data.buffers[0]) {
          // Do not call GetNullCount() as it would also set the `null_count` member
          actual_null_count = data.length - CountSetBits(data.buffers[0]->data(),
                                                         data.offset, data.length);
        } else if (data.type->storage_id() == Type::NA) {
          actual_null_count = data.length;
        } else {
          actual_null_count = 0;
        }
        if (actual_null_count != data.null_count) {
          return Status::Invalid("null_count value (", data.null_count,
                                 ") doesn't match actual number of nulls in array (",
                                 actual_null_count, ")");
        }
      }
    }
    return Status::OK();
  }

  Status ValidateFixedWidthBuffers() {
    if (data.length > 0 && !IsBufferValid(1)) {
      return Status::Invalid("Missing values buffer in non-empty fixed-width array");
    }
    return Status::OK();
  }

  template <typename BinaryType>
  Status ValidateBinaryLike(const BinaryType& type) {
    if (!IsBufferValid(2)) {
      return Status::Invalid("Value data buffer is null");
    }
    const Buffer& values = *data.buffers[2];

    // First validate offsets, to make sure the accesses below are valid
    RETURN_NOT_OK(ValidateOffsets(type, values.size()));

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
    const ArrayData& values = *data.child_data[0];
    const Status child_valid = RecurseInto(values);
    if (!child_valid.ok()) {
      return Status::Invalid("List child array invalid: ", child_valid.ToString());
    }

    // First validate offsets, to make sure the accesses below are valid
    RETURN_NOT_OK(ValidateOffsets(type, values.offset + values.length));

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

    return Status::OK();
  }

  template <typename RunEndCType>
  Status ValidateRunEndEncoded(const RunEndEncodedType& type) {
    // Overflow was already checked at this point
    if (data.offset + data.length > std::numeric_limits<RunEndCType>::max()) {
      return Status::Invalid(
          "Offset + length of a run-end encoded array must fit in a value"
          " of the run end type ",
          *type.run_end_type(), ", but offset + length is ", data.offset + data.length,
          " while the allowed maximum is ", std::numeric_limits<RunEndCType>::max());
    }
    if (!data.child_data[0]) {
      return Status::Invalid("Run ends array is null pointer");
    }
    if (!data.child_data[1]) {
      return Status::Invalid("Values array is null pointer");
    }
    const ArrayData& run_ends_data = *data.child_data[0];
    const ArrayData& values_data = *data.child_data[1];
    if (*run_ends_data.type != *type.run_end_type()) {
      return Status::Invalid("Run ends array of ", type, " must be ",
                             *type.run_end_type(), ", but run end type is ",
                             *run_ends_data.type);
    }
    if (*values_data.type != *type.value_type()) {
      return Status::Invalid("Parent type says this array encodes ", *type.value_type(),
                             " values, but value type is ", *values_data.type);
    }
    const Status run_ends_valid = RecurseInto(run_ends_data);
    if (!run_ends_valid.ok()) {
      return Status::Invalid("Run ends array invalid: ", run_ends_valid.message());
    }
    const Status values_valid = RecurseInto(values_data);
    if (!values_valid.ok()) {
      return Status::Invalid("Values array invalid: ", values_valid.message());
    }
    if (data.GetNullCount() != 0) {
      return Status::Invalid("Null count must be 0 for run-end encoded array, but is ",
                             data.GetNullCount());
    }
    if (run_ends_data.GetNullCount() != 0) {
      return Status::Invalid("Null count must be 0 for run ends array, but is ",
                             run_ends_data.GetNullCount());
    }
    if (run_ends_data.length > values_data.length) {
      return Status::Invalid("Length of run_ends is greater than the length of values: ",
                             run_ends_data.length, " > ", values_data.length);
    }
    if (run_ends_data.length == 0) {
      if (data.length == 0) {
        return Status::OK();
      }
      return Status::Invalid("Run-end encoded array has non-zero length ", data.length,
                             ", but run ends array has zero length");
    }
    if (!run_ends_data.buffers[1]->is_cpu()) {
      return Status::OK();
    }
    ArraySpan span(data);
    const auto* run_ends = ree_util::RunEnds<RunEndCType>(span);
    // The last run-end is the logical offset + the logical length.
    if (run_ends[run_ends_data.length - 1] < data.offset + data.length) {
      return Status::Invalid("Last run end is ", run_ends[run_ends_data.length - 1],
                             " but it should match ", data.offset + data.length,
                             " (offset: ", data.offset, ", length: ", data.length, ")");
    }
    if (full_validation) {
      const int64_t run_ends_length = ree_util::RunEndsArray(span).length;
      if (run_ends[0] < 1) {
        return Status::Invalid(
            "All run ends must be greater than 0 but the first run end is ", run_ends[0]);
      }
      int64_t last_run_end = run_ends[0];
      for (int64_t index = 1; index < run_ends_length; index++) {
        const int64_t run_end = run_ends[index];
        if (run_end <= last_run_end) {
          return Status::Invalid(
              "Every run end must be strictly greater than the previous run end, "
              "but run_ends[",
              index, "] is ", run_end, " and run_ends[", index - 1, "] is ",
              last_run_end);
        }
        last_run_end = run_end;
      }
    }
    return Status::OK();
  }

  template <typename TypeClass>
  Status ValidateOffsets(const TypeClass& type, int64_t offset_limit) {
    using offset_type = typename TypeClass::offset_type;

    if (!IsBufferValid(1)) {
      // For length 0, an empty offsets buffer seems accepted as a special case
      // (ARROW-544)
      if (data.length > 0) {
        return Status::Invalid("Non-empty array but offsets are null");
      }
      return Status::OK();
    }

    // An empty list array can have 0 offsets
    const auto required_offsets = (data.length > 0) ? data.length + data.offset + 1 : 0;
    const auto offsets_byte_size = data.buffers[1]->size();
    if (offsets_byte_size / static_cast<int32_t>(sizeof(offset_type)) <
        required_offsets) {
      return Status::Invalid("Offsets buffer size (bytes): ", offsets_byte_size,
                             " isn't large enough for length: ", data.length,
                             " and offset: ", data.offset);
    }

    if (full_validation && required_offsets > 0) {
      // Validate all offset values
      const offset_type* offsets = data.GetValues<offset_type>(1);

      auto prev_offset = offsets[0];
      if (prev_offset < 0) {
        return Status::Invalid(
            "Offset invariant failure: array starts at negative offset ", prev_offset);
      }
      for (int64_t i = 1; i <= data.length; ++i) {
        const auto current_offset = offsets[i];
        if (current_offset < prev_offset) {
          return Status::Invalid(
              "Offset invariant failure: non-monotonic offset at slot ", i, ": ",
              current_offset, " < ", prev_offset);
        }
        if (current_offset > offset_limit) {
          return Status::Invalid("Offset invariant failure: offset for slot ", i,
                                 " out of bounds: ", current_offset, " > ", offset_limit);
        }
        prev_offset = current_offset;
      }
    }
    return Status::OK();
  }

  template <typename DecimalType>
  Status ValidateDecimals(const DecimalType& type) {
    using CType = typename TypeTraits<DecimalType>::CType;
    if (full_validation) {
      const int32_t precision = type.precision();
      return VisitArraySpanInline<DecimalType>(
          data,
          [&](std::string_view bytes) {
            DCHECK_EQ(bytes.size(), DecimalType::kByteWidth);
            CType value(reinterpret_cast<const uint8_t*>(bytes.data()));
            if (!value.FitsInPrecision(precision)) {
              return Status::Invalid("Decimal value ", value.ToIntegerString(),
                                     " does not fit in precision of ", type);
            }
            return Status::OK();
          },
          []() { return Status::OK(); });
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
Status ValidateArray(const ArrayData& data) {
  ValidateArrayImpl validator{data, /*full_validation=*/false};
  return validator.Validate();
}

ARROW_EXPORT
Status ValidateArray(const Array& array) { return ValidateArray(*array.data()); }

ARROW_EXPORT
Status ValidateArrayFull(const ArrayData& data) {
  return ValidateArrayImpl{data, /*full_validation=*/true}.Validate();
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

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

#include "arrow/extension/variant.h"

#include <cstring>

#include "arrow/extension/variant_internal_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"

namespace arrow::extension::variant {

// Ensure view classes remain lightweight (stack-allocated, cache-friendly).
static_assert(sizeof(VariantView) <= 32, "VariantView should fit in 32 bytes");
static_assert(sizeof(VariantObjectView) <= 80,
              "VariantObjectView should fit in 80 bytes");
static_assert(sizeof(VariantArrayView) <= 64, "VariantArrayView should fit in 64 bytes");

namespace {

// ---------------------------------------------------------------------------
// Little-endian helpers (delegate to shared internal utility)
// ---------------------------------------------------------------------------

using internal::ReadUnsignedLE;

/// \brief Validate that offsets are monotonically non-decreasing and in bounds.
Status ValidateOffsets(const std::vector<uint32_t>& offsets, int64_t data_length) {
  for (size_t i = 1; i < offsets.size(); ++i) {
    if (offsets[i] < offsets[i - 1]) {
      return Status::Invalid(
          "Variant metadata: string offsets are not monotonically "
          "non-decreasing at index ",
          i);
    }
  }
  if (!offsets.empty() && offsets.back() > static_cast<uint32_t>(data_length)) {
    return Status::Invalid("Variant metadata: last string offset ", offsets.back(),
                           " exceeds data length ", data_length);
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// Recursive visitor traversal (internal)
// ---------------------------------------------------------------------------

Status VisitValueAt(const VariantMetadata& metadata, const uint8_t* data, int64_t length,
                    int64_t offset, VariantVisitor* visitor, int64_t* bytes_consumed,
                    int32_t depth);

Status VisitPrimitive(const uint8_t* data, int64_t length, int64_t offset, uint8_t header,
                      VariantVisitor* visitor, int64_t* bytes_consumed) {
  auto primitive_type = GetPrimitiveType(header);
  int64_t pos = offset + 1;

  auto check_remaining = [&](int64_t needed) -> Status {
    if (pos + needed > length) {
      return Status::Invalid("Variant value: truncated primitive at offset ", offset,
                             ", need ", needed, " bytes but only ", length - pos,
                             " remaining");
    }
    return Status::OK();
  };

  switch (primitive_type) {
    case PrimitiveType::kNull:
      ARROW_RETURN_NOT_OK(visitor->Null());
      *bytes_consumed = 1;
      return Status::OK();
    case PrimitiveType::kTrue:
      ARROW_RETURN_NOT_OK(visitor->Bool(true));
      *bytes_consumed = 1;
      return Status::OK();
    case PrimitiveType::kFalse:
      ARROW_RETURN_NOT_OK(visitor->Bool(false));
      *bytes_consumed = 1;
      return Status::OK();
    case PrimitiveType::kInt8: {
      ARROW_RETURN_NOT_OK(check_remaining(1));
      ARROW_RETURN_NOT_OK(visitor->Int8(static_cast<int8_t>(data[pos])));
      *bytes_consumed = 2;
      return Status::OK();
    }
    case PrimitiveType::kInt16: {
      ARROW_RETURN_NOT_OK(check_remaining(2));
      int16_t value;
      std::memcpy(&value, data + pos, 2);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->Int16(value));
      *bytes_consumed = 3;
      return Status::OK();
    }
    case PrimitiveType::kInt32: {
      ARROW_RETURN_NOT_OK(check_remaining(4));
      int32_t value;
      std::memcpy(&value, data + pos, 4);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->Int32(value));
      *bytes_consumed = 5;
      return Status::OK();
    }
    case PrimitiveType::kInt64: {
      ARROW_RETURN_NOT_OK(check_remaining(8));
      int64_t value;
      std::memcpy(&value, data + pos, 8);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->Int64(value));
      *bytes_consumed = 9;
      return Status::OK();
    }
    case PrimitiveType::kFloat: {
      ARROW_RETURN_NOT_OK(check_remaining(4));
      float value;
      std::memcpy(&value, data + pos, 4);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->Float(value));
      *bytes_consumed = 5;
      return Status::OK();
    }
    case PrimitiveType::kDouble: {
      ARROW_RETURN_NOT_OK(check_remaining(8));
      double value;
      std::memcpy(&value, data + pos, 8);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->Double(value));
      *bytes_consumed = 9;
      return Status::OK();
    }
    case PrimitiveType::kDecimal4: {
      ARROW_RETURN_NOT_OK(check_remaining(5));
      auto scale = static_cast<int32_t>(data[pos]);
      ARROW_RETURN_NOT_OK(visitor->Decimal4(data + pos + 1, scale));
      *bytes_consumed = 6;
      return Status::OK();
    }
    case PrimitiveType::kDecimal8: {
      ARROW_RETURN_NOT_OK(check_remaining(9));
      auto scale = static_cast<int32_t>(data[pos]);
      ARROW_RETURN_NOT_OK(visitor->Decimal8(data + pos + 1, scale));
      *bytes_consumed = 10;
      return Status::OK();
    }
    case PrimitiveType::kDecimal16: {
      ARROW_RETURN_NOT_OK(check_remaining(17));
      auto scale = static_cast<int32_t>(data[pos]);
      ARROW_RETURN_NOT_OK(visitor->Decimal16(data + pos + 1, scale));
      *bytes_consumed = 18;
      return Status::OK();
    }
    case PrimitiveType::kDate: {
      ARROW_RETURN_NOT_OK(check_remaining(4));
      int32_t value;
      std::memcpy(&value, data + pos, 4);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->Date(value));
      *bytes_consumed = 5;
      return Status::OK();
    }
    case PrimitiveType::kTimestampMicros: {
      ARROW_RETURN_NOT_OK(check_remaining(8));
      int64_t value;
      std::memcpy(&value, data + pos, 8);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->TimestampMicros(value));
      *bytes_consumed = 9;
      return Status::OK();
    }
    case PrimitiveType::kTimestampMicrosNTZ: {
      ARROW_RETURN_NOT_OK(check_remaining(8));
      int64_t value;
      std::memcpy(&value, data + pos, 8);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->TimestampMicrosNTZ(value));
      *bytes_consumed = 9;
      return Status::OK();
    }
    case PrimitiveType::kBinary: {
      ARROW_RETURN_NOT_OK(check_remaining(4));
      uint32_t bin_length;
      std::memcpy(&bin_length, data + pos, 4);
      bin_length = bit_util::FromLittleEndian(bin_length);
      ARROW_RETURN_NOT_OK(check_remaining(4 + static_cast<int64_t>(bin_length)));
      auto view =
          std::string_view(reinterpret_cast<const char*>(data + pos + 4), bin_length);
      ARROW_RETURN_NOT_OK(visitor->Binary(view));
      *bytes_consumed = 1 + 4 + static_cast<int64_t>(bin_length);
      return Status::OK();
    }
    case PrimitiveType::kString: {
      ARROW_RETURN_NOT_OK(check_remaining(4));
      uint32_t str_length;
      std::memcpy(&str_length, data + pos, 4);
      str_length = bit_util::FromLittleEndian(str_length);
      ARROW_RETURN_NOT_OK(check_remaining(4 + static_cast<int64_t>(str_length)));
      auto view =
          std::string_view(reinterpret_cast<const char*>(data + pos + 4), str_length);
      ARROW_RETURN_NOT_OK(visitor->String(view));
      *bytes_consumed = 1 + 4 + static_cast<int64_t>(str_length);
      return Status::OK();
    }
    case PrimitiveType::kTimeNTZ: {
      ARROW_RETURN_NOT_OK(check_remaining(8));
      int64_t value;
      std::memcpy(&value, data + pos, 8);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->TimeNTZ(value));
      *bytes_consumed = 9;
      return Status::OK();
    }
    case PrimitiveType::kTimestampNanos: {
      ARROW_RETURN_NOT_OK(check_remaining(8));
      int64_t value;
      std::memcpy(&value, data + pos, 8);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->TimestampNanos(value));
      *bytes_consumed = 9;
      return Status::OK();
    }
    case PrimitiveType::kTimestampNanosNTZ: {
      ARROW_RETURN_NOT_OK(check_remaining(8));
      int64_t value;
      std::memcpy(&value, data + pos, 8);
      value = bit_util::FromLittleEndian(value);
      ARROW_RETURN_NOT_OK(visitor->TimestampNanosNTZ(value));
      *bytes_consumed = 9;
      return Status::OK();
    }
    case PrimitiveType::kUUID: {
      ARROW_RETURN_NOT_OK(check_remaining(kUUIDByteLength));
      ARROW_RETURN_NOT_OK(visitor->UUID(data + pos));
      *bytes_consumed = kUUIDByteLength + 1;
      return Status::OK();
    }
    default:
      return Status::Invalid("Variant value: unknown primitive type ",
                             static_cast<int>(primitive_type));
  }
}

Status VisitShortString(const uint8_t* data, int64_t length, int64_t offset,
                        uint8_t header, VariantVisitor* visitor,
                        int64_t* bytes_consumed) {
  int32_t str_len = (header >> 2) & 0x3F;
  int64_t pos = offset + 1;
  if (pos + str_len > length) {
    return Status::Invalid("Variant value: truncated short string at offset ", offset);
  }
  auto view = std::string_view(reinterpret_cast<const char*>(data + pos), str_len);
  ARROW_RETURN_NOT_OK(visitor->String(view));
  *bytes_consumed = 1 + str_len;
  return Status::OK();
}

Status VisitObject(const VariantMetadata& metadata, const uint8_t* data, int64_t length,
                   int64_t offset, uint8_t header, VariantVisitor* visitor,
                   int64_t* bytes_consumed, int32_t depth) {
  uint8_t type_info = (header >> 2) & 0x3F;
  int32_t field_offset_size = (type_info & 0x03) + 1;
  int32_t field_id_size = ((type_info >> 2) & 0x03) + 1;
  bool is_large = ((type_info >> 4) & 0x01) != 0;
  int32_t num_fields_size = is_large ? 4 : 1;

  int64_t pos = offset + 1;
  if (pos + num_fields_size > length) {
    return Status::Invalid("Variant value: truncated object num_fields at offset ",
                           offset);
  }
  auto num_fields = static_cast<int64_t>(ReadUnsignedLE(data + pos, num_fields_size));
  pos += num_fields_size;

  int64_t field_ids_size = num_fields * field_id_size;
  if (pos + field_ids_size > length) {
    return Status::Invalid("Variant value: truncated object field_ids at offset ",
                           offset);
  }
  std::vector<uint32_t> field_ids(static_cast<size_t>(num_fields));
  for (int64_t i = 0; i < num_fields; ++i) {
    field_ids[static_cast<size_t>(i)] = ReadUnsignedLE(data + pos, field_id_size);
    pos += field_id_size;
  }

  int64_t offsets_size = (num_fields + 1) * field_offset_size;
  if (pos + offsets_size > length) {
    return Status::Invalid("Variant value: truncated object offsets at offset ", offset);
  }
  std::vector<uint32_t> value_offsets(static_cast<size_t>(num_fields + 1));
  for (int64_t i = 0; i <= num_fields; ++i) {
    value_offsets[static_cast<size_t>(i)] = ReadUnsignedLE(data + pos, field_offset_size);
    pos += field_offset_size;
  }

  int64_t data_start = pos;
  int64_t total_data_size =
      static_cast<int64_t>(value_offsets[static_cast<size_t>(num_fields)]);
  if (data_start + total_data_size > length) {
    return Status::Invalid("Variant value: object data exceeds buffer at offset ",
                           offset);
  }

  for (int64_t i = 0; i < num_fields; ++i) {
    if (value_offsets[static_cast<size_t>(i)] > static_cast<uint32_t>(total_data_size)) {
      return Status::Invalid("Variant value: object field offset ",
                             value_offsets[static_cast<size_t>(i)], " at index ", i,
                             " exceeds data size ", total_data_size);
    }
  }

  ARROW_RETURN_NOT_OK(visitor->StartObject(static_cast<int32_t>(num_fields)));

  for (int64_t i = 0; i < num_fields; ++i) {
    auto field_id = field_ids[static_cast<size_t>(i)];
    if (field_id >= metadata.strings.size()) {
      return Status::Invalid("Variant value: field_id ", field_id,
                             " exceeds metadata dictionary size ",
                             metadata.strings.size());
    }
    ARROW_RETURN_NOT_OK(visitor->FieldName(metadata.strings[field_id]));

    int64_t field_offset = data_start + value_offsets[static_cast<size_t>(i)];
    int64_t consumed = 0;
    ARROW_RETURN_NOT_OK(VisitValueAt(metadata, data, data_start + total_data_size,
                                     field_offset, visitor, &consumed, depth));
  }

  ARROW_RETURN_NOT_OK(visitor->EndObject());
  *bytes_consumed = (data_start - offset) + total_data_size;
  return Status::OK();
}

Status VisitArray(const VariantMetadata& metadata, const uint8_t* data, int64_t length,
                  int64_t offset, uint8_t header, VariantVisitor* visitor,
                  int64_t* bytes_consumed, int32_t depth) {
  uint8_t type_info = (header >> 2) & 0x3F;
  int32_t field_offset_size = (type_info & 0x03) + 1;
  bool is_large = ((type_info >> 2) & 0x01) != 0;
  int32_t num_elements_size = is_large ? 4 : 1;

  int64_t pos = offset + 1;
  if (pos + num_elements_size > length) {
    return Status::Invalid("Variant value: truncated array num_elements at offset ",
                           offset);
  }
  auto num_elements = static_cast<int64_t>(ReadUnsignedLE(data + pos, num_elements_size));
  pos += num_elements_size;

  int64_t offsets_size = (num_elements + 1) * field_offset_size;
  if (pos + offsets_size > length) {
    return Status::Invalid("Variant value: truncated array offsets at offset ", offset);
  }
  std::vector<uint32_t> value_offsets(static_cast<size_t>(num_elements + 1));
  for (int64_t i = 0; i <= num_elements; ++i) {
    value_offsets[static_cast<size_t>(i)] = ReadUnsignedLE(data + pos, field_offset_size);
    pos += field_offset_size;
  }

  for (int64_t i = 1; i <= num_elements; ++i) {
    if (value_offsets[static_cast<size_t>(i)] <
        value_offsets[static_cast<size_t>(i - 1)]) {
      return Status::Invalid(
          "Variant value: array value offsets are not monotonically "
          "non-decreasing at index ",
          i);
    }
  }

  int64_t data_start = pos;
  int64_t total_data_size =
      static_cast<int64_t>(value_offsets[static_cast<size_t>(num_elements)]);
  if (data_start + total_data_size > length) {
    return Status::Invalid("Variant value: array data exceeds buffer at offset ", offset);
  }

  ARROW_RETURN_NOT_OK(visitor->StartArray(static_cast<int32_t>(num_elements)));

  for (int64_t i = 0; i < num_elements; ++i) {
    int64_t elem_offset = data_start + value_offsets[static_cast<size_t>(i)];
    int64_t consumed = 0;
    ARROW_RETURN_NOT_OK(VisitValueAt(metadata, data, data_start + total_data_size,
                                     elem_offset, visitor, &consumed, depth));
  }

  ARROW_RETURN_NOT_OK(visitor->EndArray());
  *bytes_consumed = (data_start - offset) + total_data_size;
  return Status::OK();
}

Status VisitValueAt(const VariantMetadata& metadata, const uint8_t* data, int64_t length,
                    int64_t offset, VariantVisitor* visitor, int64_t* bytes_consumed,
                    int32_t depth) {
  if (offset >= length) {
    return Status::Invalid("Variant value: offset ", offset,
                           " is at or beyond buffer length ", length);
  }
  if (depth > kMaxNestingDepth) {
    return Status::Invalid("Variant value: nesting depth exceeds maximum of ",
                           kMaxNestingDepth);
  }

  uint8_t header = data[offset];
  auto basic_type = GetBasicType(header);

  switch (basic_type) {
    case BasicType::kPrimitive:
      return VisitPrimitive(data, length, offset, header, visitor, bytes_consumed);
    case BasicType::kShortString:
      return VisitShortString(data, length, offset, header, visitor, bytes_consumed);
    case BasicType::kObject:
      return VisitObject(metadata, data, length, offset, header, visitor, bytes_consumed,
                         depth + 1);
    case BasicType::kArray:
      return VisitArray(metadata, data, length, offset, header, visitor, bytes_consumed,
                        depth + 1);
    default:
      return Status::Invalid("Variant value: unknown basic type ",
                             static_cast<int>(basic_type));
  }
}

}  // namespace

// ===========================================================================
// Public API: Metadata
// ===========================================================================

Result<VariantMetadata> DecodeMetadata(const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("Variant metadata: buffer is null or empty");
  }

  uint8_t header = data[0];
  uint8_t version = header & 0x0F;
  if (version != kVariantVersion) {
    return Status::Invalid("Variant metadata: unsupported version ",
                           static_cast<int>(version), ", expected ",
                           static_cast<int>(kVariantVersion));
  }

  if ((header >> 5) & 0x01) {
    return Status::Invalid("Variant metadata: reserved bit 5 is set in header");
  }

  bool is_sorted = ((header >> 4) & 0x01) != 0;
  int32_t offset_size = ((header >> 6) & 0x03) + 1;

  int64_t pos = 1;
  if (pos + offset_size > length) {
    return Status::Invalid("Variant metadata: truncated dictionary size at byte ", pos);
  }
  auto dict_size = static_cast<int64_t>(ReadUnsignedLE(data + pos, offset_size));
  pos += offset_size;

  int64_t offsets_bytes = (dict_size + 1) * offset_size;
  if (pos + offsets_bytes > length) {
    return Status::Invalid("Variant metadata: truncated string offsets, need ",
                           offsets_bytes, " bytes at position ", pos,
                           " but buffer length is ", length);
  }

  std::vector<uint32_t> offsets(static_cast<size_t>(dict_size + 1));
  for (int64_t i = 0; i <= dict_size; ++i) {
    offsets[static_cast<size_t>(i)] = ReadUnsignedLE(data + pos, offset_size);
    pos += offset_size;
  }

  int64_t string_data_length = length - pos;
  ARROW_RETURN_NOT_OK(ValidateOffsets(offsets, string_data_length));

  std::vector<std::string_view> strings(static_cast<size_t>(dict_size));
  for (int64_t i = 0; i < dict_size; ++i) {
    auto start = static_cast<int64_t>(offsets[static_cast<size_t>(i)]);
    auto end = static_cast<int64_t>(offsets[static_cast<size_t>(i + 1)]);
    strings[static_cast<size_t>(i)] =
        std::string_view(reinterpret_cast<const char*>(data + pos + start), end - start);
  }

  VariantMetadata result;
  result.version = version;
  result.is_sorted = is_sorted;
  result.offset_size = offset_size;
  result.strings = std::move(strings);
  return result;
}

int32_t FindMetadataKey(const VariantMetadata& metadata, std::string_view key) {
  if (metadata.is_sorted) {
    int32_t lo = 0;
    int32_t hi = static_cast<int32_t>(metadata.strings.size()) - 1;
    while (lo <= hi) {
      int32_t mid = lo + (hi - lo) / 2;
      int cmp = metadata.strings[mid].compare(key);
      if (cmp == 0) return mid;
      if (cmp < 0)
        lo = mid + 1;
      else
        hi = mid - 1;
    }
    return -1;
  }
  for (int32_t i = 0; i < static_cast<int32_t>(metadata.strings.size()); ++i) {
    if (metadata.strings[i] == key) return i;
  }
  return -1;
}

int32_t PrimitiveValueSize(PrimitiveType primitive_type) {
  switch (primitive_type) {
    case PrimitiveType::kNull:
    case PrimitiveType::kTrue:
    case PrimitiveType::kFalse:
      return 0;
    case PrimitiveType::kInt8:
      return 1;
    case PrimitiveType::kInt16:
      return 2;
    case PrimitiveType::kInt32:
    case PrimitiveType::kFloat:
    case PrimitiveType::kDate:
      return 4;
    case PrimitiveType::kInt64:
    case PrimitiveType::kDouble:
    case PrimitiveType::kTimestampMicros:
    case PrimitiveType::kTimestampMicrosNTZ:
    case PrimitiveType::kTimeNTZ:
    case PrimitiveType::kTimestampNanos:
    case PrimitiveType::kTimestampNanosNTZ:
      return 8;
    case PrimitiveType::kDecimal4:
      return 5;
    case PrimitiveType::kDecimal8:
      return 9;
    case PrimitiveType::kDecimal16:
      return 17;
    case PrimitiveType::kUUID:
      return kUUIDByteLength;
    case PrimitiveType::kBinary:
    case PrimitiveType::kString:
      return -1;
    default:
      return -1;
  }
}

Result<int64_t> ValueSize(const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("ValueSize: buffer is null or empty");
  }

  uint8_t header = data[0];
  auto basic_type = GetBasicType(header);
  uint8_t type_info = (header >> 2) & 0x3F;

  switch (basic_type) {
    case BasicType::kShortString:
      return 1 + static_cast<int64_t>(type_info);

    case BasicType::kObject: {
      bool is_large = ((type_info >> 4) & 0x01) != 0;
      int32_t sz_bytes = is_large ? 4 : 1;
      if (1 + sz_bytes > length) {
        return Status::Invalid("ValueSize: truncated object header");
      }
      auto num_elements = static_cast<int64_t>(ReadUnsignedLE(data + 1, sz_bytes));
      int32_t id_size = ((type_info >> 2) & 0x03) + 1;
      int32_t offset_size = (type_info & 0x03) + 1;
      int64_t id_start = 1 + sz_bytes;
      int64_t offset_start = id_start + num_elements * id_size;
      int64_t data_start = offset_start + (num_elements + 1) * offset_size;
      int64_t last_offset_pos = offset_start + num_elements * offset_size;
      if (last_offset_pos + offset_size > length) {
        return Status::Invalid("ValueSize: truncated object offsets");
      }
      auto total_data =
          static_cast<int64_t>(ReadUnsignedLE(data + last_offset_pos, offset_size));
      return data_start + total_data;
    }

    case BasicType::kArray: {
      bool is_large = ((type_info >> 2) & 0x01) != 0;
      int32_t sz_bytes = is_large ? 4 : 1;
      if (1 + sz_bytes > length) {
        return Status::Invalid("ValueSize: truncated array header");
      }
      auto num_elements = static_cast<int64_t>(ReadUnsignedLE(data + 1, sz_bytes));
      int32_t offset_size = (type_info & 0x03) + 1;
      int64_t offset_start = 1 + sz_bytes;
      int64_t data_start = offset_start + (num_elements + 1) * offset_size;
      int64_t last_offset_pos = offset_start + num_elements * offset_size;
      if (last_offset_pos + offset_size > length) {
        return Status::Invalid("ValueSize: truncated array offsets");
      }
      auto total_data =
          static_cast<int64_t>(ReadUnsignedLE(data + last_offset_pos, offset_size));
      return data_start + total_data;
    }

    case BasicType::kPrimitive: {
      auto ptype = static_cast<PrimitiveType>(type_info);
      int32_t payload_size = PrimitiveValueSize(ptype);
      if (payload_size >= 0) {
        return 1 + static_cast<int64_t>(payload_size);
      }
      if (1 + 4 > length) {
        return Status::Invalid("ValueSize: truncated variable-length header");
      }
      uint32_t var_len;
      std::memcpy(&var_len, data + 1, 4);
      var_len = bit_util::FromLittleEndian(var_len);
      return 1 + 4 + static_cast<int64_t>(var_len);
    }

    default:
      return Status::Invalid("ValueSize: unknown basic type");
  }
}

// ===========================================================================
// Public API: VariantView
// ===========================================================================

VariantView::VariantView(const VariantMetadata* metadata, const uint8_t* data,
                         int64_t size, BasicType type)
    : metadata_(metadata), data_(data), size_(size), type_(type) {}

Result<VariantView> VariantView::Make(const VariantMetadata& metadata,
                                      const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("VariantView: buffer is null or empty");
  }
  ARROW_ASSIGN_OR_RAISE(auto size, ValueSize(data, length));
  if (size > length) {
    return Status::Invalid("VariantView: value size ", size, " exceeds buffer length ",
                           length);
  }
  auto type = GetBasicType(data[0]);
  return VariantView(&metadata, data, size, type);
}

bool VariantView::is_null() const {
  return type_ == BasicType::kPrimitive &&
         GetPrimitiveType(data_[0]) == PrimitiveType::kNull;
}

Status VariantView::Visit(VariantVisitor* visitor) const {
  DCHECK_NE(visitor, nullptr);
  int64_t bytes_consumed = 0;
  return VisitValueAt(*metadata_, data_, size_, 0, visitor, &bytes_consumed, 0);
}

// --- Primitive accessors ---

Result<bool> VariantView::as_bool() const {
  if (type_ != BasicType::kPrimitive) {
    return Status::Invalid("VariantView::as_bool: not a primitive");
  }
  auto pt = GetPrimitiveType(data_[0]);
  if (pt == PrimitiveType::kTrue) return true;
  if (pt == PrimitiveType::kFalse) return false;
  return Status::Invalid("VariantView::as_bool: not a boolean");
}

Result<int8_t> VariantView::as_int8() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kInt8) {
    return Status::Invalid("VariantView::as_int8: type mismatch");
  }
  return static_cast<int8_t>(data_[1]);
}

Result<int16_t> VariantView::as_int16() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kInt16) {
    return Status::Invalid("VariantView::as_int16: type mismatch");
  }
  int16_t value;
  std::memcpy(&value, data_ + 1, 2);
  return bit_util::FromLittleEndian(value);
}

Result<int32_t> VariantView::as_int32() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kInt32) {
    return Status::Invalid("VariantView::as_int32: type mismatch");
  }
  int32_t value;
  std::memcpy(&value, data_ + 1, 4);
  return bit_util::FromLittleEndian(value);
}

Result<int64_t> VariantView::as_int64() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kInt64) {
    return Status::Invalid("VariantView::as_int64: type mismatch");
  }
  int64_t value;
  std::memcpy(&value, data_ + 1, 8);
  return bit_util::FromLittleEndian(value);
}

Result<float> VariantView::as_float() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kFloat) {
    return Status::Invalid("VariantView::as_float: type mismatch");
  }
  float value;
  std::memcpy(&value, data_ + 1, 4);
  return bit_util::FromLittleEndian(value);
}

Result<double> VariantView::as_double() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kDouble) {
    return Status::Invalid("VariantView::as_double: type mismatch");
  }
  double value;
  std::memcpy(&value, data_ + 1, 8);
  return bit_util::FromLittleEndian(value);
}

Result<std::string_view> VariantView::as_string() const {
  if (type_ == BasicType::kShortString) {
    int32_t len = (data_[0] >> 2) & 0x3F;
    return std::string_view(reinterpret_cast<const char*>(data_ + 1), len);
  }
  if (type_ == BasicType::kPrimitive &&
      GetPrimitiveType(data_[0]) == PrimitiveType::kString) {
    uint32_t len;
    std::memcpy(&len, data_ + 1, 4);
    len = bit_util::FromLittleEndian(len);
    return std::string_view(reinterpret_cast<const char*>(data_ + 5), len);
  }
  return Status::Invalid("VariantView::as_string: not a string");
}

Result<std::string_view> VariantView::as_binary() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kBinary) {
    return Status::Invalid("VariantView::as_binary: not binary");
  }
  uint32_t len;
  std::memcpy(&len, data_ + 1, 4);
  len = bit_util::FromLittleEndian(len);
  return std::string_view(reinterpret_cast<const char*>(data_ + 5), len);
}

Result<int32_t> VariantView::as_date() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kDate) {
    return Status::Invalid("VariantView::as_date: type mismatch");
  }
  int32_t value;
  std::memcpy(&value, data_ + 1, 4);
  return bit_util::FromLittleEndian(value);
}

Result<int64_t> VariantView::as_timestamp_micros() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kTimestampMicros) {
    return Status::Invalid("VariantView::as_timestamp_micros: type mismatch");
  }
  int64_t value;
  std::memcpy(&value, data_ + 1, 8);
  return bit_util::FromLittleEndian(value);
}

Result<int64_t> VariantView::as_timestamp_micros_ntz() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kTimestampMicrosNTZ) {
    return Status::Invalid("VariantView::as_timestamp_micros_ntz: type mismatch");
  }
  int64_t value;
  std::memcpy(&value, data_ + 1, 8);
  return bit_util::FromLittleEndian(value);
}

Result<int64_t> VariantView::as_timestamp_nanos() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kTimestampNanos) {
    return Status::Invalid("VariantView::as_timestamp_nanos: type mismatch");
  }
  int64_t value;
  std::memcpy(&value, data_ + 1, 8);
  return bit_util::FromLittleEndian(value);
}

Result<int64_t> VariantView::as_timestamp_nanos_ntz() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kTimestampNanosNTZ) {
    return Status::Invalid("VariantView::as_timestamp_nanos_ntz: type mismatch");
  }
  int64_t value;
  std::memcpy(&value, data_ + 1, 8);
  return bit_util::FromLittleEndian(value);
}

Result<int64_t> VariantView::as_time_ntz() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kTimeNTZ) {
    return Status::Invalid("VariantView::as_time_ntz: type mismatch");
  }
  int64_t value;
  std::memcpy(&value, data_ + 1, 8);
  return bit_util::FromLittleEndian(value);
}

Result<const uint8_t*> VariantView::as_uuid() const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kUUID) {
    return Status::Invalid("VariantView::as_uuid: type mismatch");
  }
  return data_ + 1;
}

Result<const uint8_t*> VariantView::as_decimal4(int32_t* scale) const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kDecimal4) {
    return Status::Invalid("VariantView::as_decimal4: type mismatch");
  }
  *scale = static_cast<int32_t>(data_[1]);
  return data_ + 2;
}

Result<const uint8_t*> VariantView::as_decimal8(int32_t* scale) const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kDecimal8) {
    return Status::Invalid("VariantView::as_decimal8: type mismatch");
  }
  *scale = static_cast<int32_t>(data_[1]);
  return data_ + 2;
}

Result<const uint8_t*> VariantView::as_decimal16(int32_t* scale) const {
  if (type_ != BasicType::kPrimitive ||
      GetPrimitiveType(data_[0]) != PrimitiveType::kDecimal16) {
    return Status::Invalid("VariantView::as_decimal16: type mismatch");
  }
  *scale = static_cast<int32_t>(data_[1]);
  return data_ + 2;
}

Result<VariantObjectView> VariantView::as_object() const {
  if (type_ != BasicType::kObject) {
    return Status::Invalid("VariantView::as_object: not an object");
  }
  return VariantObjectView::Make(*metadata_, data_, size_);
}

Result<VariantArrayView> VariantView::as_array() const {
  if (type_ != BasicType::kArray) {
    return Status::Invalid("VariantView::as_array: not an array");
  }
  return VariantArrayView::Make(*metadata_, data_, size_);
}

// ===========================================================================
// Public API: VariantObjectView
// ===========================================================================

VariantObjectView::VariantObjectView(const VariantMetadata* metadata, const uint8_t* data,
                                     int64_t length, int32_t num_fields,
                                     int8_t field_id_size, int8_t field_offset_size,
                                     int64_t id_start, int64_t offset_start,
                                     int64_t data_start)
    : metadata_(metadata),
      data_(data),
      length_(length),
      num_fields_(num_fields),
      field_id_size_(field_id_size),
      field_offset_size_(field_offset_size),
      id_start_(id_start),
      offset_start_(offset_start),
      data_start_(data_start) {}

Result<VariantObjectView> VariantObjectView::Make(const VariantMetadata& metadata,
                                                  const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("VariantObjectView: buffer is null or empty");
  }
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kObject) {
    return Status::Invalid("VariantObjectView: not an object");
  }

  uint8_t type_info = (header >> 2) & 0x3F;
  int8_t field_offset_size = static_cast<int8_t>((type_info & 0x03) + 1);
  int8_t field_id_size = static_cast<int8_t>(((type_info >> 2) & 0x03) + 1);
  bool is_large = ((type_info >> 4) & 0x01) != 0;
  int32_t num_fields_size = is_large ? 4 : 1;

  if (1 + num_fields_size > length) {
    return Status::Invalid("VariantObjectView: truncated num_fields");
  }
  auto num_fields_raw = static_cast<int64_t>(ReadUnsignedLE(data + 1, num_fields_size));

  int64_t id_start = 1 + num_fields_size;
  int64_t offset_start = id_start + num_fields_raw * field_id_size;
  int64_t data_start = offset_start + (num_fields_raw + 1) * field_offset_size;

  if (data_start > length) {
    return Status::Invalid("VariantObjectView: truncated object structure");
  }

  // Validate last offset is within buffer
  int64_t last_offset_pos = offset_start + num_fields_raw * field_offset_size;
  auto total_data =
      static_cast<int64_t>(ReadUnsignedLE(data + last_offset_pos, field_offset_size));
  if (data_start + total_data > length) {
    return Status::Invalid("VariantObjectView: object data exceeds buffer");
  }

  auto num_fields = static_cast<int32_t>(num_fields_raw);
  return VariantObjectView(&metadata, data, length, num_fields, field_id_size,
                           field_offset_size, id_start, offset_start, data_start);
}

uint32_t VariantObjectView::field_id_at(int32_t i) const {
  return ReadUnsignedLE(data_ + id_start_ + i * field_id_size_, field_id_size_);
}

int64_t VariantObjectView::value_offset_at(int32_t i) const {
  return data_start_ +
         static_cast<int64_t>(ReadUnsignedLE(
             data_ + offset_start_ + i * field_offset_size_, field_offset_size_));
}

Result<std::string_view> VariantObjectView::field_name(int32_t index) const {
  if (index < 0 || index >= num_fields_) {
    return Status::Invalid("VariantObjectView::field_name: index out of range");
  }
  auto id = field_id_at(index);
  if (id >= metadata_->strings.size()) {
    return Status::Invalid("VariantObjectView::field_name: field_id exceeds dictionary");
  }
  return metadata_->strings[id];
}

Result<VariantView> VariantObjectView::field_value(int32_t index) const {
  if (index < 0 || index >= num_fields_) {
    return Status::Invalid("VariantObjectView::field_value: index out of range");
  }
  int64_t offset = value_offset_at(index);
  return VariantView::Make(*metadata_, data_ + offset, length_ - offset);
}

std::optional<VariantView> VariantObjectView::get(std::string_view name) const {
  // Binary search — field IDs are sorted by lexicographic key order per spec.
  int32_t lo = 0, hi = num_fields_ - 1;
  while (lo <= hi) {
    int32_t mid = lo + (hi - lo) / 2;
    auto id = field_id_at(mid);
    if (id >= metadata_->strings.size()) {
      return std::nullopt;  // Malformed data — graceful degradation
    }
    auto key = metadata_->strings[id];
    if (key == name) {
      int64_t offset = value_offset_at(mid);
      auto view = VariantView::Make(*metadata_, data_ + offset, length_ - offset);
      if (view.ok()) return *view;
      return std::nullopt;
    }
    if (key < name)
      lo = mid + 1;
    else
      hi = mid - 1;
  }
  return std::nullopt;
}

bool VariantObjectView::contains(std::string_view name) const {
  return get(name).has_value();
}

std::optional<VariantObjectView::FieldLocation> VariantObjectView::locate(
    std::string_view name) const {
  // Binary search for the field
  int32_t lo = 0, hi = num_fields_ - 1;
  while (lo <= hi) {
    int32_t mid = lo + (hi - lo) / 2;
    auto id = field_id_at(mid);
    if (id >= metadata_->strings.size()) {
      return std::nullopt;
    }
    auto key = metadata_->strings[id];
    if (key == name) {
      int64_t offset = value_offset_at(mid);
      auto size_result = ValueSize(data_ + offset, length_ - offset);
      if (!size_result.ok()) return std::nullopt;
      return FieldLocation{offset, *size_result};
    }
    if (key < name)
      lo = mid + 1;
    else
      hi = mid - 1;
  }
  return std::nullopt;
}

// Object iterator
VariantObjectView::Iterator::Iterator(const VariantObjectView* obj, int32_t index)
    : obj_(obj), index_(index) {}

VariantObjectView::Iterator::value_type VariantObjectView::Iterator::operator*() const {
  auto name = obj_->field_name(index_).ValueOrDie();
  auto value = obj_->field_value(index_).ValueOrDie();
  return {name, value};
}

VariantObjectView::Iterator& VariantObjectView::Iterator::operator++() {
  ++index_;
  return *this;
}

bool VariantObjectView::Iterator::operator!=(const Iterator& other) const {
  return index_ != other.index_;
}

VariantObjectView::Iterator VariantObjectView::begin() const { return Iterator(this, 0); }

VariantObjectView::Iterator VariantObjectView::end() const {
  return Iterator(this, num_fields_);
}

// ===========================================================================
// Public API: VariantArrayView
// ===========================================================================

VariantArrayView::VariantArrayView(const VariantMetadata* metadata, const uint8_t* data,
                                   int64_t length, int32_t num_elements,
                                   int8_t offset_size, int64_t offset_start,
                                   int64_t data_start)
    : metadata_(metadata),
      data_(data),
      length_(length),
      num_elements_(num_elements),
      offset_size_(offset_size),
      offset_start_(offset_start),
      data_start_(data_start) {}

Result<VariantArrayView> VariantArrayView::Make(const VariantMetadata& metadata,
                                                const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("VariantArrayView: buffer is null or empty");
  }
  uint8_t header = data[0];
  if (GetBasicType(header) != BasicType::kArray) {
    return Status::Invalid("VariantArrayView: not an array");
  }

  uint8_t type_info = (header >> 2) & 0x3F;
  int8_t offset_size = static_cast<int8_t>((type_info & 0x03) + 1);
  bool is_large = ((type_info >> 2) & 0x01) != 0;
  int32_t num_elements_size = is_large ? 4 : 1;

  if (1 + num_elements_size > length) {
    return Status::Invalid("VariantArrayView: truncated num_elements");
  }
  auto num_elements_raw =
      static_cast<int64_t>(ReadUnsignedLE(data + 1, num_elements_size));

  int64_t offset_start = 1 + num_elements_size;
  int64_t data_start = offset_start + (num_elements_raw + 1) * offset_size;

  if (data_start > length) {
    return Status::Invalid("VariantArrayView: truncated array structure");
  }

  // Validate monotonicity and last offset in bounds
  uint32_t prev = 0;
  for (int64_t i = 0; i <= num_elements_raw; ++i) {
    auto off = ReadUnsignedLE(data + offset_start + i * offset_size, offset_size);
    if (i > 0 && off < prev) {
      return Status::Invalid(
          "VariantArrayView: offsets not monotonically non-decreasing at index ", i);
    }
    prev = off;
  }

  auto total_data = static_cast<int64_t>(prev);  // last offset = total data size
  if (data_start + total_data > length) {
    return Status::Invalid("VariantArrayView: array data exceeds buffer");
  }

  auto num_elements = static_cast<int32_t>(num_elements_raw);
  return VariantArrayView(&metadata, data, length, num_elements, offset_size,
                          offset_start, data_start);
}

int64_t VariantArrayView::element_offset_at(int32_t i) const {
  return data_start_ + static_cast<int64_t>(ReadUnsignedLE(
                           data_ + offset_start_ + i * offset_size_, offset_size_));
}

Result<VariantView> VariantArrayView::get(int32_t index) const {
  if (index < 0 || index >= num_elements_) {
    return Status::Invalid("VariantArrayView::get: index ", index, " out of range [0, ",
                           num_elements_, ")");
  }
  int64_t offset = element_offset_at(index);
  return VariantView::Make(*metadata_, data_ + offset, length_ - offset);
}

// Array iterator
VariantArrayView::Iterator::Iterator(const VariantArrayView* arr, int32_t index)
    : arr_(arr), index_(index) {}

VariantArrayView::Iterator::value_type VariantArrayView::Iterator::operator*() const {
  return arr_->get(index_).ValueOrDie();
}

VariantArrayView::Iterator& VariantArrayView::Iterator::operator++() {
  ++index_;
  return *this;
}

bool VariantArrayView::Iterator::operator!=(const Iterator& other) const {
  return index_ != other.index_;
}

VariantArrayView::Iterator VariantArrayView::begin() const { return Iterator(this, 0); }

VariantArrayView::Iterator VariantArrayView::end() const {
  return Iterator(this, num_elements_);
}

// ===========================================================================
// Widening numeric accessors
// ===========================================================================

Result<int64_t> VariantView::as_int64_coerced() const {
  if (type_ != BasicType::kPrimitive) {
    return Status::Invalid("VariantView::as_int64_coerced: not a primitive");
  }
  auto pt = GetPrimitiveType(data_[0]);
  switch (pt) {
    case PrimitiveType::kInt8:
      return static_cast<int64_t>(static_cast<int8_t>(data_[1]));
    case PrimitiveType::kInt16: {
      int16_t value;
      std::memcpy(&value, data_ + 1, 2);
      return static_cast<int64_t>(bit_util::FromLittleEndian(value));
    }
    case PrimitiveType::kInt32: {
      int32_t value;
      std::memcpy(&value, data_ + 1, 4);
      return static_cast<int64_t>(bit_util::FromLittleEndian(value));
    }
    case PrimitiveType::kInt64: {
      int64_t value;
      std::memcpy(&value, data_ + 1, 8);
      return bit_util::FromLittleEndian(value);
    }
    default:
      return Status::Invalid("VariantView::as_int64_coerced: not an integer type");
  }
}

Result<int32_t> VariantView::as_int32_coerced() const {
  if (type_ != BasicType::kPrimitive) {
    return Status::Invalid("VariantView::as_int32_coerced: not a primitive");
  }
  auto pt = GetPrimitiveType(data_[0]);
  switch (pt) {
    case PrimitiveType::kInt8:
      return static_cast<int32_t>(static_cast<int8_t>(data_[1]));
    case PrimitiveType::kInt16: {
      int16_t value;
      std::memcpy(&value, data_ + 1, 2);
      return static_cast<int32_t>(bit_util::FromLittleEndian(value));
    }
    case PrimitiveType::kInt32: {
      int32_t value;
      std::memcpy(&value, data_ + 1, 4);
      return bit_util::FromLittleEndian(value);
    }
    default:
      return Status::Invalid(
          "VariantView::as_int32_coerced: not a 32-bit-or-narrower "
          "integer type");
  }
}

Result<double> VariantView::as_double_coerced() const {
  if (type_ != BasicType::kPrimitive) {
    return Status::Invalid("VariantView::as_double_coerced: not a primitive");
  }
  auto pt = GetPrimitiveType(data_[0]);
  switch (pt) {
    case PrimitiveType::kInt8:
      return static_cast<double>(static_cast<int8_t>(data_[1]));
    case PrimitiveType::kInt16: {
      int16_t value;
      std::memcpy(&value, data_ + 1, 2);
      return static_cast<double>(bit_util::FromLittleEndian(value));
    }
    case PrimitiveType::kInt32: {
      int32_t value;
      std::memcpy(&value, data_ + 1, 4);
      return static_cast<double>(bit_util::FromLittleEndian(value));
    }
    case PrimitiveType::kInt64: {
      int64_t value;
      std::memcpy(&value, data_ + 1, 8);
      return static_cast<double>(bit_util::FromLittleEndian(value));
    }
    case PrimitiveType::kFloat: {
      float value;
      std::memcpy(&value, data_ + 1, 4);
      return static_cast<double>(bit_util::FromLittleEndian(value));
    }
    case PrimitiveType::kDouble: {
      double value;
      std::memcpy(&value, data_ + 1, 8);
      return bit_util::FromLittleEndian(value);
    }
    default:
      return Status::Invalid("VariantView::as_double_coerced: not a numeric type");
  }
}

// ===========================================================================
// ValidateVariant — full recursive validation
// ===========================================================================

namespace {

Status ValidateVariantRecursive(const VariantMetadata& metadata, const uint8_t* data,
                                int64_t length, int64_t offset, int32_t depth) {
  if (offset >= length) {
    return Status::Invalid("ValidateVariant: offset ", offset,
                           " at or beyond buffer length ", length);
  }
  if (depth > kMaxNestingDepth) {
    return Status::Invalid("ValidateVariant: nesting depth exceeds maximum of ",
                           kMaxNestingDepth);
  }

  uint8_t header = data[offset];
  auto basic_type = GetBasicType(header);

  switch (basic_type) {
    case BasicType::kPrimitive: {
      // Validate the value size is computable and within bounds
      ARROW_ASSIGN_OR_RAISE(auto size, ValueSize(data + offset, length - offset));
      if (offset + size > length) {
        return Status::Invalid("ValidateVariant: primitive value at offset ", offset,
                               " requires ", size, " bytes but only ", length - offset,
                               " available");
      }
      return Status::OK();
    }
    case BasicType::kShortString: {
      int32_t str_len = (header >> 2) & 0x3F;
      if (offset + 1 + str_len > length) {
        return Status::Invalid("ValidateVariant: truncated short string at offset ",
                               offset);
      }
      return Status::OK();
    }
    case BasicType::kObject: {
      // Construct a view (validates structure) then recursively validate children
      ARROW_ASSIGN_OR_RAISE(
          auto obj, VariantObjectView::Make(metadata, data + offset, length - offset));
      for (int32_t i = 0; i < obj.num_fields(); ++i) {
        // Validate field name (checks field_id within dictionary bounds)
        ARROW_RETURN_NOT_OK(obj.field_name(i).status());
        // Validate field value recursively
        ARROW_ASSIGN_OR_RAISE(auto field_view, obj.field_value(i));
        auto field_offset = field_view.data() - data;
        ARROW_RETURN_NOT_OK(
            ValidateVariantRecursive(metadata, data, length, field_offset, depth + 1));
      }
      return Status::OK();
    }
    case BasicType::kArray: {
      // Construct a view (validates structure) then recursively validate elements
      ARROW_ASSIGN_OR_RAISE(
          auto arr, VariantArrayView::Make(metadata, data + offset, length - offset));
      for (int32_t i = 0; i < arr.num_elements(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto elem_view, arr.get(i));
        auto elem_offset = elem_view.data() - data;
        ARROW_RETURN_NOT_OK(
            ValidateVariantRecursive(metadata, data, length, elem_offset, depth + 1));
      }
      return Status::OK();
    }
    default:
      return Status::Invalid("ValidateVariant: unknown basic type at offset ", offset);
  }
}

}  // namespace

Status ValidateVariant(const VariantMetadata& metadata, const uint8_t* data,
                       int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("ValidateVariant: buffer is null or empty");
  }
  return ValidateVariantRecursive(metadata, data, length, 0, 0);
}

// ===========================================================================

}  // namespace arrow::extension::variant

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

#include "parquet/variant/encoding.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"
#include "parquet/exception.h"
#include "parquet/variant/encoding_internal.h"

namespace parquet::variant {

namespace bit_util = ::arrow::bit_util;

namespace {

uint32_t ReadLittleEndian(std::string_view data, size_t offset, size_t width) {
  DCHECK_LE(width, sizeof(uint32_t));
  uint32_t value = 0;
  std::memcpy(&value, data.data() + offset, width);
  return bit_util::FromLittleEndian(value);
}

void CheckAvailable(std::string_view data, size_t offset, size_t size,
                    std::string_view context) {
  if (offset > data.size() || data.size() - offset < size) {
    throw ParquetInvalidOrCorruptedFileException("Invalid Variant encoding: truncated ",
                                                 context);
  }
}

size_t PrimitivePayloadSize(std::string_view value, size_t offset,
                            VariantPrimitiveType primitive) {
  switch (primitive) {
    case VariantPrimitiveType::kNull:
    case VariantPrimitiveType::kBooleanTrue:
    case VariantPrimitiveType::kBooleanFalse:
      return 0;
    case VariantPrimitiveType::kInt8:
      return 1;
    case VariantPrimitiveType::kInt16:
      return 2;
    case VariantPrimitiveType::kInt32:
    case VariantPrimitiveType::kDate:
    case VariantPrimitiveType::kFloat:
      return 4;
    case VariantPrimitiveType::kInt64:
    case VariantPrimitiveType::kDouble:
    case VariantPrimitiveType::kTimestampMicros:
    case VariantPrimitiveType::kTimestampNTZMicros:
    case VariantPrimitiveType::kTimeNTZMicros:
    case VariantPrimitiveType::kTimestampNanos:
    case VariantPrimitiveType::kTimestampNTZNanos:
      return 8;
    case VariantPrimitiveType::kDecimal4:
      return 5;
    case VariantPrimitiveType::kDecimal8:
      return 9;
    case VariantPrimitiveType::kDecimal16:
      return 17;
    case VariantPrimitiveType::kUuid:
      return 16;
    case VariantPrimitiveType::kBinary:
    case VariantPrimitiveType::kString: {
      CheckAvailable(value, offset, 4, "variable-length size");
      const uint32_t length = ReadLittleEndian(value, offset, 4);
      return 4 + static_cast<size_t>(length);
    }
  }
  throw ParquetInvalidOrCorruptedFileException(
      "Invalid Variant encoding: unknown primitive type");
}

size_t ParsePrimitive(std::string_view value, size_t offset,
                      VariantPrimitiveType primitive) {
  if (!internal::IsKnownVariantPrimitive(primitive)) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: unknown primitive type ", static_cast<int>(primitive));
  }

  const size_t payload_size = PrimitivePayloadSize(value, offset, primitive);
  CheckAvailable(value, offset, payload_size, "primitive value");

  if (internal::IsDecimalVariantPrimitive(primitive)) {
    const auto scale = static_cast<uint8_t>(value[offset]);
    internal::ValidateDecimalScale(scale);
  }

  if (primitive == VariantPrimitiveType::kString) {
    const uint32_t length = ReadLittleEndian(value, offset, 4);
    internal::ValidateUtf8(value.substr(offset + 4, length), "primitive string value");
  }

  return payload_size;
}

size_t ParseValue(std::string_view value, const VariantMetadataView& metadata,
                  VariantValueView* out);

size_t ParseArray(std::string_view value, const VariantMetadataView& metadata,
                  uint8_t header, VariantValueView* out) {
  const auto offset_size = static_cast<uint8_t>((header & 0x03) + 1);
  const bool is_large = (header & 0x04) != 0;
  const size_t count_size = is_large ? 4 : 1;

  size_t offset = 1;
  CheckAvailable(value, offset, count_size, "array size");
  const uint32_t num_elements = ReadLittleEndian(value, offset, count_size);
  offset += count_size;

  CheckAvailable(value, offset, (static_cast<size_t>(num_elements) + 1) * offset_size,
                 "array offsets");

  std::vector<uint32_t> offsets(num_elements + 1);
  for (uint32_t i = 0; i <= num_elements; ++i) {
    offsets[i] = ReadLittleEndian(value, offset, offset_size);
    offset += offset_size;
  }

  if (offsets[0] != 0) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: first array offset must be 0");
  }
  for (uint32_t i = 0; i < num_elements; ++i) {
    if (offsets[i] > offsets[i + 1]) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant encoding: array offsets must be monotonic");
    }
  }

  const size_t values_start = offset;
  const size_t total_value_size = offsets[num_elements];
  CheckAvailable(value, values_start, total_value_size, "array values");

  size_t current = 0;
  std::vector<std::string_view> array_elements;
  if (out != nullptr) {
    array_elements.reserve(num_elements);
  }
  for (uint32_t i = 0; i < num_elements; ++i) {
    if (offsets[i] != current) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant encoding: array offset does not match value boundary");
    }
    const size_t child_consumed =
        ParseValue(value.substr(values_start + current), metadata, /*out=*/nullptr);
    current += child_consumed;
    if (current != offsets[i + 1]) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant encoding: array value does not end at next offset");
    }
    if (out != nullptr) {
      array_elements.push_back(value.substr(values_start + offsets[i], child_consumed));
    }
  }

  if (current != total_value_size) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: array values have trailing data");
  }

  const size_t consumed = values_start + total_value_size;
  if (out != nullptr) {
    *out = VariantValueView(value.substr(0, consumed), VariantBasicType::kArray,
                            VariantArrayView(std::move(array_elements)));
  }
  return consumed;
}

size_t ParseObject(std::string_view value, const VariantMetadataView& metadata,
                   uint8_t header, VariantValueView* out) {
  const auto offset_size = static_cast<uint8_t>((header & 0x03) + 1);
  const auto id_size = static_cast<uint8_t>(((header >> 2) & 0x03) + 1);
  const bool is_large = (header & 0x10) != 0;
  const size_t count_size = is_large ? 4 : 1;

  size_t offset = 1;
  CheckAvailable(value, offset, count_size, "object size");
  const uint32_t num_elements = ReadLittleEndian(value, offset, count_size);
  offset += count_size;

  CheckAvailable(value, offset, static_cast<size_t>(num_elements) * id_size,
                 "object field ids");
  std::vector<uint32_t> field_ids(num_elements);
  for (uint32_t i = 0; i < num_elements; ++i) {
    field_ids[i] = ReadLittleEndian(value, offset, id_size);
    offset += id_size;
  }

  CheckAvailable(value, offset, (static_cast<size_t>(num_elements) + 1) * offset_size,
                 "object field offsets");
  std::vector<uint32_t> field_offsets(num_elements + 1);
  for (uint32_t i = 0; i <= num_elements; ++i) {
    field_offsets[i] = ReadLittleEndian(value, offset, offset_size);
    offset += offset_size;
  }

  const size_t values_start = offset;
  const size_t total_value_size = field_offsets[num_elements];
  if (num_elements == 0 && total_value_size != 0) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: empty object must have zero value size");
  }
  CheckAvailable(value, values_start, total_value_size, "object values");

  std::vector<VariantObjectField> object_fields;
  if (out != nullptr) {
    object_fields.reserve(num_elements);
  }

  std::string_view previous_name;
  for (uint32_t i = 0; i < num_elements; ++i) {
    if (field_ids[i] >= metadata.dictionary_size()) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant encoding: object field id ", field_ids[i],
          " is outside metadata dictionary of size ", metadata.dictionary_size());
    }
    const auto name = metadata.string(field_ids[i]);
    if (i > 0 && !(previous_name < name)) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant encoding: object field names must be sorted and unique");
    }
    previous_name = name;

    const auto field_offset = field_offsets[i];
    if (field_offset >= total_value_size) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant encoding: object field offset is outside values");
    }
  }

  std::vector<uint32_t> value_offsets = field_offsets;
  std::ranges::sort(value_offsets);
  if (std::ranges::adjacent_find(value_offsets) != value_offsets.end()) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: object field offsets must be unique");
  }
  if (value_offsets.front() != 0) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: object values have leading data");
  }

  for (uint32_t i = 0; i < num_elements; ++i) {
    const uint32_t start = value_offsets[i];
    const uint32_t end = value_offsets[i + 1];
    const size_t child_consumed =
        ParseValue(value.substr(values_start + start), metadata, /*out=*/nullptr);
    if (child_consumed != end - start) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant encoding: object value does not end at next value boundary");
    }
  }

  if (out != nullptr) {
    for (uint32_t i = 0; i < num_elements; ++i) {
      const auto field_offset = field_offsets[i];
      auto offset_it = std::ranges::lower_bound(value_offsets, field_offset);
      DCHECK(offset_it != value_offsets.end());
      DCHECK(offset_it + 1 != value_offsets.end());
      DCHECK(*offset_it == field_offset);
      const auto end = *(offset_it + 1);
      object_fields.push_back(VariantObjectField{
          .name = metadata.string(field_ids[i]),
          .field_id = field_ids[i],
          .value = value.substr(values_start + field_offset, end - field_offset)});
    }
  }

  const size_t consumed = values_start + total_value_size;
  if (out != nullptr) {
    *out = VariantValueView(value.substr(0, consumed), VariantBasicType::kObject,
                            VariantObjectView(std::move(object_fields)));
  }
  return consumed;
}

size_t ParseValue(std::string_view value, const VariantMetadataView& metadata,
                  VariantValueView* out) {
  CheckAvailable(value, 0, 1, "value header");

  const auto metadata_byte = static_cast<uint8_t>(value[0]);
  const auto basic_type = static_cast<VariantBasicType>(metadata_byte & 0x03);
  const auto header = static_cast<uint8_t>(metadata_byte >> 2);

  switch (basic_type) {
    case VariantBasicType::kPrimitive: {
      const auto primitive = static_cast<VariantPrimitiveType>(header);
      const size_t payload_size = ParsePrimitive(value, 1, primitive);
      const size_t consumed = 1 + payload_size;
      if (out != nullptr) {
        *out = VariantValueView(
            value.substr(0, consumed), VariantBasicType::kPrimitive,
            VariantPrimitiveView(primitive, value.substr(1, payload_size)));
      }
      return consumed;
    }
    case VariantBasicType::kShortString: {
      CheckAvailable(value, 1, header, "short string value");
      internal::ValidateUtf8(value.substr(1, header), "short string value");
      const size_t consumed = 1 + header;
      if (out != nullptr) {
        *out = VariantValueView(value.substr(0, consumed), VariantBasicType::kShortString,
                                VariantShortStringView(value.substr(1, header)));
      }
      return consumed;
    }
    case VariantBasicType::kObject:
      return ParseObject(value, metadata, header, out);
    case VariantBasicType::kArray:
      return ParseArray(value, metadata, header, out);
  }
  throw ParquetInvalidOrCorruptedFileException(
      "Invalid Variant encoding: unknown basic type");
}

}  // namespace

VariantMetadataView VariantMetadataView::Make(std::string_view metadata) {
  CheckAvailable(metadata, 0, 1, "metadata header");
  const auto header = static_cast<uint8_t>(metadata[0]);
  const auto version = static_cast<uint8_t>(header & internal::kMetadataVersionMask);
  if (version != internal::kVariantVersion) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant metadata: expected version 1, got ", static_cast<int>(version));
  }

  VariantMetadataView view;
  view.metadata_ = metadata;
  view.sorted_strings_ = (header & internal::kMetadataSortedStringsMask) != 0;
  view.offset_size_ = static_cast<uint8_t>(((header >> 6) & 0x03) + 1);

  CheckAvailable(metadata, 1, view.offset_size_, "metadata dictionary size");
  const uint32_t dictionary_size = ReadLittleEndian(metadata, 1, view.offset_size_);
  const size_t offsets_offset = 1 + view.offset_size_;
  CheckAvailable(metadata, offsets_offset,
                 (static_cast<size_t>(dictionary_size) + 1) * view.offset_size_,
                 "metadata dictionary offsets");

  std::vector<uint32_t> offsets(dictionary_size + 1);
  for (uint32_t i = 0; i <= dictionary_size; ++i) {
    offsets[i] = ReadLittleEndian(metadata, offsets_offset + i * view.offset_size_,
                                  view.offset_size_);
  }

  if (offsets[0] != 0) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant metadata: first dictionary offset must be 0");
  }
  for (uint32_t i = 0; i < dictionary_size; ++i) {
    if (offsets[i] > offsets[i + 1]) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant metadata: dictionary offsets must be monotonic");
    }
  }

  const size_t bytes_offset =
      offsets_offset + (static_cast<size_t>(dictionary_size) + 1) * view.offset_size_;
  const size_t bytes_size = offsets[dictionary_size];
  CheckAvailable(metadata, bytes_offset, bytes_size, "metadata dictionary bytes");
  if (metadata.size() != bytes_offset + bytes_size) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant metadata: trailing bytes after dictionary");
  }

  view.strings_.reserve(dictionary_size);
  for (uint32_t i = 0; i < dictionary_size; ++i) {
    auto string = metadata.substr(bytes_offset + offsets[i], offsets[i + 1] - offsets[i]);
    internal::ValidateUtf8(string, "metadata dictionary string");
    if (view.sorted_strings_ && i > 0 && !(view.strings_.back() < string)) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant metadata: sorted dictionary strings must be unique and "
          "lexicographically sorted");
    }
    view.strings_.push_back(string);
  }

  return view;
}

std::string_view VariantMetadataView::string(uint32_t field_id) const {
  DCHECK_LT(field_id, strings_.size());
  return strings_[field_id];
}

std::optional<uint32_t> VariantMetadataView::FindString(std::string_view value) const {
  if (sorted_strings_) {
    const auto it = std::ranges::lower_bound(strings_, value);
    if (it != strings_.end() && *it == value) {
      return static_cast<uint32_t>(it - strings_.begin());
    }
    return std::nullopt;
  }

  for (uint32_t i = 0; i < strings_.size(); ++i) {
    if (strings_[i] == value) {
      return i;
    }
  }
  return std::nullopt;
}

const VariantObjectField* VariantObjectView::FindField(std::string_view name) const {
  const auto it = std::ranges::lower_bound(fields_, name, {}, &VariantObjectField::name);
  return (it == fields_.end() || it->name != name) ? nullptr : &*it;
}

bool VariantObjectView::ContainsField(std::string_view name) const {
  // The Parquet Variant encoding requires object fields to be sorted by name, so field
  // lookup can use binary search.
  return std::ranges::binary_search(fields_, name, {}, &VariantObjectField::name);
}

VariantValueView VariantValueView::Make(std::string_view value,
                                        const VariantMetadataView& metadata) {
  VariantValueView view({}, VariantBasicType::kPrimitive,
                        VariantPrimitiveView(VariantPrimitiveType::kNull, {}));
  const size_t consumed = ParseValue(value, metadata, &view);
  if (consumed != value.size()) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: trailing bytes after value");
  }
  return view;
}

void VariantValueView::Validate(std::string_view value,
                                const VariantMetadataView& metadata) {
  const size_t consumed = ParseValue(value, metadata, /*out=*/nullptr);
  if (consumed != value.size()) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant encoding: trailing bytes after value");
  }
}

}  // namespace parquet::variant

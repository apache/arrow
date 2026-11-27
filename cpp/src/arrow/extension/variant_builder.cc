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

#include <algorithm>
#include <cstring>
#include <limits>

#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"

namespace arrow::extension::variant {

namespace {

/// \brief Compute the minimum number of bytes needed to represent a value.
/// \param[in] value Must be non-negative and fit in 4 bytes (represents a size or ID).
int32_t IntSize(int64_t value) {
  DCHECK_GE(value, 0);
  DCHECK_LE(value, static_cast<int64_t>(std::numeric_limits<uint32_t>::max()));
  if (value <= 0xFF) return 1;
  if (value <= 0xFFFF) return 2;
  if (value <= 0xFFFFFF) return 3;
  return 4;
}

/// \brief Write an unsigned integer in little-endian using nbytes bytes.
void WriteUnsignedLE(uint8_t* buf, int64_t value, int32_t nbytes) {
  for (int32_t i = 0; i < nbytes; ++i) {
    buf[i] = static_cast<uint8_t>((value >> (i * 8)) & 0xFF);
  }
}

/// \brief Write a little-endian value into a vector at a given position.
void WriteUnsignedLEAt(std::vector<uint8_t>& buf, int64_t pos, int64_t value,
                       int32_t nbytes) {
  for (int32_t i = 0; i < nbytes; ++i) {
    buf[pos + i] = static_cast<uint8_t>((value >> (i * 8)) & 0xFF);
  }
}

/// \brief Construct a primitive header byte.
uint8_t MakePrimitiveHeader(PrimitiveType type) {
  return static_cast<uint8_t>(BasicType::kPrimitive) | (static_cast<uint8_t>(type) << 2);
}

/// \brief Write a fixed-size numeric primitive into the buffer.
template <typename T>
void WritePrimitive(std::vector<uint8_t>& buf, PrimitiveType type, T value) {
  buf.push_back(MakePrimitiveHeader(type));
  value = bit_util::ToLittleEndian(value);
  auto ptr = reinterpret_cast<const uint8_t*>(&value);
  buf.insert(buf.end(), ptr, ptr + sizeof(T));
}

}  // namespace

// ---------------------------------------------------------------------------
// VariantBuilder implementation
// ---------------------------------------------------------------------------

VariantBuilder::VariantBuilder() = default;

VariantBuilder::VariantBuilder(const VariantMetadata& existing_metadata) {
  for (int32_t i = 0; i < static_cast<int32_t>(existing_metadata.strings.size()); ++i) {
    std::string key(existing_metadata.strings[i]);
    dict_[key] = static_cast<uint32_t>(i);
    dict_keys_.push_back(std::move(key));
  }
}

uint32_t VariantBuilder::AddKey(std::string_view key) {
  // Transparent hasher allows direct string_view lookup without constructing
  // a std::string. This eliminates per-call allocation/copy for existing keys.
  auto it = dict_.find(key);
  if (it != dict_.end()) {
    return it->second;
  }
  // Key is new — insert into the dictionary.
  auto id = static_cast<uint32_t>(dict_keys_.size());
  dict_keys_.emplace_back(key);
  dict_.emplace(dict_keys_.back(), id);
  return id;
}

void VariantBuilder::Reset() {
  buffer_.clear();
  dict_.clear();
  dict_keys_.clear();
}

int64_t VariantBuilder::Offset() const { return static_cast<int64_t>(buffer_.size()); }

int64_t VariantBuilder::NextElement(int64_t start) const { return Offset() - start; }

VariantBuilder::FieldEntry VariantBuilder::NextField(int64_t start,
                                                     std::string_view key) {
  auto id = AddKey(key);
  return FieldEntry{std::string(key), id, Offset() - start};
}

// --- Primitive setters ---

Status VariantBuilder::Null() {
  buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kNull));
  return Status::OK();
}

Status VariantBuilder::Bool(bool value) {
  buffer_.push_back(
      MakePrimitiveHeader(value ? PrimitiveType::kTrue : PrimitiveType::kFalse));
  return Status::OK();
}

Status VariantBuilder::Int(int64_t value) {
  if (value >= std::numeric_limits<int8_t>::min() &&
      value <= std::numeric_limits<int8_t>::max()) {
    return Int8(static_cast<int8_t>(value));
  }
  if (value >= std::numeric_limits<int16_t>::min() &&
      value <= std::numeric_limits<int16_t>::max()) {
    return Int16(static_cast<int16_t>(value));
  }
  if (value >= std::numeric_limits<int32_t>::min() &&
      value <= std::numeric_limits<int32_t>::max()) {
    return Int32(static_cast<int32_t>(value));
  }
  return Int64(value);
}

Status VariantBuilder::Int8(int8_t value) {
  buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kInt8));
  buffer_.push_back(static_cast<uint8_t>(value));
  return Status::OK();
}

Status VariantBuilder::Int16(int16_t value) {
  WritePrimitive(buffer_, PrimitiveType::kInt16, value);
  return Status::OK();
}

Status VariantBuilder::Int32(int32_t value) {
  WritePrimitive(buffer_, PrimitiveType::kInt32, value);
  return Status::OK();
}

Status VariantBuilder::Int64(int64_t value) {
  WritePrimitive(buffer_, PrimitiveType::kInt64, value);
  return Status::OK();
}

Status VariantBuilder::Float(float value) {
  WritePrimitive(buffer_, PrimitiveType::kFloat, value);
  return Status::OK();
}

Status VariantBuilder::Double(double value) {
  WritePrimitive(buffer_, PrimitiveType::kDouble, value);
  return Status::OK();
}

Status VariantBuilder::Date(int32_t days_since_epoch) {
  WritePrimitive(buffer_, PrimitiveType::kDate, days_since_epoch);
  return Status::OK();
}

Status VariantBuilder::TimestampMicros(int64_t micros) {
  WritePrimitive(buffer_, PrimitiveType::kTimestampMicros, micros);
  return Status::OK();
}

Status VariantBuilder::TimestampMicrosNTZ(int64_t micros) {
  WritePrimitive(buffer_, PrimitiveType::kTimestampMicrosNTZ, micros);
  return Status::OK();
}

Status VariantBuilder::TimeNTZ(int64_t micros) {
  WritePrimitive(buffer_, PrimitiveType::kTimeNTZ, micros);
  return Status::OK();
}

Status VariantBuilder::TimestampNanos(int64_t nanos) {
  WritePrimitive(buffer_, PrimitiveType::kTimestampNanos, nanos);
  return Status::OK();
}

Status VariantBuilder::TimestampNanosNTZ(int64_t nanos) {
  WritePrimitive(buffer_, PrimitiveType::kTimestampNanosNTZ, nanos);
  return Status::OK();
}

Status VariantBuilder::Decimal4(uint8_t scale, const uint8_t* value_bytes) {
  if (scale > kMaxDecimalScale) {
    return Status::Invalid("Variant decimal scale must be in range [0, ",
                           static_cast<int>(kMaxDecimalScale), "], got ",
                           static_cast<int>(scale));
  }
  buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kDecimal4));
  buffer_.push_back(scale);
  buffer_.insert(buffer_.end(), value_bytes, value_bytes + 4);
  return Status::OK();
}

Status VariantBuilder::Decimal8(uint8_t scale, const uint8_t* value_bytes) {
  if (scale > kMaxDecimalScale) {
    return Status::Invalid("Variant decimal scale must be in range [0, ",
                           static_cast<int>(kMaxDecimalScale), "], got ",
                           static_cast<int>(scale));
  }
  buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kDecimal8));
  buffer_.push_back(scale);
  buffer_.insert(buffer_.end(), value_bytes, value_bytes + 8);
  return Status::OK();
}

Status VariantBuilder::Decimal16(uint8_t scale, const uint8_t* value_bytes) {
  if (scale > kMaxDecimalScale) {
    return Status::Invalid("Variant decimal scale must be in range [0, ",
                           static_cast<int>(kMaxDecimalScale), "], got ",
                           static_cast<int>(scale));
  }
  buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kDecimal16));
  buffer_.push_back(scale);
  buffer_.insert(buffer_.end(), value_bytes, value_bytes + 16);  // 16-byte unscaled value
  return Status::OK();
}

Status VariantBuilder::String(std::string_view value) {
  if (value.size() <= static_cast<size_t>(kMaxShortStringLength)) {
    // Short string: length encoded in header bits 2-7
    uint8_t header = static_cast<uint8_t>(BasicType::kShortString) |
                     (static_cast<uint8_t>(value.size()) << 2);
    buffer_.push_back(header);
  } else {
    // Long string: primitive type kString + 4-byte LE length
    buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kString));
    auto len = static_cast<uint32_t>(value.size());
    len = bit_util::ToLittleEndian(len);
    auto ptr = reinterpret_cast<const uint8_t*>(&len);
    buffer_.insert(buffer_.end(), ptr, ptr + 4);
  }
  buffer_.insert(buffer_.end(), value.begin(), value.end());
  return Status::OK();
}

Status VariantBuilder::Binary(std::string_view value) {
  buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kBinary));
  auto len = static_cast<uint32_t>(value.size());
  len = bit_util::ToLittleEndian(len);
  auto ptr = reinterpret_cast<const uint8_t*>(&len);
  buffer_.insert(buffer_.end(), ptr, ptr + 4);
  buffer_.insert(buffer_.end(), value.begin(), value.end());
  return Status::OK();
}

Status VariantBuilder::UUID(const uint8_t* bytes) {
  buffer_.push_back(MakePrimitiveHeader(PrimitiveType::kUUID));
  buffer_.insert(buffer_.end(), bytes, bytes + kUUIDByteLength);
  return Status::OK();
}

// --- Container construction ---

Status VariantBuilder::FinishArray(int64_t start, const std::vector<int64_t>& offsets) {
  // Note: offset fields are at most 4 bytes, so individual variant values
  // cannot exceed ~4GB. This is not validated here; such values are not
  // practically expected (Parquet row group sizes are bounded well below this).
  auto data_size = Offset() - start;
  if (data_size < 0) {
    return Status::Invalid("VariantBuilder::FinishArray: invalid start position");
  }

  auto num_elements = static_cast<int64_t>(offsets.size());
  bool is_large = num_elements > kLargeContainerThreshold;
  int32_t size_bytes = is_large ? 4 : 1;
  int32_t offset_size = IntSize(data_size);
  int64_t header_size = 1 + size_bytes + (num_elements + 1) * offset_size;

  // Validate offsets are non-negative (caller-provided)
  for (int64_t i = 0; i < num_elements; ++i) {
    if (offsets[i] < 0) {
      return Status::Invalid("VariantBuilder::FinishArray: negative offset at index ", i);
    }
  }

  // Shift existing data to make room for the header
  buffer_.resize(buffer_.size() + header_size);
  std::memmove(buffer_.data() + start + header_size, buffer_.data() + start, data_size);

  // Write header byte
  uint8_t header = static_cast<uint8_t>(BasicType::kArray) |
                   (static_cast<uint8_t>(offset_size - 1) << 2);
  if (is_large) {
    header |= (1 << 4);
  }
  buffer_[start] = header;

  // Write num_elements
  WriteUnsignedLEAt(buffer_, start + 1, num_elements, size_bytes);

  // Write offsets
  int64_t offset_pos = start + 1 + size_bytes;
  for (int64_t i = 0; i < num_elements; ++i) {
    WriteUnsignedLEAt(buffer_, offset_pos + i * offset_size, offsets[i], offset_size);
  }
  // Last offset = total data size
  WriteUnsignedLEAt(buffer_, offset_pos + num_elements * offset_size, data_size,
                    offset_size);

  return Status::OK();
}

Status VariantBuilder::FinishObject(int64_t start, std::vector<FieldEntry>& fields) {
  auto data_size = Offset() - start;
  if (data_size < 0) {
    return Status::Invalid("VariantBuilder::FinishObject: invalid start position");
  }

  // Sort fields by key name lexicographically (spec requirement).
  // Skip the sort if fields are already in order (common for schema-driven insertion).
  if (!std::is_sorted(
          fields.begin(), fields.end(),
          [](const FieldEntry& a, const FieldEntry& b) { return a.key < b.key; })) {
    std::sort(fields.begin(), fields.end(),
              [](const FieldEntry& a, const FieldEntry& b) { return a.key < b.key; });
  }

  // Handle duplicate keys: reject by default, deduplicate if allowed.
  // When allow_duplicates_ is true (used by shredding reconstruction where
  // shredded fields and residual fields may overlap on malformed input),
  // last-value-wins semantics are applied. After sort, duplicates are adjacent;
  // we keep the last entry for each key group.
  if (!allow_duplicates_) {
    for (size_t i = 1; i < fields.size(); ++i) {
      if (fields[i].key == fields[i - 1].key) {
        return Status::Invalid("VariantBuilder: duplicate key '", fields[i].key, "'");
      }
    }
  } else {
    // Last-value-wins: for adjacent duplicates after sort, keep the last one.
    // Since std::unique keeps the FIRST of each group, we reverse-iterate.
    size_t write = 0;
    for (size_t i = 0; i < fields.size(); ++i) {
      // If next entry has same key, skip this one (keep the later one)
      if (i + 1 < fields.size() && fields[i].key == fields[i + 1].key) {
        continue;
      }
      if (write != i) {
        fields[write] = std::move(fields[i]);
      }
      ++write;
    }
    fields.resize(write);
  }

  auto num_fields = static_cast<int64_t>(fields.size());
  bool is_large = num_fields > kLargeContainerThreshold;
  int32_t size_bytes = is_large ? 4 : 1;

  // Compute id_size from max dictionary ID
  uint32_t max_id = 0;
  for (const auto& f : fields) {
    max_id = std::max(max_id, f.id);
  }
  int32_t id_size = IntSize(static_cast<int64_t>(max_id));
  int32_t offset_size = IntSize(data_size);

  int64_t header_size =
      1 + size_bytes + num_fields * id_size + (num_fields + 1) * offset_size;

  // Shift existing data to make room for the header
  buffer_.resize(buffer_.size() + header_size);
  std::memmove(buffer_.data() + start + header_size, buffer_.data() + start, data_size);

  // Write header byte: basic_type=2, offset_size in bits 2-3, id_size in bits 4-5,
  //                    is_large in bit 6
  uint8_t header = static_cast<uint8_t>(BasicType::kObject) |
                   (static_cast<uint8_t>(offset_size - 1) << 2) |
                   (static_cast<uint8_t>(id_size - 1) << 4);
  if (is_large) {
    header |= (1 << 6);
  }
  buffer_[start] = header;

  // Write num_fields
  WriteUnsignedLEAt(buffer_, start + 1, num_fields, size_bytes);

  // Write field IDs (sorted by key)
  int64_t id_pos = start + 1 + size_bytes;
  for (int64_t i = 0; i < num_fields; ++i) {
    WriteUnsignedLEAt(buffer_, id_pos + i * id_size, fields[i].id, id_size);
  }

  // Write field offsets (sorted by key)
  int64_t offset_pos = id_pos + num_fields * id_size;
  for (int64_t i = 0; i < num_fields; ++i) {
    WriteUnsignedLEAt(buffer_, offset_pos + i * offset_size, fields[i].offset,
                      offset_size);
  }
  // Last offset = total data size
  WriteUnsignedLEAt(buffer_, offset_pos + num_fields * offset_size, data_size,
                    offset_size);

  return Status::OK();
}

Result<VariantBuilder::EncodedVariant> VariantBuilder::Finish() {
  // Build metadata
  auto num_keys = static_cast<int32_t>(dict_keys_.size());

  // Compute total string data size
  int64_t total_string_size = 0;
  for (const auto& k : dict_keys_) {
    total_string_size += static_cast<int64_t>(k.size());
  }

  // Validate sizes fit within the spec's 4-byte offset limit.
  // Note: Go implementation enforces a stricter 128MB limit (metadataMaxSizeLimit).
  // We only enforce the spec's 4-byte offset maximum (~4GB), which is the correct
  // upper bound per the encoding format.
  if (total_string_size > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
    return Status::Invalid("VariantBuilder: total dictionary string data (",
                           total_string_size,
                           " bytes) exceeds maximum representable by 4-byte offsets");
  }

  // Compute the offset_size: must accommodate both the largest string offset
  // (total_string_size) and the dictionary_size field itself, since both use
  // offset_size bytes in the metadata encoding.
  int32_t offset_size =
      IntSize(std::max(total_string_size, static_cast<int64_t>(num_keys)));

  // Check if dictionary is sorted.
  // Uniqueness is guaranteed by dict_ (AddKey prevents duplicates),
  // so std::is_sorted with default < is sufficient for the "sorted and unique"
  // semantics required by the spec.
  // TODO: Cache the sorted state incrementally (check only newly-added keys
  // against the previous last key) to avoid O(n) rescan on every Finish() call.
  bool is_sorted = std::is_sorted(dict_keys_.begin(), dict_keys_.end());

  // Build metadata buffer
  std::vector<uint8_t> metadata;
  // Header byte
  uint8_t meta_header = kVariantVersion;
  if (is_sorted) {
    meta_header |= (1 << 4);
  }
  meta_header |= static_cast<uint8_t>((offset_size - 1) << 6);
  metadata.push_back(meta_header);

  // Dictionary size
  metadata.resize(metadata.size() + offset_size);
  WriteUnsignedLE(metadata.data() + 1, num_keys, offset_size);

  // String offsets
  int64_t cur_offset = 0;
  for (int32_t i = 0; i <= num_keys; ++i) {
    size_t pos = metadata.size();
    metadata.resize(pos + offset_size);
    WriteUnsignedLE(metadata.data() + pos, cur_offset, offset_size);
    if (i < num_keys) {
      cur_offset += static_cast<int64_t>(dict_keys_[i].size());
    }
  }

  // String data
  for (const auto& k : dict_keys_) {
    metadata.insert(metadata.end(), k.begin(), k.end());
  }

  EncodedVariant result;
  result.metadata = std::move(metadata);
  result.value = std::move(buffer_);

  // Note: dict_ and dict_keys_ are intentionally NOT cleared here.
  // The dictionary is preserved so the builder can encode multiple values
  // sharing the same key schema without re-adding keys. Call Reset()
  // explicitly to clear everything.
  buffer_.clear();

  return result;
}

// ---------------------------------------------------------------------------
// VariantBuilder RAII support
// ---------------------------------------------------------------------------

void VariantBuilder::Truncate(int64_t offset) {
  buffer_.resize(static_cast<size_t>(offset));
}

ObjectScope VariantBuilder::StartObject() { return ObjectScope(*this); }

ListScope VariantBuilder::StartList() { return ListScope(*this); }

// ---------------------------------------------------------------------------
// ObjectScope
// ---------------------------------------------------------------------------

ObjectScope::ObjectScope(VariantBuilder& parent)
    : parent_(&parent), start_offset_(parent.Offset()), committed_(false) {}

ObjectScope::ObjectScope(ObjectScope&& other) noexcept
    : parent_(other.parent_),
      start_offset_(other.start_offset_),
      fields_(std::move(other.fields_)),
      committed_(other.committed_) {
  other.committed_ = true;  // prevent double-rollback
}

ObjectScope::~ObjectScope() {
  if (!committed_ && parent_) {
    parent_->Truncate(start_offset_);
  }
}

Status ObjectScope::Insert(std::string_view key, std::nullptr_t) {
  fields_.push_back(parent_->NextField(start_offset_, key));
  return parent_->Null();
}

Status ObjectScope::Insert(std::string_view key, bool value) {
  fields_.push_back(parent_->NextField(start_offset_, key));
  return parent_->Bool(value);
}

Status ObjectScope::Insert(std::string_view key, int64_t value) {
  fields_.push_back(parent_->NextField(start_offset_, key));
  return parent_->Int(value);
}

Status ObjectScope::Insert(std::string_view key, double value) {
  fields_.push_back(parent_->NextField(start_offset_, key));
  return parent_->Double(value);
}

Status ObjectScope::Insert(std::string_view key, std::string_view value) {
  fields_.push_back(parent_->NextField(start_offset_, key));
  return parent_->String(value);
}

ObjectScope ObjectScope::InsertObject(std::string_view key) {
  fields_.push_back(parent_->NextField(start_offset_, key));
  return ObjectScope(*parent_);
}

ListScope ObjectScope::InsertList(std::string_view key) {
  fields_.push_back(parent_->NextField(start_offset_, key));
  return ListScope(*parent_);
}

Status ObjectScope::Finish() {
  ARROW_RETURN_NOT_OK(parent_->FinishObject(start_offset_, fields_));
  committed_ = true;
  return Status::OK();
}

// ---------------------------------------------------------------------------
// ListScope
// ---------------------------------------------------------------------------

ListScope::ListScope(VariantBuilder& parent)
    : parent_(&parent), start_offset_(parent.Offset()), committed_(false) {}

ListScope::ListScope(ListScope&& other) noexcept
    : parent_(other.parent_),
      start_offset_(other.start_offset_),
      offsets_(std::move(other.offsets_)),
      committed_(other.committed_) {
  other.committed_ = true;  // prevent double-rollback
}

ListScope::~ListScope() {
  if (!committed_ && parent_) {
    parent_->Truncate(start_offset_);
  }
}

Status ListScope::Append(std::nullptr_t) {
  offsets_.push_back(parent_->NextElement(start_offset_));
  return parent_->Null();
}

Status ListScope::Append(bool value) {
  offsets_.push_back(parent_->NextElement(start_offset_));
  return parent_->Bool(value);
}

Status ListScope::Append(int64_t value) {
  offsets_.push_back(parent_->NextElement(start_offset_));
  return parent_->Int(value);
}

Status ListScope::Append(double value) {
  offsets_.push_back(parent_->NextElement(start_offset_));
  return parent_->Double(value);
}

Status ListScope::Append(std::string_view value) {
  offsets_.push_back(parent_->NextElement(start_offset_));
  return parent_->String(value);
}

ObjectScope ListScope::AppendObject() {
  offsets_.push_back(parent_->NextElement(start_offset_));
  return ObjectScope(*parent_);
}

ListScope ListScope::AppendList() {
  offsets_.push_back(parent_->NextElement(start_offset_));
  return ListScope(*parent_);
}

Status ListScope::Finish() {
  ARROW_RETURN_NOT_OK(parent_->FinishArray(start_offset_, offsets_));
  committed_ = true;
  return Status::OK();
}

}  // namespace arrow::extension::variant

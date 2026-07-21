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

#include "parquet/variant/builder.h"

#include <algorithm>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/buffer_builder.h"
#include "arrow/builder.h"  // IWYU pragma: keep
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"
#include "parquet/exception.h"
#include "parquet/variant/append_table_internal.h"
#include "parquet/variant/binary_view_column_builder_internal.h"
#include "parquet/variant/decoding.h"
#include "parquet/variant/format_internal.h"

namespace parquet::variant {

using ::arrow::binary_view;
using ::arrow::BinaryViewArray;
using ::arrow::BufferBuilder;
using ::arrow::ExtensionType;
using ::arrow::field;
using ::arrow::struct_;
using ::arrow::StructType;
using ::arrow::Type;
using ::arrow::TypedBufferBuilder;
using ::arrow::extension::VariantExtensionType;
using internal::BinaryViewColumnBuilder;

namespace bit_util = ::arrow::bit_util;

namespace {

void AppendLittleEndian(BufferBuilder& out, uint32_t value, uint8_t width) {
  DCHECK_LE(width, sizeof(uint32_t));
  const auto little_endian = bit_util::ToLittleEndian(value);
  PARQUET_THROW_NOT_OK(out.Append(&little_endian, width));
}

template <typename T>
  requires(std::is_arithmetic_v<T>)
void AppendFixedLittleEndian(BufferBuilder& out, T value) {
  const auto little_endian = bit_util::ToLittleEndian(value);
  PARQUET_THROW_NOT_OK(out.Append(&little_endian, sizeof(T)));
}

template <typename DecimalValue>
std::array<uint8_t, DecimalValue::kByteWidth> ToLittleEndianBytes(
    const DecimalValue& value) {
  auto words = value.little_endian_array();
  std::array<uint8_t, DecimalValue::kByteWidth> out{};
  size_t offset = 0;
  for (const auto& word : words) {
    const auto little_endian_word = bit_util::ToLittleEndian(word);
    std::memcpy(out.data() + offset, &little_endian_word, sizeof(little_endian_word));
    offset += sizeof(little_endian_word);
  }
  return out;
}

uint8_t WidthForValue(uint64_t value) {
  if (value <= std::numeric_limits<uint8_t>::max()) {
    return 1;
  }
  if (value <= std::numeric_limits<uint16_t>::max()) {
    return 2;
  }
  if (value <= 0xFFFFFFU) {
    return 3;
  }
  return 4;
}

class MetadataBuilder {
 public:
  virtual ~MetadataBuilder() = default;

  virtual void Reserve(int64_t capacity) = 0;
  // Returns an existing or newly assigned field ID, or throws if the field
  // cannot be added or resolved.
  virtual uint32_t TryUpsert(std::string_view name) = 0;
  virtual std::string_view FieldName(uint32_t id) const = 0;
  virtual size_t size() const = 0;
  virtual void Truncate(size_t size) = 0;
  virtual void AppendTo(BufferBuilder& out) const = 0;
};

class WritableMetadataBuilder : public MetadataBuilder {
 public:
  void Reserve(int64_t capacity) override {
    if (capacity < 0) {
      throw ParquetException("Variant metadata capacity must be non-negative");
    }
    field_ids_.reserve(capacity);
  }

  uint32_t TryUpsert(std::string_view name) override {
    auto it = field_ids_.find(name);
    if (it != field_ids_.end()) {
      return it->second;
    }
    if (field_names_.size() >= std::numeric_limits<uint32_t>::max()) {
      throw ParquetException("Variant metadata dictionary is too large");
    }
    internal::ValidateUtf8(name, "metadata dictionary string");

    const auto id = field_names_.size();
    if (field_names_.empty()) {
      is_sorted_ = true;
    } else if (is_sorted_ && !(field_names_.back() < name)) {
      is_sorted_ = false;
    }
    field_names_.emplace_back(name);
    field_ids_.emplace(field_names_.back(), id);
    return static_cast<uint32_t>(id);
  }

  std::string_view FieldName(uint32_t id) const override {
    DCHECK_LT(id, field_names_.size());
    return field_names_[id];
  }

  size_t size() const override { return field_names_.size(); }

  void Truncate(size_t size) override {
    if (size == field_names_.size()) {
      return;
    }
    DCHECK_LT(size, field_names_.size());
    while (field_names_.size() > size) {
      const auto field_name = std::string_view{field_names_.back()};
      const auto erased = field_ids_.erase(field_name);
      DCHECK_EQ(erased, 1);
      field_names_.pop_back();
    }
    is_sorted_ = std::ranges::is_sorted(field_names_);
  }

  void AppendTo(BufferBuilder& out) const override {
    uint64_t bytes_size = 0;
    for (const auto& string : field_names_) {
      bytes_size += string.size();
    }
    if (field_names_.size() > std::numeric_limits<uint32_t>::max() ||
        bytes_size > std::numeric_limits<uint32_t>::max()) {
      throw ParquetException("Variant metadata dictionary is too large");
    }

    const uint8_t offset_size =
        WidthForValue(std::max<uint64_t>(field_names_.size(), bytes_size));
    PARQUET_THROW_NOT_OK(out.Reserve(
        1 + offset_size + (field_names_.size() + 1) * offset_size + bytes_size));

    const bool sorted_strings = !field_names_.empty() && is_sorted_;
    const auto header =
        static_cast<uint8_t>(internal::kVariantVersion |
                             (sorted_strings ? internal::kMetadataSortedStringsMask : 0) |
                             ((offset_size - 1) << 6));
    PARQUET_THROW_NOT_OK(out.Append(&header, sizeof(header)));
    AppendLittleEndian(out, static_cast<uint32_t>(field_names_.size()), offset_size);

    uint32_t offset = 0;
    AppendLittleEndian(out, offset, offset_size);
    for (const auto& string : field_names_) {
      offset += static_cast<uint32_t>(string.size());
      AppendLittleEndian(out, offset, offset_size);
    }
    for (const auto& string : field_names_) {
      PARQUET_THROW_NOT_OK(out.Append(string));
    }
  }

 private:
  std::deque<std::string> field_names_;
  std::unordered_map<std::string_view, uint32_t> field_ids_;
  bool is_sorted_ = false;
};

class ReadOnlyMetadataBuilder : public MetadataBuilder {
 public:
  explicit ReadOnlyMetadataBuilder(const VariantMetadataView& metadata)
      : metadata_(metadata) {}

  void Reserve(int64_t) override {}

  uint32_t TryUpsert(std::string_view name) override {
    auto it = field_ids_.find(name);
    if (it != field_ids_.end()) {
      return it->second;
    }
    auto field_id = metadata_.FindString(name);
    if (!field_id.has_value()) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid shredded Variant: field name '", name,
          "' is not in metadata dictionary");
    }
    field_ids_.emplace(metadata_.string(*field_id), *field_id);
    return *field_id;
  }

  std::string_view FieldName(uint32_t id) const override { return metadata_.string(id); }

  size_t size() const override { return metadata_.dictionary_size(); }

  void Truncate(size_t size) override { DCHECK_EQ(size, metadata_.dictionary_size()); }

  void AppendTo(BufferBuilder& out) const override {
    PARQUET_THROW_NOT_OK(out.Append(metadata_.metadata()));
  }

 private:
  const VariantMetadataView& metadata_;
  std::unordered_map<std::string_view, uint32_t> field_ids_;
};

class VariantValueWriter {
 public:
  explicit VariantValueWriter(BufferBuilder& out) : out_(out) {}

  template <VariantPrimitiveType type>
    requires internal::HeaderOnlyVariantPrimitive<type>
  void Append() {
    AppendPrimitiveHeader<type>();
  }

  template <VariantPrimitiveType type>
    requires internal::FixedVariantPrimitive<type>
  void Append(internal::VariantFixedPrimitiveTraits<type>::CType value) {
    AppendPrimitiveHeader<type>();
    if constexpr (sizeof(value) == 1) {
      const auto byte = static_cast<uint8_t>(value);
      PARQUET_THROW_NOT_OK(out_.Append(&byte, sizeof(byte)));
    } else {
      AppendFixedLittleEndian(out_, value);
    }
  }

  template <VariantPrimitiveType type>
    requires internal::DecimalVariantPrimitive<type>
  void Append(internal::VariantDecimalPrimitiveTraits<type>::CType value, uint8_t scale) {
    internal::ValidateDecimalScale(scale);
    AppendPrimitiveHeader<type>();
    PARQUET_THROW_NOT_OK(out_.Append(&scale, sizeof(scale)));
    const auto bytes = ToLittleEndianBytes(value);
    PARQUET_THROW_NOT_OK(out_.Append(bytes.data(), static_cast<int64_t>(bytes.size())));
  }

  template <VariantPrimitiveType type>
    requires internal::LengthPrefixedVariantPrimitive<type>
  void Append(std::string_view value) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
      throw ParquetException("Variant ",
                             type == VariantPrimitiveType::kBinary ? "binary" : "string",
                             " value is too large");
    }
    if constexpr (type == VariantPrimitiveType::kString) {
      internal::ValidateUtf8(value, "primitive string value");
    }
    AppendPrimitiveHeader<type>();
    AppendLittleEndian(out_, static_cast<uint32_t>(value.size()), 4);
    PARQUET_THROW_NOT_OK(out_.Append(value));
  }

  template <VariantPrimitiveType type>
    requires internal::UuidVariantPrimitive<type>
  void Append(std::string_view big_endian_bytes) {
    if (big_endian_bytes.size() != 16) {
      throw ParquetException("Variant UUID values must be 16 bytes");
    }
    AppendPrimitiveHeader<type>();
    PARQUET_THROW_NOT_OK(out_.Append(big_endian_bytes));
  }

  void AppendVariantNull() { Append<VariantPrimitiveType::kNull>(); }

  void AppendBoolean(bool value) {
    if (value) {
      Append<VariantPrimitiveType::kBooleanTrue>();
    } else {
      Append<VariantPrimitiveType::kBooleanFalse>();
    }
  }

  void AppendShortString(std::string_view value) {
    if (value.size() >= 64) {
      throw ParquetException("Variant short string value must be shorter than 64 bytes");
    }
    internal::ValidateUtf8(value, "short string value");
    const auto header = static_cast<uint8_t>(
        (value.size() << 2) | static_cast<uint8_t>(VariantBasicType::kShortString));
    PARQUET_THROW_NOT_OK(out_.Append(&header, sizeof(header)));
    PARQUET_THROW_NOT_OK(out_.Append(value));
  }

  void AppendTimestampMicros(int64_t value, bool adjusted_to_utc) {
    if (adjusted_to_utc) {
      Append<VariantPrimitiveType::kTimestampMicros>(value);
    } else {
      Append<VariantPrimitiveType::kTimestampNTZMicros>(value);
    }
  }

  void AppendTimestampNanos(int64_t value, bool adjusted_to_utc) {
    if (adjusted_to_utc) {
      Append<VariantPrimitiveType::kTimestampNanos>(value);
    } else {
      Append<VariantPrimitiveType::kTimestampNTZNanos>(value);
    }
  }

  void AppendEncodedValue(std::string_view value) {
    if (value.empty()) {
      throw ParquetException("Encoded Variant value length cannot be 0");
    }
    PARQUET_THROW_NOT_OK(out_.Append(value));
  }

 private:
  template <VariantPrimitiveType type>
  void AppendPrimitiveHeader() {
    const auto header =
        static_cast<uint8_t>((static_cast<uint8_t>(type) << 2) |
                             static_cast<uint8_t>(VariantBasicType::kPrimitive));
    PARQUET_THROW_NOT_OK(out_.Append(&header, sizeof(header)));
  }

  BufferBuilder& out_;
};

enum class VariantContainerKind { Object, List };

struct VariantFieldDescriptor {
  uint32_t field_id = 0;
  uint32_t offset = 0;
};

struct VariantBuildFrame {
  VariantContainerKind kind;
  int64_t value_start = 0;
  size_t metadata_size = 0;
  size_t parent_frame = 0;
  size_t parent_entry_count = 0;
  bool has_parent = false;
  std::vector<VariantFieldDescriptor> fields{};
  std::vector<uint32_t> offsets{};
  std::unordered_set<uint32_t> object_field_ids{};
  bool finished = false;
};

struct VariantBuildState {
  VariantBuildState(BufferBuilder& value, std::unique_ptr<MetadataBuilder> metadata)
      : value(value), root_value_start(value.length()), metadata(std::move(metadata)) {}

  void Reset(std::unique_ptr<MetadataBuilder> new_metadata) {
    root_value_start = value.length();
    metadata = std::move(new_metadata);
    frames.clear();
    root_has_value = false;
  }

  BufferBuilder& value;
  int64_t root_value_start;
  std::unique_ptr<MetadataBuilder> metadata;
  std::vector<VariantBuildFrame> frames;
  bool root_has_value = false;
};

void CheckRootWritable(const VariantBuildState& state) {
  if (!state.frames.empty()) {
    throw ParquetException("VariantBuilder has an active container");
  }
  if (state.root_has_value) {
    throw ParquetException("VariantBuilder already has a root value");
  }
}

template <VariantContainerKind kind>
void CheckTopFrame(const VariantBuildState& state, size_t frame_index) {
  if (state.frames.empty() || frame_index + 1 != state.frames.size()) {
    throw ParquetException("Variant nested builder is not the active container");
  }
  const auto& frame = state.frames.back();
  if (frame.finished || frame.kind != kind) {
    throw ParquetException("Variant nested builder has invalid state");
  }
}

void TruncateFrameEntries(VariantBuildFrame& frame, size_t entry_count) {
  if (frame.kind == VariantContainerKind::Object) {
    frame.fields.resize(entry_count);
    frame.object_field_ids.clear();
    for (const auto& field : frame.fields) {
      frame.object_field_ids.insert(field.field_id);
    }
  } else {
    frame.offsets.resize(entry_count);
  }
}

void MakeRoomForHeader(BufferBuilder& out, int64_t offset, int64_t header_size) {
  const int64_t old_size = out.length();
  DCHECK_LE(offset, old_size);
  PARQUET_THROW_NOT_OK(out.Reserve(header_size));
  uint8_t* data = out.mutable_data();
  std::memmove(data + offset + header_size, data + offset, old_size - offset);
  out.Rewind(offset);
}

void BuildObjectHeader(VariantBuildState& state, const VariantBuildFrame& frame,
                       uint32_t values_size) {
  if (frame.fields.size() > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant object has too many fields");
  }

  std::vector<VariantFieldDescriptor> fields = frame.fields;
  std::ranges::sort(fields, [&](const VariantFieldDescriptor& left,
                                const VariantFieldDescriptor& right) {
    return state.metadata->FieldName(left.field_id) <
           state.metadata->FieldName(right.field_id);
  });

  uint32_t max_field_id = 0;
  for (const auto& field : fields) {
    max_field_id = std::max(max_field_id, field.field_id);
  }
  const uint8_t id_size = WidthForValue(max_field_id);
  const uint8_t offset_size = WidthForValue(values_size);
  const bool is_large = fields.size() > std::numeric_limits<uint8_t>::max();
  const auto field_count = static_cast<int64_t>(fields.size());
  const uint8_t count_size = is_large ? 4 : 1;
  const int64_t header_size =
      1 + count_size + field_count * id_size + (field_count + 1) * offset_size;

  MakeRoomForHeader(state.value, frame.value_start, header_size);
  const auto header = static_cast<uint8_t>(
      ((((is_large ? 1 : 0) << 4) | ((id_size - 1) << 2) | (offset_size - 1)) << 2) |
      static_cast<uint8_t>(VariantBasicType::kObject));
  PARQUET_THROW_NOT_OK(state.value.Append(&header, sizeof(header)));
  AppendLittleEndian(state.value, static_cast<uint32_t>(fields.size()), count_size);
  for (const auto& field : fields) {
    AppendLittleEndian(state.value, field.field_id, id_size);
  }
  for (const auto& field : fields) {
    AppendLittleEndian(state.value, field.offset, offset_size);
  }
  AppendLittleEndian(state.value, values_size, offset_size);
  DCHECK_EQ(state.value.length(), frame.value_start + header_size);
  state.value.UnsafeAdvance(values_size);
}

void BuildListHeader(VariantBuildState& state, const VariantBuildFrame& frame,
                     uint32_t values_size) {
  if (frame.offsets.size() > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant array has too many elements");
  }

  const uint8_t offset_size = WidthForValue(values_size);
  const bool is_large = frame.offsets.size() > std::numeric_limits<uint8_t>::max();
  const auto element_count = static_cast<int64_t>(frame.offsets.size());
  const uint8_t count_size = is_large ? 4 : 1;
  const int64_t header_size = 1 + count_size + (element_count + 1) * offset_size;

  MakeRoomForHeader(state.value, frame.value_start, header_size);
  const auto header =
      static_cast<uint8_t>(((((is_large ? 1 : 0) << 2) | (offset_size - 1)) << 2) |
                           static_cast<uint8_t>(VariantBasicType::kArray));
  PARQUET_THROW_NOT_OK(state.value.Append(&header, sizeof(header)));
  AppendLittleEndian(state.value, static_cast<uint32_t>(frame.offsets.size()),
                     count_size);
  for (const auto offset : frame.offsets) {
    AppendLittleEndian(state.value, offset, offset_size);
  }
  AppendLittleEndian(state.value, values_size, offset_size);
  DCHECK_EQ(state.value.length(), frame.value_start + header_size);
  state.value.UnsafeAdvance(values_size);
}

using RootFinishCallback = std::function<void(VariantBuildState&)>;

template <VariantContainerKind kind>
void FinishFrame(VariantBuildState& state, size_t frame_index,
                 const RootFinishCallback& callback) {
  CheckTopFrame<kind>(state, frame_index);

  auto& frame = state.frames.back();
  const auto values_size = state.value.length() - frame.value_start;
  DCHECK_GE(values_size, 0);
  if (std::cmp_greater(values_size, std::numeric_limits<uint32_t>::max())) {
    throw ParquetException("Variant container values are too large");
  }

  if constexpr (kind == VariantContainerKind::Object) {
    BuildObjectHeader(state, frame, static_cast<uint32_t>(values_size));
  } else {
    BuildListHeader(state, frame, static_cast<uint32_t>(values_size));
  }

  const bool is_root = !frame.has_parent;
  frame.finished = true;
  state.frames.pop_back();
  if (!is_root) {
    return;
  }

  state.root_has_value = true;
  if (callback) {
    callback(state);
  }
}

template <typename Write>
void AppendValueWith(VariantBuildState& state, Write&& write) {
  CheckRootWritable(state);
  const auto value_size = state.value.length();
  VariantValueWriter writer(state.value);
  try {
    std::invoke(std::forward<Write>(write), writer);
  } catch (...) {
    state.value.Rewind(value_size);
    throw;
  }
  state.root_has_value = true;
}

std::pair<size_t, size_t> PrepareObjectField(VariantBuildState& state, size_t frame_index,
                                             std::string_view field_name) {
  CheckTopFrame<VariantContainerKind::Object>(state, frame_index);
  auto& frame = state.frames.back();
  const auto metadata_size = state.metadata->size();
  const auto entry_count = frame.fields.size();

  const auto field_id = state.metadata->TryUpsert(field_name);
  if (!frame.object_field_ids.insert(field_id).second) {
    state.metadata->Truncate(metadata_size);
    throw ParquetException("Duplicate Variant object field: ", field_name);
  }
  const auto offset = state.value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (std::cmp_greater(offset, std::numeric_limits<uint32_t>::max())) {
    state.metadata->Truncate(metadata_size);
    TruncateFrameEntries(frame, entry_count);
    throw ParquetException("Variant object values are too large");
  }
  frame.fields.push_back(VariantFieldDescriptor{.field_id = field_id,
                                                .offset = static_cast<uint32_t>(offset)});
  return {metadata_size, entry_count};
}

template <typename Write>
void AppendObjectValueWith(VariantBuildState& state, size_t frame_index,
                           std::string_view field_name, Write&& write) {
  const auto value_size = state.value.length();
  const auto [metadata_size, entry_count] =
      PrepareObjectField(state, frame_index, field_name);

  VariantValueWriter writer(state.value);
  try {
    std::invoke(std::forward<Write>(write), writer);
  } catch (...) {
    state.value.Rewind(value_size);
    state.metadata->Truncate(metadata_size);
    TruncateFrameEntries(state.frames[frame_index], entry_count);
    throw;
  }
}

size_t PrepareListElement(VariantBuildState& state, size_t frame_index) {
  CheckTopFrame<VariantContainerKind::List>(state, frame_index);
  auto& frame = state.frames.back();
  const auto entry_count = frame.offsets.size();

  const auto offset = state.value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (std::cmp_greater(offset, std::numeric_limits<uint32_t>::max())) {
    throw ParquetException("Variant array values are too large");
  }
  frame.offsets.push_back(static_cast<uint32_t>(offset));
  return entry_count;
}

template <typename Write>
void AppendListValueWith(VariantBuildState& state, size_t frame_index, Write&& write) {
  const auto value_size = state.value.length();
  const auto entry_count = PrepareListElement(state, frame_index);

  VariantValueWriter writer(state.value);
  try {
    std::invoke(std::forward<Write>(write), writer);
  } catch (...) {
    state.value.Rewind(value_size);
    TruncateFrameEntries(state.frames[frame_index], entry_count);
    throw;
  }
}

}  // namespace

namespace internal {

struct NestedVariantBuilderImpl {
  NestedVariantBuilderImpl(VariantBuildState& state, size_t frame_index,
                           RootFinishCallback callback)
      : state(state), frame_index(frame_index), callback(std::move(callback)) {}

  ~NestedVariantBuilderImpl() {
    if (!active || frame_index >= state.frames.size()) {
      return;
    }
    const auto frame = state.frames[frame_index];
    state.value.Rewind(frame.value_start);
    state.metadata->Truncate(frame.metadata_size);
    if (frame.has_parent && frame.parent_frame < state.frames.size()) {
      TruncateFrameEntries(state.frames[frame.parent_frame], frame.parent_entry_count);
    }
    state.frames.resize(frame_index);
  }

  VariantBuildState& state;
  size_t frame_index = 0;
  RootFinishCallback callback;
  bool active = true;
};

}  // namespace internal

struct VariantBuilder::Impl {
  explicit Impl(MemoryPool* pool, bool validate)
      : pool(pool),
        value_builder(pool),
        validate(validate),
        state(value_builder, std::make_unique<WritableMetadataBuilder>()) {}

  void Reset() {
    value_builder.Reset();
    state.Reset(std::make_unique<WritableMetadataBuilder>());
  }

  MemoryPool* pool;
  BufferBuilder value_builder;
  bool validate;
  VariantBuildState state;
};

VariantBuilder::VariantBuilder(MemoryPool* pool, bool validate)
    : impl_(std::make_unique<Impl>(pool, validate)) {}
VariantBuilder::~VariantBuilder() = default;
VariantBuilder::VariantBuilder(VariantBuilder&&) noexcept = default;
VariantBuilder& VariantBuilder::operator=(VariantBuilder&&) noexcept = default;

void VariantBuilder::ReserveFieldNames(int64_t capacity) {
  impl_->state.metadata->Reserve(capacity);
}

uint32_t VariantBuilder::AddFieldName(std::string_view name) {
  return impl_->state.metadata->TryUpsert(name);
}

#define DEFINE_DIRECT_APPEND(NAME, decl_args, ...)                  \
  void VariantBuilder::Append##NAME decl_args {                     \
    AppendValueWith(impl_->state, [&](VariantValueWriter& writer) { \
      writer.Append<VariantPrimitiveType::k##NAME>(__VA_ARGS__);    \
    });                                                             \
  }
PARQUET_VARIANT_DIRECT_APPEND_LIST(DEFINE_DIRECT_APPEND)
#undef DEFINE_DIRECT_APPEND

#define DEFINE_SPECIAL_APPEND(NAME, decl_args, ...)                 \
  void VariantBuilder::Append##NAME decl_args {                     \
    AppendValueWith(impl_->state, [&](VariantValueWriter& writer) { \
      writer.Append##NAME(__VA_ARGS__);                             \
    });                                                             \
  }
PARQUET_VARIANT_SPECIAL_APPEND_LIST(DEFINE_SPECIAL_APPEND)
DEFINE_SPECIAL_APPEND(EncodedValue, (std::string_view value), value)
#undef DEFINE_SPECIAL_APPEND

template <VariantContainerKind kind>
std::unique_ptr<internal::NestedVariantBuilderImpl> StartContainer(
    VariantBuildState& state) {
  CheckRootWritable(state);
  state.frames.push_back(VariantBuildFrame{.kind = kind,
                                           .value_start = state.value.length(),
                                           .metadata_size = state.metadata->size()});
  return std::make_unique<internal::NestedVariantBuilderImpl>(
      state, state.frames.size() - 1, RootFinishCallback{});
}

VariantObjectBuilder VariantBuilder::StartObject() {
  return VariantObjectBuilder(StartContainer<VariantContainerKind::Object>(impl_->state));
}

VariantListBuilder VariantBuilder::StartList() {
  return VariantListBuilder(StartContainer<VariantContainerKind::List>(impl_->state));
}

EncodedVariantValue VariantBuilder::Finish() {
  if (!impl_->state.frames.empty()) {
    throw ParquetException("Cannot finish VariantBuilder with active containers");
  }
  if (!impl_->state.root_has_value) {
    throw ParquetException("Cannot finish empty VariantBuilder");
  }

  BufferBuilder metadata_builder(impl_->pool);
  impl_->state.metadata->AppendTo(metadata_builder);
  PARQUET_ASSIGN_OR_THROW(auto metadata, metadata_builder.Finish());
  PARQUET_ASSIGN_OR_THROW(auto value, impl_->state.value.Finish());
  EncodedVariantValue out{.metadata = std::move(metadata), .value = std::move(value)};
  Reset();
  if (impl_->validate) {
    auto metadata_view = VariantMetadataView::Make(std::string_view{*out.metadata});
    VariantValueView::Validate(std::string_view{*out.value}, metadata_view);
  }
  return out;
}

void VariantBuilder::Reset() { impl_->Reset(); }

VariantObjectBuilder::VariantObjectBuilder(
    std::unique_ptr<internal::NestedVariantBuilderImpl> impl)
    : impl_(std::move(impl)) {}
VariantObjectBuilder::~VariantObjectBuilder() = default;
VariantObjectBuilder::VariantObjectBuilder(VariantObjectBuilder&&) noexcept = default;
VariantObjectBuilder& VariantObjectBuilder::operator=(VariantObjectBuilder&&) noexcept =
    default;

#ifdef _MSC_VER
#  define VARIANT_OBJECT_DECL(...) (std::string_view field_name, ##__VA_ARGS__)
#else
#  define VARIANT_OBJECT_DECL(...) \
    (std::string_view field_name __VA_OPT__(, ) __VA_ARGS__)  // NOLINT
#endif

#define DEFINE_OBJECT_DIRECT_APPEND(NAME, decl_args, ...)                              \
  void VariantObjectBuilder::Append##NAME VARIANT_OBJECT_DECL decl_args {              \
    AppendObjectValueWith(impl_->state, impl_->frame_index, field_name,                \
                          [&](VariantValueWriter& writer) {                            \
                            writer.Append<VariantPrimitiveType::k##NAME>(__VA_ARGS__); \
                          });                                                          \
  }
PARQUET_VARIANT_DIRECT_APPEND_LIST(DEFINE_OBJECT_DIRECT_APPEND)
#undef DEFINE_OBJECT_DIRECT_APPEND

#define DEFINE_OBJECT_SPECIAL_APPEND(NAME, decl_args, ...)                      \
  void VariantObjectBuilder::Append##NAME VARIANT_OBJECT_DECL decl_args {       \
    AppendObjectValueWith(                                                      \
        impl_->state, impl_->frame_index, field_name,                           \
        [&](VariantValueWriter& writer) { writer.Append##NAME(__VA_ARGS__); }); \
  }
PARQUET_VARIANT_SPECIAL_APPEND_LIST(DEFINE_OBJECT_SPECIAL_APPEND)
DEFINE_OBJECT_SPECIAL_APPEND(EncodedValue, (std::string_view value), value)
#undef DEFINE_OBJECT_SPECIAL_APPEND

#undef VARIANT_OBJECT_DECL

template <VariantContainerKind kind>
std::unique_ptr<internal::NestedVariantBuilderImpl> StartChildContainer(
    internal::NestedVariantBuilderImpl& impl, size_t metadata_size, size_t entry_count) {
  impl.state.frames.push_back(VariantBuildFrame{.kind = kind,
                                                .value_start = impl.state.value.length(),
                                                .metadata_size = metadata_size,
                                                .parent_frame = impl.frame_index,
                                                .parent_entry_count = entry_count,
                                                .has_parent = true});
  return std::make_unique<internal::NestedVariantBuilderImpl>(
      impl.state, impl.state.frames.size() - 1, impl.callback);
}

template <VariantContainerKind kind>
std::unique_ptr<internal::NestedVariantBuilderImpl> ObjectStartContainer(
    internal::NestedVariantBuilderImpl& impl, std::string_view field_name) {
  const auto [metadata_size, entry_count] =
      PrepareObjectField(impl.state, impl.frame_index, field_name);
  return StartChildContainer<kind>(impl, metadata_size, entry_count);
}

VariantObjectBuilder VariantObjectBuilder::StartObject(std::string_view field_name) {
  return VariantObjectBuilder(
      ObjectStartContainer<VariantContainerKind::Object>(*impl_, field_name));
}

VariantListBuilder VariantObjectBuilder::StartList(std::string_view field_name) {
  return VariantListBuilder(
      ObjectStartContainer<VariantContainerKind::List>(*impl_, field_name));
}

void VariantObjectBuilder::Finish() {
  FinishFrame<VariantContainerKind::Object>(impl_->state, impl_->frame_index,
                                            impl_->callback);
  impl_->active = false;
}

VariantListBuilder::VariantListBuilder(
    std::unique_ptr<internal::NestedVariantBuilderImpl> impl)
    : impl_(std::move(impl)) {}
VariantListBuilder::~VariantListBuilder() = default;
VariantListBuilder::VariantListBuilder(VariantListBuilder&&) noexcept = default;
VariantListBuilder& VariantListBuilder::operator=(VariantListBuilder&&) noexcept =
    default;

#define DEFINE_LIST_DIRECT_APPEND(NAME, decl_args, ...)                              \
  void VariantListBuilder::Append##NAME decl_args {                                  \
    AppendListValueWith(impl_->state, impl_->frame_index,                            \
                        [&](VariantValueWriter& writer) {                            \
                          writer.Append<VariantPrimitiveType::k##NAME>(__VA_ARGS__); \
                        });                                                          \
  }
PARQUET_VARIANT_DIRECT_APPEND_LIST(DEFINE_LIST_DIRECT_APPEND)
#undef DEFINE_LIST_DIRECT_APPEND

#define DEFINE_LIST_SPECIAL_APPEND(NAME, decl_args, ...)                        \
  void VariantListBuilder::Append##NAME decl_args {                             \
    AppendListValueWith(                                                        \
        impl_->state, impl_->frame_index,                                       \
        [&](VariantValueWriter& writer) { writer.Append##NAME(__VA_ARGS__); }); \
  }
PARQUET_VARIANT_SPECIAL_APPEND_LIST(DEFINE_LIST_SPECIAL_APPEND)
DEFINE_LIST_SPECIAL_APPEND(EncodedValue, (std::string_view value), value)
#undef DEFINE_LIST_SPECIAL_APPEND

template <VariantContainerKind kind>
std::unique_ptr<internal::NestedVariantBuilderImpl> ListStartContainer(
    internal::NestedVariantBuilderImpl& impl) {
  const auto entry_count = PrepareListElement(impl.state, impl.frame_index);
  return StartChildContainer<kind>(impl, impl.state.metadata->size(), entry_count);
}

VariantObjectBuilder VariantListBuilder::StartObject() {
  return VariantObjectBuilder(ListStartContainer<VariantContainerKind::Object>(*impl_));
}

VariantListBuilder VariantListBuilder::StartList() {
  return VariantListBuilder(ListStartContainer<VariantContainerKind::List>(*impl_));
}

void VariantListBuilder::Finish() {
  FinishFrame<VariantContainerKind::List>(impl_->state, impl_->frame_index,
                                          impl_->callback);
  impl_->active = false;
}

struct VariantArrayBuilder::Impl {
  explicit Impl(MemoryPool* pool)
      : pool(pool),
        metadata_column(pool),
        value_column(pool),
        validity_builder(pool),
        state(value_column.bytes_builder(), std::make_unique<WritableMetadataBuilder>()) {
  }

  void AppendNull() {
    const auto metadata_mark = metadata_column.mark();
    const auto value_mark = value_column.mark();
    try {
      metadata_column.AppendEmptyValue();
      value_column.AppendEmptyValue();
      PARQUET_THROW_NOT_OK(validity_builder.Append(false));
    } catch (...) {
      metadata_column.Rollback(metadata_mark);
      value_column.Rollback(value_mark);
      throw;
    }
  }

  template <typename Write>
  void AppendValue(Write&& write) {
    state.Reset(std::make_unique<WritableMetadataBuilder>());
    AppendValueWith(state, std::forward<Write>(write));
    CommitBuiltRow(state);
  }

  void AppendEncoded(const EncodedVariantValue& value) {
    if (value.metadata == nullptr || value.value == nullptr) {
      throw ParquetException(
          "Encoded Variant metadata and value buffers must be non-null");
    }
    const auto metadata_bytes = std::string_view{*value.metadata};
    const auto value_bytes = std::string_view{*value.value};
    CommitEncodedRow(metadata_bytes, value_bytes);
  }

  void CommitBuiltRow(VariantBuildState& state) {
    if (!state.frames.empty()) {
      throw ParquetException("Cannot commit Variant row with active containers");
    }
    if (!state.root_has_value) {
      throw ParquetException("Cannot commit empty Variant row");
    }

    const auto value_start = state.root_value_start;
    const auto metadata_mark = metadata_column.mark();
    auto value_mark = value_column.mark();
    DCHECK_LT(value_start, value_mark.bytes_length);
    value_mark.bytes_length = value_start;
    try {
      const auto metadata_start = metadata_column.bytes_builder().length();
      state.metadata->AppendTo(metadata_column.bytes_builder());
      metadata_column.AppendView(metadata_start);
      value_column.AppendView(value_start);
      PARQUET_THROW_NOT_OK(validity_builder.Append(true));
    } catch (...) {
      metadata_column.Rollback(metadata_mark);
      value_column.Rollback(value_mark);
      throw;
    }
  }

  std::shared_ptr<VariantArray> Finish() {
    DCHECK_EQ(metadata_column.length(), value_column.length());
    DCHECK_EQ(metadata_column.length(), validity_builder.length());

    const int64_t null_count = validity_builder.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    if (null_count > 0) {
      PARQUET_THROW_NOT_OK(validity_builder.Finish(&null_bitmap));
    }

    auto metadata = metadata_column.Finish();
    auto value = value_column.Finish();
    auto storage_type = struct_({field("metadata", binary_view(), /*nullable=*/false),
                                 field("value", binary_view(), /*nullable=*/false)});
    PARQUET_ASSIGN_OR_THROW(auto storage,
                            StructArray::Make({metadata, value}, storage_type->fields(),
                                              std::move(null_bitmap), null_count));
    return MakeVariantArrayFromStorage(std::move(storage));
  }

  MemoryPool* pool;
  BinaryViewColumnBuilder metadata_column;
  BinaryViewColumnBuilder value_column;
  TypedBufferBuilder<bool> validity_builder;
  VariantBuildState state;

 private:
  void CommitEncodedRow(std::string_view metadata_bytes, std::string_view value_bytes) {
    const auto metadata_mark = metadata_column.mark();
    const auto value_mark = value_column.mark();
    try {
      const auto metadata_start = metadata_column.bytes_builder().length();
      PARQUET_THROW_NOT_OK(metadata_column.bytes_builder().Append(metadata_bytes));
      const auto value_start = value_column.bytes_builder().length();
      PARQUET_THROW_NOT_OK(value_column.bytes_builder().Append(value_bytes));

      metadata_column.AppendView(metadata_start);
      value_column.AppendView(value_start);
      PARQUET_THROW_NOT_OK(validity_builder.Append(true));
    } catch (...) {
      metadata_column.Rollback(metadata_mark);
      value_column.Rollback(value_mark);
      throw;
    }
  }
};

VariantArrayBuilder::VariantArrayBuilder(MemoryPool* pool)
    : impl_(std::make_unique<Impl>(pool)) {}
VariantArrayBuilder::~VariantArrayBuilder() = default;
VariantArrayBuilder::VariantArrayBuilder(VariantArrayBuilder&&) noexcept = default;
VariantArrayBuilder& VariantArrayBuilder::operator=(VariantArrayBuilder&&) noexcept =
    default;

void VariantArrayBuilder::AppendNull() { impl_->AppendNull(); }

#define DEFINE_ARRAY_DIRECT_APPEND(NAME, decl_args, ...)         \
  void VariantArrayBuilder::Append##NAME decl_args {             \
    impl_->AppendValue([&](VariantValueWriter& writer) {         \
      writer.Append<VariantPrimitiveType::k##NAME>(__VA_ARGS__); \
    });                                                          \
  }
PARQUET_VARIANT_DIRECT_APPEND_LIST(DEFINE_ARRAY_DIRECT_APPEND)
#undef DEFINE_ARRAY_DIRECT_APPEND

#define DEFINE_ARRAY_SPECIAL_APPEND(NAME, decl_args, ...)                       \
  void VariantArrayBuilder::Append##NAME decl_args {                            \
    impl_->AppendValue(                                                         \
        [&](VariantValueWriter& writer) { writer.Append##NAME(__VA_ARGS__); }); \
  }
PARQUET_VARIANT_SPECIAL_APPEND_LIST(DEFINE_ARRAY_SPECIAL_APPEND)
#undef DEFINE_ARRAY_SPECIAL_APPEND

void VariantArrayBuilder::AppendEncoded(const EncodedVariantValue& value) {
  impl_->AppendEncoded(value);
}

template <VariantContainerKind kind, typename ImplType>
std::unique_ptr<internal::NestedVariantBuilderImpl> ArrayStartContainer(ImplType& impl) {
  impl.state.Reset(std::make_unique<WritableMetadataBuilder>());
  impl.state.frames.push_back(
      VariantBuildFrame{.kind = kind,
                        .value_start = impl.state.value.length(),
                        .metadata_size = impl.state.metadata->size()});
  return std::make_unique<internal::NestedVariantBuilderImpl>(
      impl.state, impl.state.frames.size() - 1,
      std::bind_front(&ImplType::CommitBuiltRow, &impl));
}

VariantObjectBuilder VariantArrayBuilder::StartObject() {
  return VariantObjectBuilder(ArrayStartContainer<VariantContainerKind::Object>(*impl_));
}

VariantListBuilder VariantArrayBuilder::StartList() {
  return VariantListBuilder(ArrayStartContainer<VariantContainerKind::List>(*impl_));
}

std::shared_ptr<VariantArray> VariantArrayBuilder::Finish() {
  auto out = impl_->Finish();
  Reset();
  return out;
}

void VariantArrayBuilder::Reset() { impl_ = std::make_unique<Impl>(impl_->pool); }

struct VariantValueArrayBuilder::Impl {
  explicit Impl(MemoryPool* pool)
      : pool(pool), value_column(pool), validity_builder(pool) {}

  void AppendNull() {
    const auto value_mark = value_column.mark();
    try {
      value_column.AppendEmptyValue();
      PARQUET_THROW_NOT_OK(validity_builder.Append(false));
    } catch (...) {
      value_column.Rollback(value_mark);
      throw;
    }
  }

  void AppendEncodedValue(std::string_view value) {
    if (value.empty()) {
      throw ParquetException("Encoded Variant value length cannot be 0");
    }
    const auto value_mark = value_column.mark();
    try {
      const auto value_start = value_column.bytes_builder().length();
      PARQUET_THROW_NOT_OK(value_column.bytes_builder().Append(value));
      value_column.AppendView(value_start);
      PARQUET_THROW_NOT_OK(validity_builder.Append(true));
    } catch (...) {
      value_column.Rollback(value_mark);
      throw;
    }
  }

  void CommitBuiltRow(VariantBuildState& state) {
    if (!state.frames.empty()) {
      throw ParquetException("Cannot commit Variant value row with active containers");
    }
    if (!state.root_has_value) {
      throw ParquetException("Cannot commit empty Variant value row");
    }

    const auto value_start = state.root_value_start;
    auto value_mark = value_column.mark();
    DCHECK_LT(value_start, value_mark.bytes_length);
    value_mark.bytes_length = value_start;
    try {
      value_column.AppendView(value_start);
      PARQUET_THROW_NOT_OK(validity_builder.Append(true));
    } catch (...) {
      value_column.Rollback(value_mark);
      throw;
    }
  }

  void DiscardBuiltRow(VariantBuildState& state) {
    value_column.bytes_builder().Rewind(state.root_value_start);
    state.frames.clear();
    state.root_has_value = false;
  }

  std::shared_ptr<BinaryViewArray> Finish() {
    const int64_t null_count = validity_builder.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    if (null_count > 0) {
      PARQUET_THROW_NOT_OK(validity_builder.Finish(&null_bitmap));
    }
    return value_column.Finish(std::move(null_bitmap), null_count);
  }

  MemoryPool* pool;
  BinaryViewColumnBuilder value_column;
  TypedBufferBuilder<bool> validity_builder;
};

VariantValueArrayBuilder::VariantValueArrayBuilder(MemoryPool* pool)
    : impl_(std::make_unique<Impl>(pool)) {}
VariantValueArrayBuilder::~VariantValueArrayBuilder() = default;
VariantValueArrayBuilder::VariantValueArrayBuilder(VariantValueArrayBuilder&&) noexcept =
    default;
VariantValueArrayBuilder& VariantValueArrayBuilder::operator=(
    VariantValueArrayBuilder&&) noexcept = default;

void VariantValueArrayBuilder::AppendNull() { impl_->AppendNull(); }

void VariantValueArrayBuilder::AppendEncodedValue(std::string_view value) {
  impl_->AppendEncodedValue(value);
}

VariantValueRowBuilder VariantValueArrayBuilder::BindMetadata(
    const VariantMetadataView& metadata) {
  return {*this, metadata};
}

std::shared_ptr<BinaryViewArray> VariantValueArrayBuilder::Finish() {
  auto out = impl_->Finish();
  Reset();
  return out;
}

void VariantValueArrayBuilder::Reset() { impl_ = std::make_unique<Impl>(impl_->pool); }

struct VariantValueRowBuilder::Impl {
  Impl(VariantValueArrayBuilder::Impl& parent, const VariantMetadataView& metadata)
      : parent(parent),
        state(parent.value_column.bytes_builder(),
              std::make_unique<ReadOnlyMetadataBuilder>(metadata)) {}

  ~Impl() {
    if (!finished) {
      parent.DiscardBuiltRow(state);
    }
  }

  VariantValueArrayBuilder::Impl& parent;
  VariantBuildState state;
  bool finished = false;
};

VariantValueRowBuilder::VariantValueRowBuilder(VariantValueArrayBuilder& parent,
                                               const VariantMetadataView& metadata)
    : impl_(std::make_unique<Impl>(*parent.impl_, metadata)) {}
VariantValueRowBuilder::~VariantValueRowBuilder() = default;
VariantValueRowBuilder::VariantValueRowBuilder(VariantValueRowBuilder&&) noexcept =
    default;
VariantValueRowBuilder& VariantValueRowBuilder::operator=(
    VariantValueRowBuilder&&) noexcept = default;

#define DEFINE_VALUE_ROW_DIRECT_APPEND(NAME, decl_args, ...)        \
  void VariantValueRowBuilder::Append##NAME decl_args {             \
    AppendValueWith(impl_->state, [&](VariantValueWriter& writer) { \
      writer.Append<VariantPrimitiveType::k##NAME>(__VA_ARGS__);    \
    });                                                             \
  }
PARQUET_VARIANT_DIRECT_APPEND_LIST(DEFINE_VALUE_ROW_DIRECT_APPEND)
#undef DEFINE_VALUE_ROW_DIRECT_APPEND

#define DEFINE_VALUE_ROW_SPECIAL_APPEND(NAME, decl_args, ...)       \
  void VariantValueRowBuilder::Append##NAME decl_args {             \
    AppendValueWith(impl_->state, [&](VariantValueWriter& writer) { \
      writer.Append##NAME(__VA_ARGS__);                             \
    });                                                             \
  }
PARQUET_VARIANT_SPECIAL_APPEND_LIST(DEFINE_VALUE_ROW_SPECIAL_APPEND)
DEFINE_VALUE_ROW_SPECIAL_APPEND(EncodedValue, (std::string_view value), value)
#undef DEFINE_VALUE_ROW_SPECIAL_APPEND

VariantObjectBuilder VariantValueRowBuilder::StartObject() {
  return VariantObjectBuilder(StartContainer<VariantContainerKind::Object>(impl_->state));
}

VariantListBuilder VariantValueRowBuilder::StartList() {
  return VariantListBuilder(StartContainer<VariantContainerKind::List>(impl_->state));
}

void VariantValueRowBuilder::Finish() {
  if (impl_->finished) {
    throw ParquetException("Variant value row is already finished");
  }
  impl_->parent.CommitBuiltRow(impl_->state);
  impl_->finished = true;
}

std::shared_ptr<VariantArray> MakeVariantArrayFromStorage(
    std::shared_ptr<StructArray> storage) {
  if (storage == nullptr) {
    throw ParquetException("Variant storage array must be non-null");
  }
  PARQUET_ASSIGN_OR_THROW(auto type, VariantExtensionType::Make(storage->type()));
  auto array = ExtensionType::WrapArray(type, std::move(storage));
  return std::static_pointer_cast<VariantArray>(array);
}

std::shared_ptr<VariantArray> MakeVariantArrayFromChildren(
    std::shared_ptr<DataType> storage_type, std::vector<std::shared_ptr<Array>> children,
    std::shared_ptr<Buffer> null_bitmap) {
  if (storage_type->id() != Type::STRUCT) {
    throw ParquetException("Variant storage type must be struct, got ",
                           storage_type->ToString());
  }

  const auto& struct_type =
      ::arrow::internal::checked_cast<const StructType&>(*storage_type);
  if (children.size() != static_cast<size_t>(struct_type.num_fields())) {
    throw ParquetException("Variant storage expected ", struct_type.num_fields(),
                           " children, got ", children.size());
  }

  const int64_t length = children.empty() ? 0 : children[0]->length();
  for (int i = 0; i < struct_type.num_fields(); ++i) {
    if (children[i] == nullptr) {
      throw ParquetException("Variant storage child ", i, " is null");
    }
    if (!children[i]->type()->Equals(struct_type.field(i)->type())) {
      throw ParquetException("Variant storage child ", i, " has type ",
                             children[i]->type()->ToString(), ", expected ",
                             struct_type.field(i)->type()->ToString());
    }
    if (children[i]->length() != length) {
      throw ParquetException("Variant storage child lengths must match");
    }
  }

  PARQUET_ASSIGN_OR_THROW(auto storage,
                          StructArray::Make(std::move(children), struct_type.fields(),
                                            std::move(null_bitmap)));
  return MakeVariantArrayFromStorage(std::move(storage));
}

}  // namespace parquet::variant

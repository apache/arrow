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

#include "arrow/buffer_builder.h"
#include "arrow/builder.h"  // IWYU pragma: keep
#include "arrow/util/checked_cast.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"
#include "parquet/variant/encoding.h"
#include "parquet/variant/encoding_internal.h"

namespace parquet::variant {

using ::arrow::binary;
using ::arrow::BinaryBuilder;
using ::arrow::BooleanArray;
using ::arrow::BooleanBuilder;
using ::arrow::BufferBuilder;
using ::arrow::ExtensionType;
using ::arrow::field;
using ::arrow::struct_;
using ::arrow::StructType;
using ::arrow::Type;
using ::arrow::extension::VariantExtensionType;

namespace bit_util = ::arrow::bit_util;

namespace {

Status AppendLittleEndian(BufferBuilder& out, uint32_t value, uint8_t width) {
  DCHECK_LE(width, sizeof(uint32_t));
  const auto little_endian = bit_util::ToLittleEndian(value);
  return out.Append(&little_endian, width);
}

template <typename T>
  requires(std::is_arithmetic_v<T>)
Status AppendFixedLittleEndian(BufferBuilder& out, T value) {
  const auto little_endian = bit_util::ToLittleEndian(value);
  return out.Append(&little_endian, sizeof(T));
}

void AppendLittleEndianToString(std::string& out, uint32_t value, uint8_t width) {
  DCHECK_LE(width, sizeof(uint32_t));
  const auto little_endian = bit_util::ToLittleEndian(value);
  out.append(reinterpret_cast<const char*>(&little_endian), width);
}

Status InsertBytes(BufferBuilder& out, int64_t offset, std::string_view bytes) {
  const int64_t old_size = out.length();
  const auto insert_size = bytes.size();
  ARROW_RETURN_NOT_OK(out.Reserve(insert_size));
  uint8_t* data = out.mutable_data();
  std::memmove(data + offset + insert_size, data + offset, old_size - offset);
  std::memcpy(data + offset, bytes.data(), insert_size);
  out.UnsafeAdvance(insert_size);
  return Status::OK();
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

class VariantMetadataBuilder {
 public:
  explicit VariantMetadataBuilder(MemoryPool* pool) : pool_(pool) {}

  Status Reserve(int64_t capacity) {
    if (capacity < 0) {
      return Status::Invalid("Variant metadata capacity must be non-negative");
    }
    field_ids_.reserve(capacity);
    return Status::OK();
  }

  Result<uint32_t> Upsert(std::string_view name) {
    auto it = field_ids_.find(name);
    if (it != field_ids_.end()) {
      return it->second;
    }
    if (field_names_.size() >= std::numeric_limits<uint32_t>::max()) {
      return Status::Invalid("Variant metadata dictionary is too large");
    }
    ARROW_RETURN_NOT_OK(internal::ValidateUtf8(name, "metadata dictionary string"));

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

  std::string_view FieldName(uint32_t id) const {
    DCHECK_LT(id, field_names_.size());
    return field_names_[id];
  }

  size_t size() const { return field_names_.size(); }

  void Truncate(size_t size) {
    DCHECK_LE(size, field_names_.size());
    field_names_.resize(size);
    RebuildIndex();
  }

  Result<std::shared_ptr<Buffer>> Finish() const {
    uint64_t bytes_size = 0;
    for (const auto& string : field_names_) {
      bytes_size += string.size();
    }
    if (field_names_.size() > std::numeric_limits<uint32_t>::max() ||
        bytes_size > std::numeric_limits<uint32_t>::max()) {
      return Status::Invalid("Variant metadata dictionary is too large");
    }

    const uint8_t offset_size =
        WidthForValue(std::max<uint64_t>(field_names_.size(), bytes_size));
    BufferBuilder out(pool_);
    ARROW_RETURN_NOT_OK(out.Reserve(
        1 + offset_size + (field_names_.size() + 1) * offset_size + bytes_size));

    const bool sorted_strings = !field_names_.empty() && is_sorted_;
    const auto header =
        static_cast<uint8_t>(internal::kVariantVersion |
                             (sorted_strings ? internal::kMetadataSortedStringsMask : 0) |
                             ((offset_size - 1) << 6));
    ARROW_RETURN_NOT_OK(out.Append(&header, sizeof(header)));
    ARROW_RETURN_NOT_OK(
        AppendLittleEndian(out, static_cast<uint32_t>(field_names_.size()), offset_size));

    uint32_t offset = 0;
    ARROW_RETURN_NOT_OK(AppendLittleEndian(out, offset, offset_size));
    for (const auto& string : field_names_) {
      offset += static_cast<uint32_t>(string.size());
      ARROW_RETURN_NOT_OK(AppendLittleEndian(out, offset, offset_size));
    }
    for (const auto& string : field_names_) {
      ARROW_RETURN_NOT_OK(out.Append(string));
    }
    return out.Finish();
  }

 private:
  void RebuildIndex() {
    field_ids_.clear();
    field_ids_.reserve(field_names_.size());
    is_sorted_ = false;
    for (uint32_t i = 0; i < field_names_.size(); ++i) {
      field_ids_.emplace(field_names_[i], i);
      if (i == 0) {
        is_sorted_ = true;
      } else if (is_sorted_ && !(field_names_[i - 1] < field_names_[i])) {
        is_sorted_ = false;
      }
    }
  }

  MemoryPool* pool_;
  std::deque<std::string> field_names_;
  std::unordered_map<std::string_view, uint32_t> field_ids_;
  bool is_sorted_ = false;
};

class VariantValueWriter {
 public:
  explicit VariantValueWriter(BufferBuilder& out) : out_(out) {}

  template <VariantPrimitiveType type>
    requires internal::HeaderOnlyVariantPrimitive<type>
  Status Append() {
    return AppendPrimitiveHeader<type>();
  }

  template <VariantPrimitiveType type>
    requires internal::FixedVariantPrimitive<type>
  Status Append(typename internal::VariantFixedPrimitiveTraits<type>::CType value) {
    using CType = typename internal::VariantFixedPrimitiveTraits<type>::CType;
    ARROW_RETURN_NOT_OK(AppendPrimitiveHeader<type>());
    if constexpr (sizeof(CType) == 1) {
      const auto byte = static_cast<uint8_t>(value);
      return out_.Append(&byte, sizeof(byte));
    } else {
      return AppendFixedLittleEndian(out_, value);
    }
  }

  template <VariantPrimitiveType type>
    requires internal::DecimalVariantPrimitive<type>
  Status Append(
      typename internal::VariantDecimalPrimitiveTraits<type>::CType unscaled_value,
      uint8_t scale) {
    ARROW_RETURN_NOT_OK(internal::ValidateDecimalScale(scale));
    ARROW_RETURN_NOT_OK(AppendPrimitiveHeader<type>());
    ARROW_RETURN_NOT_OK(out_.Append(&scale, sizeof(scale)));
    return AppendFixedLittleEndian(out_, unscaled_value);
  }

  template <VariantPrimitiveType type>
    requires internal::Decimal16VariantPrimitive<type>
  Status Append(std::string_view little_endian_unscaled_value, uint8_t scale) {
    if (little_endian_unscaled_value.size() != 16) {
      return Status::Invalid("Variant Decimal16 values must be 16 bytes");
    }
    ARROW_RETURN_NOT_OK(internal::ValidateDecimalScale(scale));
    ARROW_RETURN_NOT_OK(AppendPrimitiveHeader<type>());
    ARROW_RETURN_NOT_OK(out_.Append(&scale, sizeof(scale)));
    return out_.Append(little_endian_unscaled_value);
  }

  template <VariantPrimitiveType type>
    requires internal::LengthPrefixedVariantPrimitive<type>
  Status Append(std::string_view value) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::Invalid("Variant ",
                             type == VariantPrimitiveType::kBinary ? "binary" : "string",
                             " value is too large");
    }
    if constexpr (type == VariantPrimitiveType::kString) {
      ARROW_RETURN_NOT_OK(internal::ValidateUtf8(value, "primitive string value"));
    }
    ARROW_RETURN_NOT_OK(AppendPrimitiveHeader<type>());
    ARROW_RETURN_NOT_OK(AppendLittleEndian(out_, static_cast<uint32_t>(value.size()), 4));
    return out_.Append(value);
  }

  template <VariantPrimitiveType type>
    requires internal::UuidVariantPrimitive<type>
  Status Append(std::string_view big_endian_bytes) {
    if (big_endian_bytes.size() != 16) {
      return Status::Invalid("Variant UUID values must be 16 bytes");
    }
    ARROW_RETURN_NOT_OK(AppendPrimitiveHeader<type>());
    return out_.Append(big_endian_bytes);
  }

  Status AppendShortString(std::string_view value) {
    if (value.size() >= 64) {
      return Status::Invalid("Variant short string value must be shorter than 64 bytes");
    }
    ARROW_RETURN_NOT_OK(internal::ValidateUtf8(value, "short string value"));
    const auto header = static_cast<uint8_t>(
        (value.size() << 2) | static_cast<uint8_t>(VariantBasicType::kShortString));
    ARROW_RETURN_NOT_OK(out_.Append(&header, sizeof(header)));
    return out_.Append(value);
  }

 private:
  template <VariantPrimitiveType type>
  Status AppendPrimitiveHeader() {
    const auto header =
        static_cast<uint8_t>((static_cast<uint8_t>(type) << 2) |
                             static_cast<uint8_t>(VariantBasicType::kPrimitive));
    return out_.Append(&header, sizeof(header));
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
  explicit VariantBuildState(MemoryPool* pool)
      : pool(pool), value(pool), metadata(pool) {}

  MemoryPool* pool;
  BufferBuilder value;
  VariantMetadataBuilder metadata;
  std::vector<VariantBuildFrame> frames;
  bool root_has_value = false;
};

Status CheckRootWritable(const VariantBuildState& state) {
  if (!state.frames.empty()) {
    return Status::Invalid("VariantBuilder has an active container");
  }
  if (state.root_has_value) {
    return Status::Invalid("VariantBuilder already has a root value");
  }
  return Status::OK();
}

Status CheckTopFrame(const VariantBuildState& state, size_t frame_index,
                     VariantContainerKind kind) {
  if (state.frames.empty() || frame_index + 1 != state.frames.size()) {
    return Status::Invalid("Variant nested builder is not the active container");
  }
  const auto& frame = state.frames.back();
  if (frame.finished || frame.kind != kind) {
    return Status::Invalid("Variant nested builder has invalid state");
  }
  return Status::OK();
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

template <typename Impl>
void RollbackIfActive(Impl* impl) {
  if (impl != nullptr && impl->active) {
    if (impl->state == nullptr || impl->frame_index >= impl->state->frames.size()) {
      return;
    }
    const auto frame = impl->state->frames[impl->frame_index];
    impl->state->value.Rewind(frame.value_start);
    impl->state->metadata.Truncate(frame.metadata_size);
    if (frame.has_parent && frame.parent_frame < impl->state->frames.size()) {
      TruncateFrameEntries(impl->state->frames[frame.parent_frame],
                           frame.parent_entry_count);
    }
    impl->state->frames.resize(impl->frame_index);
    impl->active = false;
  }
}

Result<std::string> BuildObjectHeader(const VariantBuildState& state,
                                      const VariantBuildFrame& frame,
                                      uint32_t values_size) {
  if (frame.fields.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::Invalid("Variant object has too many fields");
  }

  std::vector<VariantFieldDescriptor> fields = frame.fields;
  std::ranges::sort(fields, [&](const VariantFieldDescriptor& left,
                                const VariantFieldDescriptor& right) {
    return state.metadata.FieldName(left.field_id) <
           state.metadata.FieldName(right.field_id);
  });

  uint32_t max_field_id = 0;
  for (const auto& field : fields) {
    max_field_id = std::max(max_field_id, field.field_id);
  }
  const uint8_t id_size = WidthForValue(max_field_id);
  const uint8_t offset_size = WidthForValue(values_size);
  const bool is_large = fields.size() > std::numeric_limits<uint8_t>::max();
  const auto header = static_cast<uint8_t>(((is_large ? 1 : 0) << 4) |
                                           ((id_size - 1) << 2) | (offset_size - 1));

  std::string out;
  out.push_back(
      static_cast<char>((header << 2) | static_cast<uint8_t>(VariantBasicType::kObject)));
  AppendLittleEndianToString(out, static_cast<uint32_t>(fields.size()), is_large ? 4 : 1);
  for (const auto& field : fields) {
    AppendLittleEndianToString(out, field.field_id, id_size);
  }
  for (const auto& field : fields) {
    AppendLittleEndianToString(out, field.offset, offset_size);
  }
  AppendLittleEndianToString(out, values_size, offset_size);
  return out;
}

Result<std::string> BuildListHeader(const VariantBuildFrame& frame,
                                    uint32_t values_size) {
  if (frame.offsets.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::Invalid("Variant array has too many elements");
  }

  const uint8_t offset_size = WidthForValue(values_size);
  const bool is_large = frame.offsets.size() > std::numeric_limits<uint8_t>::max();
  const auto header = static_cast<uint8_t>(((is_large ? 1 : 0) << 2) | (offset_size - 1));

  std::string out;
  out.push_back(
      static_cast<char>((header << 2) | static_cast<uint8_t>(VariantBasicType::kArray)));
  AppendLittleEndianToString(out, static_cast<uint32_t>(frame.offsets.size()),
                             is_large ? 4 : 1);
  for (const auto offset : frame.offsets) {
    AppendLittleEndianToString(out, offset, offset_size);
  }
  AppendLittleEndianToString(out, values_size, offset_size);
  return out;
}

using RootFinishCallback = std::function<Status(EncodedVariantValue)>;

Status FinishFrame(const std::shared_ptr<VariantBuildState>& state, size_t frame_index,
                   VariantContainerKind kind, const RootFinishCallback& callback) {
  ARROW_RETURN_NOT_OK(CheckTopFrame(*state, frame_index, kind));

  auto& frame = state->frames.back();
  const auto values_size = state->value.length() - frame.value_start;
  DCHECK_GE(values_size, 0);
  if (values_size > std::numeric_limits<uint32_t>::max()) {
    return Status::Invalid("Variant container values are too large");
  }

  ARROW_ASSIGN_OR_RAISE(
      auto header,
      kind == VariantContainerKind::Object
          ? BuildObjectHeader(*state, frame, static_cast<uint32_t>(values_size))
          : BuildListHeader(frame, static_cast<uint32_t>(values_size)));
  ARROW_RETURN_NOT_OK(InsertBytes(state->value, frame.value_start, header));

  const bool is_root = !frame.has_parent;
  frame.finished = true;
  state->frames.pop_back();
  if (!is_root) {
    return Status::OK();
  }

  state->root_has_value = true;
  if (!callback) {
    return Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(auto metadata, state->metadata.Finish());
  ARROW_ASSIGN_OR_RAISE(auto value, state->value.Finish());
  return callback({.metadata = std::move(metadata), .value = std::move(value)});
}

template <typename Write>
Status AppendRootPrimitiveWith(const std::shared_ptr<VariantBuildState>& state,
                               Write&& write) {
  ARROW_RETURN_NOT_OK(CheckRootWritable(*state));
  const auto value_size = state->value.length();
  VariantValueWriter writer(state->value);
  const Status status = std::invoke(std::forward<Write>(write), writer);
  if (!status.ok()) {
    state->value.Rewind(value_size);
    return status;
  }
  state->root_has_value = true;
  return Status::OK();
}

template <VariantPrimitiveType type, typename... Args>
Status AppendRootPrimitive(const std::shared_ptr<VariantBuildState>& state,
                           Args&&... args) {
  return AppendRootPrimitiveWith(state, [&](VariantValueWriter& writer) {
    return writer.template Append<type>(std::forward<Args>(args)...);
  });
}

template <typename Write>
Status AppendObjectPrimitiveWith(const std::shared_ptr<VariantBuildState>& state,
                                 size_t frame_index, std::string_view field_name,
                                 Write&& write) {
  ARROW_RETURN_NOT_OK(CheckTopFrame(*state, frame_index, VariantContainerKind::Object));
  auto& frame = state->frames.back();
  const auto metadata_size = state->metadata.size();
  const auto value_size = state->value.length();
  const auto field_count = frame.fields.size();

  ARROW_ASSIGN_OR_RAISE(auto field_id, state->metadata.Upsert(field_name));
  if (!frame.object_field_ids.insert(field_id).second) {
    state->metadata.Truncate(metadata_size);
    return Status::Invalid("Duplicate Variant object field: ", field_name);
  }
  const auto offset = state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    return Status::Invalid("Variant object values are too large");
  }
  frame.fields.push_back(VariantFieldDescriptor{.field_id = field_id,
                                                .offset = static_cast<uint32_t>(offset)});

  VariantValueWriter writer(state->value);
  const Status status = std::invoke(std::forward<Write>(write), writer);
  if (!status.ok()) {
    state->value.Rewind(value_size);
    state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    return status;
  }
  return Status::OK();
}

template <VariantPrimitiveType type, typename... Args>
Status AppendObjectPrimitive(const std::shared_ptr<VariantBuildState>& state,
                             size_t frame_index, std::string_view field_name,
                             Args&&... args) {
  return AppendObjectPrimitiveWith(
      state, frame_index, field_name, [&](VariantValueWriter& writer) {
        return writer.template Append<type>(std::forward<Args>(args)...);
      });
}

template <typename Write>
Status AppendListPrimitiveWith(const std::shared_ptr<VariantBuildState>& state,
                               size_t frame_index, Write&& write) {
  ARROW_RETURN_NOT_OK(CheckTopFrame(*state, frame_index, VariantContainerKind::List));
  auto& frame = state->frames.back();
  const auto value_size = state->value.length();
  const auto element_count = frame.offsets.size();
  const auto offset = state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    return Status::Invalid("Variant array values are too large");
  }
  frame.offsets.push_back(static_cast<uint32_t>(offset));

  VariantValueWriter writer(state->value);
  const Status status = std::invoke(std::forward<Write>(write), writer);
  if (!status.ok()) {
    state->value.Rewind(value_size);
    TruncateFrameEntries(frame, element_count);
    return status;
  }
  return Status::OK();
}

template <VariantPrimitiveType type, typename... Args>
Status AppendListPrimitive(const std::shared_ptr<VariantBuildState>& state,
                           size_t frame_index, Args&&... args) {
  return AppendListPrimitiveWith(state, frame_index, [&](VariantValueWriter& writer) {
    return writer.template Append<type>(std::forward<Args>(args)...);
  });
}

}  // namespace

namespace internal {

struct NestedVariantBuilderImpl {
  NestedVariantBuilderImpl(std::shared_ptr<VariantBuildState> state, size_t frame_index,
                           RootFinishCallback callback)
      : state(std::move(state)),
        frame_index(frame_index),
        callback(std::move(callback)) {}

  std::shared_ptr<VariantBuildState> state;
  size_t frame_index = 0;
  RootFinishCallback callback;
  bool active = true;
};

}  // namespace internal

struct VariantBuilder::Impl {
  explicit Impl(MemoryPool* pool)
      : pool(pool), state(std::make_shared<VariantBuildState>(pool)) {}

  MemoryPool* pool;
  std::shared_ptr<VariantBuildState> state;
};

VariantBuilder::VariantBuilder(MemoryPool* pool) : impl_(std::make_unique<Impl>(pool)) {}
VariantBuilder::~VariantBuilder() = default;
VariantBuilder::VariantBuilder(VariantBuilder&&) noexcept = default;
VariantBuilder& VariantBuilder::operator=(VariantBuilder&&) noexcept = default;

Status VariantBuilder::ReserveFieldNames(int64_t capacity) {
  return impl_->state->metadata.Reserve(capacity);
}

Result<uint32_t> VariantBuilder::AddFieldName(std::string_view name) {
  return impl_->state->metadata.Upsert(name);
}

Status VariantBuilder::AppendVariantNull() {
  return AppendRootPrimitive<VariantPrimitiveType::kNull>(impl_->state);
}

Status VariantBuilder::AppendBoolean(bool value) {
  if (value) {
    return AppendRootPrimitive<VariantPrimitiveType::kBooleanTrue>(impl_->state);
  }
  return AppendRootPrimitive<VariantPrimitiveType::kBooleanFalse>(impl_->state);
}

#define VARIANT_ROOT_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)                             \
  Status VariantBuilder::Append##NAME(C_TYPE value) {                               \
    return AppendRootPrimitive<VariantPrimitiveType::k##TYPE>(impl_->state, value); \
  }

VARIANT_ROOT_APPEND_ONE_ARG(Int8, Int8, int8_t)
VARIANT_ROOT_APPEND_ONE_ARG(Int16, Int16, int16_t)
VARIANT_ROOT_APPEND_ONE_ARG(Int32, Int32, int32_t)
VARIANT_ROOT_APPEND_ONE_ARG(Int64, Int64, int64_t)
VARIANT_ROOT_APPEND_ONE_ARG(Float, Float, float)
VARIANT_ROOT_APPEND_ONE_ARG(Double, Double, double)
VARIANT_ROOT_APPEND_ONE_ARG(Binary, Binary, std::string_view)
VARIANT_ROOT_APPEND_ONE_ARG(String, String, std::string_view)
VARIANT_ROOT_APPEND_ONE_ARG(Date, Date, int32_t)
VARIANT_ROOT_APPEND_ONE_ARG(TimeNTZMicros, TimeNTZMicros, int64_t)
VARIANT_ROOT_APPEND_ONE_ARG(Uuid, Uuid, std::string_view)

#undef VARIANT_ROOT_APPEND_ONE_ARG

Status VariantBuilder::AppendDecimal4(int32_t unscaled_value, uint8_t scale) {
  return AppendRootPrimitive<VariantPrimitiveType::kDecimal4>(impl_->state,
                                                              unscaled_value, scale);
}

Status VariantBuilder::AppendDecimal8(int64_t unscaled_value, uint8_t scale) {
  return AppendRootPrimitive<VariantPrimitiveType::kDecimal8>(impl_->state,
                                                              unscaled_value, scale);
}

Status VariantBuilder::AppendDecimal16(std::string_view little_endian_unscaled_value,
                                       uint8_t scale) {
  return AppendRootPrimitive<VariantPrimitiveType::kDecimal16>(
      impl_->state, little_endian_unscaled_value, scale);
}

Status VariantBuilder::AppendShortString(std::string_view value) {
  return AppendRootPrimitiveWith(impl_->state, [&](VariantValueWriter& writer) {
    return writer.AppendShortString(value);
  });
}

Status VariantBuilder::AppendTimestampMicros(int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return AppendRootPrimitive<VariantPrimitiveType::kTimestampMicros>(impl_->state,
                                                                       micros);
  }
  return AppendRootPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(impl_->state,
                                                                        micros);
}

Status VariantBuilder::AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return AppendRootPrimitive<VariantPrimitiveType::kTimestampNanos>(impl_->state,
                                                                      nanos);
  }
  return AppendRootPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(impl_->state,
                                                                       nanos);
}

Result<VariantObjectBuilder> VariantBuilder::StartObject() {
  ARROW_RETURN_NOT_OK(CheckRootWritable(*impl_->state));
  impl_->state->frames.push_back(
      VariantBuildFrame{.kind = VariantContainerKind::Object,
                        .value_start = impl_->state->value.length(),
                        .metadata_size = impl_->state->metadata.size()});
  return VariantObjectBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, RootFinishCallback{}));
}

Result<VariantListBuilder> VariantBuilder::StartList() {
  ARROW_RETURN_NOT_OK(CheckRootWritable(*impl_->state));
  impl_->state->frames.push_back(
      VariantBuildFrame{.kind = VariantContainerKind::List,
                        .value_start = impl_->state->value.length(),
                        .metadata_size = impl_->state->metadata.size()});
  return VariantListBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, RootFinishCallback{}));
}

Result<EncodedVariantValue> VariantBuilder::Finish() {
  if (!impl_->state->frames.empty()) {
    return Status::Invalid("Cannot finish VariantBuilder with active containers");
  }
  if (!impl_->state->root_has_value) {
    return Status::Invalid("Cannot finish empty VariantBuilder");
  }

  ARROW_ASSIGN_OR_RAISE(auto metadata, impl_->state->metadata.Finish());
  ARROW_ASSIGN_OR_RAISE(auto value, impl_->state->value.Finish());
  EncodedVariantValue out{.metadata = std::move(metadata), .value = std::move(value)};
  Reset();
  return out;
}

void VariantBuilder::Reset() {
  impl_->state = std::make_shared<VariantBuildState>(impl_->pool);
}

VariantObjectBuilder::VariantObjectBuilder(
    std::unique_ptr<internal::NestedVariantBuilderImpl> impl)
    : impl_(std::move(impl)) {}
VariantObjectBuilder::~VariantObjectBuilder() { RollbackIfActive(impl_.get()); }
VariantObjectBuilder::VariantObjectBuilder(VariantObjectBuilder&&) noexcept = default;
VariantObjectBuilder& VariantObjectBuilder::operator=(VariantObjectBuilder&&) noexcept =
    default;

Status VariantObjectBuilder::AppendVariantNull(std::string_view field_name) {
  return AppendObjectPrimitive<VariantPrimitiveType::kNull>(
      impl_->state, impl_->frame_index, field_name);
}

Status VariantObjectBuilder::AppendBoolean(std::string_view field_name, bool value) {
  if (value) {
    return AppendObjectPrimitive<VariantPrimitiveType::kBooleanTrue>(
        impl_->state, impl_->frame_index, field_name);
  }
  return AppendObjectPrimitive<VariantPrimitiveType::kBooleanFalse>(
      impl_->state, impl_->frame_index, field_name);
}

#define VARIANT_OBJECT_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)                                \
  Status VariantObjectBuilder::Append##NAME(std::string_view field_name, C_TYPE value) { \
    return AppendObjectPrimitive<VariantPrimitiveType::k##TYPE>(                         \
        impl_->state, impl_->frame_index, field_name, value);                            \
  }

VARIANT_OBJECT_APPEND_ONE_ARG(Int8, Int8, int8_t)
VARIANT_OBJECT_APPEND_ONE_ARG(Int16, Int16, int16_t)
VARIANT_OBJECT_APPEND_ONE_ARG(Int32, Int32, int32_t)
VARIANT_OBJECT_APPEND_ONE_ARG(Int64, Int64, int64_t)
VARIANT_OBJECT_APPEND_ONE_ARG(Float, Float, float)
VARIANT_OBJECT_APPEND_ONE_ARG(Double, Double, double)
VARIANT_OBJECT_APPEND_ONE_ARG(Binary, Binary, std::string_view)
VARIANT_OBJECT_APPEND_ONE_ARG(String, String, std::string_view)
VARIANT_OBJECT_APPEND_ONE_ARG(Date, Date, int32_t)
VARIANT_OBJECT_APPEND_ONE_ARG(TimeNTZMicros, TimeNTZMicros, int64_t)
VARIANT_OBJECT_APPEND_ONE_ARG(Uuid, Uuid, std::string_view)

#undef VARIANT_OBJECT_APPEND_ONE_ARG

Status VariantObjectBuilder::AppendDecimal4(std::string_view field_name,
                                            int32_t unscaled_value, uint8_t scale) {
  return AppendObjectPrimitive<VariantPrimitiveType::kDecimal4>(
      impl_->state, impl_->frame_index, field_name, unscaled_value, scale);
}

Status VariantObjectBuilder::AppendDecimal8(std::string_view field_name,
                                            int64_t unscaled_value, uint8_t scale) {
  return AppendObjectPrimitive<VariantPrimitiveType::kDecimal8>(
      impl_->state, impl_->frame_index, field_name, unscaled_value, scale);
}

Status VariantObjectBuilder::AppendDecimal16(
    std::string_view field_name, std::string_view little_endian_unscaled_value,
    uint8_t scale) {
  return AppendObjectPrimitive<VariantPrimitiveType::kDecimal16>(
      impl_->state, impl_->frame_index, field_name, little_endian_unscaled_value, scale);
}

Status VariantObjectBuilder::AppendShortString(std::string_view field_name,
                                               std::string_view value) {
  return AppendObjectPrimitiveWith(
      impl_->state, impl_->frame_index, field_name,
      [&](VariantValueWriter& writer) { return writer.AppendShortString(value); });
}

Status VariantObjectBuilder::AppendTimestampMicros(std::string_view field_name,
                                                   int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return AppendObjectPrimitive<VariantPrimitiveType::kTimestampMicros>(
        impl_->state, impl_->frame_index, field_name, micros);
  }
  return AppendObjectPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(
      impl_->state, impl_->frame_index, field_name, micros);
}

Status VariantObjectBuilder::AppendTimestampNanos(std::string_view field_name,
                                                  int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return AppendObjectPrimitive<VariantPrimitiveType::kTimestampNanos>(
        impl_->state, impl_->frame_index, field_name, nanos);
  }
  return AppendObjectPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(
      impl_->state, impl_->frame_index, field_name, nanos);
}

Result<VariantObjectBuilder> VariantObjectBuilder::StartObject(
    std::string_view field_name) {
  ARROW_RETURN_NOT_OK(
      CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::Object));
  auto& frame = impl_->state->frames.back();
  const auto metadata_size = impl_->state->metadata.size();
  const auto field_count = frame.fields.size();

  ARROW_ASSIGN_OR_RAISE(auto field_id, impl_->state->metadata.Upsert(field_name));
  if (!frame.object_field_ids.insert(field_id).second) {
    impl_->state->metadata.Truncate(metadata_size);
    return Status::Invalid("Duplicate Variant object field: ", field_name);
  }
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    impl_->state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    return Status::Invalid("Variant object values are too large");
  }

  frame.fields.push_back(VariantFieldDescriptor{.field_id = field_id,
                                                .offset = static_cast<uint32_t>(offset)});
  const auto value_start = impl_->state->value.length();
  impl_->state->frames.push_back(VariantBuildFrame{.kind = VariantContainerKind::Object,
                                                   .value_start = value_start,
                                                   .metadata_size = metadata_size,
                                                   .parent_frame = impl_->frame_index,
                                                   .parent_entry_count = field_count,
                                                   .has_parent = true});
  return VariantObjectBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, impl_->callback));
}

Result<VariantListBuilder> VariantObjectBuilder::StartList(std::string_view field_name) {
  ARROW_RETURN_NOT_OK(
      CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::Object));
  auto& frame = impl_->state->frames.back();
  const auto metadata_size = impl_->state->metadata.size();
  const auto field_count = frame.fields.size();

  ARROW_ASSIGN_OR_RAISE(auto field_id, impl_->state->metadata.Upsert(field_name));
  if (!frame.object_field_ids.insert(field_id).second) {
    impl_->state->metadata.Truncate(metadata_size);
    return Status::Invalid("Duplicate Variant object field: ", field_name);
  }
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    impl_->state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    return Status::Invalid("Variant object values are too large");
  }

  frame.fields.push_back(VariantFieldDescriptor{.field_id = field_id,
                                                .offset = static_cast<uint32_t>(offset)});
  const auto value_start = impl_->state->value.length();
  impl_->state->frames.push_back(VariantBuildFrame{.kind = VariantContainerKind::List,
                                                   .value_start = value_start,
                                                   .metadata_size = metadata_size,
                                                   .parent_frame = impl_->frame_index,
                                                   .parent_entry_count = field_count,
                                                   .has_parent = true});
  return VariantListBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, impl_->callback));
}

Status VariantObjectBuilder::Finish() {
  ARROW_RETURN_NOT_OK(FinishFrame(impl_->state, impl_->frame_index,
                                  VariantContainerKind::Object, impl_->callback));
  impl_->active = false;
  return Status::OK();
}

VariantListBuilder::VariantListBuilder(
    std::unique_ptr<internal::NestedVariantBuilderImpl> impl)
    : impl_(std::move(impl)) {}
VariantListBuilder::~VariantListBuilder() { RollbackIfActive(impl_.get()); }
VariantListBuilder::VariantListBuilder(VariantListBuilder&&) noexcept = default;
VariantListBuilder& VariantListBuilder::operator=(VariantListBuilder&&) noexcept =
    default;

Status VariantListBuilder::AppendVariantNull() {
  return AppendListPrimitive<VariantPrimitiveType::kNull>(impl_->state,
                                                          impl_->frame_index);
}

Status VariantListBuilder::AppendBoolean(bool value) {
  if (value) {
    return AppendListPrimitive<VariantPrimitiveType::kBooleanTrue>(impl_->state,
                                                                   impl_->frame_index);
  }
  return AppendListPrimitive<VariantPrimitiveType::kBooleanFalse>(impl_->state,
                                                                  impl_->frame_index);
}

#define VARIANT_LIST_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)        \
  Status VariantListBuilder::Append##NAME(C_TYPE value) {      \
    return AppendListPrimitive<VariantPrimitiveType::k##TYPE>( \
        impl_->state, impl_->frame_index, value);              \
  }

VARIANT_LIST_APPEND_ONE_ARG(Int8, Int8, int8_t)
VARIANT_LIST_APPEND_ONE_ARG(Int16, Int16, int16_t)
VARIANT_LIST_APPEND_ONE_ARG(Int32, Int32, int32_t)
VARIANT_LIST_APPEND_ONE_ARG(Int64, Int64, int64_t)
VARIANT_LIST_APPEND_ONE_ARG(Float, Float, float)
VARIANT_LIST_APPEND_ONE_ARG(Double, Double, double)
VARIANT_LIST_APPEND_ONE_ARG(Binary, Binary, std::string_view)
VARIANT_LIST_APPEND_ONE_ARG(String, String, std::string_view)
VARIANT_LIST_APPEND_ONE_ARG(Date, Date, int32_t)
VARIANT_LIST_APPEND_ONE_ARG(TimeNTZMicros, TimeNTZMicros, int64_t)
VARIANT_LIST_APPEND_ONE_ARG(Uuid, Uuid, std::string_view)

#undef VARIANT_LIST_APPEND_ONE_ARG

Status VariantListBuilder::AppendDecimal4(int32_t unscaled_value, uint8_t scale) {
  return AppendListPrimitive<VariantPrimitiveType::kDecimal4>(
      impl_->state, impl_->frame_index, unscaled_value, scale);
}

Status VariantListBuilder::AppendDecimal8(int64_t unscaled_value, uint8_t scale) {
  return AppendListPrimitive<VariantPrimitiveType::kDecimal8>(
      impl_->state, impl_->frame_index, unscaled_value, scale);
}

Status VariantListBuilder::AppendDecimal16(std::string_view little_endian_unscaled_value,
                                           uint8_t scale) {
  return AppendListPrimitive<VariantPrimitiveType::kDecimal16>(
      impl_->state, impl_->frame_index, little_endian_unscaled_value, scale);
}

Status VariantListBuilder::AppendShortString(std::string_view value) {
  return AppendListPrimitiveWith(
      impl_->state, impl_->frame_index,
      [&](VariantValueWriter& writer) { return writer.AppendShortString(value); });
}

Status VariantListBuilder::AppendTimestampMicros(int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return AppendListPrimitive<VariantPrimitiveType::kTimestampMicros>(
        impl_->state, impl_->frame_index, micros);
  }
  return AppendListPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(
      impl_->state, impl_->frame_index, micros);
}

Status VariantListBuilder::AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return AppendListPrimitive<VariantPrimitiveType::kTimestampNanos>(
        impl_->state, impl_->frame_index, nanos);
  }
  return AppendListPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(
      impl_->state, impl_->frame_index, nanos);
}

Result<VariantObjectBuilder> VariantListBuilder::StartObject() {
  ARROW_RETURN_NOT_OK(
      CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::List));
  auto& frame = impl_->state->frames.back();
  const auto element_count = frame.offsets.size();
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    return Status::Invalid("Variant array values are too large");
  }

  frame.offsets.push_back(static_cast<uint32_t>(offset));
  const auto value_start = impl_->state->value.length();
  impl_->state->frames.push_back(
      VariantBuildFrame{.kind = VariantContainerKind::Object,
                        .value_start = value_start,
                        .metadata_size = impl_->state->metadata.size(),
                        .parent_frame = impl_->frame_index,
                        .parent_entry_count = element_count,
                        .has_parent = true});
  return VariantObjectBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, impl_->callback));
}

Result<VariantListBuilder> VariantListBuilder::StartList() {
  ARROW_RETURN_NOT_OK(
      CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::List));
  auto& frame = impl_->state->frames.back();
  const auto element_count = frame.offsets.size();
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    return Status::Invalid("Variant array values are too large");
  }

  frame.offsets.push_back(static_cast<uint32_t>(offset));
  const auto value_start = impl_->state->value.length();
  impl_->state->frames.push_back(
      VariantBuildFrame{.kind = VariantContainerKind::List,
                        .value_start = value_start,
                        .metadata_size = impl_->state->metadata.size(),
                        .parent_frame = impl_->frame_index,
                        .parent_entry_count = element_count,
                        .has_parent = true});
  return VariantListBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, impl_->callback));
}

Status VariantListBuilder::Finish() {
  ARROW_RETURN_NOT_OK(FinishFrame(impl_->state, impl_->frame_index,
                                  VariantContainerKind::List, impl_->callback));
  impl_->active = false;
  return Status::OK();
}

struct VariantArrayBuilder::Impl {
  explicit Impl(MemoryPool* pool)
      : pool(pool), metadata_builder(pool), value_builder(pool), validity_builder(pool) {}

  template <typename Write>
  Status AppendValue(Write&& write) {
    auto state = std::make_shared<VariantBuildState>(pool);
    ARROW_RETURN_NOT_OK(AppendRootPrimitiveWith(state, std::forward<Write>(write)));
    ARROW_ASSIGN_OR_RAISE(auto metadata, state->metadata.Finish());
    ARROW_ASSIGN_OR_RAISE(auto value, state->value.Finish());
    return AppendEncoded({.metadata = std::move(metadata), .value = std::move(value)});
  }

  template <VariantPrimitiveType type, typename... Args>
  Status AppendPrimitive(Args&&... args) {
    return AppendValue([&](VariantValueWriter& writer) {
      return writer.template Append<type>(std::forward<Args>(args)...);
    });
  }

  Status AppendShortString(std::string_view value) {
    return AppendValue(
        [&](VariantValueWriter& writer) { return writer.AppendShortString(value); });
  }

  Status AppendEncoded(const EncodedVariantValue& value) {
    if (value.metadata == nullptr || value.value == nullptr) {
      return Status::Invalid(
          "Encoded Variant metadata and value buffers must be non-null");
    }
    ARROW_ASSIGN_OR_RAISE(auto metadata,
                          VariantMetadataView::Make(std::string_view{*value.metadata}));
    ARROW_RETURN_NOT_OK(
        VariantValueView::Validate(std::string_view{*value.value}, metadata));
    ARROW_RETURN_NOT_OK(metadata_builder.Append(std::string_view{*value.metadata}));
    ARROW_RETURN_NOT_OK(value_builder.Append(std::string_view{*value.value}));
    return validity_builder.Append(true);
  }

  MemoryPool* pool;
  BinaryBuilder metadata_builder;
  BinaryBuilder value_builder;
  BooleanBuilder validity_builder;
};

VariantArrayBuilder::VariantArrayBuilder(MemoryPool* pool)
    : impl_(std::make_unique<Impl>(pool)) {}
VariantArrayBuilder::~VariantArrayBuilder() = default;
VariantArrayBuilder::VariantArrayBuilder(VariantArrayBuilder&&) noexcept = default;
VariantArrayBuilder& VariantArrayBuilder::operator=(VariantArrayBuilder&&) noexcept =
    default;

Status VariantArrayBuilder::AppendNull() {
  ARROW_RETURN_NOT_OK(impl_->metadata_builder.Append(""));
  ARROW_RETURN_NOT_OK(impl_->value_builder.Append(""));
  return impl_->validity_builder.Append(false);
}

Status VariantArrayBuilder::AppendVariantNull() {
  return impl_->AppendPrimitive<VariantPrimitiveType::kNull>();
}

Status VariantArrayBuilder::AppendBoolean(bool value) {
  if (value) {
    return impl_->AppendPrimitive<VariantPrimitiveType::kBooleanTrue>();
  }
  return impl_->AppendPrimitive<VariantPrimitiveType::kBooleanFalse>();
}

#define VARIANT_ARRAY_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)                 \
  Status VariantArrayBuilder::Append##NAME(C_TYPE value) {               \
    return impl_->AppendPrimitive<VariantPrimitiveType::k##TYPE>(value); \
  }

VARIANT_ARRAY_APPEND_ONE_ARG(Int8, Int8, int8_t)
VARIANT_ARRAY_APPEND_ONE_ARG(Int16, Int16, int16_t)
VARIANT_ARRAY_APPEND_ONE_ARG(Int32, Int32, int32_t)
VARIANT_ARRAY_APPEND_ONE_ARG(Int64, Int64, int64_t)
VARIANT_ARRAY_APPEND_ONE_ARG(Float, Float, float)
VARIANT_ARRAY_APPEND_ONE_ARG(Double, Double, double)
VARIANT_ARRAY_APPEND_ONE_ARG(Binary, Binary, std::string_view)
VARIANT_ARRAY_APPEND_ONE_ARG(String, String, std::string_view)
VARIANT_ARRAY_APPEND_ONE_ARG(Date, Date, int32_t)
VARIANT_ARRAY_APPEND_ONE_ARG(TimeNTZMicros, TimeNTZMicros, int64_t)
VARIANT_ARRAY_APPEND_ONE_ARG(Uuid, Uuid, std::string_view)

#undef VARIANT_ARRAY_APPEND_ONE_ARG

Status VariantArrayBuilder::AppendDecimal4(int32_t unscaled_value, uint8_t scale) {
  return impl_->AppendPrimitive<VariantPrimitiveType::kDecimal4>(unscaled_value, scale);
}

Status VariantArrayBuilder::AppendDecimal8(int64_t unscaled_value, uint8_t scale) {
  return impl_->AppendPrimitive<VariantPrimitiveType::kDecimal8>(unscaled_value, scale);
}

Status VariantArrayBuilder::AppendDecimal16(std::string_view little_endian_unscaled_value,
                                            uint8_t scale) {
  return impl_->AppendPrimitive<VariantPrimitiveType::kDecimal16>(
      little_endian_unscaled_value, scale);
}

Status VariantArrayBuilder::AppendShortString(std::string_view value) {
  return impl_->AppendShortString(value);
}

Status VariantArrayBuilder::AppendTimestampMicros(int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return impl_->AppendPrimitive<VariantPrimitiveType::kTimestampMicros>(micros);
  }
  return impl_->AppendPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(micros);
}

Status VariantArrayBuilder::AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    return impl_->AppendPrimitive<VariantPrimitiveType::kTimestampNanos>(nanos);
  }
  return impl_->AppendPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(nanos);
}

Status VariantArrayBuilder::AppendEncoded(const EncodedVariantValue& value) {
  return impl_->AppendEncoded(value);
}

Result<VariantObjectBuilder> VariantArrayBuilder::StartObject() {
  auto state = std::make_shared<VariantBuildState>(impl_->pool);
  state->frames.push_back(VariantBuildFrame{.kind = VariantContainerKind::Object,
                                            .value_start = state->value.length(),
                                            .metadata_size = state->metadata.size()});
  auto callback = [this](EncodedVariantValue encoded) {
    return impl_->AppendEncoded(encoded);
  };
  return VariantObjectBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      state, state->frames.size() - 1, std::move(callback)));
}

Result<VariantListBuilder> VariantArrayBuilder::StartList() {
  auto state = std::make_shared<VariantBuildState>(impl_->pool);
  state->frames.push_back(VariantBuildFrame{.kind = VariantContainerKind::List,
                                            .value_start = state->value.length(),
                                            .metadata_size = state->metadata.size()});
  auto callback = [this](EncodedVariantValue encoded) {
    return impl_->AppendEncoded(encoded);
  };
  return VariantListBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      state, state->frames.size() - 1, std::move(callback)));
}

Result<std::shared_ptr<VariantArray>> VariantArrayBuilder::Finish() {
  std::shared_ptr<BinaryArray> metadata;
  std::shared_ptr<BinaryArray> value;
  std::shared_ptr<BooleanArray> validity;
  ARROW_RETURN_NOT_OK(impl_->metadata_builder.Finish(&metadata));
  ARROW_RETURN_NOT_OK(impl_->value_builder.Finish(&value));
  ARROW_RETURN_NOT_OK(impl_->validity_builder.Finish(&validity));

  auto null_bitmap = validity->data()->buffers[1];
  const int64_t null_count = validity->false_count();
  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  ARROW_ASSIGN_OR_RAISE(auto storage,
                        StructArray::Make({metadata, value}, storage_type->fields(),
                                          null_bitmap, null_count));
  return MakeVariantArrayFromStorage(storage);
}

void VariantArrayBuilder::Reset() { impl_ = std::make_unique<Impl>(impl_->pool); }

struct VariantValueArrayBuilder::Impl {
  explicit Impl(MemoryPool* pool) : value_builder(pool) {}

  BinaryBuilder value_builder;
};

VariantValueArrayBuilder::VariantValueArrayBuilder(MemoryPool* pool)
    : impl_(std::make_unique<Impl>(pool)) {}
VariantValueArrayBuilder::VariantValueArrayBuilder(VariantValueArrayBuilder&&) noexcept =
    default;
VariantValueArrayBuilder& VariantValueArrayBuilder::operator=(
    VariantValueArrayBuilder&&) noexcept = default;
VariantValueArrayBuilder::~VariantValueArrayBuilder() = default;

Status VariantValueArrayBuilder::AppendNull() {
  return impl_->value_builder.AppendNull();
}

Status VariantValueArrayBuilder::AppendEncodedValue(std::string_view metadata,
                                                    std::string_view value) {
  ARROW_ASSIGN_OR_RAISE(auto metadata_view, VariantMetadataView::Make(metadata));
  ARROW_RETURN_NOT_OK(VariantValueView::Validate(value, metadata_view));
  return impl_->value_builder.Append(value);
}

Result<std::shared_ptr<BinaryArray>> VariantValueArrayBuilder::Finish() {
  std::shared_ptr<BinaryArray> out;
  ARROW_RETURN_NOT_OK(impl_->value_builder.Finish(&out));
  return out;
}

Result<std::shared_ptr<VariantArray>> MakeVariantArrayFromStorage(
    std::shared_ptr<StructArray> storage) {
  if (storage == nullptr) {
    return Status::Invalid("Variant storage array must be non-null");
  }
  ARROW_ASSIGN_OR_RAISE(auto type, VariantExtensionType::Make(storage->type()));
  auto array = ExtensionType::WrapArray(type, std::move(storage));
  return std::static_pointer_cast<VariantArray>(array);
}

Result<std::shared_ptr<VariantArray>> MakeVariantArrayFromChildren(
    std::shared_ptr<DataType> storage_type, std::vector<std::shared_ptr<Array>> children,
    std::shared_ptr<Buffer> null_bitmap) {
  if (storage_type->id() != Type::STRUCT) {
    return Status::Invalid("Variant storage type must be struct, got ",
                           storage_type->ToString());
  }

  const auto& struct_type =
      ::arrow::internal::checked_cast<const StructType&>(*storage_type);
  if (children.size() != static_cast<size_t>(struct_type.num_fields())) {
    return Status::Invalid("Variant storage expected ", struct_type.num_fields(),
                           " children, got ", children.size());
  }

  const int64_t length = children.empty() ? 0 : children[0]->length();
  for (int i = 0; i < struct_type.num_fields(); ++i) {
    if (children[i] == nullptr) {
      return Status::Invalid("Variant storage child ", i, " is null");
    }
    if (!children[i]->type()->Equals(struct_type.field(i)->type())) {
      return Status::Invalid("Variant storage child ", i, " has type ",
                             children[i]->type()->ToString(), ", expected ",
                             struct_type.field(i)->type()->ToString());
    }
    if (children[i]->length() != length) {
      return Status::Invalid("Variant storage child lengths must match");
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto storage,
                        StructArray::Make(std::move(children), struct_type.fields(),
                                          std::move(null_bitmap)));
  return MakeVariantArrayFromStorage(storage);
}

}  // namespace parquet::variant

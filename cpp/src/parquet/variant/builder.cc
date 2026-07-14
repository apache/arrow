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
#include "arrow/util/binary_view_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"
#include "parquet/exception.h"
#include "parquet/variant/encoding.h"
#include "parquet/variant/encoding_internal.h"

namespace parquet::variant {

using ::arrow::binary_view;
using ::arrow::BinaryViewArray;
using ::arrow::BinaryViewType;
using ::arrow::BufferBuilder;
using ::arrow::BufferVector;
using ::arrow::ExtensionType;
using ::arrow::field;
using ::arrow::struct_;
using ::arrow::StructType;
using ::arrow::Type;
using ::arrow::TypedBufferBuilder;
using ::arrow::extension::VariantExtensionType;

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

void AppendLittleEndianToString(std::string& out, uint32_t value, uint8_t width) {
  DCHECK_LE(width, sizeof(uint32_t));
  const auto little_endian = bit_util::ToLittleEndian(value);
  out.append(reinterpret_cast<const char*>(&little_endian), width);
}

void InsertBytes(BufferBuilder& out, int64_t offset, std::string_view bytes) {
  const int64_t old_size = out.length();
  const auto insert_size = bytes.size();
  PARQUET_THROW_NOT_OK(out.Reserve(insert_size));
  uint8_t* data = out.mutable_data();
  std::memmove(data + offset + insert_size, data + offset, old_size - offset);
  std::memcpy(data + offset, bytes.data(), insert_size);
  out.UnsafeAdvance(insert_size);
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

  void Reserve(int64_t capacity) {
    if (capacity < 0) {
      throw ParquetException("Variant metadata capacity must be non-negative");
    }
    field_ids_.reserve(capacity);
  }

  uint32_t Upsert(std::string_view name) {
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

  void AppendTo(BufferBuilder& out) const {
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

  std::shared_ptr<Buffer> Finish() const {
    BufferBuilder out(pool_);
    AppendTo(out);
    std::shared_ptr<Buffer> buffer;
    PARQUET_ASSIGN_OR_THROW(buffer, out.Finish());
    return buffer;
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
  void Append() {
    AppendPrimitiveHeader<type>();
  }

  template <VariantPrimitiveType type>
    requires internal::FixedVariantPrimitive<type>
  void Append(typename internal::VariantFixedPrimitiveTraits<type>::CType value) {
    using CType = typename internal::VariantFixedPrimitiveTraits<type>::CType;
    AppendPrimitiveHeader<type>();
    if constexpr (sizeof(CType) == 1) {
      const auto byte = static_cast<uint8_t>(value);
      PARQUET_THROW_NOT_OK(out_.Append(&byte, sizeof(byte)));
    } else {
      AppendFixedLittleEndian(out_, value);
    }
  }

  template <VariantPrimitiveType type>
    requires internal::DecimalVariantPrimitive<type>
  void Append(
      typename internal::VariantDecimalPrimitiveTraits<type>::CType unscaled_value,
      uint8_t scale) {
    internal::ValidateDecimalScale(scale);
    AppendPrimitiveHeader<type>();
    PARQUET_THROW_NOT_OK(out_.Append(&scale, sizeof(scale)));
    AppendFixedLittleEndian(out_, unscaled_value);
  }

  template <VariantPrimitiveType type>
    requires internal::Decimal16VariantPrimitive<type>
  void Append(std::string_view little_endian_unscaled_value, uint8_t scale) {
    if (little_endian_unscaled_value.size() != 16) {
      throw ParquetException("Variant Decimal16 values must be 16 bytes");
    }
    internal::ValidateDecimalScale(scale);
    AppendPrimitiveHeader<type>();
    PARQUET_THROW_NOT_OK(out_.Append(&scale, sizeof(scale)));
    PARQUET_THROW_NOT_OK(out_.Append(little_endian_unscaled_value));
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
  VariantBuildState(MemoryPool* pool, BufferBuilder& value)
      : value(value), root_value_start(value.length()), metadata(pool) {}

  BufferBuilder& value;
  int64_t root_value_start = 0;
  VariantMetadataBuilder metadata;
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

void CheckTopFrame(const VariantBuildState& state, size_t frame_index,
                   VariantContainerKind kind) {
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

std::string BuildObjectHeader(const VariantBuildState& state,
                              const VariantBuildFrame& frame, uint32_t values_size) {
  if (frame.fields.size() > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant object has too many fields");
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

std::string BuildListHeader(const VariantBuildFrame& frame, uint32_t values_size) {
  if (frame.offsets.size() > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant array has too many elements");
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

using RootFinishCallback = std::function<void(const std::shared_ptr<VariantBuildState>&)>;

void FinishFrame(const std::shared_ptr<VariantBuildState>& state, size_t frame_index,
                 VariantContainerKind kind, const RootFinishCallback& callback) {
  CheckTopFrame(*state, frame_index, kind);

  auto& frame = state->frames.back();
  const auto values_size = state->value.length() - frame.value_start;
  DCHECK_GE(values_size, 0);
  if (values_size > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant container values are too large");
  }

  auto header = kind == VariantContainerKind::Object
                    ? BuildObjectHeader(*state, frame, static_cast<uint32_t>(values_size))
                    : BuildListHeader(frame, static_cast<uint32_t>(values_size));
  InsertBytes(state->value, frame.value_start, header);

  const bool is_root = !frame.has_parent;
  frame.finished = true;
  state->frames.pop_back();
  if (!is_root) {
    return;
  }

  state->root_has_value = true;
  if (!callback) {
    return;
  }

  callback(state);
}

template <typename Write>
void AppendRootPrimitiveWith(const std::shared_ptr<VariantBuildState>& state,
                             Write&& write) {
  CheckRootWritable(*state);
  const auto value_size = state->value.length();
  VariantValueWriter writer(state->value);
  try {
    std::invoke(std::forward<Write>(write), writer);
  } catch (...) {
    state->value.Rewind(value_size);
    throw;
  }
  state->root_has_value = true;
}

template <VariantPrimitiveType type, typename... Args>
void AppendRootPrimitive(const std::shared_ptr<VariantBuildState>& state,
                         Args&&... args) {
  AppendRootPrimitiveWith(state, [&](VariantValueWriter& writer) {
    writer.template Append<type>(std::forward<Args>(args)...);
  });
}

template <typename Write>
void AppendObjectPrimitiveWith(const std::shared_ptr<VariantBuildState>& state,
                               size_t frame_index, std::string_view field_name,
                               Write&& write) {
  CheckTopFrame(*state, frame_index, VariantContainerKind::Object);
  auto& frame = state->frames.back();
  const auto metadata_size = state->metadata.size();
  const auto value_size = state->value.length();
  const auto field_count = frame.fields.size();

  auto field_id = state->metadata.Upsert(field_name);
  if (!frame.object_field_ids.insert(field_id).second) {
    state->metadata.Truncate(metadata_size);
    throw ParquetException("Duplicate Variant object field: ", field_name);
  }
  const auto offset = state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    throw ParquetException("Variant object values are too large");
  }
  frame.fields.push_back(VariantFieldDescriptor{.field_id = field_id,
                                                .offset = static_cast<uint32_t>(offset)});

  VariantValueWriter writer(state->value);
  try {
    std::invoke(std::forward<Write>(write), writer);
  } catch (...) {
    state->value.Rewind(value_size);
    state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    throw;
  }
}

template <VariantPrimitiveType type, typename... Args>
void AppendObjectPrimitive(const std::shared_ptr<VariantBuildState>& state,
                           size_t frame_index, std::string_view field_name,
                           Args&&... args) {
  AppendObjectPrimitiveWith(state, frame_index, field_name,
                            [&](VariantValueWriter& writer) {
                              writer.template Append<type>(std::forward<Args>(args)...);
                            });
}

template <typename Write>
void AppendListPrimitiveWith(const std::shared_ptr<VariantBuildState>& state,
                             size_t frame_index, Write&& write) {
  CheckTopFrame(*state, frame_index, VariantContainerKind::List);
  auto& frame = state->frames.back();
  const auto value_size = state->value.length();
  const auto element_count = frame.offsets.size();
  const auto offset = state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant array values are too large");
  }
  frame.offsets.push_back(static_cast<uint32_t>(offset));

  VariantValueWriter writer(state->value);
  try {
    std::invoke(std::forward<Write>(write), writer);
  } catch (...) {
    state->value.Rewind(value_size);
    TruncateFrameEntries(frame, element_count);
    throw;
  }
}

template <VariantPrimitiveType type, typename... Args>
void AppendListPrimitive(const std::shared_ptr<VariantBuildState>& state,
                         size_t frame_index, Args&&... args) {
  AppendListPrimitiveWith(state, frame_index, [&](VariantValueWriter& writer) {
    writer.template Append<type>(std::forward<Args>(args)...);
  });
}

// Local helper for array builders that owns one shared byte arena plus the
// corresponding `binary_view` slots. This lets row commit write bytes once,
// create inline or out-of-line views in place, and roll back both buffers
// together on failure without routing hot-path appends through `BinaryViewBuilder`.
class BinaryViewColumnBuilder {
 public:
  struct Mark {
    int64_t bytes_length = 0;
    int64_t views_length = 0;
    bool has_out_of_line_data = false;
  };

  explicit BinaryViewColumnBuilder(MemoryPool* pool) : bytes_(pool), views_(pool) {}

  [[nodiscard]] Mark mark() const {
    return Mark{.bytes_length = bytes_.length(),
                .views_length = views_.length(),
                .has_out_of_line_data = has_out_of_line_data_};
  }

  void Rollback(const Mark& mark) {
    bytes_.Rewind(mark.bytes_length);
    views_.bytes_builder()->Rewind(mark.views_length * sizeof(BinaryViewType::c_type));
    has_out_of_line_data_ = mark.has_out_of_line_data;
  }

  int64_t length() const { return views_.length(); }

  std::string_view SliceFrom(int64_t start) const {
    DCHECK_LE(start, bytes_.length());
    const auto size = bytes_.length() - start;
    DCHECK_GE(size, 0);
    if (size == 0) {
      return std::string_view{};
    }
    return std::string_view{reinterpret_cast<const char*>(bytes_.data() + start),
                            static_cast<size_t>(size)};
  }

  void AppendView(int64_t start) {
    const auto slice = SliceFrom(start);
    if (slice.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      throw ParquetException("Binary view value is too large");
    }
    PARQUET_THROW_NOT_OK(views_.Reserve(1));
    if (slice.size() <= BinaryViewType::kInlineSize) {
      views_.UnsafeAppend(::arrow::util::ToInlineBinaryView(slice));
      bytes_.Rewind(start);
      return;
    }
    if (start > std::numeric_limits<int32_t>::max()) {
      throw ParquetException("Binary view offset is too large");
    }
    has_out_of_line_data_ = true;
    views_.UnsafeAppend(::arrow::util::ToNonInlineBinaryView(
        slice.data(), static_cast<int32_t>(slice.size()),
        /*buffer_index=*/0, static_cast<int32_t>(start)));
  }

  void AppendEmptyValue() {
    PARQUET_THROW_NOT_OK(views_.Reserve(1));
    views_.UnsafeAppend(BinaryViewType::c_type{});
  }

  BufferBuilder& bytes_builder() { return bytes_; }

  std::shared_ptr<BinaryViewArray> Finish(std::shared_ptr<Buffer> null_bitmap = nullptr,
                                          int64_t null_count = 0) {
    const auto array_length = views_.length();
    std::shared_ptr<Buffer> views_buffer;
    PARQUET_THROW_NOT_OK(views_.Finish(&views_buffer));

    std::shared_ptr<Buffer> data_buffer;
    PARQUET_ASSIGN_OR_THROW(data_buffer, bytes_.Finish());
    BufferVector data_buffers;
    if (has_out_of_line_data_) {
      data_buffers.push_back(std::move(data_buffer));
    }
    return std::make_shared<BinaryViewArray>(
        binary_view(), array_length, std::move(views_buffer), std::move(data_buffers),
        std::move(null_bitmap), null_count);
  }

 private:
  BufferBuilder bytes_;
  TypedBufferBuilder<BinaryViewType::c_type> views_;
  bool has_out_of_line_data_ = false;
};

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
      : pool(pool),
        value_builder(pool),
        state(std::make_shared<VariantBuildState>(pool, value_builder)) {}

  void Reset() {
    value_builder.Reset();
    state = std::make_shared<VariantBuildState>(pool, value_builder);
  }

  MemoryPool* pool;
  BufferBuilder value_builder;
  std::shared_ptr<VariantBuildState> state;
};

VariantBuilder::VariantBuilder(MemoryPool* pool) : impl_(std::make_unique<Impl>(pool)) {}
VariantBuilder::~VariantBuilder() = default;
VariantBuilder::VariantBuilder(VariantBuilder&&) noexcept = default;
VariantBuilder& VariantBuilder::operator=(VariantBuilder&&) noexcept = default;

void VariantBuilder::ReserveFieldNames(int64_t capacity) {
  impl_->state->metadata.Reserve(capacity);
}

uint32_t VariantBuilder::AddFieldName(std::string_view name) {
  return impl_->state->metadata.Upsert(name);
}

void VariantBuilder::AppendVariantNull() {
  AppendRootPrimitive<VariantPrimitiveType::kNull>(impl_->state);
}

void VariantBuilder::AppendBoolean(bool value) {
  if (value) {
    AppendRootPrimitive<VariantPrimitiveType::kBooleanTrue>(impl_->state);
    return;
  }
  AppendRootPrimitive<VariantPrimitiveType::kBooleanFalse>(impl_->state);
}

#define VARIANT_ROOT_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)                      \
  void VariantBuilder::Append##NAME(C_TYPE value) {                          \
    AppendRootPrimitive<VariantPrimitiveType::k##TYPE>(impl_->state, value); \
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

void VariantBuilder::AppendDecimal4(int32_t unscaled_value, uint8_t scale) {
  AppendRootPrimitive<VariantPrimitiveType::kDecimal4>(impl_->state, unscaled_value,
                                                       scale);
}

void VariantBuilder::AppendDecimal8(int64_t unscaled_value, uint8_t scale) {
  AppendRootPrimitive<VariantPrimitiveType::kDecimal8>(impl_->state, unscaled_value,
                                                       scale);
}

void VariantBuilder::AppendDecimal16(std::string_view little_endian_unscaled_value,
                                     uint8_t scale) {
  AppendRootPrimitive<VariantPrimitiveType::kDecimal16>(
      impl_->state, little_endian_unscaled_value, scale);
}

void VariantBuilder::AppendShortString(std::string_view value) {
  AppendRootPrimitiveWith(
      impl_->state, [&](VariantValueWriter& writer) { writer.AppendShortString(value); });
}

void VariantBuilder::AppendTimestampMicros(int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    AppendRootPrimitive<VariantPrimitiveType::kTimestampMicros>(impl_->state, micros);
    return;
  }
  AppendRootPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(impl_->state, micros);
}

void VariantBuilder::AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    AppendRootPrimitive<VariantPrimitiveType::kTimestampNanos>(impl_->state, nanos);
    return;
  }
  AppendRootPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(impl_->state, nanos);
}

VariantObjectBuilder VariantBuilder::StartObject() {
  CheckRootWritable(*impl_->state);
  impl_->state->frames.push_back(
      VariantBuildFrame{.kind = VariantContainerKind::Object,
                        .value_start = impl_->state->value.length(),
                        .metadata_size = impl_->state->metadata.size()});
  return VariantObjectBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, RootFinishCallback{}));
}

VariantListBuilder VariantBuilder::StartList() {
  CheckRootWritable(*impl_->state);
  impl_->state->frames.push_back(
      VariantBuildFrame{.kind = VariantContainerKind::List,
                        .value_start = impl_->state->value.length(),
                        .metadata_size = impl_->state->metadata.size()});
  return VariantListBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      impl_->state, impl_->state->frames.size() - 1, RootFinishCallback{}));
}

EncodedVariantValue VariantBuilder::Finish() {
  if (!impl_->state->frames.empty()) {
    throw ParquetException("Cannot finish VariantBuilder with active containers");
  }
  if (!impl_->state->root_has_value) {
    throw ParquetException("Cannot finish empty VariantBuilder");
  }

  auto metadata = impl_->state->metadata.Finish();
  std::shared_ptr<Buffer> value;
  PARQUET_ASSIGN_OR_THROW(value, impl_->state->value.Finish());
  EncodedVariantValue out{.metadata = std::move(metadata), .value = std::move(value)};
  Reset();
  return out;
}

void VariantBuilder::Reset() { impl_->Reset(); }

VariantObjectBuilder::VariantObjectBuilder(
    std::unique_ptr<internal::NestedVariantBuilderImpl> impl)
    : impl_(std::move(impl)) {}
VariantObjectBuilder::~VariantObjectBuilder() { RollbackIfActive(impl_.get()); }
VariantObjectBuilder::VariantObjectBuilder(VariantObjectBuilder&&) noexcept = default;
VariantObjectBuilder& VariantObjectBuilder::operator=(VariantObjectBuilder&&) noexcept =
    default;

void VariantObjectBuilder::AppendVariantNull(std::string_view field_name) {
  AppendObjectPrimitive<VariantPrimitiveType::kNull>(impl_->state, impl_->frame_index,
                                                     field_name);
}

void VariantObjectBuilder::AppendBoolean(std::string_view field_name, bool value) {
  if (value) {
    AppendObjectPrimitive<VariantPrimitiveType::kBooleanTrue>(
        impl_->state, impl_->frame_index, field_name);
    return;
  }
  AppendObjectPrimitive<VariantPrimitiveType::kBooleanFalse>(
      impl_->state, impl_->frame_index, field_name);
}

#define VARIANT_OBJECT_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)                              \
  void VariantObjectBuilder::Append##NAME(std::string_view field_name, C_TYPE value) { \
    AppendObjectPrimitive<VariantPrimitiveType::k##TYPE>(                              \
        impl_->state, impl_->frame_index, field_name, value);                          \
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

void VariantObjectBuilder::AppendDecimal4(std::string_view field_name,
                                          int32_t unscaled_value, uint8_t scale) {
  AppendObjectPrimitive<VariantPrimitiveType::kDecimal4>(
      impl_->state, impl_->frame_index, field_name, unscaled_value, scale);
}

void VariantObjectBuilder::AppendDecimal8(std::string_view field_name,
                                          int64_t unscaled_value, uint8_t scale) {
  AppendObjectPrimitive<VariantPrimitiveType::kDecimal8>(
      impl_->state, impl_->frame_index, field_name, unscaled_value, scale);
}

void VariantObjectBuilder::AppendDecimal16(std::string_view field_name,
                                           std::string_view little_endian_unscaled_value,
                                           uint8_t scale) {
  AppendObjectPrimitive<VariantPrimitiveType::kDecimal16>(
      impl_->state, impl_->frame_index, field_name, little_endian_unscaled_value, scale);
}

void VariantObjectBuilder::AppendShortString(std::string_view field_name,
                                             std::string_view value) {
  AppendObjectPrimitiveWith(
      impl_->state, impl_->frame_index, field_name,
      [&](VariantValueWriter& writer) { writer.AppendShortString(value); });
}

void VariantObjectBuilder::AppendTimestampMicros(std::string_view field_name,
                                                 int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    AppendObjectPrimitive<VariantPrimitiveType::kTimestampMicros>(
        impl_->state, impl_->frame_index, field_name, micros);
    return;
  }
  AppendObjectPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(
      impl_->state, impl_->frame_index, field_name, micros);
}

void VariantObjectBuilder::AppendTimestampNanos(std::string_view field_name,
                                                int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    AppendObjectPrimitive<VariantPrimitiveType::kTimestampNanos>(
        impl_->state, impl_->frame_index, field_name, nanos);
    return;
  }
  AppendObjectPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(
      impl_->state, impl_->frame_index, field_name, nanos);
}

VariantObjectBuilder VariantObjectBuilder::StartObject(std::string_view field_name) {
  CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::Object);
  auto& frame = impl_->state->frames.back();
  const auto metadata_size = impl_->state->metadata.size();
  const auto field_count = frame.fields.size();

  auto field_id = impl_->state->metadata.Upsert(field_name);
  if (!frame.object_field_ids.insert(field_id).second) {
    impl_->state->metadata.Truncate(metadata_size);
    throw ParquetException("Duplicate Variant object field: ", field_name);
  }
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    impl_->state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    throw ParquetException("Variant object values are too large");
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

VariantListBuilder VariantObjectBuilder::StartList(std::string_view field_name) {
  CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::Object);
  auto& frame = impl_->state->frames.back();
  const auto metadata_size = impl_->state->metadata.size();
  const auto field_count = frame.fields.size();

  auto field_id = impl_->state->metadata.Upsert(field_name);
  if (!frame.object_field_ids.insert(field_id).second) {
    impl_->state->metadata.Truncate(metadata_size);
    throw ParquetException("Duplicate Variant object field: ", field_name);
  }
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    impl_->state->metadata.Truncate(metadata_size);
    TruncateFrameEntries(frame, field_count);
    throw ParquetException("Variant object values are too large");
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

void VariantObjectBuilder::Finish() {
  FinishFrame(impl_->state, impl_->frame_index, VariantContainerKind::Object,
              impl_->callback);
  impl_->active = false;
}

VariantListBuilder::VariantListBuilder(
    std::unique_ptr<internal::NestedVariantBuilderImpl> impl)
    : impl_(std::move(impl)) {}
VariantListBuilder::~VariantListBuilder() { RollbackIfActive(impl_.get()); }
VariantListBuilder::VariantListBuilder(VariantListBuilder&&) noexcept = default;
VariantListBuilder& VariantListBuilder::operator=(VariantListBuilder&&) noexcept =
    default;

void VariantListBuilder::AppendVariantNull() {
  AppendListPrimitive<VariantPrimitiveType::kNull>(impl_->state, impl_->frame_index);
}

void VariantListBuilder::AppendBoolean(bool value) {
  if (value) {
    AppendListPrimitive<VariantPrimitiveType::kBooleanTrue>(impl_->state,
                                                            impl_->frame_index);
    return;
  }
  AppendListPrimitive<VariantPrimitiveType::kBooleanFalse>(impl_->state,
                                                           impl_->frame_index);
}

#define VARIANT_LIST_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)                                  \
  void VariantListBuilder::Append##NAME(C_TYPE value) {                                  \
    AppendListPrimitive<VariantPrimitiveType::k##TYPE>(impl_->state, impl_->frame_index, \
                                                       value);                           \
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

void VariantListBuilder::AppendDecimal4(int32_t unscaled_value, uint8_t scale) {
  AppendListPrimitive<VariantPrimitiveType::kDecimal4>(impl_->state, impl_->frame_index,
                                                       unscaled_value, scale);
}

void VariantListBuilder::AppendDecimal8(int64_t unscaled_value, uint8_t scale) {
  AppendListPrimitive<VariantPrimitiveType::kDecimal8>(impl_->state, impl_->frame_index,
                                                       unscaled_value, scale);
}

void VariantListBuilder::AppendDecimal16(std::string_view little_endian_unscaled_value,
                                         uint8_t scale) {
  AppendListPrimitive<VariantPrimitiveType::kDecimal16>(
      impl_->state, impl_->frame_index, little_endian_unscaled_value, scale);
}

void VariantListBuilder::AppendShortString(std::string_view value) {
  AppendListPrimitiveWith(
      impl_->state, impl_->frame_index,
      [&](VariantValueWriter& writer) { writer.AppendShortString(value); });
}

void VariantListBuilder::AppendTimestampMicros(int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    AppendListPrimitive<VariantPrimitiveType::kTimestampMicros>(
        impl_->state, impl_->frame_index, micros);
    return;
  }
  AppendListPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(
      impl_->state, impl_->frame_index, micros);
}

void VariantListBuilder::AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    AppendListPrimitive<VariantPrimitiveType::kTimestampNanos>(impl_->state,
                                                               impl_->frame_index, nanos);
    return;
  }
  AppendListPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(
      impl_->state, impl_->frame_index, nanos);
}

VariantObjectBuilder VariantListBuilder::StartObject() {
  CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::List);
  auto& frame = impl_->state->frames.back();
  const auto element_count = frame.offsets.size();
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant array values are too large");
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

VariantListBuilder VariantListBuilder::StartList() {
  CheckTopFrame(*impl_->state, impl_->frame_index, VariantContainerKind::List);
  auto& frame = impl_->state->frames.back();
  const auto element_count = frame.offsets.size();
  const auto offset = impl_->state->value.length() - frame.value_start;
  DCHECK_GE(offset, 0);
  if (offset > std::numeric_limits<uint32_t>::max()) {
    throw ParquetException("Variant array values are too large");
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

void VariantListBuilder::Finish() {
  FinishFrame(impl_->state, impl_->frame_index, VariantContainerKind::List,
              impl_->callback);
  impl_->active = false;
}

struct VariantArrayBuilder::Impl {
  explicit Impl(MemoryPool* pool)
      : pool(pool), metadata_column(pool), value_column(pool), validity_builder(pool) {}

  template <typename Write>
  void AppendValue(Write&& write) {
    auto state = std::make_shared<VariantBuildState>(pool, value_column.bytes_builder());
    AppendRootPrimitiveWith(state, std::forward<Write>(write));
    CommitBuiltRow(state);
  }

  template <VariantPrimitiveType type, typename... Args>
  void AppendPrimitive(Args&&... args) {
    AppendValue([&](VariantValueWriter& writer) {
      writer.template Append<type>(std::forward<Args>(args)...);
    });
  }

  void AppendShortString(std::string_view value) {
    AppendValue([&](VariantValueWriter& writer) { writer.AppendShortString(value); });
  }

  void AppendEncoded(const EncodedVariantValue& value) {
    if (value.metadata == nullptr || value.value == nullptr) {
      throw ParquetException(
          "Encoded Variant metadata and value buffers must be non-null");
    }
    const auto metadata_bytes = std::string_view{*value.metadata};
    const auto value_bytes = std::string_view{*value.value};
    auto metadata = VariantMetadataView::Make(metadata_bytes);
    VariantValueView::Validate(value_bytes, metadata);
    CommitEncodedRow(metadata_bytes, value_bytes);
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

  void CommitBuiltRow(const std::shared_ptr<VariantBuildState>& state) {
    if (!state->frames.empty()) {
      throw ParquetException("Cannot commit Variant row with active containers");
    }
    if (!state->root_has_value) {
      throw ParquetException("Cannot commit empty Variant row");
    }

    const auto value_start = state->root_value_start;
    const auto metadata_mark = metadata_column.mark();
    auto value_mark = value_column.mark();
    DCHECK_LE(value_start, value_mark.bytes_length);
    value_mark.bytes_length = value_start;
    try {
      const auto metadata_start = metadata_column.bytes_builder().length();
      state->metadata.AppendTo(metadata_column.bytes_builder());
      const auto metadata_bytes = metadata_column.SliceFrom(metadata_start);
      auto metadata = VariantMetadataView::Make(metadata_bytes);

      const auto value_bytes = value_column.SliceFrom(value_start);
      VariantValueView::Validate(value_bytes, metadata);

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
    std::shared_ptr<StructArray> storage;
    PARQUET_ASSIGN_OR_THROW(storage,
                            StructArray::Make({metadata, value}, storage_type->fields(),
                                              std::move(null_bitmap), null_count));
    return MakeVariantArrayFromStorage(storage);
  }

  MemoryPool* pool;
  BinaryViewColumnBuilder metadata_column;
  BinaryViewColumnBuilder value_column;
  TypedBufferBuilder<bool> validity_builder;

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

void VariantArrayBuilder::AppendVariantNull() {
  impl_->AppendPrimitive<VariantPrimitiveType::kNull>();
}

void VariantArrayBuilder::AppendBoolean(bool value) {
  if (value) {
    impl_->AppendPrimitive<VariantPrimitiveType::kBooleanTrue>();
    return;
  }
  impl_->AppendPrimitive<VariantPrimitiveType::kBooleanFalse>();
}

#define VARIANT_ARRAY_APPEND_ONE_ARG(NAME, TYPE, C_TYPE)          \
  void VariantArrayBuilder::Append##NAME(C_TYPE value) {          \
    impl_->AppendPrimitive<VariantPrimitiveType::k##TYPE>(value); \
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

void VariantArrayBuilder::AppendDecimal4(int32_t unscaled_value, uint8_t scale) {
  impl_->AppendPrimitive<VariantPrimitiveType::kDecimal4>(unscaled_value, scale);
}

void VariantArrayBuilder::AppendDecimal8(int64_t unscaled_value, uint8_t scale) {
  impl_->AppendPrimitive<VariantPrimitiveType::kDecimal8>(unscaled_value, scale);
}

void VariantArrayBuilder::AppendDecimal16(std::string_view little_endian_unscaled_value,
                                          uint8_t scale) {
  impl_->AppendPrimitive<VariantPrimitiveType::kDecimal16>(little_endian_unscaled_value,
                                                           scale);
}

void VariantArrayBuilder::AppendShortString(std::string_view value) {
  impl_->AppendShortString(value);
}

void VariantArrayBuilder::AppendTimestampMicros(int64_t micros, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    impl_->AppendPrimitive<VariantPrimitiveType::kTimestampMicros>(micros);
    return;
  }
  impl_->AppendPrimitive<VariantPrimitiveType::kTimestampNTZMicros>(micros);
}

void VariantArrayBuilder::AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc) {
  if (adjusted_to_utc) {
    impl_->AppendPrimitive<VariantPrimitiveType::kTimestampNanos>(nanos);
    return;
  }
  impl_->AppendPrimitive<VariantPrimitiveType::kTimestampNTZNanos>(nanos);
}

void VariantArrayBuilder::AppendEncoded(const EncodedVariantValue& value) {
  impl_->AppendEncoded(value);
}

VariantObjectBuilder VariantArrayBuilder::StartObject() {
  auto state = std::make_shared<VariantBuildState>(impl_->pool,
                                                   impl_->value_column.bytes_builder());
  state->frames.push_back(VariantBuildFrame{.kind = VariantContainerKind::Object,
                                            .value_start = state->value.length(),
                                            .metadata_size = state->metadata.size()});
  return VariantObjectBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      state, state->frames.size() - 1,
      std::bind_front(&Impl::CommitBuiltRow, impl_.get())));
}

VariantListBuilder VariantArrayBuilder::StartList() {
  auto state = std::make_shared<VariantBuildState>(impl_->pool,
                                                   impl_->value_column.bytes_builder());
  state->frames.push_back(VariantBuildFrame{.kind = VariantContainerKind::List,
                                            .value_start = state->value.length(),
                                            .metadata_size = state->metadata.size()});
  return VariantListBuilder(std::make_unique<internal::NestedVariantBuilderImpl>(
      state, state->frames.size() - 1,
      std::bind_front(&Impl::CommitBuiltRow, impl_.get())));
}

std::shared_ptr<VariantArray> VariantArrayBuilder::Finish() {
  auto out = impl_->Finish();
  Reset();
  return out;
}

void VariantArrayBuilder::Reset() { impl_ = std::make_unique<Impl>(impl_->pool); }

std::shared_ptr<VariantArray> MakeVariantArrayFromStorage(
    std::shared_ptr<StructArray> storage) {
  if (storage == nullptr) {
    throw ParquetException("Variant storage array must be non-null");
  }
  std::shared_ptr<DataType> type;
  PARQUET_ASSIGN_OR_THROW(type, VariantExtensionType::Make(storage->type()));
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

  std::shared_ptr<StructArray> storage;
  PARQUET_ASSIGN_OR_THROW(storage,
                          StructArray::Make(std::move(children), struct_type.fields(),
                                            std::move(null_bitmap)));
  return MakeVariantArrayFromStorage(storage);
}

}  // namespace parquet::variant

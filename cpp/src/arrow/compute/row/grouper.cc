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

#include "arrow/compute/row/grouper.h"

#include <memory>
#include <mutex>

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/compute/exec/key_map.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/light_array.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/compare_internal.h"
#include "arrow/type.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

namespace {

inline const uint8_t* GetValuesAsBytes(const ArrayData& data, int64_t offset = 0) {
  int64_t absolute_byte_offset = (data.offset + offset) * data.type->byte_width();
  return data.GetValues<uint8_t>(1, absolute_byte_offset);
}

inline const uint8_t* GetValuesAsBytes(const Array& array, int64_t offset = 0) {
  return GetValuesAsBytes(*array.data(), offset);
}

inline const uint8_t* GetValuesAsBytes(const ArraySpan& data, int64_t offset = 0) {
  int64_t absolute_byte_offset = (data.offset + offset) * data.type->byte_width();
  return data.GetValues<uint8_t>(1, absolute_byte_offset);
}

template <typename Value>
Status CheckForGetNextSegment(const std::vector<Value>& values, int64_t length,
                              int64_t offset, const std::vector<TypeHolder>& key_types) {
  if (offset < 0 || offset > length) {
    return Status::Invalid("invalid grouping segmenter offset: ", offset);
  }
  if (values.size() != key_types.size()) {
    return Status::Invalid("expected batch size ", key_types.size(), " but got ",
                           values.size());
  }
  for (size_t i = 0; i < key_types.size(); i++) {
    const auto& value = values[i];
    const auto& key_type = key_types[i];
    if (*value.type() != *key_type.type) {
      return Status::Invalid("expected batch value ", i, " of type ", *key_type.type,
                             " but got ", *value.type());
    }
  }
  return Status::OK();
}

Status CheckForGetNextSegment(const ExecSpan& batch, int64_t offset,
                              const std::vector<TypeHolder>& key_types) {
  return CheckForGetNextSegment(batch.values, batch.length, offset, key_types);
}

Status CheckForGetNextSegment(const ExecBatch& batch, int64_t offset,
                              const std::vector<TypeHolder>& key_types) {
  return CheckForGetNextSegment(batch.values, batch.length, offset, key_types);
}

struct StatelessGroupingSegmenter : public GroupingSegmenter {
  Status Reset() override { return Status::OK(); }
};

GroupingSegment MakeSegment(int64_t batch_length, int64_t offset, int64_t length) {
  return GroupingSegment{offset, length, offset + length >= batch_length};
}

int64_t GetMatchLength(const uint8_t* match_bytes, int64_t match_width,
                       const uint8_t* array_bytes, int64_t offset, int64_t length) {
  int64_t cursor, byte_cursor;
  for (cursor = offset, byte_cursor = match_width * cursor; cursor < length;
       cursor++, byte_cursor += match_width) {
    if (memcmp(match_bytes, array_bytes + byte_cursor,
               static_cast<size_t>(match_width)) != 0) {
      break;
    }
  }
  return std::min(cursor, length - offset);
}

Result<GroupingSegment> GetNextSegmentChunked(
    const std::shared_ptr<ChunkedArray>& chunked_array, int64_t offset) {
  if (offset >= chunked_array->length()) {
    return MakeSegment(chunked_array->length(), chunked_array->length(), 0);
  }
  int64_t remaining_offset = offset;
  const auto& arrays = chunked_array->chunks();
  for (size_t i = 0; remaining_offset >= 0 && i < arrays.size(); i++) {
    // look up chunk containing offset
    int64_t array_length = arrays[i]->length();
    if (remaining_offset < array_length) {
      // found - switch to matching
      int64_t match_width = arrays[i]->type()->byte_width();
      const uint8_t* match_bytes = GetValuesAsBytes(*arrays[i]->data(), remaining_offset);
      int64_t total_match_length = 0;
      for (; i < arrays.size(); i++) {
        int64_t array_length = arrays[i]->length();
        if (array_length <= 0) continue;
        const uint8_t* array_bytes = GetValuesAsBytes(*arrays[i]->data());
        int64_t match_length = GetMatchLength(match_bytes, match_width, array_bytes,
                                              remaining_offset, array_length);
        total_match_length += match_length;
        remaining_offset = 0;
        if (match_length < array_length - remaining_offset) break;
      }
      return MakeSegment(chunked_array->length(), offset, total_match_length);
    }
    remaining_offset -= array_length;
  }
  return Status::Invalid("segmenting invalid chunked array value");
}

struct NoKeysGroupingSegmenter : public StatelessGroupingSegmenter {
  static Result<std::unique_ptr<GroupingSegmenter>> Make() {
    return std::make_unique<NoKeysGroupingSegmenter>();
  }

  template <typename Batch>
  Result<GroupingSegment> GetNextSegmentImpl(const Batch& batch, int64_t offset) {
    ARROW_RETURN_NOT_OK(CheckForGetNextSegment(batch, offset, {}));
    return MakeSegment(batch.length, offset, batch.length - offset);
  }

  Result<GroupingSegment> GetNextSegment(const ExecSpan& batch, int64_t offset) override {
    return GetNextSegmentImpl(batch, offset);
  }

  Result<GroupingSegment> GetNextSegment(const ExecBatch& batch,
                                         int64_t offset) override {
    return GetNextSegmentImpl(batch, offset);
  }
};

struct SimpleKeyGroupingSegmenter : public StatelessGroupingSegmenter {
  static Result<std::unique_ptr<GroupingSegmenter>> Make(
      std::vector<TypeHolder> key_types) {
    return std::make_unique<SimpleKeyGroupingSegmenter>(key_types);
  }

  explicit SimpleKeyGroupingSegmenter(std::vector<TypeHolder> key_types)
      : key_types_(std::move(key_types)) {}

  Status CheckType(const DataType& type) {
    if (!is_fixed_width(type)) {
      return Status::Invalid("SimpleKeyGroupingSegmenter does not support type ", type);
    }
    return Status::OK();
  }

  Result<GroupingSegment> GetNextSegment(const Scalar& scalar, int64_t offset,
                                         int64_t length) {
    ARROW_RETURN_NOT_OK(CheckType(*scalar.type));
    if (!scalar.is_valid) {
      return Status::Invalid("segmenting an invalid scalar");
    }
    return MakeSegment(length, 0, length);
  }

  Result<GroupingSegment> GetNextSegment(const DataType& array_type,
                                         const uint8_t* array_bytes, int64_t offset,
                                         int64_t length) {
    RETURN_NOT_OK(CheckType(array_type));
    int64_t byte_width = array_type.byte_width();
    int64_t match_length = GetMatchLength(array_bytes + offset * byte_width, byte_width,
                                          array_bytes, offset, length);
    return MakeSegment(length, offset, match_length);
  }

  Result<GroupingSegment> GetNextSegment(const ExecSpan& batch, int64_t offset) override {
    ARROW_RETURN_NOT_OK(CheckForGetNextSegment(batch, offset, key_types_));
    const auto& value = batch.values[0];
    if (value.is_scalar()) {
      return GetNextSegment(*value.scalar, offset, batch.length);
    }
    ARROW_DCHECK(value.is_array());
    const auto& array = value.array;
    if (array.GetNullCount() > 0) {
      return Status::NotImplemented("segmenting a nullable array");
    }
    return GetNextSegment(*array.type, GetValuesAsBytes(array), offset, batch.length);
  }

  Result<GroupingSegment> GetNextSegment(const ExecBatch& batch,
                                         int64_t offset) override {
    ARROW_RETURN_NOT_OK(CheckForGetNextSegment(batch, offset, key_types_));
    const auto& value = batch.values[0];
    if (value.is_scalar()) {
      return GetNextSegment(*value.scalar(), offset, batch.length);
    }
    if (value.is_array()) {
      auto array = value.array();
      if (array->GetNullCount() > 0) {
        return Status::NotImplemented("segmenting a nullable array");
      }
      return GetNextSegment(*array->type, GetValuesAsBytes(*array), offset, batch.length);
    }
    if (value.is_chunked_array()) {
      auto array = value.chunked_array();
      if (array->null_count() > 0) {
        return Status::NotImplemented("segmenting a nullable array");
      }
      return GetNextSegmentChunked(array, offset);
    }
    return Status::Invalid("segmenting unsupported value kind ", value.kind());
  }

 private:
  const std::vector<TypeHolder> key_types_;
};

struct AnyKeysGroupingSegmenter : public StatelessGroupingSegmenter {
  static Result<std::unique_ptr<GroupingSegmenter>> Make(
      std::vector<TypeHolder> key_types, ExecContext* ctx) {
    ARROW_RETURN_NOT_OK(Grouper::Make(key_types, ctx));  // check types
    return std::make_unique<AnyKeysGroupingSegmenter>(key_types, ctx);
  }

  AnyKeysGroupingSegmenter(std::vector<TypeHolder> key_types, ExecContext* ctx)
      : key_types_(std::move(key_types)), ctx_(ctx) {}

  template <typename Batch>
  Result<GroupingSegment> GetNextSegmentImpl(const Batch& batch, int64_t offset) {
    ARROW_RETURN_NOT_OK(CheckForGetNextSegment(batch, offset, key_types_));
    // TODO: make Grouper support Reset(), so it can be cached instead of recreated here
    ARROW_ASSIGN_OR_RAISE(auto grouper, Grouper::Make(key_types_, ctx_));
    ARROW_ASSIGN_OR_RAISE(auto datum, grouper->Consume(batch, offset));
    if (datum.is_array()) {
      const std::shared_ptr<ArrayData>& data = datum.array();
      ARROW_DCHECK(data->GetNullCount() == 0);
      ARROW_DCHECK(*data->type == *uint32());  // as long as Grouper::Consume gives uint32
      const uint32_t* values = data->GetValues<uint32_t>(1);
      int64_t cursor;
      for (cursor = 1; cursor < data->length; cursor++) {
        if (values[0] != values[cursor]) break;
      }
      return MakeSegment(batch.length, offset, std::min(cursor, batch.length - offset));
    } else if (datum.is_chunked_array()) {
      return GetNextSegmentChunked(datum.chunked_array(), offset);
    } else {
      return Status::Invalid("segmenting unsupported datum kind ", datum.kind());
    }
  }

  Result<GroupingSegment> GetNextSegment(const ExecSpan& batch, int64_t offset) override {
    return GetNextSegmentImpl(batch, offset);
  }

  Result<GroupingSegment> GetNextSegment(const ExecBatch& batch,
                                         int64_t offset) override {
    return GetNextSegmentImpl(batch, offset);
  }

 private:
  const std::vector<TypeHolder> key_types_;
  ExecContext* const ctx_;
};

Status CheckForConsume(const ExecSpan& batch, int64_t& consume_offset,
                       int64_t& consume_length) {
  if (consume_offset < 0) {
    return Status::Invalid("invalid grouper consume offset: ", consume_offset);
  }
  if (consume_length < 0) {
    consume_length = batch.length - consume_offset;
  }
  return Status::OK();
}

}  // namespace

Result<std::unique_ptr<GroupingSegmenter>> GroupingSegmenter::Make(
    std::vector<TypeHolder> key_types, ExecContext* ctx) {
  if (key_types.size() == 0) {
    return NoKeysGroupingSegmenter::Make();
  } else if (key_types.size() == 1) {
    const DataType* type = key_types[0].type;
    if (type != NULLPTR && is_fixed_width(*type)) {
      return SimpleKeyGroupingSegmenter::Make(std::move(key_types));
    }
  }
  return AnyKeysGroupingSegmenter::Make(std::move(key_types), ctx);
}

namespace {

struct BaseGrouper : public Grouper {
  int IndexOfChunk(const ExecBatch& batch) {
    int i = 0;
    for (const auto& value : batch.values) {
      if (value.is_chunked_array()) {
        return i;
      }
      ++i;
    }
    return -1;
  }

  bool HasConsistentChunks(const ExecBatch& batch, int index_of_chunk) {
    auto first_chunked_array = batch.values[index_of_chunk].chunked_array();
    if (first_chunked_array < 0) {
      // having no chunks is considered consistent
      return true;
    }
    int num_chunks = first_chunked_array->num_chunks();
    int64_t length = first_chunked_array->length();
    for (const auto& value : batch.values) {
      if (!value.is_chunked_array()) {
        continue;
      }
      auto curr_chunk = value.chunked_array();
      if (num_chunks != curr_chunk->num_chunks() || length != curr_chunk->length()) {
        return false;
      }
    }
    if (num_chunks > 0) {
      for (int i = 0; i < num_chunks; i++) {
        int64_t chunk_length = first_chunked_array->chunk(i)->length();
        for (const auto& value : batch.values) {
          if (!value.is_chunked_array()) {
            continue;
          }
          auto curr_chunk = value.chunked_array();
          if (chunk_length != curr_chunk->chunk(i)->length()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  using Grouper::Consume;

  Result<Datum> Consume(const ExecBatch& batch, int64_t consume_offset,
                        int64_t consume_length) override {
    int index_of_chunk = IndexOfChunk(batch);
    if (index_of_chunk < 0) {
      return Consume(ExecSpan(batch), consume_offset, consume_length);
    }
    if (!HasConsistentChunks(batch, index_of_chunk)) {
      return Status::Invalid("consuming inconsistent chunks");
    }
    auto first_chunked_array = batch.values[index_of_chunk].chunked_array();
    int num_chunks = first_chunked_array->num_chunks();
    std::vector<Datum> consumed(num_chunks);
    int64_t offset = 0;
    int chunk_idx = 0;
    while (offset < batch.length) {
      int64_t chunk_length = first_chunked_array->chunk(chunk_idx)->length();
      std::vector<ExecValue> values;
      size_t i = 0;
      for (const auto& batch_value : batch.values) {
        if (batch_value.is_scalar()) {
          values.emplace_back(batch_value.scalar().get());
        } else if (batch_value.is_array()) {
          ArraySpan array_span(*batch_value.array());
          array_span.offset = offset;
          array_span.length = chunk_length;
          values.emplace_back(array_span);
        } else if (batch_value.is_chunked_array()) {
          values.emplace_back(*batch_value.chunked_array()->chunk(chunk_idx)->data());
        } else {
          return Status::Invalid("consuming batch value ", i, " of unsupported kind ",
                                 batch_value.kind());
        }
        ++i;
      }
      ARROW_ASSIGN_OR_RAISE(consumed[chunk_idx],
                            Consume(ExecSpan(values, chunk_length), 0, chunk_length));
      offset += chunk_length;
      ++chunk_idx;
    }
    ArrayVector chunks(num_chunks);
    while (--chunk_idx >= 0) {
      DCHECK(consumed[chunk_idx].is_array());
      chunks[chunk_idx] = consumed[chunk_idx].make_array();
    }
    ARROW_ASSIGN_OR_RAISE(auto chunked_array, ChunkedArray::Make(std::move(chunks)));
    return Datum(chunked_array);
  }
};

struct GrouperNoKeysImpl : Grouper {
  Result<std::shared_ptr<Array>> MakeConstantUInt32Array(int64_t length, uint32_t value) {
    std::unique_ptr<ArrayBuilder> a_builder;
    RETURN_NOT_OK(MakeBuilder(default_memory_pool(), uint32(), &a_builder));
    auto builder = checked_cast<UInt32Builder*>(a_builder.get());
    if (length != 0) {
      RETURN_NOT_OK(builder->Resize(length));
    }
    for (int64_t i = 0; i < length; i++) {
      builder->UnsafeAppend(value);
    }
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(builder->Finish(&array));
    return std::move(array);
  }
  Result<Datum> Consume(const ExecSpan& batch, int64_t consume_offset,
                        int64_t consume_length) override {
    ARROW_ASSIGN_OR_RAISE(auto array, MakeConstantUInt32Array(consume_length, 0));
    return Datum(array);
  }
  Result<Datum> Consume(const ExecBatch& batch, int64_t consume_offset,
                        int64_t consume_length) override {
    ARROW_ASSIGN_OR_RAISE(auto array, MakeConstantUInt32Array(consume_length, 0));
    return Datum(array);
  }
  Result<ExecBatch> GetUniques() override {
    auto data = ArrayData::Make(uint32(), 1, 0);
    auto values = data->GetMutableValues<uint32_t>(0);
    values[0] = 0;
    ExecBatch out({Datum(data)}, 1);
    return std::move(out);
  }
  uint32_t num_groups() const override { return 1; }
};

struct GrouperImpl : public BaseGrouper {
  static Result<std::unique_ptr<GrouperImpl>> Make(
      const std::vector<TypeHolder>& key_types, ExecContext* ctx) {
    auto impl = std::make_unique<GrouperImpl>();

    impl->encoders_.resize(key_types.size());
    impl->ctx_ = ctx;

    for (size_t i = 0; i < key_types.size(); ++i) {
      // TODO(wesm): eliminate this probably unneeded shared_ptr copy
      std::shared_ptr<DataType> key = key_types[i].GetSharedPtr();

      if (key->id() == Type::BOOL) {
        impl->encoders_[i] = std::make_unique<internal::BooleanKeyEncoder>();
        continue;
      }

      if (key->id() == Type::DICTIONARY) {
        impl->encoders_[i] =
            std::make_unique<internal::DictionaryKeyEncoder>(key, ctx->memory_pool());
        continue;
      }

      if (is_fixed_width(key->id())) {
        impl->encoders_[i] = std::make_unique<internal::FixedWidthKeyEncoder>(key);
        continue;
      }

      if (is_binary_like(key->id())) {
        impl->encoders_[i] =
            std::make_unique<internal::VarLengthKeyEncoder<BinaryType>>(key);
        continue;
      }

      if (is_large_binary_like(key->id())) {
        impl->encoders_[i] =
            std::make_unique<internal::VarLengthKeyEncoder<LargeBinaryType>>(key);
        continue;
      }

      if (key->id() == Type::NA) {
        impl->encoders_[i] = std::make_unique<internal::NullKeyEncoder>();
        continue;
      }

      return Status::NotImplemented("Keys of type ", *key);
    }

    return std::move(impl);
  }

  using BaseGrouper::Consume;

  Result<Datum> Consume(const ExecSpan& batch, int64_t consume_offset,
                        int64_t consume_length) override {
    ARROW_RETURN_NOT_OK(CheckForConsume(batch, consume_offset, consume_length));
    if (consume_offset != 0 || consume_length != batch.length) {
      auto batch_slice = batch.ToExecBatch().Slice(consume_offset, consume_length);
      return Consume(ExecSpan(batch_slice), 0, -1);
    }
    std::vector<int32_t> offsets_batch(batch.length + 1);
    for (int i = 0; i < batch.num_values(); ++i) {
      encoders_[i]->AddLength(batch[i], batch.length, offsets_batch.data());
    }

    int32_t total_length = 0;
    for (int64_t i = 0; i < batch.length; ++i) {
      auto total_length_before = total_length;
      total_length += offsets_batch[i];
      offsets_batch[i] = total_length_before;
    }
    offsets_batch[batch.length] = total_length;

    std::vector<uint8_t> key_bytes_batch(total_length);
    std::vector<uint8_t*> key_buf_ptrs(batch.length);
    for (int64_t i = 0; i < batch.length; ++i) {
      key_buf_ptrs[i] = key_bytes_batch.data() + offsets_batch[i];
    }

    for (int i = 0; i < batch.num_values(); ++i) {
      RETURN_NOT_OK(encoders_[i]->Encode(batch[i], batch.length, key_buf_ptrs.data()));
    }

    TypedBufferBuilder<uint32_t> group_ids_batch(ctx_->memory_pool());
    RETURN_NOT_OK(group_ids_batch.Resize(batch.length));

    for (int64_t i = 0; i < batch.length; ++i) {
      int32_t key_length = offsets_batch[i + 1] - offsets_batch[i];
      std::string key(
          reinterpret_cast<const char*>(key_bytes_batch.data() + offsets_batch[i]),
          key_length);

      auto it_success = map_.emplace(key, num_groups_);
      auto group_id = it_success.first->second;

      if (it_success.second) {
        // new key; update offsets and key_bytes
        ++num_groups_;
        // Skip if there are no keys
        if (key_length > 0) {
          auto next_key_offset = static_cast<int32_t>(key_bytes_.size());
          key_bytes_.resize(next_key_offset + key_length);
          offsets_.push_back(next_key_offset + key_length);
          memcpy(key_bytes_.data() + next_key_offset, key.c_str(), key_length);
        }
      }

      group_ids_batch.UnsafeAppend(group_id);
    }

    ARROW_ASSIGN_OR_RAISE(auto group_ids, group_ids_batch.Finish());
    return Datum(UInt32Array(batch.length, std::move(group_ids)));
  }

  uint32_t num_groups() const override { return num_groups_; }

  Result<ExecBatch> GetUniques() override {
    ExecBatch out({}, num_groups_);

    std::vector<uint8_t*> key_buf_ptrs(num_groups_);
    for (int64_t i = 0; i < num_groups_; ++i) {
      key_buf_ptrs[i] = key_bytes_.data() + offsets_[i];
    }

    out.values.resize(encoders_.size());
    for (size_t i = 0; i < encoders_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          out.values[i],
          encoders_[i]->Decode(key_buf_ptrs.data(), static_cast<int32_t>(num_groups_),
                               ctx_->memory_pool()));
    }

    return out;
  }

  ExecContext* ctx_;
  std::unordered_map<std::string, uint32_t> map_;
  std::vector<int32_t> offsets_ = {0};
  std::vector<uint8_t> key_bytes_;
  uint32_t num_groups_ = 0;
  std::vector<std::unique_ptr<internal::KeyEncoder>> encoders_;
};

struct GrouperFastImpl : public BaseGrouper {
  static constexpr int kBitmapPaddingForSIMD = 64;  // bits
  static constexpr int kPaddingForSIMD = 32;        // bytes

  static bool CanUse(const std::vector<TypeHolder>& key_types) {
#if ARROW_LITTLE_ENDIAN
    for (size_t i = 0; i < key_types.size(); ++i) {
      if (is_large_binary_like(key_types[i].id())) {
        return false;
      }
    }
    return true;
#else
    return false;
#endif
  }

  static Result<std::unique_ptr<GrouperFastImpl>> Make(
      const std::vector<TypeHolder>& keys, ExecContext* ctx) {
    auto impl = std::make_unique<GrouperFastImpl>();
    impl->ctx_ = ctx;

    RETURN_NOT_OK(impl->temp_stack_.Init(ctx->memory_pool(), 64 * minibatch_size_max_));
    impl->encode_ctx_.hardware_flags =
        arrow::internal::CpuInfo::GetInstance()->hardware_flags();
    impl->encode_ctx_.stack = &impl->temp_stack_;

    auto num_columns = keys.size();
    impl->col_metadata_.resize(num_columns);
    impl->key_types_.resize(num_columns);
    impl->dictionaries_.resize(num_columns);
    for (size_t icol = 0; icol < num_columns; ++icol) {
      const TypeHolder& key = keys[icol];
      if (key.id() == Type::DICTIONARY) {
        auto bit_width = checked_cast<const FixedWidthType&>(*key).bit_width();
        ARROW_DCHECK(bit_width % 8 == 0);
        impl->col_metadata_[icol] = KeyColumnMetadata(true, bit_width / 8);
      } else if (key.id() == Type::BOOL) {
        impl->col_metadata_[icol] = KeyColumnMetadata(true, 0);
      } else if (is_fixed_width(key.id())) {
        impl->col_metadata_[icol] = KeyColumnMetadata(
            true, checked_cast<const FixedWidthType&>(*key).bit_width() / 8);
      } else if (is_binary_like(key.id())) {
        impl->col_metadata_[icol] = KeyColumnMetadata(false, sizeof(uint32_t));
      } else if (key.id() == Type::NA) {
        impl->col_metadata_[icol] = KeyColumnMetadata(true, 0, /*is_null_type_in=*/true);
      } else {
        return Status::NotImplemented("Keys of type ", *key);
      }
      impl->key_types_[icol] = key;
    }

    impl->encoder_.Init(impl->col_metadata_,
                        /* row_alignment = */ sizeof(uint64_t),
                        /* string_alignment = */ sizeof(uint64_t));
    RETURN_NOT_OK(impl->rows_.Init(ctx->memory_pool(), impl->encoder_.row_metadata()));
    RETURN_NOT_OK(
        impl->rows_minibatch_.Init(ctx->memory_pool(), impl->encoder_.row_metadata()));
    impl->minibatch_size_ = impl->minibatch_size_min_;
    GrouperFastImpl* impl_ptr = impl.get();
    impl->map_equal_impl_ =
        [impl_ptr](int num_keys_to_compare, const uint16_t* selection_may_be_null,
                   const uint32_t* group_ids, uint32_t* out_num_keys_mismatch,
                   uint16_t* out_selection_mismatch, void*) {
          KeyCompare::CompareColumnsToRows(
              num_keys_to_compare, selection_may_be_null, group_ids,
              &impl_ptr->encode_ctx_, out_num_keys_mismatch, out_selection_mismatch,
              impl_ptr->encoder_.batch_all_cols(), impl_ptr->rows_,
              /* are_cols_in_encoding_order=*/true);
        };
    impl->map_append_impl_ = [impl_ptr](int num_keys, const uint16_t* selection, void*) {
      RETURN_NOT_OK(impl_ptr->encoder_.EncodeSelected(&impl_ptr->rows_minibatch_,
                                                      num_keys, selection));
      return impl_ptr->rows_.AppendSelectionFrom(impl_ptr->rows_minibatch_, num_keys,
                                                 nullptr);
    };
    RETURN_NOT_OK(impl->map_.init(impl->encode_ctx_.hardware_flags, ctx->memory_pool()));
    impl->cols_.resize(num_columns);
    impl->minibatch_hashes_.resize(impl->minibatch_size_max_ +
                                   kPaddingForSIMD / sizeof(uint32_t));

    return std::move(impl);
  }

  ~GrouperFastImpl() { map_.cleanup(); }

  using BaseGrouper::Consume;

  Result<Datum> Consume(const ExecSpan& batch, int64_t consume_offset,
                        int64_t consume_length) override {
    ARROW_RETURN_NOT_OK(CheckForConsume(batch, consume_offset, consume_length));
    if (consume_offset != 0 || consume_length != batch.length) {
      auto batch_slice = batch.ToExecBatch().Slice(consume_offset, consume_length);
      return Consume(ExecSpan(batch_slice), 0, -1);
    }
    // ARROW-14027: broadcast scalar arguments for now
    for (int i = 0; i < batch.num_values(); i++) {
      if (batch[i].is_scalar()) {
        ExecBatch expanded = batch.ToExecBatch();
        for (int j = i; j < expanded.num_values(); j++) {
          if (expanded.values[j].is_scalar()) {
            ARROW_ASSIGN_OR_RAISE(
                expanded.values[j],
                MakeArrayFromScalar(*expanded.values[j].scalar(), expanded.length,
                                    ctx_->memory_pool()));
          }
        }
        return ConsumeImpl(ExecSpan(expanded));
      }
    }
    return ConsumeImpl(batch);
  }

  Result<Datum> ConsumeImpl(const ExecSpan& batch) {
    int64_t num_rows = batch.length;
    int num_columns = batch.num_values();
    // Process dictionaries
    for (int icol = 0; icol < num_columns; ++icol) {
      if (key_types_[icol].id() == Type::DICTIONARY) {
        const ArraySpan& data = batch[icol].array;
        auto dict = MakeArray(data.dictionary().ToArrayData());
        if (dictionaries_[icol]) {
          if (!dictionaries_[icol]->Equals(dict)) {
            // TODO(bkietz) unify if necessary. For now, just error if any batch's
            // dictionary differs from the first we saw for this key
            return Status::NotImplemented("Unifying differing dictionaries");
          }
        } else {
          dictionaries_[icol] = std::move(dict);
        }
      }
    }

    std::shared_ptr<arrow::Buffer> group_ids;
    ARROW_ASSIGN_OR_RAISE(
        group_ids, AllocateBuffer(sizeof(uint32_t) * num_rows, ctx_->memory_pool()));

    for (int icol = 0; icol < num_columns; ++icol) {
      const uint8_t* non_nulls = NULLPTR;
      const uint8_t* fixedlen = NULLPTR;
      const uint8_t* varlen = NULLPTR;

      // Skip if the key's type is NULL
      if (key_types_[icol].id() != Type::NA) {
        if (batch[icol].array.buffers[0].data != NULLPTR) {
          non_nulls = batch[icol].array.buffers[0].data;
        }
        fixedlen = batch[icol].array.buffers[1].data;
        if (!col_metadata_[icol].is_fixed_length) {
          varlen = batch[icol].array.buffers[2].data;
        }
      }

      int64_t offset = batch[icol].array.offset;

      auto col_base = KeyColumnArray(col_metadata_[icol], offset + num_rows, non_nulls,
                                     fixedlen, varlen);

      cols_[icol] = col_base.Slice(offset, num_rows);
    }

    // Split into smaller mini-batches
    //
    for (uint32_t start_row = 0; start_row < num_rows;) {
      uint32_t batch_size_next = std::min(static_cast<uint32_t>(minibatch_size_),
                                          static_cast<uint32_t>(num_rows) - start_row);

      // Encode
      rows_minibatch_.Clean();
      encoder_.PrepareEncodeSelected(start_row, batch_size_next, cols_);

      // Compute hash
      Hashing32::HashMultiColumn(encoder_.batch_all_cols(), &encode_ctx_,
                                 minibatch_hashes_.data());

      // Map
      auto match_bitvector =
          util::TempVectorHolder<uint8_t>(&temp_stack_, (batch_size_next + 7) / 8);
      {
        auto local_slots = util::TempVectorHolder<uint8_t>(&temp_stack_, batch_size_next);
        map_.early_filter(batch_size_next, minibatch_hashes_.data(),
                          match_bitvector.mutable_data(), local_slots.mutable_data());
        map_.find(batch_size_next, minibatch_hashes_.data(),
                  match_bitvector.mutable_data(), local_slots.mutable_data(),
                  reinterpret_cast<uint32_t*>(group_ids->mutable_data()) + start_row,
                  &temp_stack_, map_equal_impl_, nullptr);
      }
      auto ids = util::TempVectorHolder<uint16_t>(&temp_stack_, batch_size_next);
      int num_ids;
      util::bit_util::bits_to_indexes(0, encode_ctx_.hardware_flags, batch_size_next,
                                      match_bitvector.mutable_data(), &num_ids,
                                      ids.mutable_data());

      RETURN_NOT_OK(map_.map_new_keys(
          num_ids, ids.mutable_data(), minibatch_hashes_.data(),
          reinterpret_cast<uint32_t*>(group_ids->mutable_data()) + start_row,
          &temp_stack_, map_equal_impl_, map_append_impl_, nullptr));

      start_row += batch_size_next;

      if (minibatch_size_ * 2 <= minibatch_size_max_) {
        minibatch_size_ *= 2;
      }
    }

    return Datum(UInt32Array(batch.length, std::move(group_ids)));
  }

  uint32_t num_groups() const override { return static_cast<uint32_t>(rows_.length()); }

  // Make sure padded buffers end up with the right logical size

  Result<std::shared_ptr<Buffer>> AllocatePaddedBitmap(int64_t length) {
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Buffer> buf,
        AllocateBitmap(length + kBitmapPaddingForSIMD, ctx_->memory_pool()));
    return SliceMutableBuffer(buf, 0, bit_util::BytesForBits(length));
  }

  Result<std::shared_ptr<Buffer>> AllocatePaddedBuffer(int64_t size) {
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Buffer> buf,
        AllocateBuffer(size + kBitmapPaddingForSIMD, ctx_->memory_pool()));
    return SliceMutableBuffer(buf, 0, size);
  }

  Result<ExecBatch> GetUniques() override {
    auto num_columns = static_cast<uint32_t>(col_metadata_.size());
    int64_t num_groups = rows_.length();

    std::vector<std::shared_ptr<Buffer>> non_null_bufs(num_columns);
    std::vector<std::shared_ptr<Buffer>> fixedlen_bufs(num_columns);
    std::vector<std::shared_ptr<Buffer>> varlen_bufs(num_columns);

    for (size_t i = 0; i < num_columns; ++i) {
      if (col_metadata_[i].is_null_type) {
        uint8_t* non_nulls = NULLPTR;
        uint8_t* fixedlen = NULLPTR;
        cols_[i] =
            KeyColumnArray(col_metadata_[i], num_groups, non_nulls, fixedlen, NULLPTR);
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(non_null_bufs[i], AllocatePaddedBitmap(num_groups));
      if (col_metadata_[i].is_fixed_length && !col_metadata_[i].is_null_type) {
        if (col_metadata_[i].fixed_length == 0) {
          ARROW_ASSIGN_OR_RAISE(fixedlen_bufs[i], AllocatePaddedBitmap(num_groups));
        } else {
          ARROW_ASSIGN_OR_RAISE(
              fixedlen_bufs[i],
              AllocatePaddedBuffer(num_groups * col_metadata_[i].fixed_length));
        }
      } else {
        ARROW_ASSIGN_OR_RAISE(fixedlen_bufs[i],
                              AllocatePaddedBuffer((num_groups + 1) * sizeof(uint32_t)));
      }
      cols_[i] =
          KeyColumnArray(col_metadata_[i], num_groups, non_null_bufs[i]->mutable_data(),
                         fixedlen_bufs[i]->mutable_data(), nullptr);
    }

    for (int64_t start_row = 0; start_row < num_groups;) {
      int64_t batch_size_next =
          std::min(num_groups - start_row, static_cast<int64_t>(minibatch_size_max_));
      encoder_.DecodeFixedLengthBuffers(start_row, start_row, batch_size_next, rows_,
                                        &cols_, encode_ctx_.hardware_flags, &temp_stack_);
      start_row += batch_size_next;
    }

    if (!rows_.metadata().is_fixed_length) {
      for (size_t i = 0; i < num_columns; ++i) {
        if (!col_metadata_[i].is_fixed_length) {
          auto varlen_size =
              reinterpret_cast<const uint32_t*>(fixedlen_bufs[i]->data())[num_groups];
          ARROW_ASSIGN_OR_RAISE(varlen_bufs[i], AllocatePaddedBuffer(varlen_size));
          cols_[i] = KeyColumnArray(
              col_metadata_[i], num_groups, non_null_bufs[i]->mutable_data(),
              fixedlen_bufs[i]->mutable_data(), varlen_bufs[i]->mutable_data());
        }
      }

      for (int64_t start_row = 0; start_row < num_groups;) {
        int64_t batch_size_next =
            std::min(num_groups - start_row, static_cast<int64_t>(minibatch_size_max_));
        encoder_.DecodeVaryingLengthBuffers(start_row, start_row, batch_size_next, rows_,
                                            &cols_, encode_ctx_.hardware_flags,
                                            &temp_stack_);
        start_row += batch_size_next;
      }
    }

    ExecBatch out({}, num_groups);
    out.values.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
      if (col_metadata_[i].is_null_type) {
        out.values[i] = ArrayData::Make(null(), num_groups, {nullptr}, num_groups);
        continue;
      }
      auto valid_count = arrow::internal::CountSetBits(
          non_null_bufs[i]->data(), /*offset=*/0, static_cast<int64_t>(num_groups));
      int null_count = static_cast<int>(num_groups) - static_cast<int>(valid_count);

      if (col_metadata_[i].is_fixed_length) {
        out.values[i] = ArrayData::Make(
            key_types_[i].GetSharedPtr(), num_groups,
            {std::move(non_null_bufs[i]), std::move(fixedlen_bufs[i])}, null_count);
      } else {
        out.values[i] =
            ArrayData::Make(key_types_[i].GetSharedPtr(), num_groups,
                            {std::move(non_null_bufs[i]), std::move(fixedlen_bufs[i]),
                             std::move(varlen_bufs[i])},
                            null_count);
      }
    }

    // Process dictionaries
    for (size_t icol = 0; icol < num_columns; ++icol) {
      if (key_types_[icol].id() == Type::DICTIONARY) {
        if (dictionaries_[icol]) {
          out.values[icol].array()->dictionary = dictionaries_[icol]->data();
        } else {
          ARROW_ASSIGN_OR_RAISE(auto dict,
                                MakeArrayOfNull(key_types_[icol].GetSharedPtr(), 0));
          out.values[icol].array()->dictionary = dict->data();
        }
      }
    }

    return out;
  }

  static constexpr int log_minibatch_max_ = 10;
  static constexpr int minibatch_size_max_ = 1 << log_minibatch_max_;
  static constexpr int minibatch_size_min_ = 128;
  int minibatch_size_;

  ExecContext* ctx_;
  arrow::util::TempVectorStack temp_stack_;
  LightContext encode_ctx_;

  std::vector<TypeHolder> key_types_;
  std::vector<KeyColumnMetadata> col_metadata_;
  std::vector<KeyColumnArray> cols_;
  std::vector<uint32_t> minibatch_hashes_;

  std::vector<std::shared_ptr<Array>> dictionaries_;

  RowTableImpl rows_;
  RowTableImpl rows_minibatch_;
  RowTableEncoder encoder_;
  SwissTable map_;
  SwissTable::EqualImpl map_equal_impl_;
  SwissTable::AppendImpl map_append_impl_;
};

}  // namespace

Result<std::unique_ptr<Grouper>> Grouper::Make(const std::vector<TypeHolder>& key_types,
                                               ExecContext* ctx) {
  if (GrouperFastImpl::CanUse(key_types)) {
    return GrouperFastImpl::Make(key_types, ctx);
  }
  return GrouperImpl::Make(key_types, ctx);
}

Result<std::shared_ptr<ListArray>> Grouper::ApplyGroupings(const ListArray& groupings,
                                                           const Array& array,
                                                           ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum sorted,
                        compute::Take(array, groupings.data()->child_data[0],
                                      TakeOptions::NoBoundsCheck(), ctx));

  return std::make_shared<ListArray>(list(array.type()), groupings.length(),
                                     groupings.value_offsets(), sorted.make_array());
}

Result<std::shared_ptr<ListArray>> Grouper::MakeGroupings(const UInt32Array& ids,
                                                          uint32_t num_groups,
                                                          ExecContext* ctx) {
  if (ids.null_count() != 0) {
    return Status::Invalid("MakeGroupings with null ids");
  }

  ARROW_ASSIGN_OR_RAISE(auto offsets, AllocateBuffer(sizeof(int32_t) * (num_groups + 1),
                                                     ctx->memory_pool()));
  auto raw_offsets = reinterpret_cast<int32_t*>(offsets->mutable_data());

  std::memset(raw_offsets, 0, offsets->size());
  for (int i = 0; i < ids.length(); ++i) {
    DCHECK_LT(ids.Value(i), num_groups);
    raw_offsets[ids.Value(i)] += 1;
  }
  int32_t length = 0;
  for (uint32_t id = 0; id < num_groups; ++id) {
    auto offset = raw_offsets[id];
    raw_offsets[id] = length;
    length += offset;
  }
  raw_offsets[num_groups] = length;
  DCHECK_EQ(ids.length(), length);

  ARROW_ASSIGN_OR_RAISE(auto offsets_copy,
                        offsets->CopySlice(0, offsets->size(), ctx->memory_pool()));
  raw_offsets = reinterpret_cast<int32_t*>(offsets_copy->mutable_data());

  ARROW_ASSIGN_OR_RAISE(auto sort_indices, AllocateBuffer(sizeof(int32_t) * ids.length(),
                                                          ctx->memory_pool()));
  auto raw_sort_indices = reinterpret_cast<int32_t*>(sort_indices->mutable_data());
  for (int i = 0; i < ids.length(); ++i) {
    raw_sort_indices[raw_offsets[ids.Value(i)]++] = i;
  }

  return std::make_shared<ListArray>(
      list(int32()), num_groups, std::move(offsets),
      std::make_shared<Int32Array>(ids.length(), std::move(sort_indices)));
}

}  // namespace compute
}  // namespace arrow

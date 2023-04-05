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
#include <type_traits>

#include "arrow/array/builder_primitive.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/row_encoder_internal.h"
#include "arrow/compute/key_hash.h"
#include "arrow/compute/light_array.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/compare_internal.h"
#include "arrow/compute/row/grouper_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"

namespace arrow {

using internal::checked_cast;
using internal::PrimitiveScalarBase;

namespace compute {

namespace {

constexpr uint32_t kNoGroupId = std::numeric_limits<uint32_t>::max();

using group_id_t = std::remove_const<decltype(kNoGroupId)>::type;
using GroupIdType = CTypeTraits<group_id_t>::ArrowType;
auto g_group_id_type = std::make_shared<GroupIdType>();

inline const uint8_t* GetValuesAsBytes(const ArraySpan& data, int64_t offset = 0) {
  DCHECK_GT(data.type->byte_width(), 0);
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

template <typename Batch>
enable_if_t<std::is_same<Batch, ExecSpan>::value || std::is_same<Batch, ExecBatch>::value,
            Status>
CheckForGetNextSegment(const Batch& batch, int64_t offset,
                       const std::vector<TypeHolder>& key_types) {
  return CheckForGetNextSegment(batch.values, batch.length, offset, key_types);
}

struct BaseRowSegmenter : public RowSegmenter {
  explicit BaseRowSegmenter(const std::vector<TypeHolder>& key_types)
      : key_types_(key_types) {}

  const std::vector<TypeHolder>& key_types() const override { return key_types_; }

  std::vector<TypeHolder> key_types_;
};

Segment MakeSegment(int64_t batch_length, int64_t offset, int64_t length, bool extends) {
  return Segment{offset, length, offset + length >= batch_length, extends};
}

// Used by SimpleKeySegmenter::GetNextSegment to find the match-length of a value within a
// fixed-width buffer
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
  return std::min(cursor, length) - offset;
}

using ExtendFunc = std::function<bool(const void*)>;
constexpr bool kDefaultExtends = true;  // by default, the first segment extends
constexpr bool kEmptyExtends = true;    // an empty segment extends too

struct NoKeysSegmenter : public BaseRowSegmenter {
  static std::unique_ptr<RowSegmenter> Make() {
    return std::make_unique<NoKeysSegmenter>();
  }

  NoKeysSegmenter() : BaseRowSegmenter({}) {}

  Status Reset() override { return Status::OK(); }

  Result<Segment> GetNextSegment(const ExecSpan& batch, int64_t offset) override {
    ARROW_RETURN_NOT_OK(CheckForGetNextSegment(batch, offset, {}));
    return MakeSegment(batch.length, offset, batch.length - offset, kDefaultExtends);
  }
};

struct SimpleKeySegmenter : public BaseRowSegmenter {
  static Result<std::unique_ptr<RowSegmenter>> Make(TypeHolder key_type) {
    return std::make_unique<SimpleKeySegmenter>(key_type);
  }

  explicit SimpleKeySegmenter(TypeHolder key_type)
      : BaseRowSegmenter({key_type}),
        key_type_(key_types_[0]),
        save_key_data_(static_cast<size_t>(key_type_.type->byte_width())),
        extend_was_called_(false) {}

  Status CheckType(const DataType& type) {
    if (!is_fixed_width(type)) {
      return Status::Invalid("SimpleKeySegmenter does not support type ", type);
    }
    return Status::OK();
  }

  Status Reset() override {
    extend_was_called_ = false;
    return Status::OK();
  }

  // Checks whether the given grouping data extends the current segment, i.e., is equal to
  // previously seen grouping data, which is updated with each invocation.
  bool Extend(const void* data) {
    bool extends = !extend_was_called_
                       ? kDefaultExtends
                       : 0 == memcmp(save_key_data_.data(), data, save_key_data_.size());
    extend_was_called_ = true;
    memcpy(save_key_data_.data(), data, save_key_data_.size());
    return extends;
  }

  Result<Segment> GetNextSegment(const Scalar& scalar, int64_t offset, int64_t length) {
    ARROW_RETURN_NOT_OK(CheckType(*scalar.type));
    if (!scalar.is_valid) {
      return Status::Invalid("segmenting an invalid scalar");
    }
    auto data = checked_cast<const PrimitiveScalarBase&>(scalar).data();
    bool extends = length > 0 ? Extend(data) : kEmptyExtends;
    return MakeSegment(length, offset, length, extends);
  }

  Result<Segment> GetNextSegment(const DataType& array_type, const uint8_t* array_bytes,
                                 int64_t offset, int64_t length) {
    RETURN_NOT_OK(CheckType(array_type));
    DCHECK_LE(offset, length);
    int64_t byte_width = array_type.byte_width();
    int64_t match_length = GetMatchLength(array_bytes + offset * byte_width, byte_width,
                                          array_bytes, offset, length);
    bool extends = length > 0 ? Extend(array_bytes + offset * byte_width) : kEmptyExtends;
    return MakeSegment(length, offset, match_length, extends);
  }

  Result<Segment> GetNextSegment(const ExecSpan& batch, int64_t offset) override {
    ARROW_RETURN_NOT_OK(CheckForGetNextSegment(batch, offset, {key_type_}));
    if (offset == batch.length) {
      return MakeSegment(batch.length, offset, 0, kEmptyExtends);
    }
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

 private:
  TypeHolder key_type_;
  std::vector<uint8_t> save_key_data_;  // previusly seen segment-key grouping data
  bool extend_was_called_;
};

struct AnyKeysSegmenter : public BaseRowSegmenter {
  static Result<std::unique_ptr<RowSegmenter>> Make(
      const std::vector<TypeHolder>& key_types, ExecContext* ctx) {
    ARROW_RETURN_NOT_OK(Grouper::Make(key_types, ctx));  // check types
    return std::make_unique<AnyKeysSegmenter>(key_types, ctx);
  }

  AnyKeysSegmenter(const std::vector<TypeHolder>& key_types, ExecContext* ctx)
      : BaseRowSegmenter(key_types),
        ctx_(ctx),
        grouper_(nullptr),
        save_group_id_(kNoGroupId) {}

  Status Reset() override {
    grouper_ = nullptr;
    save_group_id_ = kNoGroupId;
    return Status::OK();
  }

  bool Extend(const void* data) {
    auto group_id = *static_cast<const group_id_t*>(data);
    bool extends =
        save_group_id_ == kNoGroupId ? kDefaultExtends : save_group_id_ == group_id;
    save_group_id_ = group_id;
    return extends;
  }

  // Runs the grouper on a single row.  This is used to determine the group id of the
  // first row of a new segment to see if it extends the previous segment.
  template <typename Batch>
  Result<group_id_t> MapGroupIdAt(const Batch& batch, int64_t offset) {
    if (!grouper_) return kNoGroupId;
    ARROW_ASSIGN_OR_RAISE(auto datum, grouper_->Consume(batch, offset,
                                                        /*length=*/1));
    if (!datum.is_array()) {
      return Status::Invalid("accessing unsupported datum kind ", datum.kind());
    }
    const std::shared_ptr<ArrayData>& data = datum.array();
    ARROW_DCHECK(data->GetNullCount() == 0);
    DCHECK_EQ(data->type->id(), GroupIdType::type_id);
    DCHECK_EQ(1, data->length);
    const group_id_t* values = data->GetValues<group_id_t>(1);
    return values[0];
  }

  Result<Segment> GetNextSegment(const ExecSpan& batch, int64_t offset) override {
    ARROW_RETURN_NOT_OK(CheckForGetNextSegment(batch, offset, key_types_));
    if (offset == batch.length) {
      return MakeSegment(batch.length, offset, 0, kEmptyExtends);
    }
    // ARROW-18311: make Grouper support Reset()
    // so it can be reset instead of recreated below
    //
    // the group id must be computed prior to resetting the grouper, since it is compared
    // to save_group_id_, and after resetting the grouper produces incomparable group ids
    ARROW_ASSIGN_OR_RAISE(auto group_id, MapGroupIdAt(batch, offset));
    ExtendFunc bound_extend = [this, group_id](const void* data) {
      bool extends = Extend(&group_id);
      save_group_id_ = *static_cast<const group_id_t*>(data);
      return extends;
    };
    // resetting drops grouper's group-ids, freeing-up memory for the next segment
    ARROW_ASSIGN_OR_RAISE(grouper_, Grouper::Make(key_types_, ctx_));  // TODO: reset it
    // GH-34475: cache the grouper-consume result across invocations of GetNextSegment
    ARROW_ASSIGN_OR_RAISE(auto datum, grouper_->Consume(batch, offset));
    if (datum.is_array()) {
      // `data` is an array whose index-0 corresponds to index `offset` of `batch`
      const std::shared_ptr<ArrayData>& data = datum.array();
      DCHECK_EQ(data->length, batch.length - offset);
      ARROW_DCHECK(data->GetNullCount() == 0);
      DCHECK_EQ(data->type->id(), GroupIdType::type_id);
      const group_id_t* values = data->GetValues<group_id_t>(1);
      int64_t cursor;
      for (cursor = 1; cursor < data->length; cursor++) {
        if (values[0] != values[cursor]) break;
      }
      int64_t length = cursor;
      bool extends = length > 0 ? bound_extend(values) : kEmptyExtends;
      return MakeSegment(batch.length, offset, length, extends);
    } else {
      return Status::Invalid("segmenting unsupported datum kind ", datum.kind());
    }
  }

 private:
  ExecContext* const ctx_;
  std::unique_ptr<Grouper> grouper_;
  group_id_t save_group_id_;
};

Status CheckAndCapLengthForConsume(int64_t batch_length, int64_t& consume_offset,
                                   int64_t* consume_length) {
  if (consume_offset < 0) {
    return Status::Invalid("invalid grouper consume offset: ", consume_offset);
  }
  if (*consume_length < 0) {
    *consume_length = batch_length - consume_offset;
  }
  return Status::OK();
}

}  // namespace

Result<std::unique_ptr<RowSegmenter>> MakeAnyKeysSegmenter(
    const std::vector<TypeHolder>& key_types, ExecContext* ctx) {
  return AnyKeysSegmenter::Make(key_types, ctx);
}

Result<std::unique_ptr<RowSegmenter>> RowSegmenter::Make(
    const std::vector<TypeHolder>& key_types, bool nullable_keys, ExecContext* ctx) {
  if (key_types.size() == 0) {
    return NoKeysSegmenter::Make();
  } else if (!nullable_keys && key_types.size() == 1) {
    const DataType* type = key_types[0].type;
    if (type != NULLPTR && is_fixed_width(*type)) {
      return SimpleKeySegmenter::Make(key_types[0]);
    }
  }
  return AnyKeysSegmenter::Make(key_types, ctx);
}

namespace {

struct GrouperNoKeysImpl : Grouper {
  Result<std::shared_ptr<Array>> MakeConstantGroupIdArray(int64_t length,
                                                          group_id_t value) {
    std::unique_ptr<ArrayBuilder> a_builder;
    RETURN_NOT_OK(MakeBuilder(default_memory_pool(), g_group_id_type, &a_builder));
    using GroupIdBuilder = typename TypeTraits<GroupIdType>::BuilderType;
    auto builder = checked_cast<GroupIdBuilder*>(a_builder.get());
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
  Result<Datum> Consume(const ExecSpan& batch, int64_t offset, int64_t length) override {
    ARROW_ASSIGN_OR_RAISE(auto array, MakeConstantGroupIdArray(length, 0));
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

struct GrouperImpl : public Grouper {
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

  Result<Datum> Consume(const ExecSpan& batch, int64_t offset, int64_t length) override {
    ARROW_RETURN_NOT_OK(CheckAndCapLengthForConsume(batch.length, offset, &length));
    if (offset != 0 || length != batch.length) {
      auto batch_slice = batch.ToExecBatch().Slice(offset, length);
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

struct GrouperFastImpl : public Grouper {
  static constexpr int kBitmapPaddingForSIMD = 64;  // bits
  static constexpr int kPaddingForSIMD = 32;        // bytes

  static bool CanUse(const std::vector<TypeHolder>& key_types) {
    if (key_types.size() == 0) {
      return false;
    }
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

  Result<Datum> Consume(const ExecSpan& batch, int64_t offset, int64_t length) override {
    ARROW_RETURN_NOT_OK(CheckAndCapLengthForConsume(batch.length, offset, &length));
    if (offset != 0 || length != batch.length) {
      auto batch_slice = batch.ToExecBatch().Slice(offset, length);
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

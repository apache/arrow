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

#include "arrow/compute/api_aggregate.h"

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/buffer_builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::FirstTimeBitmapWriter;

namespace compute {
namespace internal {
namespace {

struct KeyEncoder {
  // the first byte of an encoded key is used to indicate nullity
  static constexpr bool kExtraByteForNull = true;

  static constexpr uint8_t kNullByte = 1;
  static constexpr uint8_t kValidByte = 0;

  virtual ~KeyEncoder() = default;

  virtual void AddLength(const ArrayData&, int32_t* lengths) = 0;

  virtual Status Encode(const ArrayData&, uint8_t** encoded_bytes) = 0;

  virtual Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes,
                                                    int32_t length, MemoryPool*) = 0;

  // extract the null bitmap from the leading nullity bytes of encoded keys
  static Status DecodeNulls(MemoryPool* pool, int32_t length, uint8_t** encoded_bytes,
                            std::shared_ptr<Buffer>* null_bitmap, int32_t* null_count) {
    // first count nulls to determine if a null bitmap is necessary
    *null_count = 0;
    for (int32_t i = 0; i < length; ++i) {
      *null_count += (encoded_bytes[i][0] == kNullByte);
    }

    if (*null_count > 0) {
      ARROW_ASSIGN_OR_RAISE(*null_bitmap, AllocateBitmap(length, pool));
      uint8_t* validity = (*null_bitmap)->mutable_data();

      FirstTimeBitmapWriter writer(validity, 0, length);
      for (int32_t i = 0; i < length; ++i) {
        if (encoded_bytes[i][0] == kValidByte) {
          writer.Set();
        } else {
          writer.Clear();
        }
        writer.Next();
        encoded_bytes[i] += 1;
      }
      writer.Finish();
    } else {
      for (int32_t i = 0; i < length; ++i) {
        encoded_bytes[i] += 1;
      }
    }
    return Status ::OK();
  }
};

struct BooleanKeyEncoder : KeyEncoder {
  static constexpr int kByteWidth = 1;

  void AddLength(const ArrayData& data, int32_t* lengths) override {
    for (int64_t i = 0; i < data.length; ++i) {
      lengths[i] += kByteWidth + kExtraByteForNull;
    }
  }

  Status Encode(const ArrayData& data, uint8_t** encoded_bytes) override {
    VisitArrayDataInline<BooleanType>(
        data,
        [&](bool value) {
          auto& encoded_ptr = *encoded_bytes++;
          *encoded_ptr++ = kValidByte;
          *encoded_ptr++ = value;
        },
        [&] {
          auto& encoded_ptr = *encoded_bytes++;
          *encoded_ptr++ = kNullByte;
          *encoded_ptr++ = 0;
        });
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override {
    std::shared_ptr<Buffer> null_buf;
    int32_t null_count;
    RETURN_NOT_OK(DecodeNulls(pool, length, encoded_bytes, &null_buf, &null_count));

    ARROW_ASSIGN_OR_RAISE(auto key_buf, AllocateBitmap(length, pool));

    uint8_t* raw_output = key_buf->mutable_data();
    for (int32_t i = 0; i < length; ++i) {
      auto& encoded_ptr = encoded_bytes[i];
      BitUtil::SetBitTo(raw_output, i, encoded_ptr[0] != 0);
      encoded_ptr += 1;
    }

    return ArrayData::Make(boolean(), length, {std::move(null_buf), std::move(key_buf)},
                           null_count);
  }
};

struct FixedWidthKeyEncoder : KeyEncoder {
  explicit FixedWidthKeyEncoder(std::shared_ptr<DataType> type)
      : type_(std::move(type)),
        byte_width_(checked_cast<const FixedWidthType&>(*type_).bit_width() / 8) {}

  void AddLength(const ArrayData& data, int32_t* lengths) override {
    for (int64_t i = 0; i < data.length; ++i) {
      lengths[i] += byte_width_ + kExtraByteForNull;
    }
  }

  Status Encode(const ArrayData& data, uint8_t** encoded_bytes) override {
    ArrayData viewed(fixed_size_binary(byte_width_), data.length, data.buffers,
                     data.null_count, data.offset);

    VisitArrayDataInline<FixedSizeBinaryType>(
        viewed,
        [&](util::string_view bytes) {
          auto& encoded_ptr = *encoded_bytes++;
          *encoded_ptr++ = kValidByte;
          memcpy(encoded_ptr, bytes.data(), byte_width_);
          encoded_ptr += byte_width_;
        },
        [&] {
          auto& encoded_ptr = *encoded_bytes++;
          *encoded_ptr++ = kNullByte;
          memset(encoded_ptr, 0, byte_width_);
          encoded_ptr += byte_width_;
        });
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override {
    std::shared_ptr<Buffer> null_buf;
    int32_t null_count;
    RETURN_NOT_OK(DecodeNulls(pool, length, encoded_bytes, &null_buf, &null_count));

    ARROW_ASSIGN_OR_RAISE(auto key_buf, AllocateBuffer(length * byte_width_, pool));

    uint8_t* raw_output = key_buf->mutable_data();
    for (int32_t i = 0; i < length; ++i) {
      auto& encoded_ptr = encoded_bytes[i];
      std::memcpy(raw_output, encoded_ptr, byte_width_);
      encoded_ptr += byte_width_;
      raw_output += byte_width_;
    }

    return ArrayData::Make(type_, length, {std::move(null_buf), std::move(key_buf)},
                           null_count);
  }

  std::shared_ptr<DataType> type_;
  int byte_width_;
};

struct DictionaryKeyEncoder : FixedWidthKeyEncoder {
  DictionaryKeyEncoder(std::shared_ptr<DataType> type, MemoryPool* pool)
      : FixedWidthKeyEncoder(std::move(type)), pool_(pool) {}

  Status Encode(const ArrayData& data, uint8_t** encoded_bytes) override {
    auto dict = MakeArray(data.dictionary);
    if (dictionary_) {
      if (!dictionary_->Equals(dict)) {
        // TODO(bkietz) unify if necessary. For now, just error if any batch's dictionary
        // differs from the first we saw for this key
        return Status::NotImplemented("Unifying differing dictionaries");
      }
    } else {
      dictionary_ = std::move(dict);
    }
    return FixedWidthKeyEncoder::Encode(data, encoded_bytes);
  }

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override {
    ARROW_ASSIGN_OR_RAISE(auto data,
                          FixedWidthKeyEncoder::Decode(encoded_bytes, length, pool));

    if (dictionary_) {
      data->dictionary = dictionary_->data();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto dict, MakeArrayOfNull(type_, 0));
      data->dictionary = dict->data();
    }

    data->type = type_;
    return data;
  }

  MemoryPool* pool_;
  std::shared_ptr<Array> dictionary_;
};

template <typename T>
struct VarLengthKeyEncoder : KeyEncoder {
  using Offset = typename T::offset_type;

  void AddLength(const ArrayData& data, int32_t* lengths) override {
    int64_t i = 0;
    VisitArrayDataInline<T>(
        data,
        [&](util::string_view bytes) {
          lengths[i++] +=
              kExtraByteForNull + sizeof(Offset) + static_cast<int32_t>(bytes.size());
        },
        [&] { lengths[i++] += kExtraByteForNull + sizeof(Offset); });
  }

  Status Encode(const ArrayData& data, uint8_t** encoded_bytes) override {
    VisitArrayDataInline<T>(
        data,
        [&](util::string_view bytes) {
          auto& encoded_ptr = *encoded_bytes++;
          *encoded_ptr++ = kValidByte;
          util::SafeStore(encoded_ptr, static_cast<Offset>(bytes.size()));
          encoded_ptr += sizeof(Offset);
          memcpy(encoded_ptr, bytes.data(), bytes.size());
          encoded_ptr += bytes.size();
        },
        [&] {
          auto& encoded_ptr = *encoded_bytes++;
          *encoded_ptr++ = kNullByte;
          util::SafeStore(encoded_ptr, static_cast<Offset>(0));
          encoded_ptr += sizeof(Offset);
        });
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override {
    std::shared_ptr<Buffer> null_buf;
    int32_t null_count;
    RETURN_NOT_OK(DecodeNulls(pool, length, encoded_bytes, &null_buf, &null_count));

    Offset length_sum = 0;
    for (int32_t i = 0; i < length; ++i) {
      length_sum += util::SafeLoadAs<Offset>(encoded_bytes[i]);
    }

    ARROW_ASSIGN_OR_RAISE(auto offset_buf,
                          AllocateBuffer(sizeof(Offset) * (1 + length), pool));
    ARROW_ASSIGN_OR_RAISE(auto key_buf, AllocateBuffer(length_sum));

    auto raw_offsets = reinterpret_cast<Offset*>(offset_buf->mutable_data());
    auto raw_keys = key_buf->mutable_data();

    Offset current_offset = 0;
    for (int32_t i = 0; i < length; ++i) {
      raw_offsets[i] = current_offset;

      auto key_length = util::SafeLoadAs<Offset>(encoded_bytes[i]);
      encoded_bytes[i] += sizeof(Offset);

      memcpy(raw_keys + current_offset, encoded_bytes[i], key_length);
      encoded_bytes[i] += key_length;

      current_offset += key_length;
    }
    raw_offsets[length] = current_offset;

    return ArrayData::Make(
        type_, length, {std::move(null_buf), std::move(offset_buf), std::move(key_buf)},
        null_count);
  }

  explicit VarLengthKeyEncoder(std::shared_ptr<DataType> type) : type_(std::move(type)) {}

  std::shared_ptr<DataType> type_;
};

struct GrouperImpl : Grouper {
  static Result<std::unique_ptr<GrouperImpl>> Make(const std::vector<ValueDescr>& keys,
                                                   ExecContext* ctx) {
    auto impl = ::arrow::internal::make_unique<GrouperImpl>();

    impl->encoders_.resize(keys.size());
    impl->ctx_ = ctx;

    for (size_t i = 0; i < keys.size(); ++i) {
      const auto& key = keys[i].type;

      if (key->id() == Type::BOOL) {
        impl->encoders_[i] = ::arrow::internal::make_unique<BooleanKeyEncoder>();
        continue;
      }

      if (key->id() == Type::DICTIONARY) {
        impl->encoders_[i] =
            ::arrow::internal::make_unique<DictionaryKeyEncoder>(key, ctx->memory_pool());
        continue;
      }

      if (is_fixed_width(key->id())) {
        impl->encoders_[i] = ::arrow::internal::make_unique<FixedWidthKeyEncoder>(key);
        continue;
      }

      if (is_binary_like(key->id())) {
        impl->encoders_[i] =
            ::arrow::internal::make_unique<VarLengthKeyEncoder<BinaryType>>(key);
        continue;
      }

      if (is_large_binary_like(key->id())) {
        impl->encoders_[i] =
            ::arrow::internal::make_unique<VarLengthKeyEncoder<LargeBinaryType>>(key);
        continue;
      }

      return Status::NotImplemented("Keys of type ", *key);
    }

    return std::move(impl);
  }

  Result<Datum> Consume(const ExecBatch& batch) override {
    std::vector<int32_t> offsets_batch(batch.length + 1);
    for (int i = 0; i < batch.num_values(); ++i) {
      encoders_[i]->AddLength(*batch[i].array(), offsets_batch.data());
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
      RETURN_NOT_OK(encoders_[i]->Encode(*batch[i].array(), key_buf_ptrs.data()));
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
        auto next_key_offset = static_cast<int32_t>(key_bytes_.size());
        key_bytes_.resize(next_key_offset + key_length);
        offsets_.push_back(next_key_offset + key_length);
        memcpy(key_bytes_.data() + next_key_offset, key.c_str(), key_length);
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
  std::vector<std::unique_ptr<KeyEncoder>> encoders_;
};

/// C++ abstract base class for the HashAggregateKernel interface.
/// Implementations should be default constructible and perform initialization in
/// Init().
struct GroupedAggregator : KernelState {
  virtual Status Init(ExecContext*, const FunctionOptions*,
                      const std::shared_ptr<DataType>&) = 0;

  virtual Status Consume(const ExecBatch& batch) = 0;

  virtual Result<Datum> Finalize() = 0;

  template <typename Reserve>
  Status MaybeReserve(int64_t old_num_groups, const ExecBatch& batch,
                      const Reserve& reserve) {
    int64_t new_num_groups = batch[2].scalar_as<UInt32Scalar>().value;
    if (new_num_groups <= old_num_groups) {
      return Status::OK();
    }
    return reserve(new_num_groups - old_num_groups);
  }

  virtual std::shared_ptr<DataType> out_type() const = 0;
};

// ----------------------------------------------------------------------
// Count implementation

struct GroupedCountImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const FunctionOptions* options,
              const std::shared_ptr<DataType>&) override {
    options_ = checked_cast<const CountOptions&>(*options);
    counts_ = BufferBuilder(ctx->memory_pool());
    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    RETURN_NOT_OK(MaybeReserve(counts_.length(), batch, [&](int64_t added_groups) {
      num_groups_ += added_groups;
      return counts_.Append(added_groups * sizeof(int64_t), 0);
    }));

    auto group_ids = batch[1].array()->GetValues<uint32_t>(1);
    auto raw_counts = reinterpret_cast<int64_t*>(counts_.mutable_data());

    const auto& input = batch[0].array();

    if (options_.count_mode == CountOptions::COUNT_NULL) {
      for (int64_t i = 0, input_i = input->offset; i < input->length; ++i, ++input_i) {
        auto g = group_ids[i];
        raw_counts[g] += !BitUtil::GetBit(input->buffers[0]->data(), input_i);
      }
      return Status::OK();
    }

    arrow::internal::VisitSetBitRunsVoid(
        input->buffers[0], input->offset, input->length,
        [&](int64_t begin, int64_t length) {
          for (int64_t input_i = begin, i = begin - input->offset;
               input_i < begin + length; ++input_i, ++i) {
            auto g = group_ids[i];
            raw_counts[g] += 1;
          }
        });
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto counts, counts_.Finish());
    return std::make_shared<Int64Array>(num_groups_, std::move(counts));
  }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  int64_t num_groups_ = 0;
  CountOptions options_;
  BufferBuilder counts_;
};

// ----------------------------------------------------------------------
// Sum implementation

struct GroupedSumImpl : public GroupedAggregator {
  // NB: whether we are accumulating into double, int64_t, or uint64_t
  // we always have 64 bits per group in the sums buffer.
  static constexpr size_t kSumSize = sizeof(int64_t);

  using ConsumeImpl = std::function<void(const std::shared_ptr<ArrayData>&,
                                         const uint32_t*, void*, int64_t*)>;

  struct GetConsumeImpl {
    template <typename T, typename AccType = typename FindAccumulatorType<T>::Type>
    Status Visit(const T&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input, const uint32_t* group,
                        void* boxed_sums, int64_t* counts) {
        auto sums = reinterpret_cast<typename TypeTraits<AccType>::CType*>(boxed_sums);

        VisitArrayDataInline<T>(
            *input,
            [&](typename TypeTraits<T>::CType value) {
              sums[*group] += value;
              counts[*group] += 1;
              ++group;
            },
            [&] { ++group; });
      };
      out_type = TypeTraits<AccType>::type_singleton();
      return Status::OK();
    }

    Status Visit(const HalfFloatType& type) {
      return Status::NotImplemented("Summing data of type ", type);
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("Summing data of type ", type);
    }

    ConsumeImpl consume_impl;
    std::shared_ptr<DataType> out_type;
  };

  Status Init(ExecContext* ctx, const FunctionOptions*,
              const std::shared_ptr<DataType>& input_type) override {
    pool_ = ctx->memory_pool();
    sums_ = BufferBuilder(pool_);
    counts_ = BufferBuilder(pool_);

    GetConsumeImpl get_consume_impl;
    RETURN_NOT_OK(VisitTypeInline(*input_type, &get_consume_impl));

    consume_impl_ = std::move(get_consume_impl.consume_impl);
    out_type_ = std::move(get_consume_impl.out_type);

    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    RETURN_NOT_OK(MaybeReserve(num_groups_, batch, [&](int64_t added_groups) {
      num_groups_ += added_groups;
      RETURN_NOT_OK(sums_.Append(added_groups * kSumSize, 0));
      RETURN_NOT_OK(counts_.Append(added_groups * sizeof(int64_t), 0));
      return Status::OK();
    }));

    auto group_ids = batch[1].array()->GetValues<uint32_t>(1);
    consume_impl_(batch[0].array(), group_ids, sums_.mutable_data(),
                  reinterpret_cast<int64_t*>(counts_.mutable_data()));
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    std::shared_ptr<Buffer> null_bitmap;
    int64_t null_count = 0;

    for (int64_t i = 0; i < num_groups_; ++i) {
      if (reinterpret_cast<const int64_t*>(counts_.data())[i] > 0) continue;

      if (null_bitmap == nullptr) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_groups_, pool_));
        BitUtil::SetBitsTo(null_bitmap->mutable_data(), 0, num_groups_, true);
      }

      null_count += 1;
      BitUtil::SetBitTo(null_bitmap->mutable_data(), i, false);
    }

    ARROW_ASSIGN_OR_RAISE(auto sums, sums_.Finish());

    return ArrayData::Make(std::move(out_type_), num_groups_,
                           {std::move(null_bitmap), std::move(sums)}, null_count);
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  // NB: counts are used here instead of a simple "has_values_" bitmap since
  // we expect to reuse this kernel to handle Mean
  int64_t num_groups_ = 0;
  BufferBuilder sums_, counts_;
  std::shared_ptr<DataType> out_type_;
  ConsumeImpl consume_impl_;
  MemoryPool* pool_;
};

// ----------------------------------------------------------------------
// MinMax implementation

template <typename CType>
struct Extrema : std::numeric_limits<CType> {};

template <>
struct Extrema<float> {
  static constexpr float min() { return -std::numeric_limits<float>::infinity(); }
  static constexpr float max() { return std::numeric_limits<float>::infinity(); }
};

template <>
struct Extrema<double> {
  static constexpr double min() { return -std::numeric_limits<double>::infinity(); }
  static constexpr double max() { return std::numeric_limits<double>::infinity(); }
};

struct GroupedMinMaxImpl : public GroupedAggregator {
  using ConsumeImpl =
      std::function<void(const std::shared_ptr<ArrayData>&, const uint32_t*, void*, void*,
                         uint8_t*, uint8_t*)>;

  using ResizeImpl = std::function<Status(BufferBuilder*, int64_t)>;

  template <typename CType>
  static ResizeImpl MakeResizeImpl(CType anti_extreme) {
    // resize a min or max buffer, storing the correct anti extreme
    return [anti_extreme](BufferBuilder* builder, int64_t added_groups) {
      TypedBufferBuilder<CType> typed_builder(std::move(*builder));
      RETURN_NOT_OK(typed_builder.Append(added_groups, anti_extreme));
      *builder = std::move(*typed_builder.bytes_builder());
      return Status::OK();
    };
  }

  struct GetImpl {
    template <typename T, typename CType = typename TypeTraits<T>::CType>
    enable_if_number<T, Status> Visit(const T&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input, const uint32_t* group,
                        void* mins, void* maxes, uint8_t* has_values,
                        uint8_t* has_nulls) {
        auto raw_mins = reinterpret_cast<CType*>(mins);
        auto raw_maxes = reinterpret_cast<CType*>(maxes);

        VisitArrayDataInline<T>(
            *input,
            [&](CType val) {
              raw_maxes[*group] = std::max(raw_maxes[*group], val);
              raw_mins[*group] = std::min(raw_mins[*group], val);
              BitUtil::SetBit(has_values, *group++);
            },
            [&] { BitUtil::SetBit(has_nulls, *group++); });
      };

      resize_min_impl = MakeResizeImpl(Extrema<CType>::max());
      resize_max_impl = MakeResizeImpl(Extrema<CType>::min());
      return Status::OK();
    }

    Status Visit(const BooleanType& type) {
      return Status::NotImplemented("Grouped MinMax data of type ", type);
    }

    Status Visit(const HalfFloatType& type) {
      return Status::NotImplemented("Grouped MinMax data of type ", type);
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("Grouped MinMax data of type ", type);
    }

    ConsumeImpl consume_impl;
    ResizeImpl resize_min_impl, resize_max_impl;
  };

  Status Init(ExecContext* ctx, const FunctionOptions* options,
              const std::shared_ptr<DataType>& input_type) override {
    options_ = *checked_cast<const MinMaxOptions*>(options);
    type_ = input_type;

    mins_ = BufferBuilder(ctx->memory_pool());
    maxes_ = BufferBuilder(ctx->memory_pool());
    has_values_ = BufferBuilder(ctx->memory_pool());
    has_nulls_ = BufferBuilder(ctx->memory_pool());

    GetImpl get_impl;
    RETURN_NOT_OK(VisitTypeInline(*input_type, &get_impl));

    consume_impl_ = std::move(get_impl.consume_impl);
    resize_min_impl_ = std::move(get_impl.resize_min_impl);
    resize_max_impl_ = std::move(get_impl.resize_max_impl);
    resize_bitmap_impl_ = MakeResizeImpl(false);

    return Status::OK();
  }

  Status Consume(const ExecBatch& batch) override {
    RETURN_NOT_OK(MaybeReserve(num_groups_, batch, [&](int64_t added_groups) {
      num_groups_ += added_groups;
      RETURN_NOT_OK(resize_min_impl_(&mins_, added_groups));
      RETURN_NOT_OK(resize_max_impl_(&maxes_, added_groups));
      RETURN_NOT_OK(resize_bitmap_impl_(&has_values_, added_groups));
      RETURN_NOT_OK(resize_bitmap_impl_(&has_nulls_, added_groups));
      return Status::OK();
    }));

    auto group_ids = batch[1].array()->GetValues<uint32_t>(1);
    consume_impl_(batch[0].array(), group_ids, mins_.mutable_data(),
                  maxes_.mutable_data(), has_values_.mutable_data(),
                  has_nulls_.mutable_data());
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    // aggregation for group is valid if there was at least one value in that group
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_values_.Finish());

    if (options_.null_handling == MinMaxOptions::EMIT_NULL) {
      // ... and there were no nulls in that group
      ARROW_ASSIGN_OR_RAISE(auto has_nulls, has_nulls_.Finish());
      arrow::internal::BitmapAndNot(null_bitmap->data(), 0, has_nulls->data(), 0,
                                    num_groups_, 0, null_bitmap->mutable_data());
    }

    auto mins = ArrayData::Make(type_, num_groups_, {null_bitmap, nullptr});
    auto maxes = ArrayData::Make(type_, num_groups_, {std::move(null_bitmap), nullptr});
    ARROW_ASSIGN_OR_RAISE(mins->buffers[1], mins_.Finish());
    ARROW_ASSIGN_OR_RAISE(maxes->buffers[1], maxes_.Finish());

    return ArrayData::Make(out_type(), num_groups_, {nullptr},
                           {std::move(mins), std::move(maxes)});
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", type_), field("max", type_)});
  }

  int64_t num_groups_;
  BufferBuilder mins_, maxes_, has_values_, has_nulls_;
  std::shared_ptr<DataType> type_;
  ConsumeImpl consume_impl_;
  ResizeImpl resize_min_impl_, resize_max_impl_, resize_bitmap_impl_;
  MinMaxOptions options_;
};

template <typename Impl>
HashAggregateKernel MakeKernel(InputType argument_type) {
  HashAggregateKernel kernel;

  kernel.init = [](KernelContext* ctx,
                   const KernelInitArgs& args) -> std::unique_ptr<KernelState> {
    auto impl = ::arrow::internal::make_unique<Impl>();
    // FIXME(bkietz) Init should not take a type. That should be an unboxed template arg
    // for the Impl. Otherwise we're not exposing dispatch as well as we should.
    ctx->SetStatus(impl->Init(ctx->exec_context(), args.options, args.inputs[0].type));
    if (ctx->HasError()) return nullptr;
    return std::move(impl);
  };

  kernel.signature = KernelSignature::Make(
      {std::move(argument_type), InputType::Array(Type::UINT32),
       InputType::Scalar(Type::UINT32)},
      OutputType(
          [](KernelContext* ctx, const std::vector<ValueDescr>&) -> Result<ValueDescr> {
            return checked_cast<GroupedAggregator*>(ctx->state())->out_type();
          }));

  kernel.consume = [](KernelContext* ctx, const ExecBatch& batch) {
    ctx->SetStatus(checked_cast<GroupedAggregator*>(ctx->state())->Consume(batch));
  };

  kernel.merge = [](KernelContext* ctx, KernelState&&, KernelState*) {
    // TODO(ARROW-11840) merge two hash tables
    ctx->SetStatus(Status::NotImplemented("Merge hashed aggregations"));
  };

  kernel.finalize = [](KernelContext* ctx, Datum* out) {
    KERNEL_ASSIGN_OR_RAISE(*out, ctx,
                           checked_cast<GroupedAggregator*>(ctx->state())->Finalize());
  };

  return kernel;
}

Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<Aggregate>& aggregates,
    const std::vector<ValueDescr>& in_descrs) {
  if (aggregates.size() != in_descrs.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_descrs.size(), " arguments were provided.");
  }

  std::vector<const HashAggregateKernel*> kernels(in_descrs.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto function,
                          ctx->func_registry()->GetFunction(aggregates[i].function));
    ARROW_ASSIGN_OR_RAISE(
        const Kernel* kernel,
        function->DispatchExact(
            {in_descrs[i], ValueDescr::Array(uint32()), ValueDescr::Scalar(uint32())}));
    kernels[i] = static_cast<const HashAggregateKernel*>(kernel);
  }
  return kernels;
}

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates, const std::vector<ValueDescr>& in_descrs) {
  std::vector<std::unique_ptr<KernelState>> states(kernels.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    auto options = aggregates[i].options;

    if (options == nullptr) {
      // use known default options for the named function if possible
      auto maybe_function = ctx->func_registry()->GetFunction(aggregates[i].function);
      if (maybe_function.ok()) {
        options = maybe_function.ValueOrDie()->default_options();
      }
    }

    KernelContext kernel_ctx{ctx};
    states[i] = kernels[i]->init(&kernel_ctx, KernelInitArgs{kernels[i],
                                                             {
                                                                 in_descrs[i].type,
                                                                 uint32(),
                                                                 uint32(),
                                                             },
                                                             options});
    if (kernel_ctx.HasError()) return kernel_ctx.status();
  }

  return std::move(states);
}

Result<FieldVector> ResolveKernels(
    const std::vector<Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<ValueDescr>& descrs) {
  FieldVector fields(descrs.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    ARROW_ASSIGN_OR_RAISE(auto descr, kernels[i]->signature->out_type().Resolve(
                                          &kernel_ctx, {
                                                           descrs[i].type,
                                                           uint32(),
                                                           uint32(),
                                                       }));
    fields[i] = field(aggregates[i].function, std::move(descr.type));
  }
  return fields;
}

}  // namespace

Result<std::unique_ptr<Grouper>> Grouper::Make(const std::vector<ValueDescr>& descrs,
                                               ExecContext* ctx) {
  return GrouperImpl::Make(descrs, ctx);
}

Result<Datum> GroupBy(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
                      const std::vector<Aggregate>& aggregates, ExecContext* ctx) {
  // Construct and initialize HashAggregateKernels
  ARROW_ASSIGN_OR_RAISE(auto argument_descrs,
                        ExecBatch::Make(arguments).Map(
                            [](ExecBatch batch) { return batch.GetDescriptors(); }));

  ARROW_ASSIGN_OR_RAISE(auto kernels, GetKernels(ctx, aggregates, argument_descrs));

  ARROW_ASSIGN_OR_RAISE(auto states,
                        InitKernels(kernels, ctx, aggregates, argument_descrs));

  ARROW_ASSIGN_OR_RAISE(
      FieldVector out_fields,
      ResolveKernels(aggregates, kernels, states, ctx, argument_descrs));

  using arrow::compute::detail::ExecBatchIterator;

  ARROW_ASSIGN_OR_RAISE(auto argument_batch_iterator,
                        ExecBatchIterator::Make(arguments, ctx->exec_chunksize()));

  // Construct Grouper
  ARROW_ASSIGN_OR_RAISE(auto key_descrs, ExecBatch::Make(keys).Map([](ExecBatch batch) {
    return batch.GetDescriptors();
  }));

  ARROW_ASSIGN_OR_RAISE(auto grouper, Grouper::Make(key_descrs, ctx));

  int i = 0;
  for (ValueDescr& key_descr : key_descrs) {
    out_fields.push_back(field("key_" + std::to_string(i++), std::move(key_descr.type)));
  }

  ARROW_ASSIGN_OR_RAISE(auto key_batch_iterator,
                        ExecBatchIterator::Make(keys, ctx->exec_chunksize()));

  // start "streaming" execution
  ExecBatch key_batch, argument_batch;
  while (argument_batch_iterator->Next(&argument_batch) &&
         key_batch_iterator->Next(&key_batch)) {
    if (key_batch.length == 0) continue;

    // compute a batch of group ids
    ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(key_batch));

    // consume group ids with HashAggregateKernels
    for (size_t i = 0; i < kernels.size(); ++i) {
      KernelContext batch_ctx{ctx};
      batch_ctx.SetState(states[i].get());
      ARROW_ASSIGN_OR_RAISE(auto batch, ExecBatch::Make({argument_batch[i], id_batch,
                                                         Datum(grouper->num_groups())}));
      kernels[i]->consume(&batch_ctx, batch);
      if (batch_ctx.HasError()) return batch_ctx.status();
    }
  }

  // Finalize output
  ArrayDataVector out_data(arguments.size() + keys.size());
  auto it = out_data.begin();

  for (size_t i = 0; i < kernels.size(); ++i) {
    KernelContext batch_ctx{ctx};
    batch_ctx.SetState(states[i].get());
    Datum out;
    kernels[i]->finalize(&batch_ctx, &out);
    if (batch_ctx.HasError()) return batch_ctx.status();
    *it++ = out.array();
  }

  ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, grouper->GetUniques());
  for (const auto& key : out_keys.values) {
    *it++ = key.array();
  }

  int64_t length = out_data[0]->length;
  return ArrayData::Make(struct_(std::move(out_fields)), length,
                         {/*null_bitmap=*/nullptr}, std::move(out_data),
                         /*null_count=*/0);
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

namespace {
const FunctionDoc hash_count_doc{"Count the number of null / non-null values",
                                 ("By default, non-null values are counted.\n"
                                  "This can be changed through CountOptions."),
                                 {"array", "group_id_array", "group_count"},
                                 "CountOptions"};

const FunctionDoc hash_sum_doc{"Sum values of a numeric array",
                               ("Null values are ignored."),
                               {"array", "group_id_array", "group_count"}};

const FunctionDoc hash_min_max_doc{
    "Compute the minimum and maximum values of a numeric array",
    ("Null values are ignored by default.\n"
     "This can be changed through MinMaxOptions."),
    {"array", "group_id_array", "group_count"},
    "MinMaxOptions"};
}  // namespace

void RegisterHashAggregateBasic(FunctionRegistry* registry) {
  {
    static auto default_count_options = CountOptions::Defaults();
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_count", Arity::Ternary(), &hash_count_doc, &default_count_options);
    DCHECK_OK(func->AddKernel(MakeKernel<GroupedCountImpl>(ValueDescr::ARRAY)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>("hash_sum", Arity::Ternary(),
                                                        &hash_sum_doc);
    DCHECK_OK(func->AddKernel(MakeKernel<GroupedSumImpl>(ValueDescr::ARRAY)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    static auto default_minmax_options = MinMaxOptions::Defaults();
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_min_max", Arity::Ternary(), &hash_min_max_doc, &default_minmax_options);
    DCHECK_OK(func->AddKernel(MakeKernel<GroupedMinMaxImpl>(ValueDescr::ARRAY)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

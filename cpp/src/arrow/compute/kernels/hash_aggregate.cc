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

#include "arrow/compute/kernels/hash_aggregate_internal.h"

#include <unordered_map>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/kernels/aggregate_basic_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace compute {
namespace aggregate {

struct GroupByImpl {
  using AddLengthImpl = std::function<void(const std::shared_ptr<ArrayData>&, int32_t*)>;

  struct GetAddLengthImpl {
    static constexpr int32_t null_extra_byte = 1;

    static void AddFixedLength(int32_t fixed_length, int64_t num_repeats,
                               int32_t* lengths) {
      for (int64_t i = 0; i < num_repeats; ++i) {
        lengths[i] += fixed_length + null_extra_byte;
      }
    }

    static void AddVarLength(const std::shared_ptr<ArrayData>& data, int32_t* lengths) {
      using offset_type = typename StringType::offset_type;
      constexpr int32_t length_extra_bytes = sizeof(offset_type);
      auto offset = data->offset;
      const auto offsets = data->GetValues<offset_type>(1);
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();

        for (int64_t i = 0; i < data->length; ++i) {
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          if (is_null) {
            lengths[i] += null_extra_byte + length_extra_bytes;
          } else {
            lengths[i] += null_extra_byte + length_extra_bytes + offsets[offset + i + 1] -
                          offsets[offset + i];
          }
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          lengths[i] += null_extra_byte + length_extra_bytes + offsets[offset + i + 1] -
                        offsets[offset + i];
        }
      }
    }

    template <typename T>
    Status Visit(const T& input_type) {
      int32_t num_bytes = (bit_width(input_type.id()) + 7) / 8;
      add_length_impl = [num_bytes](const std::shared_ptr<ArrayData>& data,
                                    int32_t* lengths) {
        AddFixedLength(num_bytes, data->length, lengths);
      };
      return Status::OK();
    }

    Status Visit(const StringType&) {
      add_length_impl = [](const std::shared_ptr<ArrayData>& data, int32_t* lengths) {
        AddVarLength(data, lengths);
      };
      return Status::OK();
    }

    Status Visit(const BinaryType&) {
      add_length_impl = [](const std::shared_ptr<ArrayData>& data, int32_t* lengths) {
        AddVarLength(data, lengths);
      };
      return Status::OK();
    }

    Status Visit(const FixedSizeBinaryType& type) {
      int32_t num_bytes = type.byte_width();
      add_length_impl = [num_bytes](const std::shared_ptr<ArrayData>& data,
                                    int32_t* lengths) {
        AddFixedLength(num_bytes, data->length, lengths);
      };
      return Status::OK();
    }

    AddLengthImpl add_length_impl;
  };

  using EncodeNextImpl =
      std::function<void(const std::shared_ptr<ArrayData>&, uint8_t**)>;

  struct GetEncodeNextImpl {
    template <int NumBits>
    static void EncodeSmallFixed(const std::shared_ptr<ArrayData>& data,
                                 uint8_t** encoded_bytes) {
      switch (NumBits) {
        case 1:
          return VisitArrayDataInline<BooleanType>(
              *data,
              [&](bool bit) {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 0;
                *encoded_ptr++ = bit;
              },
              [&] {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 1;
                *encoded_ptr++ = 0;
              });

        case 8:
          return VisitArrayDataInline<UInt8Type>(
              *data,
              [&](uint8_t byte) {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 0;
                *encoded_ptr++ = byte;
              },
              [&] {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 1;
                *encoded_ptr++ = 0;
              });

        case 16:
          return VisitArrayDataInline<UInt16Type>(
              *data,
              [&](uint16_t word) {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 0;
                util::SafeStore(encoded_ptr, word);
                encoded_ptr += sizeof(uint16_t);
              },
              [&] {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 1;
                util::SafeStore(encoded_ptr, uint16_t(0));
                encoded_ptr += sizeof(uint16_t);
              });

        case 32:
          return VisitArrayDataInline<UInt32Type>(
              *data,
              [&](uint32_t quad) {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 0;
                util::SafeStore(encoded_ptr, quad);
                encoded_ptr += sizeof(uint32_t);
              },
              [&] {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 1;
                util::SafeStore(encoded_ptr, uint32_t(0));
                encoded_ptr += sizeof(uint32_t);
              });

        case 64:
          return VisitArrayDataInline<UInt64Type>(
              *data,
              [&](uint64_t oct) {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 0;
                util::SafeStore(encoded_ptr, oct);
                encoded_ptr += sizeof(uint64_t);
              },
              [&] {
                auto& encoded_ptr = *encoded_bytes++;
                *encoded_ptr++ = 1;
                util::SafeStore(encoded_ptr, uint64_t(0));
                encoded_ptr += sizeof(uint64_t);
              });
      }
    }

    static void EncodeBigFixed(const std::shared_ptr<ArrayData>& data,
                               uint8_t** encoded_bytes) {
      auto num_bytes = checked_cast<const FixedSizeBinaryType&>(*data->type).byte_width();
      return VisitArrayDataInline<FixedSizeBinaryType>(
          *data,
          [&](util::string_view bytes) {
            auto& encoded_ptr = *encoded_bytes++;
            *encoded_ptr++ = 0;
            memcpy(encoded_ptr, bytes.data(), num_bytes);
            encoded_ptr += num_bytes;
          },
          [&] {
            auto& encoded_ptr = *encoded_bytes++;
            *encoded_ptr++ = 1;
            memset(encoded_ptr, 0, num_bytes);
            encoded_ptr += num_bytes;
          });
    }

    template <typename T>
    static void EncodeVarLength(const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
      using offset_type = typename T::offset_type;

      return VisitArrayDataInline<T>(
          *data,
          [&](util::string_view bytes) {
            auto& encoded_ptr = *encoded_bytes++;
            *encoded_ptr++ = 0;
            util::SafeStore(encoded_ptr, static_cast<offset_type>(bytes.size()));
            encoded_ptr += sizeof(offset_type);
            memcpy(encoded_ptr, bytes.data(), bytes.size());
            encoded_ptr += bytes.size();
          },
          [&] {
            auto& encoded_ptr = *encoded_bytes++;
            *encoded_ptr++ = 1;
            util::SafeStore(encoded_ptr, static_cast<offset_type>(0));
            encoded_ptr += sizeof(offset_type);
          });
    }

    template <typename T, typename CType = typename T::c_type>
    typename std::enable_if<sizeof(CType) <= sizeof(uint64_t), Status>::type Visit(
        const T& input_type) {
      int32_t num_bits = bit_width(input_type.id());
      switch (num_bits) {
        case 1:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<1>(data, encoded_bytes);
          };
          break;
        case 8:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<8>(data, encoded_bytes);
          };
          break;
        case 16:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<16>(data, encoded_bytes);
          };
          break;
        case 32:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<32>(data, encoded_bytes);
          };
          break;
        case 64:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<64>(data, encoded_bytes);
          };
          break;
      }
      return Status::OK();
    }

    template <typename T>
    enable_if_base_binary<T, Status> Visit(const T&) {
      encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                            uint8_t** encoded_bytes) {
        EncodeVarLength<T>(data, encoded_bytes);
      };
      return Status::OK();
    }

    Status Visit(const FixedSizeBinaryType&) {
      encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                            uint8_t** encoded_bytes) {
        EncodeBigFixed(data, encoded_bytes);
      };
      return Status::OK();
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("encoding hashes for type ", type);
    }

    EncodeNextImpl encode_next_impl;
  };

  using DecodeNextImpl = std::function<void(KernelContext*, int32_t, uint8_t**,
                                            std::shared_ptr<ArrayData>*)>;

  struct GetDecodeNextImpl {
    static Status DecodeNulls(KernelContext* ctx, int32_t length, uint8_t** encoded_bytes,
                              std::shared_ptr<ResizableBuffer>* null_buf,
                              int32_t* null_count) {
      // Do we have nulls?
      *null_count = 0;
      for (int32_t i = 0; i < length; ++i) {
        *null_count += encoded_bytes[i][0];
      }
      if (*null_count > 0) {
        ARROW_ASSIGN_OR_RAISE(*null_buf, ctx->AllocateBitmap(length));
        uint8_t* nulls = (*null_buf)->mutable_data();
        memset(nulls, 0, (*null_buf)->size());
        for (int32_t i = 0; i < length; ++i) {
          if (!encoded_bytes[i][0]) {
            BitUtil::SetBit(nulls, i);
          }
          encoded_bytes[i] += 1;
        }
      } else {
        for (int32_t i = 0; i < length; ++i) {
          encoded_bytes[i] += 1;
        }
      }
      return Status ::OK();
    }

    template <int NumBits>
    static void DecodeSmallFixed(KernelContext* ctx, const Type::type& output_type,
                                 int32_t length, uint8_t** encoded_bytes,
                                 std::shared_ptr<ArrayData>* out) {
      std::shared_ptr<ResizableBuffer> null_buf;
      int32_t null_count;
      KERNEL_RETURN_IF_ERROR(
          ctx, DecodeNulls(ctx, length, encoded_bytes, &null_buf, &null_count));

      KERNEL_ASSIGN_OR_RAISE(
          auto key_buf, ctx,
          ctx->Allocate(NumBits == 1 ? (length + 7) / 8 : (NumBits / 8) * length));

      uint8_t* raw_output = key_buf->mutable_data();
      for (int32_t i = 0; i < length; ++i) {
        auto& encoded_ptr = encoded_bytes[i];
        if (NumBits == 1) {
          BitUtil::SetBitTo(raw_output, i, encoded_ptr[0] != 0);
          encoded_ptr += 1;
        }
        if (NumBits == 8) {
          raw_output[i] = encoded_ptr[0];
          encoded_ptr += 1;
        }
        if (NumBits == 16) {
          reinterpret_cast<uint16_t*>(raw_output)[i] =
              reinterpret_cast<const uint16_t*>(encoded_bytes[i])[0];
          encoded_ptr += 2;
        }
        if (NumBits == 32) {
          reinterpret_cast<uint32_t*>(raw_output)[i] =
              reinterpret_cast<const uint32_t*>(encoded_bytes[i])[0];
          encoded_ptr += 4;
        }
        if (NumBits == 64) {
          reinterpret_cast<uint64_t*>(raw_output)[i] =
              reinterpret_cast<const uint64_t*>(encoded_bytes[i])[0];
          encoded_ptr += 8;
        }
      }

      DCHECK(is_integer(output_type) || output_type == Type::BOOL);
      *out = ArrayData::Make(int64(), length, {null_buf, key_buf}, null_count);
    }

    static void DecodeBigFixed(KernelContext* ctx, int num_bytes, int32_t length,
                               uint8_t** encoded_bytes, std::shared_ptr<ArrayData>* out) {
      std::shared_ptr<ResizableBuffer> null_buf;
      int32_t null_count;
      KERNEL_RETURN_IF_ERROR(
          ctx, DecodeNulls(ctx, length, encoded_bytes, &null_buf, &null_count));

      KERNEL_ASSIGN_OR_RAISE(auto key_buf, ctx, ctx->Allocate(num_bytes * length));
      auto raw_output = key_buf->mutable_data();
      for (int32_t i = 0; i < length; ++i) {
        memcpy(raw_output + i * num_bytes, encoded_bytes[i], num_bytes);
        encoded_bytes[i] += num_bytes;
      }

      *out = ArrayData::Make(fixed_size_binary(num_bytes), length, {null_buf, key_buf},
                             null_count);
    }

    static void DecodeVarLength(KernelContext* ctx, bool is_string, int32_t length,
                                uint8_t** encoded_bytes,
                                std::shared_ptr<ArrayData>* out) {
      std::shared_ptr<ResizableBuffer> null_buf;
      int32_t null_count;
      KERNEL_RETURN_IF_ERROR(
          ctx, DecodeNulls(ctx, length, encoded_bytes, &null_buf, &null_count));

      using offset_type = typename StringType::offset_type;

      int32_t length_sum = 0;
      for (int32_t i = 0; i < length; ++i) {
        length_sum += reinterpret_cast<offset_type*>(encoded_bytes)[0];
      }

      KERNEL_ASSIGN_OR_RAISE(auto offset_buf, ctx,
                             ctx->Allocate(sizeof(offset_type) * (1 + length)));
      KERNEL_ASSIGN_OR_RAISE(auto key_buf, ctx, ctx->Allocate(length_sum));

      auto raw_offsets = offset_buf->mutable_data();
      auto raw_keys = key_buf->mutable_data();
      int32_t current_offset = 0;
      for (int32_t i = 0; i < length; ++i) {
        offset_type key_length = reinterpret_cast<offset_type*>(encoded_bytes[i])[0];
        reinterpret_cast<offset_type*>(raw_offsets)[i] = current_offset;
        encoded_bytes[i] += sizeof(offset_type);
        memcpy(raw_keys + current_offset, encoded_bytes[i], key_length);
        encoded_bytes[i] += key_length;
        current_offset += key_length;
      }
      reinterpret_cast<offset_type*>(raw_offsets)[length] = current_offset;

      if (is_string) {
        *out = ArrayData::Make(utf8(), length, {null_buf, offset_buf, key_buf},
                               null_count, 0);
      } else {
        *out = ArrayData::Make(binary(), length, {null_buf, offset_buf, key_buf},
                               null_count, 0);
      }
    }

    template <typename T>
    Status Visit(const T& input_type) {
      int32_t num_bits = bit_width(input_type.id());
      auto type_id = input_type.id();
      switch (num_bits) {
        case 1:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<1>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 8:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<8>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 16:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<16>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 32:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<32>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 64:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<64>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
      }
      return Status::OK();
    }

    Status Visit(const StringType&) {
      decode_next_impl = [](KernelContext* ctx, int32_t length, uint8_t** encoded_bytes,
                            std::shared_ptr<ArrayData>* out) {
        DecodeVarLength(ctx, true, length, encoded_bytes, out);
      };
      return Status::OK();
    }

    Status Visit(const BinaryType&) {
      decode_next_impl = [](KernelContext* ctx, int32_t length, uint8_t** encoded_bytes,
                            std::shared_ptr<ArrayData>* out) {
        DecodeVarLength(ctx, false, length, encoded_bytes, out);
      };
      return Status::OK();
    }

    Status Visit(const FixedSizeBinaryType& type) {
      int32_t num_bytes = type.byte_width();
      decode_next_impl = [num_bytes](KernelContext* ctx, int32_t length,
                                     uint8_t** encoded_bytes,
                                     std::shared_ptr<ArrayData>* out) {
        DecodeBigFixed(ctx, num_bytes, length, encoded_bytes, out);
      };
      return Status::OK();
    }

    DecodeNextImpl decode_next_impl;
  };

  void Consume(KernelContext* ctx, const ExecBatch& batch) {
    ArrayDataVector aggregands, keys;

    size_t i;
    for (i = 0; i < aggregators.size(); ++i) {
      aggregands.push_back(batch[i].array());
    }
    while (i < static_cast<size_t>(batch.num_values())) {
      keys.push_back(batch[i++].array());
    }

    offsets_batch_.clear();
    offsets_batch_.resize(batch.length + 1);
    offsets_batch_[0] = 0;
    memset(offsets_batch_.data(), 0, sizeof(offsets_batch_[0]) * offsets_batch_.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      add_length_impl[i].add_length_impl(keys[i], offsets_batch_.data());
    }
    int32_t total_length = 0;
    for (int64_t i = 0; i < batch.length; ++i) {
      auto total_length_before = total_length;
      total_length += offsets_batch_[i];
      offsets_batch_[i] = total_length_before;
    }
    offsets_batch_[batch.length] = total_length;

    key_bytes_batch_.clear();
    key_bytes_batch_.resize(total_length);
    key_buf_ptrs_.clear();
    key_buf_ptrs_.resize(batch.length);
    for (int64_t i = 0; i < batch.length; ++i) {
      key_buf_ptrs_[i] = key_bytes_batch_.data() + offsets_batch_[i];
    }
    for (size_t i = 0; i < keys.size(); ++i) {
      encode_next_impl[i].encode_next_impl(keys[i], key_buf_ptrs_.data());
    }

    group_ids_batch_.clear();
    group_ids_batch_.resize(batch.length);
    for (int64_t i = 0; i < batch.length; ++i) {
      int32_t key_length = offsets_batch_[i + 1] - offsets_batch_[i];
      std::string key(
          reinterpret_cast<const char*>(key_bytes_batch_.data() + offsets_batch_[i]),
          key_length);
      auto iter = map_.find(key);
      if (iter == map_.end()) {
        group_ids_batch_[i] = n_groups++;
        auto next_key_offset = static_cast<int32_t>(key_bytes_.size());
        key_bytes_.resize(next_key_offset + key_length);
        offsets_.push_back(next_key_offset + key_length);
        memcpy(key_bytes_.data() + next_key_offset, key.c_str(), key_length);
        map_.insert(std::make_pair(key, group_ids_batch_[i]));
      } else {
        group_ids_batch_[i] = iter->second;
      }
    }

    UInt32Array group_ids(batch.length, Buffer::Wrap(group_ids_batch_));
    for (size_t i = 0; i < aggregators.size(); ++i) {
      KernelContext batch_ctx{ctx->exec_context()};
      batch_ctx.SetState(aggregator_states[i].get());
      ExecBatch batch({aggregands[i], group_ids, Datum(n_groups)}, group_ids.length());
      aggregators[i].consume(&batch_ctx, batch);
      ctx->SetStatus(batch_ctx.status());
      if (ctx->HasError()) return;
    }
  }

  void Finalize(KernelContext* ctx, Datum* out) {
    size_t n_keys = decode_next_impl.size();
    ArrayDataVector out_columns(aggregators.size() + n_keys);
    for (size_t i = 0; i < aggregators.size(); ++i) {
      KernelContext batch_ctx{ctx->exec_context()};
      batch_ctx.SetState(aggregator_states[i].get());
      Datum aggregated;
      aggregators[i].finalize(&batch_ctx, &aggregated);
      ctx->SetStatus(batch_ctx.status());
      if (ctx->HasError()) return;
      out_columns[i] = aggregated.array();
    }

    key_buf_ptrs_.clear();
    key_buf_ptrs_.resize(n_groups);
    for (int64_t i = 0; i < n_groups; ++i) {
      key_buf_ptrs_[i] = key_bytes_.data() + offsets_[i];
    }

    int64_t length = n_groups;
    for (size_t i = 0; i < n_keys; ++i) {
      std::shared_ptr<ArrayData> key_array;
      decode_next_impl[i].decode_next_impl(ctx, static_cast<int32_t>(length),
                                           key_buf_ptrs_.data(), &key_array);
      out_columns[aggregators.size() + i] = std::move(key_array);
    }

    *out = ArrayData::Make(std::move(out_type), length, {/*null_bitmap=*/nullptr},
                           std::move(out_columns));
  }

  std::vector<int32_t> offsets_batch_;
  std::vector<uint8_t> key_bytes_batch_;
  std::vector<uint8_t*> key_buf_ptrs_;
  std::vector<uint32_t> group_ids_batch_;

  std::unordered_map<std::string, uint32_t> map_;
  std::vector<int32_t> offsets_;
  std::vector<uint8_t> key_bytes_;
  uint32_t n_groups;

  std::shared_ptr<DataType> out_type;
  GroupByOptions options;
  std::vector<HashAggregateKernel> aggregators;
  std::vector<std::unique_ptr<KernelState>> aggregator_states;

  std::vector<GetAddLengthImpl> add_length_impl;
  std::vector<GetEncodeNextImpl> encode_next_impl;
  std::vector<GetDecodeNextImpl> decode_next_impl;
};

Result<HashAggregateKernel> MakeKernel(GroupByOptions::Aggregate);

Result<GroupByImpl> GroupByInit(ExecContext* ctx, const std::vector<Datum>& aggregands,
                                const std::vector<Datum>& keys,
                                const GroupByOptions& options) {
  GroupByImpl impl;
  impl.options = options;
  const auto& aggregates = impl.options.aggregates;

  impl.n_groups = 0;
  impl.offsets_.push_back(0);

  if (aggregates.size() != aggregands.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           aggregands.size(), " aggregands were provided.");
  }

  FieldVector out_fields;

  impl.aggregators.resize(aggregates.size());
  impl.aggregator_states.resize(aggregates.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    auto a = aggregates[i];

    if (a.options == nullptr) {
      // use known default options for the named function if possible
      auto maybe_function = ctx->func_registry()->GetFunction(a.function);
      if (maybe_function.ok()) {
        a.options = maybe_function.ValueOrDie()->default_options();
      }
    }

    ARROW_ASSIGN_OR_RAISE(impl.aggregators[i], MakeKernel(a));

    KernelContext kernel_ctx{ctx};
    impl.aggregator_states[i] = impl.aggregators[i].init(
        &kernel_ctx,
        KernelInitArgs{&impl.aggregators[i], {aggregands[i].type()}, a.options});
    if (kernel_ctx.HasError()) return kernel_ctx.status();

    kernel_ctx.SetState(impl.aggregator_states[i].get());
    ARROW_ASSIGN_OR_RAISE(auto descr, impl.aggregators[i].signature->out_type().Resolve(
                                          &kernel_ctx, {
                                                           aggregands[i].type(),
                                                           uint32(),
                                                           uint32(),
                                                       }));
    out_fields.push_back(field("", descr.type));
  }

  for (const auto& key : keys) {
    const auto& key_type = key.type();
    switch (key_type->id()) {
      // Supported types of keys
      case Type::BOOL:
      case Type::UINT8:
      case Type::INT8:
      case Type::UINT16:
      case Type::INT16:
      case Type::UINT32:
      case Type::INT32:
      case Type::UINT64:
      case Type::INT64:
      case Type::STRING:
      case Type::BINARY:
      case Type::FIXED_SIZE_BINARY:
        break;
      default:
        return Status::NotImplemented("Key of type", key_type->ToString());
    }
    out_fields.push_back(field("", key_type));
  }

  impl.add_length_impl.resize(keys.size());
  impl.encode_next_impl.resize(keys.size());
  impl.decode_next_impl.resize(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    const auto& key_type = keys[i].type();
    RETURN_NOT_OK(VisitTypeInline(*key_type, &impl.add_length_impl[i]));
    RETURN_NOT_OK(VisitTypeInline(*key_type, &impl.encode_next_impl[i]));
    RETURN_NOT_OK(VisitTypeInline(*key_type, &impl.decode_next_impl[i]));
  }

  impl.out_type = struct_(std::move(out_fields));
  return impl;
}

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

  template <typename Resize>
  Status MaybeResize(int64_t old_num_groups, const ExecBatch& batch,
                     const Resize& resize) {
    int64_t new_num_groups = batch[2].scalar_as<UInt32Scalar>().value;
    if (new_num_groups <= old_num_groups) {
      return Status::OK();
    }
    return resize(old_num_groups, new_num_groups);
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
      *builder = std::move(typed_builder.bytes_builder());
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

      GetResizeImpls<T>();
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

    template <typename T, typename CType = typename TypeTraits<T>::CType>
    enable_if_floating_point<T> GetResizeImpls() {
      auto inf = std::numeric_limits<CType>::infinity();
      resize_min_impl = MakeResizeImpl(inf);
      resize_max_impl = MakeResizeImpl(-inf);
    }

    template <typename T, typename CType = typename TypeTraits<T>::CType>
    enable_if_integer<T> GetResizeImpls() {
      resize_max_impl = MakeResizeImpl(std::numeric_limits<CType>::min());
      resize_min_impl = MakeResizeImpl(std::numeric_limits<CType>::max());
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

template <typename A>
KernelInit MakeInit(GroupByOptions::Aggregate a) {
  return [a](KernelContext* ctx,
             const KernelInitArgs& args) -> std::unique_ptr<KernelState> {
    auto impl = ::arrow::internal::make_unique<A>();
    ctx->SetStatus(impl->Init(ctx->exec_context(), a.options, args.inputs[0].type));
    if (ctx->HasError()) return nullptr;
    return impl;
  };
}

Result<HashAggregateKernel> MakeKernel(GroupByOptions::Aggregate a) {
  HashAggregateKernel kernel;

  if (a.function == "count") {
    kernel.init = MakeInit<GroupedCountImpl>(a);
  } else if (a.function == "sum") {
    kernel.init = MakeInit<GroupedSumImpl>(a);
  } else if (a.function == "min_max") {
    kernel.init = MakeInit<GroupedMinMaxImpl>(a);
  } else {
    return Status::NotImplemented("Grouped aggregate ", a.function);
  }

  // this isn't really in the spirit of things, but I'll get around to defining
  // HashAggregateFunctions later
  kernel.signature = KernelSignature::Make(
      {{}, {}, {}}, OutputType([](KernelContext* ctx,
                                  const std::vector<ValueDescr>&) -> Result<ValueDescr> {
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

}  // namespace aggregate

Result<Datum> GroupBy(const std::vector<Datum>& aggregands,
                      const std::vector<Datum>& keys, const GroupByOptions& options,
                      ExecContext* ctx) {
  if (ctx == nullptr) {
    ExecContext default_ctx;
    return GroupBy(aggregands, keys, options, &default_ctx);
  }

  std::vector<Datum> args = aggregands;
  for (const Datum& key : keys) {
    args.push_back(key);
  }

  ARROW_ASSIGN_OR_RAISE(auto impl,
                        aggregate::GroupByInit(ctx, aggregands, keys, options));

  ARROW_ASSIGN_OR_RAISE(auto batch_iterator,
                        detail::ExecBatchIterator::Make(args, ctx->exec_chunksize()));

  KernelContext kernel_ctx{ctx};

  ExecBatch batch;
  while (batch_iterator->Next(&batch)) {
    if (batch.length > 0) {
      impl.Consume(&kernel_ctx, batch);
      if (kernel_ctx.HasError()) return kernel_ctx.status();
    }
  }

  Datum out;
  impl.Finalize(&kernel_ctx, &out);
  if (kernel_ctx.HasError()) return kernel_ctx.status();
  return out;
}

}  // namespace compute
}  // namespace arrow

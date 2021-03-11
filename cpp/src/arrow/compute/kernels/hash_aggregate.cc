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

// ----------------------------------------------------------------------
// Count implementation

struct GroupedCountImpl : public GroupedAggregator {
  void Init(KernelContext* ctx, const FunctionOptions* options,
            const std::shared_ptr<DataType>&) override {
    options_ = checked_cast<const CountOptions&>(*options);
    KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(0).Value(&counts_));
  }

  void Resize(KernelContext* ctx, int64_t new_num_groups) override {
    auto old_size = num_groups();
    KERNEL_RETURN_IF_ERROR(ctx, counts_->TypedResize<int64_t>(new_num_groups));
    auto new_size = num_groups();

    auto raw_counts = reinterpret_cast<int64_t*>(counts_->mutable_data());
    for (auto i = old_size; i < new_size; ++i) {
      raw_counts[i] = 0;
    }
  }

  void Consume(KernelContext* ctx, const Datum& aggregand,
               const uint32_t* group_ids) override {
    MaybeResize(ctx, aggregand.length(), group_ids);
    if (ctx->HasError()) return;

    auto raw_counts = reinterpret_cast<int64_t*>(counts_->mutable_data());

    const auto& input = aggregand.array();

    if (options_.count_mode == CountOptions::COUNT_NULL) {
      for (int64_t i = 0, input_i = input->offset; i < input->length; ++i, ++input_i) {
        auto g = group_ids[i];
        raw_counts[g] += !BitUtil::GetBit(input->buffers[0]->data(), input_i);
      }
      return;
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
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    auto length = num_groups();
    *out = std::make_shared<Int64Array>(length, std::move(counts_));
  }

  int64_t num_groups() const override { return counts_->size() / sizeof(int64_t); }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  CountOptions options_;
  std::shared_ptr<ResizableBuffer> counts_;
};

// ----------------------------------------------------------------------
// Sum implementation

struct GroupedSumImpl : public GroupedAggregator {
  // NB: whether we are accumulating into double, int64_t, or uint64_t
  // we always have 64 bits per group in the sums buffer.
  static constexpr size_t kSumSize = sizeof(int64_t);

  using ConsumeImpl = std::function<void(const std::shared_ptr<ArrayData>&,
                                         const uint32_t*, Buffer*, Buffer*)>;

  struct GetConsumeImpl {
    template <typename T,
              typename AccumulatorType = typename FindAccumulatorType<T>::Type>
    Status Visit(const T&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input,
                        const uint32_t* group_ids, Buffer* sums, Buffer* counts) {
        auto raw_input = reinterpret_cast<const typename TypeTraits<T>::CType*>(
            input->buffers[1]->data());
        auto raw_sums = reinterpret_cast<typename TypeTraits<AccumulatorType>::CType*>(
            sums->mutable_data());
        auto raw_counts = reinterpret_cast<int64_t*>(counts->mutable_data());

        arrow::internal::VisitSetBitRunsVoid(
            input->buffers[0], input->offset, input->length,
            [&](int64_t begin, int64_t length) {
              for (int64_t input_i = begin, i = begin - input->offset;
                   input_i < begin + length; ++input_i, ++i) {
                auto g = group_ids[i];
                raw_sums[g] += raw_input[input_i];
                raw_counts[g] += 1;
              }
            });
      };
      out_type = TypeTraits<AccumulatorType>::type_singleton();
      return Status::OK();
    }

    Status Visit(const BooleanType&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input,
                        const uint32_t* group_ids, Buffer* sums, Buffer* counts) {
        auto raw_input = input->buffers[1]->data();
        auto raw_sums = reinterpret_cast<uint64_t*>(sums->mutable_data());
        auto raw_counts = reinterpret_cast<int64_t*>(counts->mutable_data());

        arrow::internal::VisitSetBitRunsVoid(
            input->buffers[0], input->offset, input->length,
            [&](int64_t begin, int64_t length) {
              for (int64_t input_i = begin, i = begin - input->offset;
                   input_i < begin + length; ++input_i) {
                auto g = group_ids[i];
                raw_sums[g] += BitUtil::GetBit(raw_input, input_i);
                raw_counts[g] += 1;
              }
            });
      };
      out_type = uint64();
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

  void Init(KernelContext* ctx, const FunctionOptions*,
            const std::shared_ptr<DataType>& input_type) override {
    KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(0).Value(&sums_));
    KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(0).Value(&counts_));

    GetConsumeImpl get_consume_impl;
    KERNEL_RETURN_IF_ERROR(ctx, VisitTypeInline(*input_type, &get_consume_impl));

    consume_impl_ = std::move(get_consume_impl.consume_impl);
    out_type_ = std::move(get_consume_impl.out_type);
  }

  void Resize(KernelContext* ctx, int64_t new_num_groups) override {
    auto old_size = num_groups() * kSumSize;
    KERNEL_RETURN_IF_ERROR(ctx, sums_->Resize(new_num_groups * kSumSize));
    KERNEL_RETURN_IF_ERROR(ctx, counts_->Resize(new_num_groups * sizeof(int64_t)));
    auto new_size = num_groups() * kSumSize;
    std::memset(sums_->mutable_data() + old_size, 0, new_size - old_size);
    std::memset(counts_->mutable_data() + old_size, 0, new_size - old_size);
  }

  void Consume(KernelContext* ctx, const Datum& aggregand,
               const uint32_t* group_ids) override {
    MaybeResize(ctx, aggregand.length(), group_ids);
    if (ctx->HasError()) return;
    consume_impl_(aggregand.array(), group_ids, sums_.get(), counts_.get());
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    std::shared_ptr<Buffer> null_bitmap;
    int64_t null_count = 0;

    for (int64_t i = 0; i < num_groups(); ++i) {
      if (reinterpret_cast<const int64_t*>(counts_->data())[i] > 0) continue;

      if (null_bitmap == nullptr) {
        KERNEL_ASSIGN_OR_RAISE(null_bitmap, ctx, ctx->AllocateBitmap(num_groups()));
        BitUtil::SetBitsTo(null_bitmap->mutable_data(), 0, num_groups(), true);
      }

      null_count += 1;
      BitUtil::SetBitTo(null_bitmap->mutable_data(), i, false);
    }

    *out = ArrayData::Make(std::move(out_type_), num_groups(),
                           {std::move(null_bitmap), std::move(sums_)}, null_count);
  }

  int64_t num_groups() const override { return counts_->size() / sizeof(int64_t); }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  // NB: counts are used here instead of a simple "has_values_" bitmap since
  // we expect to reuse this kernel to handle Mean
  std::shared_ptr<ResizableBuffer> sums_, counts_;
  std::shared_ptr<DataType> out_type_;
  ConsumeImpl consume_impl_;
};

// ----------------------------------------------------------------------
// MinMax implementation

struct GroupedMinMaxImpl : public GroupedAggregator {
  using ConsumeImpl = std::function<void(const std::shared_ptr<ArrayData>&,
                                         const uint32_t*, BufferVector*)>;

  using ResizeImpl = std::function<Status(Buffer*, int64_t)>;

  struct GetImpl {
    template <typename T, typename CType = typename TypeTraits<T>::CType>
    enable_if_number<T, Status> Visit(const T&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input,
                        const uint32_t* group_ids, BufferVector* buffers) {
        auto raw_mins = reinterpret_cast<CType*>(buffers->at(0)->mutable_data());
        auto raw_maxes = reinterpret_cast<CType*>(buffers->at(1)->mutable_data());

        auto raw_has_nulls = buffers->at(2)->mutable_data();
        auto raw_has_values = buffers->at(3)->mutable_data();

        auto g = group_ids;
        VisitArrayDataInline<T>(
            *input,
            [&](CType val) {
              raw_maxes[*g] = std::max(raw_maxes[*g], val);
              raw_mins[*g] = std::min(raw_mins[*g], val);
              BitUtil::SetBit(raw_has_values, *g++);
            },
            [&] { BitUtil::SetBit(raw_has_nulls, *g++); });
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

    template <typename CType>
    ResizeImpl MakeResizeImpl(CType anti_extreme) {
      // resize a min or max buffer, storing the correct anti extreme
      return [anti_extreme](Buffer* vals, int64_t new_num_groups) {
        int64_t old_num_groups = vals->size() / sizeof(CType);

        int64_t new_size = new_num_groups * sizeof(CType);
        RETURN_NOT_OK(checked_cast<ResizableBuffer*>(vals)->Resize(new_size));

        auto raw_vals = reinterpret_cast<CType*>(vals->mutable_data());
        for (int64_t i = old_num_groups; i != new_num_groups; ++i) {
          raw_vals[i] = anti_extreme;
        }
        return Status::OK();
      };
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

  void Init(KernelContext* ctx, const FunctionOptions* options,
            const std::shared_ptr<DataType>& input_type) override {
    options_ = *checked_cast<const MinMaxOptions*>(options);
    type_ = input_type;

    buffers_.resize(4);
    for (auto& buf : buffers_) {
      KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(0).Value(&buf));
    }

    GetImpl get_impl;
    KERNEL_RETURN_IF_ERROR(ctx, VisitTypeInline(*input_type, &get_impl));

    consume_impl_ = std::move(get_impl.consume_impl);
    resize_min_impl_ = std::move(get_impl.resize_min_impl);
    resize_max_impl_ = std::move(get_impl.resize_max_impl);
  }

  void Resize(KernelContext* ctx, int64_t new_num_groups) override {
    auto old_num_groups = num_groups_;
    num_groups_ = new_num_groups;

    KERNEL_RETURN_IF_ERROR(ctx, resize_min_impl_(buffers_[0].get(), new_num_groups));
    KERNEL_RETURN_IF_ERROR(ctx, resize_max_impl_(buffers_[1].get(), new_num_groups));

    for (auto buffer : {buffers_[2].get(), buffers_[3].get()}) {
      KERNEL_RETURN_IF_ERROR(ctx, checked_cast<ResizableBuffer*>(buffer)->Resize(
                                      BitUtil::BytesForBits(new_num_groups)));
      BitUtil::SetBitsTo(buffer->mutable_data(), old_num_groups, new_num_groups, false);
    }
  }

  void Consume(KernelContext* ctx, const Datum& aggregand,
               const uint32_t* group_ids) override {
    MaybeResize(ctx, aggregand.length(), group_ids);
    if (ctx->HasError()) return;
    consume_impl_(aggregand.array(), group_ids, &buffers_);
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    // aggregation for group is valid if there was at least one value in that group
    std::shared_ptr<Buffer> null_bitmap = std::move(buffers_[3]);

    if (options_.null_handling == MinMaxOptions::EMIT_NULL) {
      // ... and there were no nulls in that group
      arrow::internal::BitmapAndNot(null_bitmap->data(), 0, buffers_[2]->data(), 0,
                                    num_groups(), 0, null_bitmap->mutable_data());
    }

    auto mins =
        ArrayData::Make(type_, num_groups(), {null_bitmap, std::move(buffers_[0])});

    auto maxes = ArrayData::Make(type_, num_groups(),
                                 {std::move(null_bitmap), std::move(buffers_[1])});

    *out = ArrayData::Make(out_type(), num_groups(), {nullptr},
                           {std::move(mins), std::move(maxes)});
  }

  int64_t num_groups() const override { return num_groups_; }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", type_), field("max", type_)});
  }

  int64_t num_groups_;
  BufferVector buffers_;
  std::shared_ptr<DataType> type_;
  ConsumeImpl consume_impl_;
  ResizeImpl resize_min_impl_, resize_max_impl_;
  MinMaxOptions options_;
};

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
      auto raw_input = data->buffers[1]->data();
      auto offset = data->offset;
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          encoded_ptr[0] = is_null ? 1 : 0;
          encoded_ptr += 1;
          uint64_t null_multiplier = is_null ? 0 : 1;
          if (NumBits == 1) {
            encoded_ptr[0] = static_cast<uint8_t>(
                null_multiplier * (BitUtil::GetBit(raw_input, offset + i) ? 1 : 0));
            encoded_ptr += 1;
          }
          if (NumBits == 8) {
            encoded_ptr[0] =
                static_cast<uint8_t>(null_multiplier * reinterpret_cast<const uint8_t*>(
                                                           raw_input)[offset + i]);
            encoded_ptr += 1;
          }
          if (NumBits == 16) {
            reinterpret_cast<uint16_t*>(encoded_ptr)[0] =
                static_cast<uint16_t>(null_multiplier * reinterpret_cast<const uint16_t*>(
                                                            raw_input)[offset + i]);
            encoded_ptr += 2;
          }
          if (NumBits == 32) {
            reinterpret_cast<uint32_t*>(encoded_ptr)[0] =
                static_cast<uint32_t>(null_multiplier * reinterpret_cast<const uint32_t*>(
                                                            raw_input)[offset + i]);
            encoded_ptr += 4;
          }
          if (NumBits == 64) {
            reinterpret_cast<uint64_t*>(encoded_ptr)[0] =
                static_cast<uint64_t>(null_multiplier * reinterpret_cast<const uint64_t*>(
                                                            raw_input)[offset + i]);
            encoded_ptr += 8;
          }
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          encoded_ptr[0] = 0;
          encoded_ptr += 1;
          if (NumBits == 1) {
            encoded_ptr[0] = (BitUtil::GetBit(raw_input, offset + i) ? 1 : 0);
            encoded_ptr += 1;
          }
          if (NumBits == 8) {
            encoded_ptr[0] = reinterpret_cast<const uint8_t*>(raw_input)[offset + i];
            encoded_ptr += 1;
          }
          if (NumBits == 16) {
            reinterpret_cast<uint16_t*>(encoded_ptr)[0] =
                reinterpret_cast<const uint16_t*>(raw_input)[offset + i];
            encoded_ptr += 2;
          }
          if (NumBits == 32) {
            reinterpret_cast<uint32_t*>(encoded_ptr)[0] =
                reinterpret_cast<const uint32_t*>(raw_input)[offset + i];
            encoded_ptr += 4;
          }
          if (NumBits == 64) {
            reinterpret_cast<uint64_t*>(encoded_ptr)[0] =
                reinterpret_cast<const uint64_t*>(raw_input)[offset + i];
            encoded_ptr += 8;
          }
        }
      }
    }

    static void EncodeBigFixed(int num_bytes, const std::shared_ptr<ArrayData>& data,
                               uint8_t** encoded_bytes) {
      auto raw_input = data->buffers[1]->data();
      auto offset = data->offset;
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          encoded_ptr[0] = is_null ? 1 : 0;
          encoded_ptr += 1;
          if (is_null) {
            memset(encoded_ptr, 0, num_bytes);
          } else {
            memcpy(encoded_ptr, raw_input + num_bytes * (offset + i), num_bytes);
          }
          encoded_ptr += num_bytes;
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          encoded_ptr[0] = 0;
          encoded_ptr += 1;
          memcpy(encoded_ptr, raw_input + num_bytes * (offset + i), num_bytes);
          encoded_ptr += num_bytes;
        }
      }
    }

    static void EncodeVarLength(const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
      using offset_type = typename StringType::offset_type;
      auto offset = data->offset;
      const auto offsets = data->GetValues<offset_type>(1);
      auto raw_input = data->buffers[2]->data();
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          if (is_null) {
            encoded_ptr[0] = 1;
            encoded_ptr++;
            reinterpret_cast<offset_type*>(encoded_ptr)[0] = 0;
            encoded_ptr += sizeof(offset_type);
          } else {
            encoded_ptr[0] = 0;
            encoded_ptr++;
            size_t num_bytes = offsets[offset + i + 1] - offsets[offset + i];
            reinterpret_cast<offset_type*>(encoded_ptr)[0] = num_bytes;
            encoded_ptr += sizeof(offset_type);
            memcpy(encoded_ptr, raw_input + offsets[offset + i], num_bytes);
            encoded_ptr += num_bytes;
          }
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          encoded_ptr[0] = 0;
          encoded_ptr++;
          size_t num_bytes = offsets[offset + i + 1] - offsets[offset + i];
          reinterpret_cast<offset_type*>(encoded_ptr)[0] = num_bytes;
          encoded_ptr += sizeof(offset_type);
          memcpy(encoded_ptr, raw_input + offsets[offset + i], num_bytes);
          encoded_ptr += num_bytes;
        }
      }
    }

    template <typename T>
    Status Visit(const T& input_type) {
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

    Status Visit(const StringType&) {
      encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                            uint8_t** encoded_bytes) {
        EncodeVarLength(data, encoded_bytes);
      };
      return Status::OK();
    }

    Status Visit(const BinaryType&) {
      encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                            uint8_t** encoded_bytes) {
        EncodeVarLength(data, encoded_bytes);
      };
      return Status::OK();
    }

    Status Visit(const FixedSizeBinaryType& type) {
      int32_t num_bytes = type.byte_width();
      encode_next_impl = [num_bytes](const std::shared_ptr<ArrayData>& data,
                                     uint8_t** encoded_bytes) {
        EncodeBigFixed(num_bytes, data, encoded_bytes);
      };
      return Status::OK();
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

    for (size_t i = 0; i < aggregators.size(); ++i) {
      aggregators[i]->Consume(ctx, aggregands[i], group_ids_batch_.data());
      if (ctx->HasError()) return;
    }
  }

  void MergeFrom(KernelContext* ctx, KernelState&& src) {
    // TODO(ARROW-11840) merge two hash tables
    ctx->SetStatus(Status::NotImplemented("merging grouped aggregations"));
  }

  void Finalize(KernelContext* ctx, Datum* out) {
    size_t n_keys = decode_next_impl.size();
    ArrayDataVector out_columns(aggregators.size() + n_keys);
    for (size_t i = 0; i < aggregators.size(); ++i) {
      Datum aggregand;
      aggregators[i]->Finalize(ctx, &aggregand);
      if (ctx->HasError()) return;
      out_columns[i] = aggregand.array();
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
  std::vector<std::unique_ptr<GroupedAggregator>> aggregators;

  std::vector<GetAddLengthImpl> add_length_impl;
  std::vector<GetEncodeNextImpl> encode_next_impl;
  std::vector<GetDecodeNextImpl> decode_next_impl;
};

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
  for (size_t i = 0; i < aggregates.size(); ++i) {
    const std::string& function = aggregates[i].function;

    if (function == "count") {
      impl.aggregators[i] = ::arrow::internal::make_unique<GroupedCountImpl>();
    } else if (function == "sum") {
      impl.aggregators[i] = ::arrow::internal::make_unique<GroupedSumImpl>();
    } else if (function == "min_max") {
      impl.aggregators[i] = ::arrow::internal::make_unique<GroupedMinMaxImpl>();
    } else {
      return Status::NotImplemented("Grouped aggregate ", function);
    }

    const FunctionOptions* options = aggregates[i].options;
    if (options == nullptr) {
      // use known default options for the named function if possible
      auto maybe_function = ctx->func_registry()->GetFunction(function);
      if (maybe_function.ok()) {
        options = maybe_function.ValueOrDie()->default_options();
      }
    }

    KernelContext kernel_ctx{ctx};
    impl.aggregators[i]->Init(&kernel_ctx, options, aggregands[i].type());
    if (kernel_ctx.HasError()) return kernel_ctx.status();

    out_fields.push_back(field("", impl.aggregators[i]->out_type()));
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

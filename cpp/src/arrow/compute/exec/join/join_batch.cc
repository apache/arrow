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

#include "arrow/compute/exec/join/join_batch.h"

#include <memory.h>

#include <algorithm>

#include "arrow/compute/exec/join/join_hashtable.h"
#include "arrow/compute/exec/key_encode.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace compute {

ShuffleOutputDesc::ShuffleOutputDesc(
    std::vector<std::shared_ptr<ResizableBuffer>>& in_buffers, int64_t in_length,
    bool in_has_nulls) {
  offset = in_length;
  for (int i = 0; i < 3; ++i) {
    DCHECK(static_cast<int>(in_buffers.size()) > i && in_buffers[i].get() != nullptr);
    buffer[i] = in_buffers[i].get();
  }
  has_nulls = in_has_nulls;
}

Status ShuffleOutputDesc::ResizeBufferNonNull(int num_new_rows) {
  int64_t new_num_rows = offset + num_new_rows;
  int64_t old_size = BitUtil::BytesForBits(offset);
  int64_t new_size = BitUtil::BytesForBits(new_num_rows);
  RETURN_NOT_OK(buffer[0]->Resize(new_size, false));
  uint8_t* data = buffer[0]->mutable_data();
  if (!has_nulls) {
    memset(data, 0xff, BitUtil::BytesForBits(offset));
  }
  if (offset % 8 > 0) {
    data[old_size - 1] |= static_cast<uint8_t>(0xff << (offset % 8));
  }
  memset(data + old_size, 0xff, new_size - old_size);
  return Status::OK();
}

Status ShuffleOutputDesc::ResizeBufferFixedLen(
    int num_new_rows, const KeyEncoder::KeyColumnMetadata& metadata) {
  int64_t new_num_rows = offset + num_new_rows;
  int64_t new_size =
      metadata.is_fixed_length
          ? (metadata.fixed_length == 0 ? BitUtil::BytesForBits(new_num_rows)
                                        : new_num_rows * metadata.fixed_length)
          : (new_num_rows + 1) * sizeof(uint32_t);
  RETURN_NOT_OK(buffer[1]->Resize(new_size, false));
  if (offset == 0 && offset == 0) {
    reinterpret_cast<uint32_t*>(buffer[1]->mutable_data())[0] = 0;
  }
  return Status::OK();
}

Status ShuffleOutputDesc::ResizeBufferVarLen(int num_new_rows) {
  const uint32_t* offsets = reinterpret_cast<const uint32_t*>(buffer[1]->mutable_data());
  int64_t new_num_rows = offset + num_new_rows;
  constexpr int64_t extra_padding_for_data_move = sizeof(uint64_t);
  RETURN_NOT_OK(
      buffer[2]->Resize(offsets[new_num_rows] + extra_padding_for_data_move, false));
  return Status::OK();
}

BatchShuffle::ShuffleInputDesc::ShuffleInputDesc(
    const uint8_t* non_null_buf, const uint8_t* fixed_len_buf, const uint8_t* var_len_buf,
    int in_num_rows, int in_start_row, const uint16_t* in_opt_row_ids,
    const KeyEncoder::KeyColumnMetadata& in_metadata)
    : num_rows(in_num_rows),
      opt_row_ids(in_opt_row_ids),
      offset(in_start_row),
      metadata(in_metadata.is_fixed_length, in_metadata.fixed_length) {
  buffer[0] = non_null_buf;
  buffer[1] = fixed_len_buf;
  buffer[2] = var_len_buf;
}

Status BatchShuffle::ShuffleNull(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                                 Shuffle_ThreadLocal& ctx, bool* out_has_nulls) {
  uint8_t* dst = output.buffer[0]->mutable_data();
  const uint8_t* src = input.buffer[0];

  *out_has_nulls = output.has_nulls;
  if (!output.has_nulls && !src) {
    return Status::OK();
  }

  auto temp_bytes_buf =
      util::TempVectorHolder<uint8_t>(ctx.temp_stack, ctx.minibatch_size);
  uint8_t* temp_bytes = temp_bytes_buf.mutable_data();

  bool output_buffer_resized = false;

  for (int start = 0; start < input.num_rows; start += ctx.minibatch_size) {
    int mini_batch_size = std::min(input.num_rows - start, ctx.minibatch_size);
    uint8_t byte_and = 0xff;
    if (input.opt_row_ids) {
      for (int i = 0; i < mini_batch_size; ++i) {
        uint8_t next_byte =
            BitUtil::GetBit(src, input.offset + input.opt_row_ids[start + i]) ? 0xFF
                                                                              : 0x00;
        temp_bytes[i] = next_byte;
        byte_and &= next_byte;
      }
    } else {
      util::BitUtil::bits_to_bytes(ctx.hardware_flags, mini_batch_size, src, temp_bytes,
                                   static_cast<int>(input.offset + start));
    }
    if (byte_and == 0) {
      *out_has_nulls = true;
      if (!output_buffer_resized) {
        RETURN_NOT_OK(output.ResizeBufferNonNull(input.num_rows));
        output_buffer_resized = true;
      }
      util::BitUtil::bytes_to_bits(ctx.hardware_flags, mini_batch_size, temp_bytes, dst,
                                   static_cast<int>(output.offset + start));
    }
  }

  if (!output_buffer_resized && output.has_nulls) {
    RETURN_NOT_OK(output.ResizeBufferNonNull(input.num_rows));
    output_buffer_resized = true;
  }

  return Status::OK();
}

void BatchShuffle::ShuffleBit(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                              Shuffle_ThreadLocal& ctx) {
  uint8_t* dst = output.buffer[1]->mutable_data();
  const uint8_t* src = input.buffer[1];

  auto temp_bytes_buf =
      util::TempVectorHolder<uint8_t>(ctx.temp_stack, ctx.minibatch_size);
  uint8_t* temp_bytes = temp_bytes_buf.mutable_data();

  for (int start = 0; start < input.num_rows; start += ctx.minibatch_size) {
    int mini_batch_size = std::min(input.num_rows - start, ctx.minibatch_size);
    if (input.opt_row_ids) {
      for (int i = 0; i < mini_batch_size; ++i) {
        temp_bytes[i] = BitUtil::GetBit(src, input.offset + input.opt_row_ids[start + i])
                            ? 0xFF
                            : 0x00;
      }
    } else {
      util::BitUtil::bits_to_bytes(ctx.hardware_flags, mini_batch_size, src, temp_bytes,
                                   static_cast<int>(input.offset + start));
    }
    util::BitUtil::bytes_to_bits(ctx.hardware_flags, mini_batch_size, temp_bytes, dst,
                                 static_cast<int>(output.offset + start));
  }
}

template <typename T>
void BatchShuffle::ShuffleInteger(ShuffleOutputDesc& output,
                                  const ShuffleInputDesc& input) {
  T* dst = reinterpret_cast<T*>(output.buffer[1]->mutable_data());
  const T* src = reinterpret_cast<const T*>(input.buffer[1]);
  dst += output.offset;
  src += input.offset;
  if (input.opt_row_ids) {
    for (int i = 0; i < input.num_rows; ++i) {
      dst[i] = src[input.opt_row_ids[i]];
    }
  } else {
    memcpy(dst, src, input.num_rows * sizeof(T));
  }
}

void BatchShuffle::ShuffleBinary(ShuffleOutputDesc& output,
                                 const ShuffleInputDesc& input) {
  uint8_t* dst = output.buffer[1]->mutable_data();
  const uint8_t* src = input.buffer[1];
  int binary_width = static_cast<int>(input.metadata.fixed_length);
  dst += binary_width * output.offset;
  src += binary_width * input.offset;
  if (input.opt_row_ids) {
    if (binary_width % sizeof(uint64_t) == 0) {
      uint64_t* dst64 = reinterpret_cast<uint64_t*>(dst);
      const uint64_t* src64 = reinterpret_cast<const uint64_t*>(src);
      int num_words = binary_width / sizeof(uint64_t);
      for (int i = 0; i < input.num_rows; ++i) {
        for (int word = 0; word < num_words; ++word) {
          dst64[i * num_words + word] = src64[input.opt_row_ids[i] * num_words + word];
        }
      }
    } else {
      for (int i = 0; i < input.num_rows; ++i) {
        memcpy(dst + i * binary_width, src + input.opt_row_ids[i] * binary_width,
               binary_width);
      }
    }
  } else {
    memcpy(dst, src, binary_width * input.num_rows);
  }
}

void BatchShuffle::ShuffleOffset(ShuffleOutputDesc& output,
                                 const ShuffleInputDesc& input) {
  uint32_t* dst = reinterpret_cast<uint32_t*>(output.buffer[1]->mutable_data());
  if (output.offset == 0) {
    dst[0] = 0;
  }
  const uint32_t* src = reinterpret_cast<const uint32_t*>(input.buffer[1]);
  dst += output.offset;
  src += input.offset;

  if (input.opt_row_ids) {
    uint32_t last_dst_offset = dst[0];
    for (int i = 0; i < input.num_rows; ++i) {
      int src_pos = input.opt_row_ids[i];
      last_dst_offset += src[src_pos + 1] - src[src_pos];
      dst[i + 1] = last_dst_offset;
    }
  } else {
    int delta = dst[0] - src[0];
    for (int i = 0; i < input.num_rows; ++i) {
      dst[i + 1] = static_cast<uint32_t>(static_cast<int>(src[i + 1]) + delta);
    }
  }
}

void BatchShuffle::ShuffleVarBinary(ShuffleOutputDesc& output,
                                    const ShuffleInputDesc& input) {
  uint8_t* dst = output.buffer[2]->mutable_data();
  const uint8_t* src = input.buffer[2];
  uint32_t* dst_offsets = reinterpret_cast<uint32_t*>(output.buffer[1]->mutable_data());
  const uint32_t* src_offsets = reinterpret_cast<const uint32_t*>(input.buffer[1]);
  dst_offsets += output.offset;
  src_offsets += input.offset;

  if (input.opt_row_ids) {
    for (int i = 0; i < input.num_rows; ++i) {
      memcpy(dst + dst_offsets[i], src + src_offsets[input.opt_row_ids[i]],
             dst_offsets[i + 1] - dst_offsets[i]);
    }
  } else {
    memcpy(dst + dst_offsets[0], src + src_offsets[0],
           dst_offsets[input.num_rows] - dst_offsets[0]);
  }
}

Status BatchShuffle::Shuffle(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                             Shuffle_ThreadLocal& ctx, bool* out_has_nulls) {
  if (input.num_rows == 0) {
    return Status::OK();
  }
  RETURN_NOT_OK(output.ResizeBufferFixedLen(input.num_rows, input.metadata));
  if (!input.metadata.is_fixed_length) {
    ShuffleOffset(output, input);
    RETURN_NOT_OK(output.ResizeBufferVarLen(input.num_rows));
    ShuffleVarBinary(output, input);
  } else {
    switch (input.metadata.fixed_length) {
      case 0:
        ShuffleBit(output, input, ctx);
        break;
      case 1:
        ShuffleInteger<uint8_t>(output, input);
        break;
      case 2:
        ShuffleInteger<uint16_t>(output, input);
        break;
      case 4:
        ShuffleInteger<uint32_t>(output, input);
        break;
      case 8:
        ShuffleInteger<uint64_t>(output, input);
        break;
      default:
        ShuffleBinary(output, input);
        break;
    }
  }
  RETURN_NOT_OK(ShuffleNull(output, input, ctx, out_has_nulls));
  return Status::OK();
}

KeyRowArrayShuffle::ShuffleInputDesc::ShuffleInputDesc(
    const KeyEncoder::KeyRowArray& in_rows, int in_column_id, int in_num_rows,
    const key_id_type* in_row_ids)
    : rows(&in_rows),
      column_id(in_column_id),
      num_rows(in_num_rows),
      row_ids(in_row_ids) {
  const KeyEncoder::KeyRowMetadata& row_metadata = rows->metadata();
  int column_id_after_reordering = -1;
  for (uint32_t i = 0; i < row_metadata.num_cols(); ++i) {
    if (row_metadata.encoded_field_order(i) == static_cast<uint32_t>(column_id)) {
      column_id_after_reordering = static_cast<int>(i);
      break;
    }
  }
  DCHECK_GE(column_id_after_reordering, 0);
  null_bit_id = column_id_after_reordering;
  offset_within_row = row_metadata.encoded_field_offset(column_id_after_reordering);
  metadata = row_metadata.column_metadatas[column_id];
  if (!metadata.is_fixed_length) {
    int delta = static_cast<int>(offset_within_row) -
                static_cast<int>(row_metadata.varbinary_end_array_offset);
    DCHECK_GE(delta, 0);
    DCHECK(delta % sizeof(uint32_t) == 0);
    varbinary_id = delta / sizeof(uint32_t);
  } else {
    varbinary_id = -1;
  }
}

Status KeyRowArrayShuffle::ShuffleNull(ShuffleOutputDesc& output,
                                       const ShuffleInputDesc& input,
                                       Shuffle_ThreadLocal& ctx, bool* out_has_nulls) {
  KeyEncoder::KeyEncoderContext encoder_ctx;
  encoder_ctx.hardware_flags = ctx.hardware_flags;
  encoder_ctx.stack = ctx.temp_stack;
  bool input_has_nulls = input.rows->has_any_nulls(&encoder_ctx);
  bool output_has_nulls = output.has_nulls;
  *out_has_nulls = output.has_nulls;
  if (!input_has_nulls && !output_has_nulls) {
    return Status::OK();
  }

  // Allocate temporary buffers for mini batch of elements
  //
  auto temp_bytes_buf =
      util::TempVectorHolder<uint8_t>(ctx.temp_stack, ctx.minibatch_size);
  uint8_t* temp_bytes = temp_bytes_buf.mutable_data();

  // Prepare metadata
  //
  const uint8_t* null_masks = input.rows->null_masks();
  int null_masks_bytes_per_row = input.rows->metadata().null_masks_bytes_per_row;
  int null_bit_id = input.null_bit_id;

  bool output_buffer_resized = false;

  // Split input into mini batches
  //
  for (int start = 0; start < input.num_rows; start += ctx.minibatch_size) {
    int batch_size = std::min(input.num_rows - start, ctx.minibatch_size);
    uint8_t byte_and = 0xff;
    for (int i = 0; i < batch_size; ++i) {
      int64_t row_id = input.row_ids[start + i];
      uint8_t next_byte =
          BitUtil::GetBit(null_masks, row_id * null_masks_bytes_per_row * 8 + null_bit_id)
              ? 0
              : 0xff;
      temp_bytes[i] = next_byte;
      byte_and &= next_byte;
    }
    if (byte_and == 0) {
      *out_has_nulls = true;
      if (!output_buffer_resized) {
        RETURN_NOT_OK(output.ResizeBufferNonNull(input.num_rows));
        output_buffer_resized = true;
      }
      util::BitUtil::bytes_to_bits(ctx.hardware_flags, batch_size, temp_bytes,
                                   output.buffer[0]->mutable_data(),
                                   static_cast<int>(output.offset + start));
    }
  }

  if (!output_buffer_resized && output.has_nulls) {
    RETURN_NOT_OK(output.ResizeBufferNonNull(input.num_rows));
    output_buffer_resized = true;
  }

  return Status::OK();
}

void KeyRowArrayShuffle::ShuffleBit(ShuffleOutputDesc& output,
                                    const ShuffleInputDesc& input,
                                    Shuffle_ThreadLocal& ctx) {
  auto metadata = input.rows->metadata();
  uint32_t offset_within_row = input.offset_within_row;

  auto temp_bytes_buf =
      util::TempVectorHolder<uint8_t>(ctx.temp_stack, ctx.minibatch_size);
  uint8_t* temp_bytes = temp_bytes_buf.mutable_data();

  // Split input into mini batches
  //
  for (int start = 0; start < input.num_rows; start += ctx.minibatch_size) {
    int batch_size = std::min(input.num_rows - start, ctx.minibatch_size);
    if (metadata.is_fixed_length) {
      const uint8_t* src = input.rows->data(1) + offset_within_row;
      for (int i = 0; i < batch_size; ++i) {
        temp_bytes[i] =
            *(src + input.row_ids[start + i] * metadata.fixed_length) == 0 ? 0 : 0xff;
      }
    } else {
      const uint32_t* offsets = input.rows->offsets();
      const uint8_t* src = input.rows->data(2) + offset_within_row;
      for (int i = 0; i < batch_size; ++i) {
        temp_bytes[i] = *(src + offsets[input.row_ids[start + i]]) == 0 ? 0 : 0xff;
      }
    }
    util::BitUtil::bytes_to_bits(ctx.hardware_flags, batch_size, temp_bytes,
                                 output.buffer[1]->mutable_data(),
                                 static_cast<int>(output.offset + start));
  }
}

template <typename T>
void KeyRowArrayShuffle::ShuffleInteger(ShuffleOutputDesc& output,
                                        const ShuffleInputDesc& input) {
  auto metadata = input.rows->metadata();
  uint32_t offset_within_row = input.offset_within_row;

  T* dst = reinterpret_cast<T*>(output.buffer[1]->mutable_data());
  if (metadata.is_fixed_length) {
    const uint8_t* src = input.rows->data(1) + offset_within_row;
    for (int i = 0; i < input.num_rows; ++i) {
      dst[output.offset + i] =
          *reinterpret_cast<const T*>(src + input.row_ids[i] * metadata.fixed_length);
    }
  } else {
    const uint32_t* offsets = input.rows->offsets();
    const uint8_t* src = input.rows->data(2) + offset_within_row;
    for (int i = 0; i < input.num_rows; ++i) {
      dst[output.offset + i] =
          *reinterpret_cast<const T*>(src + offsets[input.row_ids[i]]);
    }
  }
}

void KeyRowArrayShuffle::ShuffleBinary(ShuffleOutputDesc& output,
                                       const ShuffleInputDesc& input) {
  auto metadata = input.rows->metadata();
  auto column_metadata = input.metadata;
  uint32_t offset_within_row = input.offset_within_row;

  uint8_t* dst = output.buffer[1]->mutable_data();
  const uint8_t* src = input.rows->data(1) + offset_within_row;

  if (column_metadata.fixed_length % sizeof(uint64_t) == 0) {
    int num_words = column_metadata.fixed_length / sizeof(uint64_t);
    if (metadata.is_fixed_length) {
      for (int i = 0; i < input.num_rows; ++i) {
        for (int word = 0; word < num_words; ++word) {
          reinterpret_cast<uint64_t*>(dst)[(output.offset + i) * num_words + word] =
              reinterpret_cast<const uint64_t*>(src + input.row_ids[i] *
                                                          metadata.fixed_length)[word];
        }
      }
    } else {
      const uint32_t* offsets = input.rows->offsets();
      for (int i = 0; i < input.num_rows; ++i) {
        for (int word = 0; word < num_words; ++word) {
          reinterpret_cast<uint64_t*>(dst)[(output.offset + i) * num_words + word] =
              reinterpret_cast<const uint64_t*>(src + offsets[input.row_ids[i]])[word];
        }
      }
    }
  } else {
    if (metadata.is_fixed_length) {
      for (int i = 0; i < input.num_rows; ++i) {
        memcpy(dst + (output.offset + i) * column_metadata.fixed_length,
               src + input.row_ids[i] * metadata.fixed_length,
               column_metadata.fixed_length);
      }
    } else {
      const uint32_t* offsets = input.rows->offsets();
      for (int i = 0; i < input.num_rows; ++i) {
        memcpy(dst + (output.offset + i) * column_metadata.fixed_length,
               src + offsets[input.row_ids[i]], column_metadata.fixed_length);
      }
    }
  }
}

void KeyRowArrayShuffle::ShuffleOffset(ShuffleOutputDesc& output,
                                       const ShuffleInputDesc& input) {
  auto metadata = input.rows->metadata();
  int varbinary_id = input.varbinary_id;
  uint32_t offset_within_row = input.offset_within_row;

  uint32_t* dst = reinterpret_cast<uint32_t*>(output.buffer[1]->mutable_data());
  if (output.offset == 0) {
    dst[0] = 0;
  }
  const uint8_t* src = input.rows->data(1) + offset_within_row;
  const uint32_t* offsets = input.rows->offsets();

  if (varbinary_id == 0) {
    int prev_value = dst[output.offset];
    for (int i = 0; i < input.num_rows; ++i) {
      prev_value += *reinterpret_cast<const uint32_t*>(src + offsets[input.row_ids[i]]);
      dst[output.offset + i + 1] = prev_value;
    }
  } else {
    int prev_value = dst[output.offset];
    for (int i = 0; i < input.num_rows; ++i) {
      const uint32_t* varbinary_end =
          reinterpret_cast<const uint32_t*>(src + offsets[input.row_ids[i]]);
      prev_value += varbinary_end[0] - varbinary_end[-1];
      dst[output.offset + i + 1] = prev_value;
    }
  }
}

void KeyRowArrayShuffle::ShuffleVarBinary(ShuffleOutputDesc& output,
                                          const ShuffleInputDesc& input) {
  auto metadata = input.rows->metadata();
  int varbinary_id = input.varbinary_id;

  uint8_t* dst = output.buffer[2]->mutable_data();
  const uint32_t* dst_offsets =
      reinterpret_cast<const uint32_t*>(output.buffer[1]->data());
  const uint8_t* src = input.rows->data(2);
  const uint32_t* src_offsets = input.rows->offsets();

  if (varbinary_id == 0) {
    for (int i = 0; i < input.num_rows; ++i) {
      const uint8_t* src_row = src + src_offsets[input.row_ids[i]];
      uint32_t offset_within_row;
      uint32_t length;
      metadata.first_varbinary_offset_and_length(src_row, &offset_within_row, &length);
      src_row += offset_within_row;
      int64_t num_words = BitUtil::CeilDiv(length, sizeof(uint64_t));
      uint8_t* dst_row = dst + dst_offsets[output.offset + i];
      for (int64_t word = 0; word < num_words; ++word) {
        util::SafeStore(dst_row + word * sizeof(uint64_t),
                        reinterpret_cast<const uint64_t*>(src_row)[word]);
      }
    }
  } else {
    for (int i = 0; i < input.num_rows; ++i) {
      const uint8_t* src_row = src + src_offsets[input.row_ids[i]];
      uint32_t offset_within_row;
      uint32_t length;
      metadata.nth_varbinary_offset_and_length(src_row, varbinary_id, &offset_within_row,
                                               &length);
      src_row += offset_within_row;
      int64_t num_words = BitUtil::CeilDiv(length, sizeof(uint64_t));
      uint8_t* dst_row = dst + dst_offsets[output.offset + i];
      for (int64_t word = 0; word < num_words; ++word) {
        util::SafeStore(dst_row + word * sizeof(uint64_t),
                        reinterpret_cast<const uint64_t*>(src_row)[word]);
      }
    }
  }
}

Status KeyRowArrayShuffle::Shuffle(ShuffleOutputDesc& output,
                                   const ShuffleInputDesc& input,
                                   Shuffle_ThreadLocal& ctx, bool* out_has_nulls) {
  if (input.num_rows == 0) {
    return Status::OK();
  }
  RETURN_NOT_OK(output.ResizeBufferFixedLen(input.num_rows, input.metadata));
  if (!input.metadata.is_fixed_length) {
    ShuffleOffset(output, input);
    RETURN_NOT_OK(output.ResizeBufferVarLen(input.num_rows));
    ShuffleVarBinary(output, input);
  } else {
    switch (input.metadata.fixed_length) {
      case 0:
        ShuffleBit(output, input, ctx);
        break;
      case 1:
        ShuffleInteger<uint8_t>(output, input);
        break;
      case 2:
        ShuffleInteger<uint16_t>(output, input);
        break;
      case 4:
        ShuffleInteger<uint32_t>(output, input);
        break;
      case 8:
        ShuffleInteger<uint64_t>(output, input);
        break;
      default:
        ShuffleBinary(output, input);
        break;
    }
  }
  RETURN_NOT_OK(ShuffleNull(output, input, ctx, out_has_nulls));
  return Status::OK();
}

BatchWithJoinData::BatchWithJoinData(int in_join_side, const ExecBatch& in_batch) {
  join_side = in_join_side;
  batch = in_batch;
  hashes = nullptr;
}

Status BatchWithJoinData::ComputeHashIfMissing(MemoryPool* pool,
                                               JoinColumnMapper* schema_mgr,
                                               JoinHashTable_ThreadLocal* locals) {
  if (!hashes) {
    ARROW_ASSIGN_OR_RAISE(
        hashes, AllocateResizableBuffer(sizeof(hash_type) * batch.length, pool));
    hash_type* hash_values = reinterpret_cast<hash_type*>(hashes->mutable_data());

    // Encode
    RETURN_NOT_OK(Encode(
        0, batch.length, locals->key_encoder, locals->keys_minibatch, schema_mgr,
        join_side == 0 ? JoinSchemaHandle::FIRST_INPUT : JoinSchemaHandle::SECOND_INPUT,
        join_side == 0 ? JoinSchemaHandle::FIRST_KEY : JoinSchemaHandle::SECOND_KEY));

    if (locals->key_encoder.row_metadata().is_fixed_length) {
      ::arrow::compute::Hashing::hash_fixed(
          locals->encoder_ctx.hardware_flags, static_cast<uint32_t>(batch.length),
          locals->key_encoder.row_metadata().fixed_length, locals->keys_minibatch.data(1),
          hash_values);
    } else {
      auto hash_temp_buf = ::arrow::util::TempVectorHolder<uint32_t>(
          &locals->stack, 4 * locals->minibatch_size);

      for (int64_t start = 0; start < batch.length; start += locals->minibatch_size) {
        int next_batch_size =
            std::min(static_cast<int>(batch.length - start), locals->minibatch_size);

        ::arrow::compute::Hashing::hash_varlen(
            locals->encoder_ctx.hardware_flags, next_batch_size,
            locals->keys_minibatch.offsets() + start, locals->keys_minibatch.data(2),
            hash_temp_buf.mutable_data(), hash_values + start);
      }
    }
  }
  return Status::OK();
}

Status BatchWithJoinData::Encode(int64_t start_row, int64_t num_rows, KeyEncoder& encoder,
                                 KeyEncoder::KeyRowArray& rows,
                                 JoinColumnMapper* schema_mgr,
                                 JoinSchemaHandle batch_schema,
                                 JoinSchemaHandle output_schema) const {
  int num_output_cols = schema_mgr->num_cols(output_schema);
  const int* col_map = schema_mgr->map(output_schema, batch_schema);
  std::vector<KeyEncoder::KeyColumnArray> temp_cols(num_output_cols);

  for (int output_col = 0; output_col < num_output_cols; ++output_col) {
    int input_col = col_map[output_col];
    KeyEncoder::KeyColumnMetadata col_metadata =
        schema_mgr->data_type(output_schema, output_col);
    const uint8_t* non_nulls = nullptr;
    if (batch[input_col].array()->buffers[0] != NULLPTR) {
      non_nulls = batch[input_col].array()->buffers[0]->data();
    }
    const uint8_t* fixedlen = batch[input_col].array()->buffers[1]->data();
    const uint8_t* varlen = nullptr;
    if (!col_metadata.is_fixed_length) {
      varlen = batch[input_col].array()->buffers[2]->data();
    }
    int64_t offset = batch[input_col].array()->offset;
    auto col_base = arrow::compute::KeyEncoder::KeyColumnArray(
        col_metadata, offset + start_row + num_rows, non_nulls, fixedlen, varlen);
    temp_cols[output_col] = arrow::compute::KeyEncoder::KeyColumnArray(
        col_base, offset + start_row, num_rows);
  }

  rows.Clean();
  RETURN_NOT_OK(encoder.PrepareOutputForEncode(0, num_rows, &rows, temp_cols));
  encoder.Encode(0, num_rows, &rows, temp_cols);

  return Status::OK();
}

Status BatchAccumulation::Init(JoinSchemaHandle schema, JoinColumnMapper* schema_mgr,
                               int64_t max_batch_size, MemoryPool* pool) {
  pool_ = pool;
  schema_ = schema;
  max_batch_size_ = max_batch_size;
  schema_mgr_ = schema_mgr;

  RETURN_NOT_OK(AllocateEmptyBuffers());
  return Status::OK();
}

ShuffleOutputDesc BatchAccumulation::GetColumn(int column_id) {
  return ShuffleOutputDesc(output_buffers_[column_id], output_length_,
                           output_buffer_has_nulls_[column_id]);
}

Result<std::unique_ptr<ExecBatch>> BatchAccumulation::MakeBatch() {
  std::unique_ptr<ExecBatch> out = ::arrow::internal::make_unique<ExecBatch>();
  out->length = output_length_;
  int num_columns = schema_mgr_->num_cols(schema_);
  out->values.resize(num_columns);
  for (int i = 0; i < num_columns; ++i) {
    auto field = schema_mgr_->field(schema_, i);
    int null_count = 0;
    if (output_buffer_has_nulls_[i]) {
      auto valid_count = arrow::internal::CountSetBits(
          output_buffers_[i][0]->data(), /*offset=*/0, static_cast<int>(output_length_));
      null_count = static_cast<int>(output_length_) - static_cast<int>(valid_count);
    }

    if (field->data_type.is_fixed_length) {
      if (null_count > 0) {
        out->values[i] = ArrayData::Make(
            field->full_data_type, output_length_,
            {std::move(output_buffers_[i][0]), std::move(output_buffers_[i][1])},
            null_count);
      } else {
        out->values[i] = ArrayData::Make(field->full_data_type, output_length_,
                                         {nullptr, std::move(output_buffers_[i][1])}, 0);
      }
    } else {
      if (null_count > 0) {
        out->values[i] = ArrayData::Make(
            field->full_data_type, output_length_,
            {std::move(output_buffers_[i][0]), std::move(output_buffers_[i][1]),
             std::move(output_buffers_[i][2])},
            null_count);
      } else {
        out->values[i] = ArrayData::Make(
            field->full_data_type, output_length_,
            {nullptr, std::move(output_buffers_[i][1]), std::move(output_buffers_[i][2])},
            0);
      }
    }
  }

  RETURN_NOT_OK(AllocateEmptyBuffers());

  return out;
}

Result<std::unique_ptr<BatchWithJoinData>> BatchAccumulation::MakeBatchWithJoinData() {
  std::unique_ptr<BatchWithJoinData> out =
      ::arrow::internal::make_unique<BatchWithJoinData>();
  std::unique_ptr<ExecBatch> out_batch;
  if (has_hashes_) {
    out->hashes = std::move(hashes_);
  } else {
    out->hashes = nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(out_batch, MakeBatch());
  out->batch = *(out_batch.release());
  return out;
}

Status BatchAccumulation::AllocateEmptyBuffers() {
  output_length_ = 0;
  int num_columns = schema_mgr_->num_cols(schema_);
  output_buffers_.resize(num_columns);
  output_buffer_has_nulls_.resize(num_columns);
  for (int i = 0; i < num_columns; ++i) {
    output_buffers_[i].resize(3);
    output_buffer_has_nulls_[i] = false;
    ARROW_ASSIGN_OR_RAISE(output_buffers_[i][0], AllocateResizableBuffer(0, pool_));
    ARROW_ASSIGN_OR_RAISE(output_buffers_[i][1], AllocateResizableBuffer(0, pool_));
    ARROW_ASSIGN_OR_RAISE(output_buffers_[i][2], AllocateResizableBuffer(0, pool_));
  }
  if (!hashes_) {
    ARROW_ASSIGN_OR_RAISE(hashes_, AllocateResizableBuffer(0, pool_));
  }
  has_hashes_ = false;
  return Status::OK();
}

Status BatchJoinAssembler::Init(MemoryPool* pool, int64_t max_batch_size,
                                JoinColumnMapper* schema_mgr) {
  schema_mgr_ = schema_mgr;
  RETURN_NOT_OK(
      output_buffers_.Init(JoinSchemaHandle::OUTPUT, schema_mgr, max_batch_size, pool));
  bound_batch_side_ = 0;
  bound_hash_table_side_ = 0;
  bound_batch_ = nullptr;
  bound_keys_ = nullptr;
  bound_payload_ = nullptr;
  return Status::OK();
}

void BatchJoinAssembler::BindSourceBatch(int side, const ExecBatch* batch) {
  bound_batch_side_ = side;
  bound_batch_ = batch;
}

void BatchJoinAssembler::BindSourceHashTable(int side,
                                             const KeyEncoder::KeyRowArray* keys,
                                             const KeyEncoder::KeyRowArray* payload) {
  bound_hash_table_side_ = side;
  bound_keys_ = keys;
  bound_payload_ = payload;
}

Result<std::unique_ptr<ExecBatch>> BatchJoinAssembler::Push(
    Shuffle_ThreadLocal& ctx, int num_rows, int hash_table_side, bool is_batch_present,
    bool is_hash_table_present, int batch_start_row, const uint16_t* opt_batch_row_ids,
    const key_id_type* opt_key_ids, const key_id_type* opt_payload_ids,
    int* out_num_rows_processed) {
  int num_rows_clamped =
      std::min(num_rows, static_cast<int>(output_buffers_.space_left()));
  *out_num_rows_processed = num_rows_clamped;

  // For each output batch column find the input column which could be either in an
  // input batch or in a hash table.
  //
  int num_output_columns = schema_mgr_->num_cols(JoinSchemaHandle::OUTPUT);
  for (int i = 0; i < num_output_columns; ++i) {
    // 0 - input batch, 1 - hash table key, 2 - hash table payload
    int source, column_id;
    column_id = schema_mgr_->map(JoinSchemaHandle::OUTPUT,
                                 hash_table_side == 0 ? JoinSchemaHandle::FIRST_KEY
                                                      : JoinSchemaHandle::SECOND_KEY)[i];
    if (column_id != schema_mgr_->kMissingField) {
      source = 1;
    } else {
      column_id =
          schema_mgr_->map(JoinSchemaHandle::OUTPUT,
                           hash_table_side == 0 ? JoinSchemaHandle::FIRST_PAYLOAD
                                                : JoinSchemaHandle::SECOND_PAYLOAD)[i];
      if (column_id != schema_mgr_->kMissingField) {
        source = 2;
      } else {
        column_id =
            schema_mgr_->map(JoinSchemaHandle::OUTPUT,
                             hash_table_side == 0 ? JoinSchemaHandle::SECOND_INPUT
                                                  : JoinSchemaHandle::FIRST_INPUT)[i];
        DCHECK_NE(column_id, schema_mgr_->kMissingField);
        source = 0;
      }
    }
    // Switch key from hash table to input batch if input batch is present.
    //
    if (source == 1 && is_batch_present) {
      source = 0;
      if (hash_table_side == 0) {
        column_id = schema_mgr_->map(JoinSchemaHandle::SECOND_KEY,
                                     JoinSchemaHandle::SECOND_INPUT)[column_id];
      } else {
        column_id = schema_mgr_->map(JoinSchemaHandle::FIRST_KEY,
                                     JoinSchemaHandle::FIRST_INPUT)[column_id];
      }
    }
    // Construct output descriptor
    //
    ShuffleOutputDesc output = output_buffers_.GetColumn(i);
    KeyEncoder::KeyColumnMetadata column_metadata =
        schema_mgr_->data_type(JoinSchemaHandle::OUTPUT, i);

    // Find whether the input source is missing, which
    // means outputting nulls, or present, which means copying values.
    //
    if ((source == 0 && !is_batch_present) || (source == 1 && !is_hash_table_present) ||
        (source == 2 && !is_hash_table_present)) {
      RETURN_NOT_OK(AppendNulls(output, column_metadata, num_rows));
    } else {
      // Construct input descriptor and perform appropriate shuffle
      //
      if (source == 0) {
        DCHECK(is_batch_present);
        DCHECK(bound_batch_);
        DCHECK(bound_batch_side_ == 1 - hash_table_side);
        auto array = bound_batch_->values[column_id].array();
        BatchShuffle::ShuffleInputDesc input(
            array->buffers[0]->data(), array->buffers[1]->data(),
            array->buffers[2]->data(), num_rows, batch_start_row, opt_batch_row_ids,
            column_metadata);
        bool out_has_nulls;
        RETURN_NOT_OK(BatchShuffle::Shuffle(output, input, ctx, &out_has_nulls));
        if (out_has_nulls) {
          output_buffers_.SetHasNulls(i);
        }
      } else {
        DCHECK(is_hash_table_present);
        DCHECK((source == 1 && bound_keys_) || (source == 2 && bound_payload_));
        DCHECK(bound_hash_table_side_ == hash_table_side);
        const KeyEncoder::KeyRowArray& rows =
            source == 1 ? *bound_keys_ : *bound_payload_;
        KeyRowArrayShuffle::ShuffleInputDesc input(
            rows, column_id, num_rows, source == 1 ? opt_key_ids : opt_payload_ids);
        bool out_has_nulls;
        RETURN_NOT_OK(KeyRowArrayShuffle::Shuffle(output, input, ctx, &out_has_nulls));
        if (out_has_nulls) {
          output_buffers_.SetHasNulls(i);
        }
      }
    }
  }

  output_buffers_.IncreaseLength(num_rows_clamped);

  if (output_buffers_.space_left() == 0) {
    return output_buffers_.MakeBatch();
  }
  return Status::OK();
}

Result<std::unique_ptr<ExecBatch>> BatchJoinAssembler::Flush() {
  if (!output_buffers_.is_empty()) {
    return output_buffers_.MakeBatch();
  } else {
    return std::unique_ptr<ExecBatch>();
  }
}

Status BatchJoinAssembler::AppendNulls(ShuffleOutputDesc& output,
                                       const KeyEncoder::KeyColumnMetadata& metadata,
                                       int num_rows) {
  if (num_rows == 0) {
    return Status::OK();
  }
  RETURN_NOT_OK(output.ResizeBufferFixedLen(num_rows, metadata));
  if (!metadata.is_fixed_length) {
    uint32_t* offsets = reinterpret_cast<uint32_t*>(output.buffer[1]->mutable_data());
    offsets += output.offset;
    uint32_t value = offsets[0];
    for (int i = 0; i < num_rows; ++i) {
      offsets[i + 1] = value;
    }
  }
  RETURN_NOT_OK(output.ResizeBufferNonNull(num_rows));
  uint8_t* non_nulls = output.buffer[0]->mutable_data();
  int64_t old_size = BitUtil::BytesForBits(output.offset);
  if (output.offset % 8 > 0) {
    non_nulls[old_size - 1] &= static_cast<uint8_t>((1 << (output.offset % 8)) - 1);
  }
  memset(non_nulls + old_size, 0, BitUtil::BytesForBits(output.offset + num_rows));
  return Status::OK();
}

void BatchEarlyFilterEval::Init(int join_side, MemoryPool* pool,
                                JoinColumnMapper* schema_mgr) {
  side_ = join_side;
  pool_ = pool;
  schema_mgr_ = schema_mgr;
}

void BatchEarlyFilterEval::SetFilter(bool is_other_side_empty,
                                     const std::vector<bool>& null_field_means_no_match,
                                     const ApproximateMembershipTest* hash_based_filter) {
  is_other_side_empty_ = is_other_side_empty;
  null_field_means_no_match_.resize(null_field_means_no_match.size());
  for (size_t i = 0; i < null_field_means_no_match.size(); ++i) {
    null_field_means_no_match_[i] = null_field_means_no_match[i];
  }
  hash_based_filter_ = hash_based_filter;
}

Status BatchEarlyFilterEval::EvalFilter(BatchWithJoinData& batch, int64_t start_row,
                                        int64_t num_rows, uint8_t* filter_bit_vector,
                                        JoinHashTable_ThreadLocal* locals) {
  if (is_other_side_empty_) {
    memset(filter_bit_vector, 0, BitUtil::BytesForBits(num_rows));
    return Status::OK();
  }
  DCHECK(hash_based_filter_);
  RETURN_NOT_OK(batch.ComputeHashIfMissing(pool_, schema_mgr_, locals));

  auto byte_vector_buf =
      util::TempVectorHolder<uint8_t>(&locals->stack, locals->minibatch_size);
  auto byte_vector = byte_vector_buf.mutable_data();

  for (int64_t batch_start = 0; batch_start < num_rows; ++batch_start) {
    int next_batch_size =
        std::min(static_cast<int>(num_rows - batch_start), locals->minibatch_size);
    hash_based_filter_->MayHaveHash(
        locals->encoder_ctx.hardware_flags, next_batch_size,
        reinterpret_cast<const hash_type*>(batch.hashes->data()) + start_row +
            batch_start,
        byte_vector);
    util::BitUtil::bytes_to_bits(locals->encoder_ctx.hardware_flags, next_batch_size,
                                 byte_vector, filter_bit_vector,
                                 static_cast<int>(batch_start));
  }

  EvalNullFilter(batch.batch, start_row, num_rows, filter_bit_vector, locals);

  return Status::OK();
}

Result<std::unique_ptr<BatchWithJoinData>> BatchEarlyFilterEval::FilterBatch(
    const BatchWithJoinData& batch, int64_t start, int64_t num_rows,
    const uint8_t* filter_bit_vector, int* out_num_rows_processed) {
  // TODO: Not implemented yet
  return Status::OK();
}

Result<std::unique_ptr<ExecBatch>> BatchEarlyFilterEval::Flush() {
  if (!output_buffers_.is_empty()) {
    return output_buffers_.MakeBatch();
  } else {
    return std::unique_ptr<ExecBatch>();
  }
}

void BatchEarlyFilterEval::EvalNullFilter(const ExecBatch& batch, int64_t start_row,
                                          int64_t num_rows, uint8_t* bit_vector_to_update,
                                          JoinHashTable_ThreadLocal* locals) {
  JoinSchemaHandle batch_schema =
      side_ == 0 ? JoinSchemaHandle::FIRST_INPUT : JoinSchemaHandle::SECOND_INPUT;
  JoinSchemaHandle key_schema =
      side_ == 0 ? JoinSchemaHandle::FIRST_KEY : JoinSchemaHandle::SECOND_KEY;
  int num_key_columns = schema_mgr_->num_cols(key_schema);
  const int* column_map = schema_mgr_->map(key_schema, batch_schema);
  for (int column_id = 0; column_id < num_key_columns; ++column_id) {
    if (!null_field_means_no_match_[column_id]) {
      continue;
    }
    int batch_column_id = column_map[column_id];
    const uint8_t* non_nulls = nullptr;
    if (batch[batch_column_id].array()->buffers[0] != NULLPTR) {
      non_nulls = batch[batch_column_id].array()->buffers[0]->data();
    }
    if (!non_nulls) {
      continue;
    }

    int64_t offset = batch[batch_column_id].array()->offset + start_row;

    auto ids_buf =
        util::TempVectorHolder<uint16_t>(&locals->stack, locals->minibatch_size);
    auto ids = ids_buf.mutable_data();
    int num_ids;

    for (int64_t start_row_minibatch = 0; start_row_minibatch < num_rows;
         start_row_minibatch += locals->minibatch_size) {
      int next_batch_size = std::min(static_cast<int>(num_rows - start_row_minibatch),
                                     locals->minibatch_size);
      util::BitUtil::bits_to_indexes(0, locals->encoder_ctx.hardware_flags,
                                     next_batch_size, non_nulls, &num_ids, ids,
                                     static_cast<int>(offset + start_row_minibatch));
      for (int i = 0; i < num_ids; ++i) {
        BitUtil::ClearBit(bit_vector_to_update, start_row_minibatch + ids[i]);
      }
    }
  }
}

}  // namespace compute
}  // namespace arrow

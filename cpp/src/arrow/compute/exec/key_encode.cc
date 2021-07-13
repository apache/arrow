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

#include "arrow/compute/exec/key_encode.h"

#include <memory.h>

#include <algorithm>

#include "arrow/compute/exec/util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace compute {

KeyEncoder::KeyRowArray::KeyRowArray()
    : pool_(nullptr), rows_capacity_(0), bytes_capacity_(0) {}

Status KeyEncoder::KeyRowArray::Init(MemoryPool* pool, const KeyRowMetadata& metadata) {
  pool_ = pool;
  metadata_ = metadata;

  DCHECK(!null_masks_ && !offsets_ && !rows_);

  constexpr int64_t rows_capacity = 8;
  constexpr int64_t bytes_capacity = 1024;

  // Null masks
  ARROW_ASSIGN_OR_RAISE(auto null_masks,
                        AllocateResizableBuffer(size_null_masks(rows_capacity), pool_));
  null_masks_ = std::move(null_masks);
  memset(null_masks_->mutable_data(), 0, size_null_masks(rows_capacity));

  // Offsets and rows
  if (!metadata.is_fixed_length) {
    ARROW_ASSIGN_OR_RAISE(auto offsets,
                          AllocateResizableBuffer(size_offsets(rows_capacity), pool_));
    offsets_ = std::move(offsets);
    memset(offsets_->mutable_data(), 0, size_offsets(rows_capacity));
    reinterpret_cast<uint32_t*>(offsets_->mutable_data())[0] = 0;

    ARROW_ASSIGN_OR_RAISE(
        auto rows,
        AllocateResizableBuffer(size_rows_varying_length(bytes_capacity), pool_));
    rows_ = std::move(rows);
    memset(rows_->mutable_data(), 0, size_rows_varying_length(bytes_capacity));
    bytes_capacity_ = size_rows_varying_length(bytes_capacity) - padding_for_vectors;
  } else {
    ARROW_ASSIGN_OR_RAISE(
        auto rows, AllocateResizableBuffer(size_rows_fixed_length(rows_capacity), pool_));
    rows_ = std::move(rows);
    memset(rows_->mutable_data(), 0, size_rows_fixed_length(rows_capacity));
    bytes_capacity_ = size_rows_fixed_length(rows_capacity) - padding_for_vectors;
  }

  update_buffer_pointers();

  rows_capacity_ = rows_capacity;

  num_rows_ = 0;
  num_rows_for_has_any_nulls_ = 0;
  has_any_nulls_ = false;

  return Status::OK();
}

void KeyEncoder::KeyRowArray::Clean() {
  num_rows_ = 0;
  num_rows_for_has_any_nulls_ = 0;
  has_any_nulls_ = false;

  if (!metadata_.is_fixed_length) {
    reinterpret_cast<uint32_t*>(offsets_->mutable_data())[0] = 0;
  }
}

int64_t KeyEncoder::KeyRowArray::size_null_masks(int64_t num_rows) {
  return num_rows * metadata_.null_masks_bytes_per_row + padding_for_vectors;
}

int64_t KeyEncoder::KeyRowArray::size_offsets(int64_t num_rows) {
  return (num_rows + 1) * sizeof(uint32_t) + padding_for_vectors;
}

int64_t KeyEncoder::KeyRowArray::size_rows_fixed_length(int64_t num_rows) {
  return num_rows * metadata_.fixed_length + padding_for_vectors;
}

int64_t KeyEncoder::KeyRowArray::size_rows_varying_length(int64_t num_bytes) {
  return num_bytes + padding_for_vectors;
}

void KeyEncoder::KeyRowArray::update_buffer_pointers() {
  buffers_[0] = mutable_buffers_[0] = null_masks_->mutable_data();
  if (metadata_.is_fixed_length) {
    buffers_[1] = mutable_buffers_[1] = rows_->mutable_data();
    buffers_[2] = mutable_buffers_[2] = nullptr;
  } else {
    buffers_[1] = mutable_buffers_[1] = offsets_->mutable_data();
    buffers_[2] = mutable_buffers_[2] = rows_->mutable_data();
  }
}

Status KeyEncoder::KeyRowArray::ResizeFixedLengthBuffers(int64_t num_extra_rows) {
  if (rows_capacity_ >= num_rows_ + num_extra_rows) {
    return Status::OK();
  }

  int64_t rows_capacity_new = std::max(static_cast<int64_t>(1), 2 * rows_capacity_);
  while (rows_capacity_new < num_rows_ + num_extra_rows) {
    rows_capacity_new *= 2;
  }

  // Null masks
  RETURN_NOT_OK(null_masks_->Resize(size_null_masks(rows_capacity_new), false));
  memset(null_masks_->mutable_data() + size_null_masks(rows_capacity_), 0,
         size_null_masks(rows_capacity_new) - size_null_masks(rows_capacity_));

  // Either offsets or rows
  if (!metadata_.is_fixed_length) {
    RETURN_NOT_OK(offsets_->Resize(size_offsets(rows_capacity_new), false));
    memset(offsets_->mutable_data() + size_offsets(rows_capacity_), 0,
           size_offsets(rows_capacity_new) - size_offsets(rows_capacity_));
  } else {
    RETURN_NOT_OK(rows_->Resize(size_rows_fixed_length(rows_capacity_new), false));
    memset(rows_->mutable_data() + size_rows_fixed_length(rows_capacity_), 0,
           size_rows_fixed_length(rows_capacity_new) -
               size_rows_fixed_length(rows_capacity_));
    bytes_capacity_ = size_rows_fixed_length(rows_capacity_new) - padding_for_vectors;
  }

  update_buffer_pointers();

  rows_capacity_ = rows_capacity_new;

  return Status::OK();
}

Status KeyEncoder::KeyRowArray::ResizeOptionalVaryingLengthBuffer(
    int64_t num_extra_bytes) {
  int64_t num_bytes = offsets()[num_rows_];
  if (bytes_capacity_ >= num_bytes + num_extra_bytes || metadata_.is_fixed_length) {
    return Status::OK();
  }

  int64_t bytes_capacity_new = std::max(static_cast<int64_t>(1), 2 * bytes_capacity_);
  while (bytes_capacity_new < num_bytes + num_extra_bytes) {
    bytes_capacity_new *= 2;
  }

  RETURN_NOT_OK(rows_->Resize(size_rows_varying_length(bytes_capacity_new), false));
  memset(rows_->mutable_data() + size_rows_varying_length(bytes_capacity_), 0,
         size_rows_varying_length(bytes_capacity_new) -
             size_rows_varying_length(bytes_capacity_));

  update_buffer_pointers();

  bytes_capacity_ = bytes_capacity_new;

  return Status::OK();
}

Status KeyEncoder::KeyRowArray::AppendSelectionFrom(const KeyRowArray& from,
                                                    uint32_t num_rows_to_append,
                                                    const uint16_t* source_row_ids) {
  DCHECK(metadata_.is_compatible(from.metadata()));

  RETURN_NOT_OK(ResizeFixedLengthBuffers(num_rows_to_append));

  if (!metadata_.is_fixed_length) {
    // Varying-length rows
    auto from_offsets = reinterpret_cast<const uint32_t*>(from.offsets_->data());
    auto to_offsets = reinterpret_cast<uint32_t*>(offsets_->mutable_data());
    uint32_t total_length = to_offsets[num_rows_];
    uint32_t total_length_to_append = 0;
    for (uint32_t i = 0; i < num_rows_to_append; ++i) {
      uint16_t row_id = source_row_ids[i];
      uint32_t length = from_offsets[row_id + 1] - from_offsets[row_id];
      total_length_to_append += length;
      to_offsets[num_rows_ + i + 1] = total_length + total_length_to_append;
    }

    RETURN_NOT_OK(ResizeOptionalVaryingLengthBuffer(total_length_to_append));

    const uint8_t* src = from.rows_->data();
    uint8_t* dst = rows_->mutable_data() + total_length;
    for (uint32_t i = 0; i < num_rows_to_append; ++i) {
      uint16_t row_id = source_row_ids[i];
      uint32_t length = from_offsets[row_id + 1] - from_offsets[row_id];
      auto src64 = reinterpret_cast<const uint64_t*>(src + from_offsets[row_id]);
      auto dst64 = reinterpret_cast<uint64_t*>(dst);
      for (uint32_t j = 0; j < (length + 7) / 8; ++j) {
        dst64[j] = src64[j];
      }
      dst += length;
    }
  } else {
    // Fixed-length rows
    const uint8_t* src = from.rows_->data();
    uint8_t* dst = rows_->mutable_data() + num_rows_ * metadata_.fixed_length;
    for (uint32_t i = 0; i < num_rows_to_append; ++i) {
      uint16_t row_id = source_row_ids[i];
      uint32_t length = metadata_.fixed_length;
      auto src64 = reinterpret_cast<const uint64_t*>(src + length * row_id);
      auto dst64 = reinterpret_cast<uint64_t*>(dst);
      for (uint32_t j = 0; j < (length + 7) / 8; ++j) {
        dst64[j] = src64[j];
      }
      dst += length;
    }
  }

  // Null masks
  uint32_t byte_length = metadata_.null_masks_bytes_per_row;
  uint64_t dst_byte_offset = num_rows_ * byte_length;
  const uint8_t* src_base = from.null_masks_->data();
  uint8_t* dst_base = null_masks_->mutable_data();
  for (uint32_t i = 0; i < num_rows_to_append; ++i) {
    uint32_t row_id = source_row_ids[i];
    int64_t src_byte_offset = row_id * byte_length;
    const uint8_t* src = src_base + src_byte_offset;
    uint8_t* dst = dst_base + dst_byte_offset;
    for (uint32_t ibyte = 0; ibyte < byte_length; ++ibyte) {
      dst[ibyte] = src[ibyte];
    }
    dst_byte_offset += byte_length;
  }

  num_rows_ += num_rows_to_append;

  return Status::OK();
}

Status KeyEncoder::KeyRowArray::AppendEmpty(uint32_t num_rows_to_append,
                                            uint32_t num_extra_bytes_to_append) {
  RETURN_NOT_OK(ResizeFixedLengthBuffers(num_rows_to_append));
  RETURN_NOT_OK(ResizeOptionalVaryingLengthBuffer(num_extra_bytes_to_append));
  num_rows_ += num_rows_to_append;
  if (metadata_.row_alignment > 1 || metadata_.string_alignment > 1) {
    memset(rows_->mutable_data(), 0, bytes_capacity_);
  }
  return Status::OK();
}

bool KeyEncoder::KeyRowArray::has_any_nulls(const KeyEncoderContext* ctx) const {
  if (has_any_nulls_) {
    return true;
  }
  if (num_rows_for_has_any_nulls_ < num_rows_) {
    auto size_per_row = metadata().null_masks_bytes_per_row;
    has_any_nulls_ = !util::BitUtil::are_all_bytes_zero(
        ctx->hardware_flags, null_masks() + size_per_row * num_rows_for_has_any_nulls_,
        static_cast<uint32_t>(size_per_row * (num_rows_ - num_rows_for_has_any_nulls_)));
    num_rows_for_has_any_nulls_ = num_rows_;
  }
  return has_any_nulls_;
}

KeyEncoder::KeyColumnArray::KeyColumnArray(const KeyColumnMetadata& metadata,
                                           const KeyColumnArray& left,
                                           const KeyColumnArray& right,
                                           int buffer_id_to_replace) {
  metadata_ = metadata;
  length_ = left.length();
  for (int i = 0; i < max_buffers_; ++i) {
    buffers_[i] = left.buffers_[i];
    mutable_buffers_[i] = left.mutable_buffers_[i];
  }
  buffers_[buffer_id_to_replace] = right.buffers_[buffer_id_to_replace];
  mutable_buffers_[buffer_id_to_replace] = right.mutable_buffers_[buffer_id_to_replace];
  bit_offset_[0] = left.bit_offset_[0];
  bit_offset_[1] = left.bit_offset_[1];
  if (buffer_id_to_replace < max_buffers_ - 1) {
    bit_offset_[buffer_id_to_replace] = right.bit_offset_[buffer_id_to_replace];
  }
}

KeyEncoder::KeyColumnArray::KeyColumnArray(const KeyColumnMetadata& metadata,
                                           int64_t length, const uint8_t* buffer0,
                                           const uint8_t* buffer1, const uint8_t* buffer2,
                                           int bit_offset0, int bit_offset1) {
  metadata_ = metadata;
  length_ = length;
  buffers_[0] = buffer0;
  buffers_[1] = buffer1;
  buffers_[2] = buffer2;
  mutable_buffers_[0] = mutable_buffers_[1] = mutable_buffers_[2] = nullptr;
  bit_offset_[0] = bit_offset0;
  bit_offset_[1] = bit_offset1;
}

KeyEncoder::KeyColumnArray::KeyColumnArray(const KeyColumnMetadata& metadata,
                                           int64_t length, uint8_t* buffer0,
                                           uint8_t* buffer1, uint8_t* buffer2,
                                           int bit_offset0, int bit_offset1) {
  metadata_ = metadata;
  length_ = length;
  buffers_[0] = mutable_buffers_[0] = buffer0;
  buffers_[1] = mutable_buffers_[1] = buffer1;
  buffers_[2] = mutable_buffers_[2] = buffer2;
  bit_offset_[0] = bit_offset0;
  bit_offset_[1] = bit_offset1;
}

KeyEncoder::KeyColumnArray::KeyColumnArray(const KeyColumnArray& from, int64_t start,
                                           int64_t length) {
  metadata_ = from.metadata_;
  length_ = length;
  uint32_t fixed_size =
      !metadata_.is_fixed_length ? sizeof(uint32_t) : metadata_.fixed_length;

  buffers_[0] =
      from.buffers_[0] ? from.buffers_[0] + (from.bit_offset_[0] + start) / 8 : nullptr;
  mutable_buffers_[0] = from.mutable_buffers_[0]
                            ? from.mutable_buffers_[0] + (from.bit_offset_[0] + start) / 8
                            : nullptr;
  bit_offset_[0] = (from.bit_offset_[0] + start) % 8;

  if (fixed_size == 0) {
    buffers_[1] =
        from.buffers_[1] ? from.buffers_[1] + (from.bit_offset_[1] + start) / 8 : nullptr;
    mutable_buffers_[1] = from.mutable_buffers_[1] ? from.mutable_buffers_[1] +
                                                         (from.bit_offset_[1] + start) / 8
                                                   : nullptr;
    bit_offset_[1] = (from.bit_offset_[1] + start) % 8;
  } else {
    buffers_[1] = from.buffers_[1] ? from.buffers_[1] + start * fixed_size : nullptr;
    mutable_buffers_[1] = from.mutable_buffers_[1]
                              ? from.mutable_buffers_[1] + start * fixed_size
                              : nullptr;
    bit_offset_[1] = 0;
  }

  buffers_[2] = from.buffers_[2];
  mutable_buffers_[2] = from.mutable_buffers_[2];
}

KeyEncoder::KeyColumnArray KeyEncoder::TransformBoolean::ArrayReplace(
    const KeyColumnArray& column, const KeyColumnArray& temp) {
  // Make sure that the temp buffer is large enough
  DCHECK(temp.length() >= column.length() && temp.metadata().is_fixed_length &&
         temp.metadata().fixed_length >= sizeof(uint8_t));
  KeyColumnMetadata metadata;
  metadata.is_fixed_length = true;
  metadata.fixed_length = sizeof(uint8_t);
  constexpr int buffer_index = 1;
  KeyColumnArray result = KeyColumnArray(metadata, column, temp, buffer_index);
  return result;
}

void KeyEncoder::TransformBoolean::PreEncode(const KeyColumnArray& input,
                                             KeyColumnArray* output,
                                             KeyEncoderContext* ctx) {
  // Make sure that metadata and lengths are compatible.
  DCHECK(output->metadata().is_fixed_length == input.metadata().is_fixed_length);
  DCHECK(output->metadata().fixed_length == 1 && input.metadata().fixed_length == 0);
  DCHECK(output->length() == input.length());
  constexpr int buffer_index = 1;
  DCHECK(input.data(buffer_index) != nullptr);
  DCHECK(output->mutable_data(buffer_index) != nullptr);
  util::BitUtil::bits_to_bytes(
      ctx->hardware_flags, static_cast<int>(input.length()), input.data(buffer_index),
      output->mutable_data(buffer_index), input.bit_offset(buffer_index));
}

void KeyEncoder::TransformBoolean::PostDecode(const KeyColumnArray& input,
                                              KeyColumnArray* output,
                                              KeyEncoderContext* ctx) {
  // Make sure that metadata and lengths are compatible.
  DCHECK(output->metadata().is_fixed_length == input.metadata().is_fixed_length);
  DCHECK(output->metadata().fixed_length == 0 && input.metadata().fixed_length == 1);
  DCHECK(output->length() == input.length());
  constexpr int buffer_index = 1;
  DCHECK(input.data(buffer_index) != nullptr);
  DCHECK(output->mutable_data(buffer_index) != nullptr);

  util::BitUtil::bytes_to_bits(
      ctx->hardware_flags, static_cast<int>(input.length()), input.data(buffer_index),
      output->mutable_data(buffer_index), output->bit_offset(buffer_index));
}

bool KeyEncoder::EncoderInteger::IsBoolean(const KeyColumnMetadata& metadata) {
  return metadata.is_fixed_length && metadata.fixed_length == 0;
}

bool KeyEncoder::EncoderInteger::UsesTransform(const KeyColumnArray& column) {
  return IsBoolean(column.metadata());
}

KeyEncoder::KeyColumnArray KeyEncoder::EncoderInteger::ArrayReplace(
    const KeyColumnArray& column, const KeyColumnArray& temp) {
  if (IsBoolean(column.metadata())) {
    return TransformBoolean::ArrayReplace(column, temp);
  }
  return column;
}

void KeyEncoder::EncoderInteger::PreEncode(const KeyColumnArray& input,
                                           KeyColumnArray* output,
                                           KeyEncoderContext* ctx) {
  if (IsBoolean(input.metadata())) {
    TransformBoolean::PreEncode(input, output, ctx);
  }
}

void KeyEncoder::EncoderInteger::PostDecode(const KeyColumnArray& input,
                                            KeyColumnArray* output,
                                            KeyEncoderContext* ctx) {
  if (IsBoolean(output->metadata())) {
    TransformBoolean::PostDecode(input, output, ctx);
  }
}

void KeyEncoder::EncoderInteger::Encode(uint32_t offset_within_row, KeyRowArray* rows,
                                        const KeyColumnArray& col, KeyEncoderContext* ctx,
                                        KeyColumnArray* temp) {
  KeyColumnArray col_prep;
  if (UsesTransform(col)) {
    col_prep = ArrayReplace(col, *temp);
    PreEncode(col, &col_prep, ctx);
  } else {
    col_prep = col;
  }

  const auto num_rows = static_cast<uint32_t>(col.length());

  // When we have a single fixed length column we can just do memcpy
  if (rows->metadata().is_fixed_length &&
      rows->metadata().fixed_length == col.metadata().fixed_length) {
    DCHECK_EQ(offset_within_row, 0);
    uint32_t row_size = col.metadata().fixed_length;
    memcpy(rows->mutable_data(1), col.data(1), num_rows * row_size);
  } else if (rows->metadata().is_fixed_length) {
    uint32_t row_size = rows->metadata().fixed_length;
    uint8_t* row_base = rows->mutable_data(1) + offset_within_row;
    const uint8_t* col_base = col_prep.data(1);
    switch (col_prep.metadata().fixed_length) {
      case 1:
        for (uint32_t i = 0; i < num_rows; ++i) {
          row_base[i * row_size] = col_base[i];
        }
        break;
      case 2:
        for (uint32_t i = 0; i < num_rows; ++i) {
          *reinterpret_cast<uint16_t*>(row_base + i * row_size) =
              reinterpret_cast<const uint16_t*>(col_base)[i];
        }
        break;
      case 4:
        for (uint32_t i = 0; i < num_rows; ++i) {
          *reinterpret_cast<uint32_t*>(row_base + i * row_size) =
              reinterpret_cast<const uint32_t*>(col_base)[i];
        }
        break;
      case 8:
        for (uint32_t i = 0; i < num_rows; ++i) {
          *reinterpret_cast<uint64_t*>(row_base + i * row_size) =
              reinterpret_cast<const uint64_t*>(col_base)[i];
        }
        break;
      default:
        DCHECK(false);
    }
  } else {
    const uint32_t* row_offsets = rows->offsets();
    uint8_t* row_base = rows->mutable_data(2) + offset_within_row;
    const uint8_t* col_base = col_prep.data(1);
    switch (col_prep.metadata().fixed_length) {
      case 1:
        for (uint32_t i = 0; i < num_rows; ++i) {
          row_base[row_offsets[i]] = col_base[i];
        }
        break;
      case 2:
        for (uint32_t i = 0; i < num_rows; ++i) {
          *reinterpret_cast<uint16_t*>(row_base + row_offsets[i]) =
              reinterpret_cast<const uint16_t*>(col_base)[i];
        }
        break;
      case 4:
        for (uint32_t i = 0; i < num_rows; ++i) {
          *reinterpret_cast<uint32_t*>(row_base + row_offsets[i]) =
              reinterpret_cast<const uint32_t*>(col_base)[i];
        }
        break;
      case 8:
        for (uint32_t i = 0; i < num_rows; ++i) {
          *reinterpret_cast<uint64_t*>(row_base + row_offsets[i]) =
              reinterpret_cast<const uint64_t*>(col_base)[i];
        }
        break;
      default:
        DCHECK(false);
    }
  }
}

void KeyEncoder::EncoderInteger::Decode(uint32_t start_row, uint32_t num_rows,
                                        uint32_t offset_within_row,
                                        const KeyRowArray& rows, KeyColumnArray* col,
                                        KeyEncoderContext* ctx, KeyColumnArray* temp) {
  KeyColumnArray col_prep;
  if (UsesTransform(*col)) {
    col_prep = ArrayReplace(*col, *temp);
  } else {
    col_prep = *col;
  }

  // When we have a single fixed length column we can just do memcpy
  if (rows.metadata().is_fixed_length &&
      col_prep.metadata().fixed_length == rows.metadata().fixed_length) {
    DCHECK_EQ(offset_within_row, 0);
    uint32_t row_size = rows.metadata().fixed_length;
    memcpy(col_prep.mutable_data(1), rows.data(1) + start_row * row_size,
           num_rows * row_size);
  } else if (rows.metadata().is_fixed_length) {
    uint32_t row_size = rows.metadata().fixed_length;
    const uint8_t* row_base = rows.data(1) + start_row * row_size;
    row_base += offset_within_row;
    uint8_t* col_base = col_prep.mutable_data(1);
    switch (col_prep.metadata().fixed_length) {
      case 1:
        for (uint32_t i = 0; i < num_rows; ++i) {
          col_base[i] = row_base[i * row_size];
        }
        break;
      case 2:
        for (uint32_t i = 0; i < num_rows; ++i) {
          reinterpret_cast<uint16_t*>(col_base)[i] =
              *reinterpret_cast<const uint16_t*>(row_base + i * row_size);
        }
        break;
      case 4:
        for (uint32_t i = 0; i < num_rows; ++i) {
          reinterpret_cast<uint32_t*>(col_base)[i] =
              *reinterpret_cast<const uint32_t*>(row_base + i * row_size);
        }
        break;
      case 8:
        for (uint32_t i = 0; i < num_rows; ++i) {
          reinterpret_cast<uint64_t*>(col_base)[i] =
              *reinterpret_cast<const uint64_t*>(row_base + i * row_size);
        }
        break;
      default:
        DCHECK(false);
    }
  } else {
    const uint32_t* row_offsets = rows.offsets() + start_row;
    const uint8_t* row_base = rows.data(2);
    row_base += offset_within_row;
    uint8_t* col_base = col_prep.mutable_data(1);
    switch (col_prep.metadata().fixed_length) {
      case 1:
        for (uint32_t i = 0; i < num_rows; ++i) {
          col_base[i] = row_base[row_offsets[i]];
        }
        break;
      case 2:
        for (uint32_t i = 0; i < num_rows; ++i) {
          reinterpret_cast<uint16_t*>(col_base)[i] =
              *reinterpret_cast<const uint16_t*>(row_base + row_offsets[i]);
        }
        break;
      case 4:
        for (uint32_t i = 0; i < num_rows; ++i) {
          reinterpret_cast<uint32_t*>(col_base)[i] =
              *reinterpret_cast<const uint32_t*>(row_base + row_offsets[i]);
        }
        break;
      case 8:
        for (uint32_t i = 0; i < num_rows; ++i) {
          reinterpret_cast<uint64_t*>(col_base)[i] =
              *reinterpret_cast<const uint64_t*>(row_base + row_offsets[i]);
        }
        break;
      default:
        DCHECK(false);
    }
  }

  if (UsesTransform(*col)) {
    PostDecode(col_prep, col, ctx);
  }
}

bool KeyEncoder::EncoderBinary::IsInteger(const KeyColumnMetadata& metadata) {
  bool is_fixed_length = metadata.is_fixed_length;
  auto size = metadata.fixed_length;
  return is_fixed_length &&
         (size == 0 || size == 1 || size == 2 || size == 4 || size == 8);
}

void KeyEncoder::EncoderBinary::Encode(uint32_t offset_within_row, KeyRowArray* rows,
                                       const KeyColumnArray& col, KeyEncoderContext* ctx,
                                       KeyColumnArray* temp) {
  if (IsInteger(col.metadata())) {
    EncoderInteger::Encode(offset_within_row, rows, col, ctx, temp);
  } else {
    KeyColumnArray col_prep;
    if (EncoderInteger::UsesTransform(col)) {
      col_prep = EncoderInteger::ArrayReplace(col, *temp);
      EncoderInteger::PreEncode(col, &col_prep, ctx);
    } else {
      col_prep = col;
    }

    bool is_row_fixed_length = rows->metadata().is_fixed_length;

#if defined(ARROW_HAVE_AVX2)
    if (ctx->has_avx2()) {
      EncodeHelper_avx2(is_row_fixed_length, offset_within_row, rows, col);
    } else {
#endif
      if (is_row_fixed_length) {
        EncodeImp<true>(offset_within_row, rows, col);
      } else {
        EncodeImp<false>(offset_within_row, rows, col);
      }
#if defined(ARROW_HAVE_AVX2)
    }
#endif
  }

  DCHECK(temp->metadata().is_fixed_length);
  DCHECK(temp->length() * temp->metadata().fixed_length >=
         col.length() * static_cast<int64_t>(sizeof(uint16_t)));

  KeyColumnArray temp16bit(KeyColumnMetadata(true, sizeof(uint16_t)), col.length(),
                           nullptr, temp->mutable_data(1), nullptr);
  ColumnMemsetNulls(offset_within_row, rows, col, ctx, &temp16bit, 0xae);
}

void KeyEncoder::EncoderBinary::Decode(uint32_t start_row, uint32_t num_rows,
                                       uint32_t offset_within_row,
                                       const KeyRowArray& rows, KeyColumnArray* col,
                                       KeyEncoderContext* ctx, KeyColumnArray* temp) {
  if (IsInteger(col->metadata())) {
    EncoderInteger::Decode(start_row, num_rows, offset_within_row, rows, col, ctx, temp);
  } else {
    KeyColumnArray col_prep;
    if (EncoderInteger::UsesTransform(*col)) {
      col_prep = EncoderInteger::ArrayReplace(*col, *temp);
    } else {
      col_prep = *col;
    }

    bool is_row_fixed_length = rows.metadata().is_fixed_length;

#if defined(ARROW_HAVE_AVX2)
    if (ctx->has_avx2()) {
      DecodeHelper_avx2(is_row_fixed_length, start_row, num_rows, offset_within_row, rows,
                        col);
    } else {
#endif
      if (is_row_fixed_length) {
        DecodeImp<true>(start_row, num_rows, offset_within_row, rows, col);
      } else {
        DecodeImp<false>(start_row, num_rows, offset_within_row, rows, col);
      }
#if defined(ARROW_HAVE_AVX2)
    }
#endif

    if (EncoderInteger::UsesTransform(*col)) {
      EncoderInteger::PostDecode(col_prep, col, ctx);
    }
  }
}

template <bool is_row_fixed_length>
void KeyEncoder::EncoderBinary::EncodeImp(uint32_t offset_within_row, KeyRowArray* rows,
                                          const KeyColumnArray& col) {
  EncodeDecodeHelper<is_row_fixed_length, true>(
      0, static_cast<uint32_t>(col.length()), offset_within_row, rows, rows, &col,
      nullptr, [](uint8_t* dst, const uint8_t* src, int64_t length) {
        auto dst64 = reinterpret_cast<uint64_t*>(dst);
        auto src64 = reinterpret_cast<const uint64_t*>(src);
        uint32_t istripe;
        for (istripe = 0; istripe < length / 8; ++istripe) {
          dst64[istripe] = util::SafeLoad(src64 + istripe);
        }
        if ((length % 8) > 0) {
          uint64_t mask_last = ~0ULL >> (8 * (8 * (istripe + 1) - length));
          dst64[istripe] = (dst64[istripe] & ~mask_last) |
                           (util::SafeLoad(src64 + istripe) & mask_last);
        }
      });
}

template <bool is_row_fixed_length>
void KeyEncoder::EncoderBinary::DecodeImp(uint32_t start_row, uint32_t num_rows,
                                          uint32_t offset_within_row,
                                          const KeyRowArray& rows, KeyColumnArray* col) {
  EncodeDecodeHelper<is_row_fixed_length, false>(
      start_row, num_rows, offset_within_row, &rows, nullptr, col, col,
      [](uint8_t* dst, const uint8_t* src, int64_t length) {
        for (uint32_t istripe = 0; istripe < (length + 7) / 8; ++istripe) {
          auto dst64 = reinterpret_cast<uint64_t*>(dst);
          auto src64 = reinterpret_cast<const uint64_t*>(src);
          util::SafeStore(dst64 + istripe, src64[istripe]);
        }
      });
}

void KeyEncoder::EncoderBinary::ColumnMemsetNulls(
    uint32_t offset_within_row, KeyRowArray* rows, const KeyColumnArray& col,
    KeyEncoderContext* ctx, KeyColumnArray* temp_vector_16bit, uint8_t byte_value) {
  using ColumnMemsetNullsImp_t = void (*)(uint32_t, KeyRowArray*, const KeyColumnArray&,
                                          KeyEncoderContext*, KeyColumnArray*, uint8_t);
  static const ColumnMemsetNullsImp_t ColumnMemsetNullsImp_fn[] = {
      ColumnMemsetNullsImp<false, 1>,  ColumnMemsetNullsImp<false, 2>,
      ColumnMemsetNullsImp<false, 4>,  ColumnMemsetNullsImp<false, 8>,
      ColumnMemsetNullsImp<false, 16>, ColumnMemsetNullsImp<true, 1>,
      ColumnMemsetNullsImp<true, 2>,   ColumnMemsetNullsImp<true, 4>,
      ColumnMemsetNullsImp<true, 8>,   ColumnMemsetNullsImp<true, 16>};
  uint32_t col_width = col.metadata().fixed_length;
  int dispatch_const =
      (rows->metadata().is_fixed_length ? 5 : 0) +
      (col_width == 1 ? 0
                      : col_width == 2 ? 1 : col_width == 4 ? 2 : col_width == 8 ? 3 : 4);
  ColumnMemsetNullsImp_fn[dispatch_const](offset_within_row, rows, col, ctx,
                                          temp_vector_16bit, byte_value);
}

template <bool is_row_fixed_length, uint32_t col_width>
void KeyEncoder::EncoderBinary::ColumnMemsetNullsImp(
    uint32_t offset_within_row, KeyRowArray* rows, const KeyColumnArray& col,
    KeyEncoderContext* ctx, KeyColumnArray* temp_vector_16bit, uint8_t byte_value) {
  // Nothing to do when there are no nulls
  if (!col.data(0)) {
    return;
  }

  const auto num_rows = static_cast<uint32_t>(col.length());

  // Temp vector needs space for the required number of rows
  DCHECK(temp_vector_16bit->length() >= num_rows);
  DCHECK(temp_vector_16bit->metadata().is_fixed_length &&
         temp_vector_16bit->metadata().fixed_length == sizeof(uint16_t));
  auto temp_vector = reinterpret_cast<uint16_t*>(temp_vector_16bit->mutable_data(1));

  // Bit vector to index vector of null positions
  int num_selected;
  util::BitUtil::bits_to_indexes(0, ctx->hardware_flags, static_cast<int>(col.length()),
                                 col.data(0), &num_selected, temp_vector,
                                 col.bit_offset(0));

  for (int i = 0; i < num_selected; ++i) {
    uint32_t row_id = temp_vector[i];

    // Target binary field pointer
    uint8_t* dst;
    if (is_row_fixed_length) {
      dst = rows->mutable_data(1) + rows->metadata().fixed_length * row_id;
    } else {
      dst = rows->mutable_data(2) + rows->offsets()[row_id];
    }
    dst += offset_within_row;

    if (col_width == 1) {
      *dst = byte_value;
    } else if (col_width == 2) {
      *reinterpret_cast<uint16_t*>(dst) =
          (static_cast<uint16_t>(byte_value) * static_cast<uint16_t>(0x0101));
    } else if (col_width == 4) {
      *reinterpret_cast<uint32_t*>(dst) =
          (static_cast<uint32_t>(byte_value) * static_cast<uint32_t>(0x01010101));
    } else if (col_width == 8) {
      *reinterpret_cast<uint64_t*>(dst) =
          (static_cast<uint64_t>(byte_value) * 0x0101010101010101ULL);
    } else {
      uint64_t value = (static_cast<uint64_t>(byte_value) * 0x0101010101010101ULL);
      uint32_t col_width_actual = col.metadata().fixed_length;
      uint32_t j;
      for (j = 0; j < col_width_actual / 8; ++j) {
        reinterpret_cast<uint64_t*>(dst)[j] = value;
      }
      int tail = col_width_actual % 8;
      if (tail) {
        uint64_t mask = ~0ULL >> (8 * (8 - tail));
        reinterpret_cast<uint64_t*>(dst)[j] =
            (reinterpret_cast<const uint64_t*>(dst)[j] & ~mask) | (value & mask);
      }
    }
  }
}

void KeyEncoder::EncoderBinaryPair::Encode(uint32_t offset_within_row, KeyRowArray* rows,
                                           const KeyColumnArray& col1,
                                           const KeyColumnArray& col2,
                                           KeyEncoderContext* ctx, KeyColumnArray* temp1,
                                           KeyColumnArray* temp2) {
  DCHECK(CanProcessPair(col1.metadata(), col2.metadata()));

  KeyColumnArray col_prep[2];
  if (EncoderInteger::UsesTransform(col1)) {
    col_prep[0] = EncoderInteger::ArrayReplace(col1, *temp1);
    EncoderInteger::PreEncode(col1, &(col_prep[0]), ctx);
  } else {
    col_prep[0] = col1;
  }
  if (EncoderInteger::UsesTransform(col2)) {
    col_prep[1] = EncoderInteger::ArrayReplace(col2, *temp2);
    EncoderInteger::PreEncode(col2, &(col_prep[1]), ctx);
  } else {
    col_prep[1] = col2;
  }

  uint32_t col_width1 = col_prep[0].metadata().fixed_length;
  uint32_t col_width2 = col_prep[1].metadata().fixed_length;
  int log_col_width1 =
      col_width1 == 8 ? 3 : col_width1 == 4 ? 2 : col_width1 == 2 ? 1 : 0;
  int log_col_width2 =
      col_width2 == 8 ? 3 : col_width2 == 4 ? 2 : col_width2 == 2 ? 1 : 0;

  bool is_row_fixed_length = rows->metadata().is_fixed_length;

  const auto num_rows = static_cast<uint32_t>(col1.length());
  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2() && col_width1 == col_width2) {
    num_processed = EncodeHelper_avx2(is_row_fixed_length, col_width1, offset_within_row,
                                      rows, col_prep[0], col_prep[1]);
  }
#endif
  if (num_processed < num_rows) {
    using EncodeImp_t = void (*)(uint32_t, uint32_t, KeyRowArray*, const KeyColumnArray&,
                                 const KeyColumnArray&);
    static const EncodeImp_t EncodeImp_fn[] = {
        EncodeImp<false, uint8_t, uint8_t>,   EncodeImp<false, uint16_t, uint8_t>,
        EncodeImp<false, uint32_t, uint8_t>,  EncodeImp<false, uint64_t, uint8_t>,
        EncodeImp<false, uint8_t, uint16_t>,  EncodeImp<false, uint16_t, uint16_t>,
        EncodeImp<false, uint32_t, uint16_t>, EncodeImp<false, uint64_t, uint16_t>,
        EncodeImp<false, uint8_t, uint32_t>,  EncodeImp<false, uint16_t, uint32_t>,
        EncodeImp<false, uint32_t, uint32_t>, EncodeImp<false, uint64_t, uint32_t>,
        EncodeImp<false, uint8_t, uint64_t>,  EncodeImp<false, uint16_t, uint64_t>,
        EncodeImp<false, uint32_t, uint64_t>, EncodeImp<false, uint64_t, uint64_t>,
        EncodeImp<true, uint8_t, uint8_t>,    EncodeImp<true, uint16_t, uint8_t>,
        EncodeImp<true, uint32_t, uint8_t>,   EncodeImp<true, uint64_t, uint8_t>,
        EncodeImp<true, uint8_t, uint16_t>,   EncodeImp<true, uint16_t, uint16_t>,
        EncodeImp<true, uint32_t, uint16_t>,  EncodeImp<true, uint64_t, uint16_t>,
        EncodeImp<true, uint8_t, uint32_t>,   EncodeImp<true, uint16_t, uint32_t>,
        EncodeImp<true, uint32_t, uint32_t>,  EncodeImp<true, uint64_t, uint32_t>,
        EncodeImp<true, uint8_t, uint64_t>,   EncodeImp<true, uint16_t, uint64_t>,
        EncodeImp<true, uint32_t, uint64_t>,  EncodeImp<true, uint64_t, uint64_t>};
    int dispatch_const = (log_col_width2 << 2) | log_col_width1;
    dispatch_const += (is_row_fixed_length ? 16 : 0);
    EncodeImp_fn[dispatch_const](num_processed, offset_within_row, rows, col_prep[0],
                                 col_prep[1]);
  }
}

template <bool is_row_fixed_length, typename col1_type, typename col2_type>
void KeyEncoder::EncoderBinaryPair::EncodeImp(uint32_t num_rows_to_skip,
                                              uint32_t offset_within_row,
                                              KeyRowArray* rows,
                                              const KeyColumnArray& col1,
                                              const KeyColumnArray& col2) {
  const uint8_t* src_A = col1.data(1);
  const uint8_t* src_B = col2.data(1);

  const auto num_rows = static_cast<uint32_t>(col1.length());

  uint32_t fixed_length = rows->metadata().fixed_length;
  const uint32_t* offsets;
  uint8_t* dst_base;
  if (is_row_fixed_length) {
    dst_base = rows->mutable_data(1) + offset_within_row;
    offsets = nullptr;
  } else {
    dst_base = rows->mutable_data(2) + offset_within_row;
    offsets = rows->offsets();
  }

  using col1_type_const = typename std::add_const<col1_type>::type;
  using col2_type_const = typename std::add_const<col2_type>::type;

  if (is_row_fixed_length) {
    uint8_t* dst = dst_base + num_rows_to_skip * fixed_length;
    for (uint32_t i = num_rows_to_skip; i < num_rows; ++i) {
      *reinterpret_cast<col1_type*>(dst) = reinterpret_cast<col1_type_const*>(src_A)[i];
      *reinterpret_cast<col2_type*>(dst + sizeof(col1_type)) =
          reinterpret_cast<col2_type_const*>(src_B)[i];
      dst += fixed_length;
    }
  } else {
    for (uint32_t i = num_rows_to_skip; i < num_rows; ++i) {
      uint8_t* dst = dst_base + offsets[i];
      *reinterpret_cast<col1_type*>(dst) = reinterpret_cast<col1_type_const*>(src_A)[i];
      *reinterpret_cast<col2_type*>(dst + sizeof(col1_type)) =
          reinterpret_cast<col2_type_const*>(src_B)[i];
    }
  }
}

void KeyEncoder::EncoderBinaryPair::Decode(uint32_t start_row, uint32_t num_rows,
                                           uint32_t offset_within_row,
                                           const KeyRowArray& rows, KeyColumnArray* col1,
                                           KeyColumnArray* col2, KeyEncoderContext* ctx,
                                           KeyColumnArray* temp1, KeyColumnArray* temp2) {
  DCHECK(CanProcessPair(col1->metadata(), col2->metadata()));

  KeyColumnArray col_prep[2];
  if (EncoderInteger::UsesTransform(*col1)) {
    col_prep[0] = EncoderInteger::ArrayReplace(*col1, *temp1);
  } else {
    col_prep[0] = *col1;
  }
  if (EncoderInteger::UsesTransform(*col2)) {
    col_prep[1] = EncoderInteger::ArrayReplace(*col2, *temp2);
  } else {
    col_prep[1] = *col2;
  }

  uint32_t col_width1 = col_prep[0].metadata().fixed_length;
  uint32_t col_width2 = col_prep[1].metadata().fixed_length;
  int log_col_width1 =
      col_width1 == 8 ? 3 : col_width1 == 4 ? 2 : col_width1 == 2 ? 1 : 0;
  int log_col_width2 =
      col_width2 == 8 ? 3 : col_width2 == 4 ? 2 : col_width2 == 2 ? 1 : 0;

  bool is_row_fixed_length = rows.metadata().is_fixed_length;

  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2() && col_width1 == col_width2) {
    num_processed =
        DecodeHelper_avx2(is_row_fixed_length, col_width1, start_row, num_rows,
                          offset_within_row, rows, &col_prep[0], &col_prep[1]);
  }
#endif
  if (num_processed < num_rows) {
    using DecodeImp_t = void (*)(uint32_t, uint32_t, uint32_t, uint32_t,
                                 const KeyRowArray&, KeyColumnArray*, KeyColumnArray*);
    static const DecodeImp_t DecodeImp_fn[] = {
        DecodeImp<false, uint8_t, uint8_t>,   DecodeImp<false, uint16_t, uint8_t>,
        DecodeImp<false, uint32_t, uint8_t>,  DecodeImp<false, uint64_t, uint8_t>,
        DecodeImp<false, uint8_t, uint16_t>,  DecodeImp<false, uint16_t, uint16_t>,
        DecodeImp<false, uint32_t, uint16_t>, DecodeImp<false, uint64_t, uint16_t>,
        DecodeImp<false, uint8_t, uint32_t>,  DecodeImp<false, uint16_t, uint32_t>,
        DecodeImp<false, uint32_t, uint32_t>, DecodeImp<false, uint64_t, uint32_t>,
        DecodeImp<false, uint8_t, uint64_t>,  DecodeImp<false, uint16_t, uint64_t>,
        DecodeImp<false, uint32_t, uint64_t>, DecodeImp<false, uint64_t, uint64_t>,
        DecodeImp<true, uint8_t, uint8_t>,    DecodeImp<true, uint16_t, uint8_t>,
        DecodeImp<true, uint32_t, uint8_t>,   DecodeImp<true, uint64_t, uint8_t>,
        DecodeImp<true, uint8_t, uint16_t>,   DecodeImp<true, uint16_t, uint16_t>,
        DecodeImp<true, uint32_t, uint16_t>,  DecodeImp<true, uint64_t, uint16_t>,
        DecodeImp<true, uint8_t, uint32_t>,   DecodeImp<true, uint16_t, uint32_t>,
        DecodeImp<true, uint32_t, uint32_t>,  DecodeImp<true, uint64_t, uint32_t>,
        DecodeImp<true, uint8_t, uint64_t>,   DecodeImp<true, uint16_t, uint64_t>,
        DecodeImp<true, uint32_t, uint64_t>,  DecodeImp<true, uint64_t, uint64_t>};
    int dispatch_const =
        (log_col_width2 << 2) | log_col_width1 | (is_row_fixed_length ? 16 : 0);
    DecodeImp_fn[dispatch_const](num_processed, start_row, num_rows, offset_within_row,
                                 rows, &(col_prep[0]), &(col_prep[1]));
  }

  if (EncoderInteger::UsesTransform(*col1)) {
    EncoderInteger::PostDecode(col_prep[0], col1, ctx);
  }
  if (EncoderInteger::UsesTransform(*col2)) {
    EncoderInteger::PostDecode(col_prep[1], col2, ctx);
  }
}

template <bool is_row_fixed_length, typename col1_type, typename col2_type>
void KeyEncoder::EncoderBinaryPair::DecodeImp(uint32_t num_rows_to_skip,
                                              uint32_t start_row, uint32_t num_rows,
                                              uint32_t offset_within_row,
                                              const KeyRowArray& rows,
                                              KeyColumnArray* col1,
                                              KeyColumnArray* col2) {
  DCHECK(rows.length() >= start_row + num_rows);
  DCHECK(col1->length() == num_rows && col2->length() == num_rows);

  uint8_t* dst_A = col1->mutable_data(1);
  uint8_t* dst_B = col2->mutable_data(1);

  uint32_t fixed_length = rows.metadata().fixed_length;
  const uint32_t* offsets;
  const uint8_t* src_base;
  if (is_row_fixed_length) {
    src_base = rows.data(1) + fixed_length * start_row + offset_within_row;
    offsets = nullptr;
  } else {
    src_base = rows.data(2) + offset_within_row;
    offsets = rows.offsets() + start_row;
  }

  using col1_type_const = typename std::add_const<col1_type>::type;
  using col2_type_const = typename std::add_const<col2_type>::type;

  if (is_row_fixed_length) {
    const uint8_t* src = src_base + num_rows_to_skip * fixed_length;
    for (uint32_t i = num_rows_to_skip; i < num_rows; ++i) {
      reinterpret_cast<col1_type*>(dst_A)[i] = *reinterpret_cast<col1_type_const*>(src);
      reinterpret_cast<col2_type*>(dst_B)[i] =
          *reinterpret_cast<col2_type_const*>(src + sizeof(col1_type));
      src += fixed_length;
    }
  } else {
    for (uint32_t i = num_rows_to_skip; i < num_rows; ++i) {
      const uint8_t* src = src_base + offsets[i];
      reinterpret_cast<col1_type*>(dst_A)[i] = *reinterpret_cast<col1_type_const*>(src);
      reinterpret_cast<col2_type*>(dst_B)[i] =
          *reinterpret_cast<col2_type_const*>(src + sizeof(col1_type));
    }
  }
}

void KeyEncoder::EncoderOffsets::Encode(KeyRowArray* rows,
                                        const std::vector<KeyColumnArray>& varbinary_cols,
                                        KeyEncoderContext* ctx) {
  DCHECK(!varbinary_cols.empty());

  // Rows and columns must all be varying-length
  DCHECK(!rows->metadata().is_fixed_length);
  for (const auto& col : varbinary_cols) {
    DCHECK(!col.metadata().is_fixed_length);
  }

  const auto num_rows = static_cast<uint32_t>(varbinary_cols[0].length());

  uint32_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  // Whether any of the columns has non-zero starting bit offset for non-nulls bit vector
  bool has_bit_offset = false;

  // The space in columns must be exactly equal to a space for offsets in rows
  DCHECK(rows->length() == num_rows);
  for (const auto& col : varbinary_cols) {
    DCHECK(col.length() == num_rows);
    if (col.bit_offset(0) != 0) {
      has_bit_offset = true;
    }
  }

  if (ctx->has_avx2() && !has_bit_offset) {
    // Create a temp vector sized based on the number of columns
    auto temp_buffer_holder = util::TempVectorHolder<uint32_t>(
        ctx->stack, static_cast<uint32_t>(varbinary_cols.size()) * 8);
    auto temp_buffer_32B_per_col = KeyColumnArray(
        KeyColumnMetadata(true, sizeof(uint32_t)), varbinary_cols.size() * 8, nullptr,
        reinterpret_cast<uint8_t*>(temp_buffer_holder.mutable_data()), nullptr);

    num_processed = EncodeImp_avx2(rows, varbinary_cols, &temp_buffer_32B_per_col);
  }
#endif
  if (num_processed < num_rows) {
    EncodeImp(num_processed, rows, varbinary_cols);
  }
}

void KeyEncoder::EncoderOffsets::EncodeImp(
    uint32_t num_rows_already_processed, KeyRowArray* rows,
    const std::vector<KeyColumnArray>& varbinary_cols) {
  DCHECK_GT(varbinary_cols.size(), 0);

  int row_alignment = rows->metadata().row_alignment;
  int string_alignment = rows->metadata().string_alignment;

  uint32_t* row_offsets = rows->mutable_offsets();
  uint8_t* row_values = rows->mutable_data(2);
  const auto num_rows = static_cast<uint32_t>(varbinary_cols[0].length());

  if (num_rows_already_processed == 0) {
    row_offsets[0] = 0;
  }

  uint32_t row_offset = row_offsets[num_rows_already_processed];
  for (uint32_t i = num_rows_already_processed; i < num_rows; ++i) {
    uint32_t* varbinary_end =
        rows->metadata().varbinary_end_array(row_values + row_offset);

    // Zero out lengths for nulls.
    // Add lengths of all columns to get row size.
    // Store varbinary field ends while summing their lengths.

    uint32_t offset_within_row = rows->metadata().fixed_length;

    for (size_t col = 0; col < varbinary_cols.size(); ++col) {
      const uint32_t* col_offsets = varbinary_cols[col].offsets();
      uint32_t col_length = col_offsets[i + 1] - col_offsets[i];

      const int bit_offset = varbinary_cols[col].bit_offset(0);

      const uint8_t* non_nulls = varbinary_cols[col].data(0);
      if (non_nulls && BitUtil::GetBit(non_nulls, bit_offset + i) == 0) {
        col_length = 0;
      }

      offset_within_row +=
          KeyRowMetadata::padding_for_alignment(offset_within_row, string_alignment);
      offset_within_row += col_length;

      varbinary_end[col] = offset_within_row;
    }

    offset_within_row +=
        KeyRowMetadata::padding_for_alignment(offset_within_row, row_alignment);
    row_offset += offset_within_row;
    row_offsets[i + 1] = row_offset;
  }
}

void KeyEncoder::EncoderOffsets::Decode(
    uint32_t start_row, uint32_t num_rows, const KeyRowArray& rows,
    std::vector<KeyColumnArray>* varbinary_cols,
    const std::vector<uint32_t>& varbinary_cols_base_offset, KeyEncoderContext* ctx) {
  DCHECK(!varbinary_cols->empty());
  DCHECK(varbinary_cols->size() == varbinary_cols_base_offset.size());

  DCHECK(!rows.metadata().is_fixed_length);
  DCHECK(rows.length() >= start_row + num_rows);
  for (const auto& col : *varbinary_cols) {
    // Rows and columns must all be varying-length
    DCHECK(!col.metadata().is_fixed_length);
    // The space in columns must be exactly equal to a subset of rows selected
    DCHECK(col.length() == num_rows);
  }

  // Offsets of varbinary columns data within each encoded row are stored
  // in the same encoded row as an array of 32-bit integers.
  // This array follows immediately the data of fixed-length columns.
  // There is one element for each varying-length column.
  // The Nth element is the sum of all the lengths of varbinary columns data in
  // that row, up to and including Nth varbinary column.

  const uint32_t* row_offsets = rows.offsets() + start_row;

  // Set the base offset for each column
  for (size_t col = 0; col < varbinary_cols->size(); ++col) {
    uint32_t* col_offsets = (*varbinary_cols)[col].mutable_offsets();
    col_offsets[0] = varbinary_cols_base_offset[col];
  }

  int string_alignment = rows.metadata().string_alignment;

  for (uint32_t i = 0; i < num_rows; ++i) {
    // Find the beginning of cumulative lengths array for next row
    const uint8_t* row = rows.data(2) + row_offsets[i];
    const uint32_t* varbinary_ends = rows.metadata().varbinary_end_array(row);

    // Update the offset of each column
    uint32_t offset_within_row = rows.metadata().fixed_length;
    for (size_t col = 0; col < varbinary_cols->size(); ++col) {
      offset_within_row +=
          KeyRowMetadata::padding_for_alignment(offset_within_row, string_alignment);
      uint32_t length = varbinary_ends[col] - offset_within_row;
      offset_within_row = varbinary_ends[col];
      uint32_t* col_offsets = (*varbinary_cols)[col].mutable_offsets();
      col_offsets[i + 1] = col_offsets[i] + length;
    }
  }
}

void KeyEncoder::EncoderVarBinary::Encode(uint32_t varbinary_col_id, KeyRowArray* rows,
                                          const KeyColumnArray& col,
                                          KeyEncoderContext* ctx) {
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2()) {
    EncodeHelper_avx2(varbinary_col_id, rows, col);
  } else {
#endif
    if (varbinary_col_id == 0) {
      EncodeImp<true>(varbinary_col_id, rows, col);
    } else {
      EncodeImp<false>(varbinary_col_id, rows, col);
    }
#if defined(ARROW_HAVE_AVX2)
  }
#endif
}

void KeyEncoder::EncoderVarBinary::Decode(uint32_t start_row, uint32_t num_rows,
                                          uint32_t varbinary_col_id,
                                          const KeyRowArray& rows, KeyColumnArray* col,
                                          KeyEncoderContext* ctx) {
  // Output column varbinary buffer needs an extra 32B
  // at the end in avx2 version and 8B otherwise.
#if defined(ARROW_HAVE_AVX2)
  if (ctx->has_avx2()) {
    DecodeHelper_avx2(start_row, num_rows, varbinary_col_id, rows, col);
  } else {
#endif
    if (varbinary_col_id == 0) {
      DecodeImp<true>(start_row, num_rows, varbinary_col_id, rows, col);
    } else {
      DecodeImp<false>(start_row, num_rows, varbinary_col_id, rows, col);
    }
#if defined(ARROW_HAVE_AVX2)
  }
#endif
}

template <bool first_varbinary_col>
void KeyEncoder::EncoderVarBinary::EncodeImp(uint32_t varbinary_col_id, KeyRowArray* rows,
                                             const KeyColumnArray& col) {
  EncodeDecodeHelper<first_varbinary_col, true>(
      0, static_cast<uint32_t>(col.length()), varbinary_col_id, rows, rows, &col, nullptr,
      [](uint8_t* dst, const uint8_t* src, int64_t length) {
        auto dst64 = reinterpret_cast<uint64_t*>(dst);
        auto src64 = reinterpret_cast<const uint64_t*>(src);
        uint32_t istripe;
        for (istripe = 0; istripe < length / 8; ++istripe) {
          dst64[istripe] = util::SafeLoad(src64 + istripe);
        }
        if ((length % 8) > 0) {
          uint64_t mask_last = ~0ULL >> (8 * (8 * (istripe + 1) - length));
          dst64[istripe] = (dst64[istripe] & ~mask_last) |
                           (util::SafeLoad(src64 + istripe) & mask_last);
        }
      });
}

template <bool first_varbinary_col>
void KeyEncoder::EncoderVarBinary::DecodeImp(uint32_t start_row, uint32_t num_rows,
                                             uint32_t varbinary_col_id,
                                             const KeyRowArray& rows,
                                             KeyColumnArray* col) {
  EncodeDecodeHelper<first_varbinary_col, false>(
      start_row, num_rows, varbinary_col_id, &rows, nullptr, col, col,
      [](uint8_t* dst, const uint8_t* src, int64_t length) {
        for (uint32_t istripe = 0; istripe < (length + 7) / 8; ++istripe) {
          auto dst64 = reinterpret_cast<uint64_t*>(dst);
          auto src64 = reinterpret_cast<const uint64_t*>(src);
          util::SafeStore(dst64 + istripe, src64[istripe]);
        }
      });
}

void KeyEncoder::EncoderNulls::Encode(KeyRowArray* rows,
                                      const std::vector<KeyColumnArray>& cols,
                                      KeyEncoderContext* ctx,
                                      KeyColumnArray* temp_vector_16bit) {
  DCHECK_GT(cols.size(), 0);
  const auto num_rows = static_cast<uint32_t>(rows->length());

  // All input columns should have the same number of rows.
  // They may or may not have non-nulls bit-vectors allocated.
  for (const auto& col : cols) {
    DCHECK(col.length() == num_rows);
  }

  // Temp vector needs space for the required number of rows
  DCHECK(temp_vector_16bit->length() >= num_rows);
  DCHECK(temp_vector_16bit->metadata().is_fixed_length &&
         temp_vector_16bit->metadata().fixed_length == sizeof(uint16_t));

  uint8_t* null_masks = rows->null_masks();
  uint32_t null_masks_bytes_per_row = rows->metadata().null_masks_bytes_per_row;
  memset(null_masks, 0, null_masks_bytes_per_row * num_rows);
  for (size_t col = 0; col < cols.size(); ++col) {
    const uint8_t* non_nulls = cols[col].data(0);
    if (!non_nulls) {
      continue;
    }
    int bit_offset = cols[col].bit_offset(0);
    DCHECK_LT(bit_offset, 8);
    int num_selected;
    util::BitUtil::bits_to_indexes(
        0, ctx->hardware_flags, num_rows, non_nulls, &num_selected,
        reinterpret_cast<uint16_t*>(temp_vector_16bit->mutable_data(1)), bit_offset);
    for (int i = 0; i < num_selected; ++i) {
      uint16_t row_id = reinterpret_cast<const uint16_t*>(temp_vector_16bit->data(1))[i];
      int64_t null_masks_bit_id = row_id * null_masks_bytes_per_row * 8 + col;
      BitUtil::SetBit(null_masks, null_masks_bit_id);
    }
  }
}

void KeyEncoder::EncoderNulls::Decode(uint32_t start_row, uint32_t num_rows,
                                      const KeyRowArray& rows,
                                      std::vector<KeyColumnArray>* cols) {
  // Every output column needs to have a space for exactly the required number
  // of rows. It also needs to have non-nulls bit-vector allocated and mutable.
  DCHECK_GT(cols->size(), 0);
  for (auto& col : *cols) {
    DCHECK(col.length() == num_rows);
    DCHECK(col.mutable_data(0));
  }

  const uint8_t* null_masks = rows.null_masks();
  uint32_t null_masks_bytes_per_row = rows.metadata().null_masks_bytes_per_row;
  for (size_t col = 0; col < cols->size(); ++col) {
    uint8_t* non_nulls = (*cols)[col].mutable_data(0);
    const int bit_offset = (*cols)[col].bit_offset(0);
    DCHECK_LT(bit_offset, 8);
    non_nulls[0] |= 0xff << (bit_offset);
    if (bit_offset + num_rows > 8) {
      int bits_in_first_byte = 8 - bit_offset;
      memset(non_nulls + 1, 0xff, BitUtil::BytesForBits(num_rows - bits_in_first_byte));
    }
    for (uint32_t row = 0; row < num_rows; ++row) {
      uint32_t null_masks_bit_id =
          (start_row + row) * null_masks_bytes_per_row * 8 + static_cast<uint32_t>(col);
      bool is_set = BitUtil::GetBit(null_masks, null_masks_bit_id);
      if (is_set) {
        BitUtil::ClearBit(non_nulls, bit_offset + row);
      }
    }
  }
}

uint32_t KeyEncoder::KeyRowMetadata::num_varbinary_cols() const {
  uint32_t result = 0;
  for (auto column_metadata : column_metadatas) {
    if (!column_metadata.is_fixed_length) {
      ++result;
    }
  }
  return result;
}

bool KeyEncoder::KeyRowMetadata::is_compatible(const KeyRowMetadata& other) const {
  if (other.num_cols() != num_cols()) {
    return false;
  }
  if (row_alignment != other.row_alignment ||
      string_alignment != other.string_alignment) {
    return false;
  }
  for (size_t i = 0; i < column_metadatas.size(); ++i) {
    if (column_metadatas[i].is_fixed_length !=
        other.column_metadatas[i].is_fixed_length) {
      return false;
    }
    if (column_metadatas[i].fixed_length != other.column_metadatas[i].fixed_length) {
      return false;
    }
  }
  return true;
}

void KeyEncoder::KeyRowMetadata::FromColumnMetadataVector(
    const std::vector<KeyColumnMetadata>& cols, int in_row_alignment,
    int in_string_alignment) {
  column_metadatas.resize(cols.size());
  for (size_t i = 0; i < cols.size(); ++i) {
    column_metadatas[i] = cols[i];
  }

  const auto num_cols = static_cast<uint32_t>(cols.size());

  // Sort columns.
  // Columns are sorted based on the size in bytes of their fixed-length part.
  // For the varying-length column, the fixed-length part is the 32-bit field storing
  // cumulative length of varying-length fields.
  // The rules are:
  // a) Boolean column, marked with fixed-length 0, is considered to have fixed-length
  // part of 1 byte. b) Columns with fixed-length part being power of 2 or multiple of row
  // alignment precede other columns. They are sorted among themselves based on size of
  // fixed-length part. c) Fixed-length columns precede varying-length columns when both
  // have the same size fixed-length part.
  column_order.resize(num_cols);
  for (uint32_t i = 0; i < num_cols; ++i) {
    column_order[i] = i;
  }
  std::sort(
      column_order.begin(), column_order.end(), [&cols](uint32_t left, uint32_t right) {
        bool is_left_pow2 =
            !cols[left].is_fixed_length || ARROW_POPCOUNT64(cols[left].fixed_length) <= 1;
        bool is_right_pow2 = !cols[right].is_fixed_length ||
                             ARROW_POPCOUNT64(cols[right].fixed_length) <= 1;
        bool is_left_fixedlen = cols[left].is_fixed_length;
        bool is_right_fixedlen = cols[right].is_fixed_length;
        uint32_t width_left =
            cols[left].is_fixed_length ? cols[left].fixed_length : sizeof(uint32_t);
        uint32_t width_right =
            cols[right].is_fixed_length ? cols[right].fixed_length : sizeof(uint32_t);
        if (is_left_pow2 != is_right_pow2) {
          return is_left_pow2;
        }
        if (!is_left_pow2) {
          return left < right;
        }
        if (width_left != width_right) {
          return width_left > width_right;
        }
        if (is_left_fixedlen != is_right_fixedlen) {
          return is_left_fixedlen;
        }
        return left < right;
      });

  row_alignment = in_row_alignment;
  string_alignment = in_string_alignment;
  varbinary_end_array_offset = 0;

  column_offsets.resize(num_cols);
  uint32_t num_varbinary_cols = 0;
  uint32_t offset_within_row = 0;
  for (uint32_t i = 0; i < num_cols; ++i) {
    const KeyColumnMetadata& col = cols[column_order[i]];
    offset_within_row +=
        KeyRowMetadata::padding_for_alignment(offset_within_row, string_alignment, col);
    column_offsets[i] = offset_within_row;
    if (!col.is_fixed_length) {
      if (num_varbinary_cols == 0) {
        varbinary_end_array_offset = offset_within_row;
      }
      DCHECK(column_offsets[i] - varbinary_end_array_offset ==
             num_varbinary_cols * sizeof(uint32_t));
      ++num_varbinary_cols;
      offset_within_row += sizeof(uint32_t);
    } else {
      // Boolean column is a bit-vector, which is indicated by
      // setting fixed length in column metadata to zero.
      // It will be stored as a byte in output row.
      if (col.fixed_length == 0) {
        offset_within_row += 1;
      } else {
        offset_within_row += col.fixed_length;
      }
    }
  }

  is_fixed_length = (num_varbinary_cols == 0);
  fixed_length =
      offset_within_row +
      KeyRowMetadata::padding_for_alignment(
          offset_within_row, num_varbinary_cols == 0 ? row_alignment : string_alignment);

  // We set the number of bytes per row storing null masks of individual key columns
  // to be a power of two. This is not required. It could be also set to the minimal
  // number of bytes required for a given number of bits (one bit per column).
  null_masks_bytes_per_row = 1;
  while (static_cast<uint32_t>(null_masks_bytes_per_row * 8) < num_cols) {
    null_masks_bytes_per_row *= 2;
  }
}

void KeyEncoder::Init(const std::vector<KeyColumnMetadata>& cols, KeyEncoderContext* ctx,
                      int row_alignment, int string_alignment) {
  ctx_ = ctx;
  row_metadata_.FromColumnMetadataVector(cols, row_alignment, string_alignment);
  uint32_t num_cols = row_metadata_.num_cols();
  uint32_t num_varbinary_cols = row_metadata_.num_varbinary_cols();
  batch_all_cols_.resize(num_cols);
  batch_varbinary_cols_.resize(num_varbinary_cols);
  batch_varbinary_cols_base_offsets_.resize(num_varbinary_cols);
}

void KeyEncoder::PrepareKeyColumnArrays(int64_t start_row, int64_t num_rows,
                                        const std::vector<KeyColumnArray>& cols_in) {
  const auto num_cols = static_cast<uint32_t>(cols_in.size());
  DCHECK(batch_all_cols_.size() == num_cols);

  uint32_t num_varbinary_visited = 0;
  for (uint32_t i = 0; i < num_cols; ++i) {
    const KeyColumnArray& col = cols_in[row_metadata_.column_order[i]];
    KeyColumnArray col_window(col, start_row, num_rows);
    batch_all_cols_[i] = col_window;
    if (!col.metadata().is_fixed_length) {
      DCHECK(num_varbinary_visited < batch_varbinary_cols_.size());
      // If start row is zero, then base offset of varbinary column is also zero.
      if (start_row == 0) {
        batch_varbinary_cols_base_offsets_[num_varbinary_visited] = 0;
      } else {
        batch_varbinary_cols_base_offsets_[num_varbinary_visited] =
            col.offsets()[start_row];
      }
      batch_varbinary_cols_[num_varbinary_visited++] = col_window;
    }
  }
}

Status KeyEncoder::PrepareOutputForEncode(int64_t start_row, int64_t num_rows,
                                          KeyRowArray* rows,
                                          const std::vector<KeyColumnArray>& all_cols) {
  int64_t num_bytes_required = 0;

  int64_t fixed_part = row_metadata_.fixed_length * num_rows;
  int64_t var_part = 0;
  for (const auto& col : all_cols) {
    if (!col.metadata().is_fixed_length) {
      DCHECK(col.length() >= start_row + num_rows);
      const uint32_t* offsets = col.offsets();
      var_part += offsets[start_row + num_rows] - offsets[start_row];
      // Include maximum padding that can be added to align the start of varbinary fields.
      var_part += num_rows * row_metadata_.string_alignment;
    }
  }
  // Include maximum padding that can be added to align the start of the rows.
  if (!row_metadata_.is_fixed_length) {
    fixed_part += row_metadata_.row_alignment * num_rows;
  }
  num_bytes_required = fixed_part + var_part;

  rows->Clean();
  RETURN_NOT_OK(rows->AppendEmpty(static_cast<uint32_t>(num_rows),
                                  static_cast<uint32_t>(num_bytes_required)));

  return Status::OK();
}

void KeyEncoder::Encode(int64_t start_row, int64_t num_rows, KeyRowArray* rows,
                        const std::vector<KeyColumnArray>& cols) {
  // Prepare column array vectors
  PrepareKeyColumnArrays(start_row, num_rows, cols);

  // Create two temp vectors with 16-bit elements
  auto temp_buffer_holder_A =
      util::TempVectorHolder<uint16_t>(ctx_->stack, static_cast<uint32_t>(num_rows));
  auto temp_buffer_A = KeyColumnArray(
      KeyColumnMetadata(true, sizeof(uint16_t)), num_rows, nullptr,
      reinterpret_cast<uint8_t*>(temp_buffer_holder_A.mutable_data()), nullptr);
  auto temp_buffer_holder_B =
      util::TempVectorHolder<uint16_t>(ctx_->stack, static_cast<uint32_t>(num_rows));
  auto temp_buffer_B = KeyColumnArray(
      KeyColumnMetadata(true, sizeof(uint16_t)), num_rows, nullptr,
      reinterpret_cast<uint8_t*>(temp_buffer_holder_B.mutable_data()), nullptr);

  bool is_row_fixed_length = row_metadata_.is_fixed_length;
  if (!is_row_fixed_length) {
    // This call will generate and fill in data for both:
    // - offsets to the entire encoded arrays
    // - offsets for individual varbinary fields within each row
    EncoderOffsets::Encode(rows, batch_varbinary_cols_, ctx_);

    for (size_t i = 0; i < batch_varbinary_cols_.size(); ++i) {
      // Memcpy varbinary fields into precomputed in the previous step
      // positions in the output row buffer.
      EncoderVarBinary::Encode(static_cast<uint32_t>(i), rows, batch_varbinary_cols_[i],
                               ctx_);
    }
  }

  // Process fixed length columns
  const auto num_cols = static_cast<uint32_t>(batch_all_cols_.size());
  for (uint32_t i = 0; i < num_cols;) {
    if (!batch_all_cols_[i].metadata().is_fixed_length) {
      i += 1;
      continue;
    }
    bool can_process_pair =
        (i + 1 < num_cols) && batch_all_cols_[i + 1].metadata().is_fixed_length &&
        EncoderBinaryPair::CanProcessPair(batch_all_cols_[i].metadata(),
                                          batch_all_cols_[i + 1].metadata());
    if (!can_process_pair) {
      EncoderBinary::Encode(row_metadata_.column_offsets[i], rows, batch_all_cols_[i],
                            ctx_, &temp_buffer_A);
      i += 1;
    } else {
      EncoderBinaryPair::Encode(row_metadata_.column_offsets[i], rows, batch_all_cols_[i],
                                batch_all_cols_[i + 1], ctx_, &temp_buffer_A,
                                &temp_buffer_B);
      i += 2;
    }
  }

  // Process nulls
  EncoderNulls::Encode(rows, batch_all_cols_, ctx_, &temp_buffer_A);
}

void KeyEncoder::DecodeFixedLengthBuffers(int64_t start_row_input,
                                          int64_t start_row_output, int64_t num_rows,
                                          const KeyRowArray& rows,
                                          std::vector<KeyColumnArray>* cols) {
  // Prepare column array vectors
  PrepareKeyColumnArrays(start_row_output, num_rows, *cols);

  // Create two temp vectors with 16-bit elements
  auto temp_buffer_holder_A =
      util::TempVectorHolder<uint16_t>(ctx_->stack, static_cast<uint32_t>(num_rows));
  auto temp_buffer_A = KeyColumnArray(
      KeyColumnMetadata(true, sizeof(uint16_t)), num_rows, nullptr,
      reinterpret_cast<uint8_t*>(temp_buffer_holder_A.mutable_data()), nullptr);
  auto temp_buffer_holder_B =
      util::TempVectorHolder<uint16_t>(ctx_->stack, static_cast<uint32_t>(num_rows));
  auto temp_buffer_B = KeyColumnArray(
      KeyColumnMetadata(true, sizeof(uint16_t)), num_rows, nullptr,
      reinterpret_cast<uint8_t*>(temp_buffer_holder_B.mutable_data()), nullptr);

  bool is_row_fixed_length = row_metadata_.is_fixed_length;
  if (!is_row_fixed_length) {
    EncoderOffsets::Decode(static_cast<uint32_t>(start_row_input),
                           static_cast<uint32_t>(num_rows), rows, &batch_varbinary_cols_,
                           batch_varbinary_cols_base_offsets_, ctx_);
  }

  // Process fixed length columns
  const auto num_cols = static_cast<uint32_t>(batch_all_cols_.size());
  for (uint32_t i = 0; i < num_cols;) {
    if (!batch_all_cols_[i].metadata().is_fixed_length) {
      i += 1;
      continue;
    }
    bool can_process_pair =
        (i + 1 < num_cols) && batch_all_cols_[i + 1].metadata().is_fixed_length &&
        EncoderBinaryPair::CanProcessPair(batch_all_cols_[i].metadata(),
                                          batch_all_cols_[i + 1].metadata());
    if (!can_process_pair) {
      EncoderBinary::Decode(static_cast<uint32_t>(start_row_input),
                            static_cast<uint32_t>(num_rows),
                            row_metadata_.column_offsets[i], rows, &batch_all_cols_[i],
                            ctx_, &temp_buffer_A);
      i += 1;
    } else {
      EncoderBinaryPair::Decode(
          static_cast<uint32_t>(start_row_input), static_cast<uint32_t>(num_rows),
          row_metadata_.column_offsets[i], rows, &batch_all_cols_[i],
          &batch_all_cols_[i + 1], ctx_, &temp_buffer_A, &temp_buffer_B);
      i += 2;
    }
  }

  // Process nulls
  EncoderNulls::Decode(static_cast<uint32_t>(start_row_input),
                       static_cast<uint32_t>(num_rows), rows, &batch_all_cols_);
}

void KeyEncoder::DecodeVaryingLengthBuffers(int64_t start_row_input,
                                            int64_t start_row_output, int64_t num_rows,
                                            const KeyRowArray& rows,
                                            std::vector<KeyColumnArray>* cols) {
  // Prepare column array vectors
  PrepareKeyColumnArrays(start_row_output, num_rows, *cols);

  bool is_row_fixed_length = row_metadata_.is_fixed_length;
  if (!is_row_fixed_length) {
    for (size_t i = 0; i < batch_varbinary_cols_.size(); ++i) {
      // Memcpy varbinary fields into precomputed in the previous step
      // positions in the output row buffer.
      EncoderVarBinary::Decode(static_cast<uint32_t>(start_row_input),
                               static_cast<uint32_t>(num_rows), static_cast<uint32_t>(i),
                               rows, &batch_varbinary_cols_[i], ctx_);
    }
  }
}

}  // namespace compute
}  // namespace arrow

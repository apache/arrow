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
      uint16_t row_id = source_row_ids ? source_row_ids[i] : i;
      uint32_t length = from_offsets[row_id + 1] - from_offsets[row_id];
      total_length_to_append += length;
      to_offsets[num_rows_ + i + 1] = total_length + total_length_to_append;
    }

    RETURN_NOT_OK(ResizeOptionalVaryingLengthBuffer(total_length_to_append));

    const uint8_t* src = from.rows_->data();
    uint8_t* dst = rows_->mutable_data() + total_length;
    for (uint32_t i = 0; i < num_rows_to_append; ++i) {
      uint16_t row_id = source_row_ids ? source_row_ids[i] : i;
      uint32_t length = from_offsets[row_id + 1] - from_offsets[row_id];
      auto src64 = reinterpret_cast<const uint64_t*>(src + from_offsets[row_id]);
      auto dst64 = reinterpret_cast<uint64_t*>(dst);
      for (uint32_t j = 0; j < bit_util::CeilDiv(length, 8); ++j) {
        dst64[j] = src64[j];
      }
      dst += length;
    }
  } else {
    // Fixed-length rows
    const uint8_t* src = from.rows_->data();
    uint8_t* dst = rows_->mutable_data() + num_rows_ * metadata_.fixed_length;
    for (uint32_t i = 0; i < num_rows_to_append; ++i) {
      uint16_t row_id = source_row_ids ? source_row_ids[i] : i;
      uint32_t length = metadata_.fixed_length;
      auto src64 = reinterpret_cast<const uint64_t*>(src + length * row_id);
      auto dst64 = reinterpret_cast<uint64_t*>(dst);
      for (uint32_t j = 0; j < bit_util::CeilDiv(length, 8); ++j) {
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
    uint32_t row_id = source_row_ids ? source_row_ids[i] : i;
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
    has_any_nulls_ = !util::bit_util::are_all_bytes_zero(
        ctx->hardware_flags, null_masks() + size_per_row * num_rows_for_has_any_nulls_,
        static_cast<uint32_t>(size_per_row * (num_rows_ - num_rows_for_has_any_nulls_)));
    num_rows_for_has_any_nulls_ = num_rows_;
  }
  return has_any_nulls_;
}

KeyColumnArray KeyEncoder::TransformBoolean::ArrayReplace(const KeyColumnArray& column,
                                                          const KeyColumnArray& temp) {
  // Make sure that the temp buffer is large enough
  DCHECK(temp.length() >= column.length() && temp.metadata().is_fixed_length &&
         temp.metadata().fixed_length >= sizeof(uint8_t));
  KeyColumnMetadata metadata;
  metadata.is_fixed_length = true;
  metadata.fixed_length = sizeof(uint8_t);
  constexpr int buffer_index = 1;
  return column.WithBufferFrom(temp, buffer_index).WithMetadata(metadata);
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

  util::bit_util::bytes_to_bits(
      ctx->hardware_flags, static_cast<int>(input.length()), input.data(buffer_index),
      output->mutable_data(buffer_index), output->bit_offset(buffer_index));
}

bool KeyEncoder::EncoderInteger::IsBoolean(const KeyColumnMetadata& metadata) {
  return metadata.is_fixed_length && metadata.fixed_length == 0 && !metadata.is_null_type;
}

bool KeyEncoder::EncoderInteger::UsesTransform(const KeyColumnArray& column) {
  return IsBoolean(column.metadata());
}

KeyColumnArray KeyEncoder::EncoderInteger::ArrayReplace(const KeyColumnArray& column,
                                                        const KeyColumnArray& temp) {
  if (IsBoolean(column.metadata())) {
    return TransformBoolean::ArrayReplace(column, temp);
  }
  return column;
}

void KeyEncoder::EncoderInteger::PostDecode(const KeyColumnArray& input,
                                            KeyColumnArray* output,
                                            KeyEncoderContext* ctx) {
  if (IsBoolean(output->metadata())) {
    TransformBoolean::PostDecode(input, output, ctx);
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
  if (metadata.is_null_type) {
    return false;
  }
  bool is_fixed_length = metadata.is_fixed_length;
  auto size = metadata.fixed_length;
  return is_fixed_length &&
         (size == 0 || size == 1 || size == 2 || size == 4 || size == 8);
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
void KeyEncoder::EncoderBinary::DecodeImp(uint32_t start_row, uint32_t num_rows,
                                          uint32_t offset_within_row,
                                          const KeyRowArray& rows, KeyColumnArray* col) {
  DecodeHelper<is_row_fixed_length>(
      start_row, num_rows, offset_within_row, &rows, nullptr, col, col,
      [](uint8_t* dst, const uint8_t* src, int64_t length) {
        for (uint32_t istripe = 0; istripe < bit_util::CeilDiv(length, 8); ++istripe) {
          auto dst64 = reinterpret_cast<uint64_t*>(dst);
          auto src64 = reinterpret_cast<const uint64_t*>(src);
          util::SafeStore(dst64 + istripe, src64[istripe]);
        }
      });
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
  int log_col_width1 = col_width1 == 8   ? 3
                       : col_width1 == 4 ? 2
                       : col_width1 == 2 ? 1
                                         : 0;
  int log_col_width2 = col_width2 == 8   ? 3
                       : col_width2 == 4 ? 2
                       : col_width2 == 2 ? 1
                                         : 0;

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
void KeyEncoder::EncoderVarBinary::DecodeImp(uint32_t start_row, uint32_t num_rows,
                                             uint32_t varbinary_col_id,
                                             const KeyRowArray& rows,
                                             KeyColumnArray* col) {
  DecodeHelper<first_varbinary_col>(
      start_row, num_rows, varbinary_col_id, &rows, nullptr, col, col,
      [](uint8_t* dst, const uint8_t* src, int64_t length) {
        for (uint32_t istripe = 0; istripe < bit_util::CeilDiv(length, 8); ++istripe) {
          auto dst64 = reinterpret_cast<uint64_t*>(dst);
          auto src64 = reinterpret_cast<const uint64_t*>(src);
          util::SafeStore(dst64 + istripe, src64[istripe]);
        }
      });
}

void KeyEncoder::EncoderNulls::Decode(uint32_t start_row, uint32_t num_rows,
                                      const KeyRowArray& rows,
                                      std::vector<KeyColumnArray>* cols) {
  // Every output column needs to have a space for exactly the required number
  // of rows. It also needs to have non-nulls bit-vector allocated and mutable.
  DCHECK_GT(cols->size(), 0);
  for (auto& col : *cols) {
    DCHECK(col.length() == num_rows);
    DCHECK(col.mutable_data(0) || col.metadata().is_null_type);
  }

  const uint8_t* null_masks = rows.null_masks();
  uint32_t null_masks_bytes_per_row = rows.metadata().null_masks_bytes_per_row;
  for (size_t col = 0; col < cols->size(); ++col) {
    if ((*cols)[col].metadata().is_null_type) {
      continue;
    }
    uint8_t* non_nulls = (*cols)[col].mutable_data(0);
    const int bit_offset = (*cols)[col].bit_offset(0);
    DCHECK_LT(bit_offset, 8);
    non_nulls[0] |= 0xff << (bit_offset);
    if (bit_offset + num_rows > 8) {
      int bits_in_first_byte = 8 - bit_offset;
      memset(non_nulls + 1, 0xff, bit_util::BytesForBits(num_rows - bits_in_first_byte));
    }
    for (uint32_t row = 0; row < num_rows; ++row) {
      uint32_t null_masks_bit_id =
          (start_row + row) * null_masks_bytes_per_row * 8 + static_cast<uint32_t>(col);
      bool is_set = bit_util::GetBit(null_masks, null_masks_bit_id);
      if (is_set) {
        bit_util::ClearBit(non_nulls, bit_offset + row);
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
  //
  // Columns are sorted based on the size in bytes of their fixed-length part.
  // For the varying-length column, the fixed-length part is the 32-bit field storing
  // cumulative length of varying-length fields.
  //
  // The rules are:
  //
  // a) Boolean column, marked with fixed-length 0, is considered to have fixed-length
  // part of 1 byte.
  //
  // b) Columns with fixed-length part being power of 2 or multiple of row
  // alignment precede other columns. They are sorted in decreasing order of the size of
  // their fixed-length part.
  //
  // c) Fixed-length columns precede varying-length columns when
  // both have the same size fixed-length part.
  //
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
    if (col.is_fixed_length && col.fixed_length != 0 &&
        ARROW_POPCOUNT64(col.fixed_length) != 1) {
      offset_within_row +=
          KeyRowMetadata::padding_for_alignment(offset_within_row, string_alignment, col);
    }
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
    KeyColumnArray col_window = col.Slice(start_row, num_rows);

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
    if (!batch_all_cols_[i].metadata().is_fixed_length ||
        batch_all_cols_[i].metadata().is_null_type) {
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

template <class COPY_FN, class SET_NULL_FN>
void KeyEncoder::EncoderBinary::EncodeSelectedImp(
    uint32_t offset_within_row, KeyRowArray* rows, const KeyColumnArray& col,
    uint32_t num_selected, const uint16_t* selection, COPY_FN copy_fn,
    SET_NULL_FN set_null_fn) {
  bool is_fixed_length = rows->metadata().is_fixed_length;
  if (is_fixed_length) {
    uint32_t row_width = rows->metadata().fixed_length;
    const uint8_t* src_base = col.data(1);
    uint8_t* dst = rows->mutable_data(1) + offset_within_row;
    for (uint32_t i = 0; i < num_selected; ++i) {
      copy_fn(dst, src_base, selection[i]);
      dst += row_width;
    }
    if (col.data(0)) {
      const uint8_t* non_null_bits = col.data(0);
      uint8_t* dst = rows->mutable_data(1) + offset_within_row;
      for (uint32_t i = 0; i < num_selected; ++i) {
        bool is_null = !bit_util::GetBit(non_null_bits, selection[i] + col.bit_offset(0));
        if (is_null) {
          set_null_fn(dst);
        }
        dst += row_width;
      }
    }
  } else {
    const uint8_t* src_base = col.data(1);
    uint8_t* dst = rows->mutable_data(2) + offset_within_row;
    const uint32_t* offsets = rows->offsets();
    for (uint32_t i = 0; i < num_selected; ++i) {
      copy_fn(dst + offsets[i], src_base, selection[i]);
    }
    if (col.data(0)) {
      const uint8_t* non_null_bits = col.data(0);
      uint8_t* dst = rows->mutable_data(2) + offset_within_row;
      const uint32_t* offsets = rows->offsets();
      for (uint32_t i = 0; i < num_selected; ++i) {
        bool is_null = !bit_util::GetBit(non_null_bits, selection[i] + col.bit_offset(0));
        if (is_null) {
          set_null_fn(dst + offsets[i]);
        }
      }
    }
  }
}

void KeyEncoder::EncoderBinary::EncodeSelected(uint32_t offset_within_row,
                                               KeyRowArray* rows,
                                               const KeyColumnArray& col,
                                               uint32_t num_selected,
                                               const uint16_t* selection) {
  if (col.metadata().is_null_type) {
    return;
  }
  uint32_t col_width = col.metadata().fixed_length;
  if (col_width == 0) {
    int bit_offset = col.bit_offset(1);
    EncodeSelectedImp(
        offset_within_row, rows, col, num_selected, selection,
        [bit_offset](uint8_t* dst, const uint8_t* src_base, uint16_t irow) {
          *dst = bit_util::GetBit(src_base, irow + bit_offset) ? 0xff : 0x00;
        },
        [](uint8_t* dst) { *dst = 0xae; });
  } else if (col_width == 1) {
    EncodeSelectedImp(
        offset_within_row, rows, col, num_selected, selection,
        [](uint8_t* dst, const uint8_t* src_base, uint16_t irow) {
          *dst = src_base[irow];
        },
        [](uint8_t* dst) { *dst = 0xae; });
  } else if (col_width == 2) {
    EncodeSelectedImp(
        offset_within_row, rows, col, num_selected, selection,
        [](uint8_t* dst, const uint8_t* src_base, uint16_t irow) {
          *reinterpret_cast<uint16_t*>(dst) =
              reinterpret_cast<const uint16_t*>(src_base)[irow];
        },
        [](uint8_t* dst) { *reinterpret_cast<uint16_t*>(dst) = 0xaeae; });
  } else if (col_width == 4) {
    EncodeSelectedImp(
        offset_within_row, rows, col, num_selected, selection,
        [](uint8_t* dst, const uint8_t* src_base, uint16_t irow) {
          *reinterpret_cast<uint32_t*>(dst) =
              reinterpret_cast<const uint32_t*>(src_base)[irow];
        },
        [](uint8_t* dst) {
          *reinterpret_cast<uint32_t*>(dst) = static_cast<uint32_t>(0xaeaeaeae);
        });
  } else if (col_width == 8) {
    EncodeSelectedImp(
        offset_within_row, rows, col, num_selected, selection,
        [](uint8_t* dst, const uint8_t* src_base, uint16_t irow) {
          *reinterpret_cast<uint64_t*>(dst) =
              reinterpret_cast<const uint64_t*>(src_base)[irow];
        },
        [](uint8_t* dst) { *reinterpret_cast<uint64_t*>(dst) = 0xaeaeaeaeaeaeaeaeULL; });
  } else {
    EncodeSelectedImp(
        offset_within_row, rows, col, num_selected, selection,
        [col_width](uint8_t* dst, const uint8_t* src_base, uint16_t irow) {
          memcpy(dst, src_base + col_width * irow, col_width);
        },
        [col_width](uint8_t* dst) { memset(dst, 0xae, col_width); });
  }
}

void KeyEncoder::EncoderOffsets::GetRowOffsetsSelected(
    KeyRowArray* rows, const std::vector<KeyColumnArray>& cols, uint32_t num_selected,
    const uint16_t* selection) {
  if (rows->metadata().is_fixed_length) {
    return;
  }

  uint32_t* row_offsets = rows->mutable_offsets();
  for (uint32_t i = 0; i < num_selected; ++i) {
    row_offsets[i] = rows->metadata().fixed_length;
  }

  for (size_t icol = 0; icol < cols.size(); ++icol) {
    bool is_fixed_length = (cols[icol].metadata().is_fixed_length);
    if (!is_fixed_length) {
      const uint32_t* col_offsets = cols[icol].offsets();
      for (uint32_t i = 0; i < num_selected; ++i) {
        uint32_t irow = selection[i];
        uint32_t length = col_offsets[irow + 1] - col_offsets[irow];
        row_offsets[i] += KeyRowMetadata::padding_for_alignment(
            row_offsets[i], rows->metadata().string_alignment);
        row_offsets[i] += length;
      }
      const uint8_t* non_null_bits = cols[icol].data(0);
      if (non_null_bits) {
        const uint32_t* col_offsets = cols[icol].offsets();
        for (uint32_t i = 0; i < num_selected; ++i) {
          uint32_t irow = selection[i];
          bool is_null =
              !bit_util::GetBit(non_null_bits, irow + cols[icol].bit_offset(0));
          if (is_null) {
            uint32_t length = col_offsets[irow + 1] - col_offsets[irow];
            row_offsets[i] -= length;
          }
        }
      }
    }
  }

  uint32_t sum = 0;
  int row_alignment = rows->metadata().row_alignment;
  for (uint32_t i = 0; i < num_selected; ++i) {
    uint32_t length = row_offsets[i];
    length += KeyRowMetadata::padding_for_alignment(length, row_alignment);
    row_offsets[i] = sum;
    sum += length;
  }
  row_offsets[num_selected] = sum;
}

template <bool has_nulls, bool is_first_varbinary>
void KeyEncoder::EncoderOffsets::EncodeSelectedImp(
    uint32_t ivarbinary, KeyRowArray* rows, const std::vector<KeyColumnArray>& cols,
    uint32_t num_selected, const uint16_t* selection) {
  const uint32_t* row_offsets = rows->offsets();
  uint8_t* row_base = rows->mutable_data(2) +
                      rows->metadata().varbinary_end_array_offset +
                      ivarbinary * sizeof(uint32_t);
  const uint32_t* col_offsets = cols[ivarbinary].offsets();
  const uint8_t* col_non_null_bits = cols[ivarbinary].data(0);

  for (uint32_t i = 0; i < num_selected; ++i) {
    uint32_t irow = selection[i];
    uint32_t length = col_offsets[irow + 1] - col_offsets[irow];
    if (has_nulls) {
      uint32_t null_multiplier =
          bit_util::GetBit(col_non_null_bits, irow + cols[ivarbinary].bit_offset(0)) ? 1
                                                                                     : 0;
      length *= null_multiplier;
    }
    uint32_t* row = reinterpret_cast<uint32_t*>(row_base + row_offsets[i]);
    if (is_first_varbinary) {
      row[0] = rows->metadata().fixed_length + length;
    } else {
      row[0] = row[-1] +
               KeyRowMetadata::padding_for_alignment(row[-1],
                                                     rows->metadata().string_alignment) +
               length;
    }
  }
}

void KeyEncoder::EncoderOffsets::EncodeSelected(KeyRowArray* rows,
                                                const std::vector<KeyColumnArray>& cols,
                                                uint32_t num_selected,
                                                const uint16_t* selection) {
  if (rows->metadata().is_fixed_length) {
    return;
  }
  uint32_t ivarbinary = 0;
  for (size_t icol = 0; icol < cols.size(); ++icol) {
    if (!cols[icol].metadata().is_fixed_length) {
      const uint8_t* non_null_bits = cols[icol].data(0);
      if (non_null_bits && ivarbinary == 0) {
        EncodeSelectedImp<true, true>(ivarbinary, rows, cols, num_selected, selection);
      } else if (non_null_bits && ivarbinary > 0) {
        EncodeSelectedImp<true, false>(ivarbinary, rows, cols, num_selected, selection);
      } else if (!non_null_bits && ivarbinary == 0) {
        EncodeSelectedImp<false, true>(ivarbinary, rows, cols, num_selected, selection);
      } else {
        EncodeSelectedImp<false, false>(ivarbinary, rows, cols, num_selected, selection);
      }
      ivarbinary++;
    }
  }
}

void KeyEncoder::EncoderVarBinary::EncodeSelected(uint32_t ivarbinary, KeyRowArray* rows,
                                                  const KeyColumnArray& cols,
                                                  uint32_t num_selected,
                                                  const uint16_t* selection) {
  const uint32_t* row_offsets = rows->offsets();
  uint8_t* row_base = rows->mutable_data(2);
  const uint32_t* col_offsets = cols.offsets();
  const uint8_t* col_base = cols.data(2);

  if (ivarbinary == 0) {
    for (uint32_t i = 0; i < num_selected; ++i) {
      uint8_t* row = row_base + row_offsets[i];
      uint32_t row_offset;
      uint32_t length;
      rows->metadata().first_varbinary_offset_and_length(row, &row_offset, &length);
      uint32_t irow = selection[i];
      memcpy(row + row_offset, col_base + col_offsets[irow], length);
    }
  } else {
    for (uint32_t i = 0; i < num_selected; ++i) {
      uint8_t* row = row_base + row_offsets[i];
      uint32_t row_offset;
      uint32_t length;
      rows->metadata().nth_varbinary_offset_and_length(row, ivarbinary, &row_offset,
                                                       &length);
      uint32_t irow = selection[i];
      memcpy(row + row_offset, col_base + col_offsets[irow], length);
    }
  }
}

void KeyEncoder::EncoderNulls::EncodeSelected(KeyRowArray* rows,
                                              const std::vector<KeyColumnArray>& cols,
                                              uint32_t num_selected,
                                              const uint16_t* selection) {
  uint8_t* null_masks = rows->null_masks();
  uint32_t null_mask_num_bytes = rows->metadata().null_masks_bytes_per_row;
  memset(null_masks, 0, null_mask_num_bytes * num_selected);
  for (size_t icol = 0; icol < cols.size(); ++icol) {
    const uint8_t* non_null_bits = cols[icol].data(0);
    if (non_null_bits) {
      for (uint32_t i = 0; i < num_selected; ++i) {
        uint32_t irow = selection[i];
        bool is_null = !bit_util::GetBit(non_null_bits, irow + cols[icol].bit_offset(0));
        if (is_null) {
          bit_util::SetBit(null_masks, i * null_mask_num_bytes * 8 + icol);
        }
      }
    }
  }
}

void KeyEncoder::PrepareEncodeSelected(int64_t start_row, int64_t num_rows,
                                       const std::vector<KeyColumnArray>& cols) {
  // Prepare column array vectors
  PrepareKeyColumnArrays(start_row, num_rows, cols);
}

Status KeyEncoder::EncodeSelected(KeyRowArray* rows, uint32_t num_selected,
                                  const uint16_t* selection) {
  rows->Clean();
  RETURN_NOT_OK(
      rows->AppendEmpty(static_cast<uint32_t>(num_selected), static_cast<uint32_t>(0)));

  EncoderOffsets::GetRowOffsetsSelected(rows, batch_varbinary_cols_, num_selected,
                                        selection);

  RETURN_NOT_OK(rows->AppendEmpty(static_cast<uint32_t>(0),
                                  static_cast<uint32_t>(rows->offsets()[num_selected])));

  for (size_t icol = 0; icol < batch_all_cols_.size(); ++icol) {
    if (batch_all_cols_[icol].metadata().is_fixed_length) {
      uint32_t offset_within_row = rows->metadata().column_offsets[icol];
      EncoderBinary::EncodeSelected(offset_within_row, rows, batch_all_cols_[icol],
                                    num_selected, selection);
    }
  }

  EncoderOffsets::EncodeSelected(rows, batch_varbinary_cols_, num_selected, selection);

  for (size_t icol = 0; icol < batch_varbinary_cols_.size(); ++icol) {
    EncoderVarBinary::EncodeSelected(static_cast<uint32_t>(icol), rows,
                                     batch_varbinary_cols_[icol], num_selected,
                                     selection);
  }

  EncoderNulls::EncodeSelected(rows, batch_all_cols_, num_selected, selection);

  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

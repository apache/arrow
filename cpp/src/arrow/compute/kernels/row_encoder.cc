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

#include "arrow/compute/kernels/row_encoder.h"

#include "arrow/util/bitmap_writer.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

namespace arrow {

using internal::FirstTimeBitmapWriter;

namespace compute {
namespace internal {

// extract the null bitmap from the leading nullity bytes of encoded keys
Status KeyEncoder::DecodeNulls(MemoryPool* pool, int32_t length, uint8_t** encoded_bytes,
                               std::shared_ptr<Buffer>* null_bitmap,
                               int32_t* null_count) {
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

void BooleanKeyEncoder::AddLength(const Datum& data, int64_t batch_length,
                                  int32_t* lengths) {
  for (int64_t i = 0; i < batch_length; ++i) {
    lengths[i] += kByteWidth + kExtraByteForNull;
  }
}

void BooleanKeyEncoder::AddLengthNull(int32_t* length) {
  *length += kByteWidth + kExtraByteForNull;
}

Status BooleanKeyEncoder::Encode(const Datum& data, int64_t batch_length,
                                 uint8_t** encoded_bytes) {
  if (data.is_array()) {
    VisitArrayDataInline<BooleanType>(
        *data.array(),
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
  } else {
    const auto& scalar = data.scalar_as<BooleanScalar>();
    bool value = scalar.is_valid && scalar.value;
    for (int64_t i = 0; i < batch_length; i++) {
      auto& encoded_ptr = *encoded_bytes++;
      *encoded_ptr++ = kValidByte;
      *encoded_ptr++ = value;
    }
  }
  return Status::OK();
}

void BooleanKeyEncoder::EncodeNull(uint8_t** encoded_bytes) {
  auto& encoded_ptr = *encoded_bytes;
  *encoded_ptr++ = kNullByte;
  *encoded_ptr++ = 0;
}

Result<std::shared_ptr<ArrayData>> BooleanKeyEncoder::Decode(uint8_t** encoded_bytes,
                                                             int32_t length,
                                                             MemoryPool* pool) {
  std::shared_ptr<Buffer> null_buf;
  int32_t null_count;
  RETURN_NOT_OK(DecodeNulls(pool, length, encoded_bytes, &null_buf, &null_count));

  ARROW_ASSIGN_OR_RAISE(auto key_buf, AllocateBitmap(length, pool));

  uint8_t* raw_output = key_buf->mutable_data();
  memset(raw_output, 0, BitUtil::BytesForBits(length));
  for (int32_t i = 0; i < length; ++i) {
    auto& encoded_ptr = encoded_bytes[i];
    BitUtil::SetBitTo(raw_output, i, encoded_ptr[0] != 0);
    encoded_ptr += 1;
  }

  return ArrayData::Make(boolean(), length, {std::move(null_buf), std::move(key_buf)},
                         null_count);
}

void FixedWidthKeyEncoder::AddLength(const Datum& data, int64_t batch_length,
                                     int32_t* lengths) {
  for (int64_t i = 0; i < batch_length; ++i) {
    lengths[i] += byte_width_ + kExtraByteForNull;
  }
}

void FixedWidthKeyEncoder::AddLengthNull(int32_t* length) {
  *length += byte_width_ + kExtraByteForNull;
}

Status FixedWidthKeyEncoder::Encode(const Datum& data, int64_t batch_length,
                                    uint8_t** encoded_bytes) {
  if (data.is_array()) {
    const auto& arr = *data.array();
    ArrayData viewed(fixed_size_binary(byte_width_), arr.length, arr.buffers,
                     arr.null_count, arr.offset);

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
  } else {
    const auto& scalar = data.scalar_as<arrow::internal::PrimitiveScalarBase>();
    if (scalar.is_valid) {
      const util::string_view data = scalar.view();
      DCHECK_EQ(data.size(), static_cast<size_t>(byte_width_));
      for (int64_t i = 0; i < batch_length; i++) {
        auto& encoded_ptr = *encoded_bytes++;
        *encoded_ptr++ = kValidByte;
        memcpy(encoded_ptr, data.data(), data.size());
        encoded_ptr += byte_width_;
      }
    } else {
      for (int64_t i = 0; i < batch_length; i++) {
        auto& encoded_ptr = *encoded_bytes++;
        *encoded_ptr++ = kNullByte;
        memset(encoded_ptr, 0, byte_width_);
        encoded_ptr += byte_width_;
      }
    }
  }
  return Status::OK();
}

void FixedWidthKeyEncoder::EncodeNull(uint8_t** encoded_bytes) {
  auto& encoded_ptr = *encoded_bytes;
  *encoded_ptr++ = kNullByte;
  memset(encoded_ptr, 0, byte_width_);
  encoded_ptr += byte_width_;
}

Result<std::shared_ptr<ArrayData>> FixedWidthKeyEncoder::Decode(uint8_t** encoded_bytes,
                                                                int32_t length,
                                                                MemoryPool* pool) {
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

Status DictionaryKeyEncoder::Encode(const Datum& data, int64_t batch_length,
                                    uint8_t** encoded_bytes) {
  auto dict = data.is_array() ? MakeArray(data.array()->dictionary)
                              : data.scalar_as<DictionaryScalar>().value.dictionary;
  if (dictionary_) {
    if (!dictionary_->Equals(dict)) {
      // TODO(bkietz) unify if necessary. For now, just error if any batch's dictionary
      // differs from the first we saw for this key
      return Status::NotImplemented("Unifying differing dictionaries");
    }
  } else {
    dictionary_ = std::move(dict);
  }
  if (data.is_array()) {
    return FixedWidthKeyEncoder::Encode(data, batch_length, encoded_bytes);
  }
  return FixedWidthKeyEncoder::Encode(data.scalar_as<DictionaryScalar>().value.index,
                                      batch_length, encoded_bytes);
}

Result<std::shared_ptr<ArrayData>> DictionaryKeyEncoder::Decode(uint8_t** encoded_bytes,
                                                                int32_t length,
                                                                MemoryPool* pool) {
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

void RowEncoder::Init(const std::vector<ValueDescr>& column_types, ExecContext* ctx) {
  ctx_ = ctx;
  encoders_.resize(column_types.size());

  for (size_t i = 0; i < column_types.size(); ++i) {
    const auto& column_type = column_types[i].type;

    if (column_type->id() == Type::BOOL) {
      encoders_[i] = std::make_shared<BooleanKeyEncoder>();
      continue;
    }

    if (column_type->id() == Type::DICTIONARY) {
      encoders_[i] =
          std::make_shared<DictionaryKeyEncoder>(column_type, ctx->memory_pool());
      continue;
    }

    if (is_fixed_width(column_type->id())) {
      encoders_[i] = std::make_shared<FixedWidthKeyEncoder>(column_type);
      continue;
    }

    if (is_binary_like(column_type->id())) {
      encoders_[i] = std::make_shared<VarLengthKeyEncoder<BinaryType>>(column_type);
      continue;
    }

    if (is_large_binary_like(column_type->id())) {
      encoders_[i] = std::make_shared<VarLengthKeyEncoder<LargeBinaryType>>(column_type);
      continue;
    }

    // We should not get here
    ARROW_DCHECK(false);
  }

  int32_t total_length = 0;
  for (size_t i = 0; i < column_types.size(); ++i) {
    encoders_[i]->AddLengthNull(&total_length);
  }
  encoded_nulls_.resize(total_length);
  uint8_t* buf_ptr = encoded_nulls_.data();
  for (size_t i = 0; i < column_types.size(); ++i) {
    encoders_[i]->EncodeNull(&buf_ptr);
  }
}

void RowEncoder::Clear() {
  offsets_.clear();
  bytes_.clear();
}

Status RowEncoder::EncodeAndAppend(const ExecBatch& batch) {
  if (offsets_.empty()) {
    offsets_.resize(1);
    offsets_[0] = 0;
  }
  size_t length_before = offsets_.size() - 1;
  offsets_.resize(length_before + batch.length + 1);
  for (int64_t i = 0; i < batch.length; ++i) {
    offsets_[length_before + 1 + i] = 0;
  }

  for (int i = 0; i < batch.num_values(); ++i) {
    encoders_[i]->AddLength(batch[i], batch.length, offsets_.data() + length_before + 1);
  }

  int32_t total_length = offsets_[length_before];
  for (int64_t i = 0; i < batch.length; ++i) {
    total_length += offsets_[length_before + 1 + i];
    offsets_[length_before + 1 + i] = total_length;
  }

  bytes_.resize(total_length);
  std::vector<uint8_t*> buf_ptrs(batch.length);
  for (int64_t i = 0; i < batch.length; ++i) {
    buf_ptrs[i] = bytes_.data() + offsets_[length_before + i];
  }

  for (int i = 0; i < batch.num_values(); ++i) {
    RETURN_NOT_OK(encoders_[i]->Encode(batch[i], batch.length, buf_ptrs.data()));
  }

  return Status::OK();
}

Result<ExecBatch> RowEncoder::Decode(int64_t num_rows, const int32_t* row_ids) {
  ExecBatch out({}, num_rows);

  std::vector<uint8_t*> buf_ptrs(num_rows);
  for (int64_t i = 0; i < num_rows; ++i) {
    buf_ptrs[i] = (row_ids[i] == kRowIdForNulls()) ? encoded_nulls_.data()
                                                   : bytes_.data() + offsets_[row_ids[i]];
  }

  out.values.resize(encoders_.size());
  for (size_t i = 0; i < encoders_.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(
        out.values[i],
        encoders_[i]->Decode(buf_ptrs.data(), static_cast<int32_t>(num_rows),
                             ctx_->memory_pool()));
  }

  return out;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

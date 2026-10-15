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

#include "arrow/array/buffer_truncate.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"

namespace arrow {
namespace array {

namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

using bit_util::RoundUpToMultipleOf8;
using internal::CopyBitmap;
using internal::checked_cast;

// PaddedLength is defined in arrow/ipc/util.h; replicate here to avoid
// pulling the entire IPC header into an array utility.
static inline int64_t PaddedLength(int64_t nbytes) {
  return RoundUpToMultipleOf8(nbytes);
}

static inline bool NeedTruncate(int64_t offset, const Buffer* buffer,
                                int64_t min_length) {
  if (buffer == nullptr) return false;
  return offset != 0 || min_length < buffer->size();
}

/// Copy a bitmap and shift it so that `offset` becomes bit 0, then truncate
/// to `length` bits.  If offset == 0 and the bitmap is already sized
/// correctly, returns the input buffer as-is.
Result<std::shared_ptr<Buffer>> TruncateValidityBitmap(
    int64_t offset, int64_t length, const std::shared_ptr<Buffer>& input,
    MemoryPool* pool) {
  if (!input) return input;
  const int64_t min_length = PaddedLength(bit_util::BytesForBits(length));
  if (offset != 0 || min_length < input->size()) {
    ARROW_ASSIGN_OR_RAISE(auto out,
                          CopyBitmap(pool, input->data(), offset, length));
    return out;
  }
  return input;
}

/// Slice a fixed-width buffer (byte_width elements per item) to cover only
/// the range [offset, offset + length).  Returns the input if no truncation
/// is needed.
Result<std::shared_ptr<Buffer>> TruncateFixedWidthBuffer(
    int64_t offset, int64_t length, int32_t byte_width,
    const std::shared_ptr<Buffer>& input, MemoryPool* pool) {
  ARROW_UNUSED(pool);  // only used by CopyBitmap, not needed here
  if (!input) return input;
  const int64_t padded = PaddedLength(length * byte_width);
  if (offset != 0 || padded < input->size()) {
    return SliceBuffer(input, offset * byte_width,
                       std::min(padded, input->size() - offset * byte_width));
  }
  return input;
}

// ---------------------------------------------------------------------------
// Build zero-based value offsets for variable-width types (Binary/String/List).
// ---------------------------------------------------------------------------
template <typename offset_type>
Result<std::shared_ptr<Buffer>> GetZeroBasedValueOffsets(
    const std::shared_ptr<Buffer>& offsets, int64_t array_offset,
    int64_t array_length, MemoryPool* pool) {
  const int64_t required_bytes = sizeof(offset_type) * (array_length + 1);
  if (array_offset != 0) {
    // Rebase offsets to start at zero
    const offset_type* src = reinterpret_cast<const offset_type*>(offsets->data());
    offset_type first = src[array_offset];

    ARROW_ASSIGN_OR_RAISE(auto out, AllocateBuffer(required_bytes, pool));
    offset_type* dst = reinterpret_cast<offset_type*>(out->mutable_data());
    for (int64_t i = 0; i <= array_length; ++i) {
      dst[i] = src[array_offset + i] - first;
    }
    return out;
  }

  // Slice if larger than required
  if (offsets && offsets->size() > required_bytes) {
    return SliceBuffer(offsets, 0, required_bytes);
  }
  return offsets;
}

// ---------------------------------------------------------------------------
// Truncation per type id
// ---------------------------------------------------------------------------

Status TruncateBuffersImpl(const std::shared_ptr<ArrayData>& data,
                           std::shared_ptr<ArrayData>* out,
                           MemoryPool* pool) {
  DCHECK(data != nullptr);
  const auto& type = data->type;
  const Type::type id = type->id();
  const int64_t offset = data->offset;
  const int64_t length = data->length;

  // Buffers are laid out as [validity, data_or_offsets_or_type_codes, ...]
  // Buffer 0 is the validity bitmap for types that have one.
  auto buffers = data->buffers;
  auto child_data = data->child_data;
  std::shared_ptr<ArrayData> dictionary = data->dictionary;

  // --- Validity bitmap (buffer 0) -----------------------------------------
  // Null, dense-union, and sparse-union have no validity bitmap at the
  // ArrayData level (validity lives inside each child array for unions).
  const bool has_validity =
      (buffers.size() > 0 && buffers[0] &&
       (id != Type::NA && id != Type::DENSE_UNION && id != Type::SPARSE_UNION &&
        id != Type::RUN_END_ENCODED));
  if (has_validity) {
    ARROW_ASSIGN_OR_RAISE(
        buffers[0],
        TruncateValidityBitmap(offset, length, buffers[0], pool));
  }

  switch (id) {
    case Type::NA:
    case Type::BOOL: {
      // BOOL buffer 1 is the actual bit-packed values buffer (not the
      // validity bitmap, which is buffer 0).  We need to shift & copy it
      // just like the validity bitmap.
      if (id == Type::BOOL && buffers.size() > 1 && buffers[1]) {
        ARROW_ASSIGN_OR_RAISE(
            buffers[1], TruncateValidityBitmap(offset, length, buffers[1], pool));
      }
      break;
    }

#define TRUNCATE_FIXED_WIDTH(TYPE_ENUM)                                          \
  case TYPE_ENUM: {                                                              \
    const int64_t byte_width = type->byte_width();                               \
    if (buffers.size() > 1 && buffers[1]) {                                      \
      ARROW_ASSIGN_OR_RAISE(                                                     \
          buffers[1],                                                            \
          TruncateFixedWidthBuffer(offset, length,                               \
                                   static_cast<int32_t>(byte_width),             \
                                   buffers[1], pool));                           \
    }                                                                            \
    break;                                                                       \
  }

    TRUNCATE_FIXED_WIDTH(Type::INT8)
    TRUNCATE_FIXED_WIDTH(Type::INT16)
    TRUNCATE_FIXED_WIDTH(Type::INT32)
    TRUNCATE_FIXED_WIDTH(Type::INT64)
    TRUNCATE_FIXED_WIDTH(Type::UINT8)
    TRUNCATE_FIXED_WIDTH(Type::UINT16)
    TRUNCATE_FIXED_WIDTH(Type::UINT32)
    TRUNCATE_FIXED_WIDTH(Type::UINT64)
    TRUNCATE_FIXED_WIDTH(Type::HALF_FLOAT)
    TRUNCATE_FIXED_WIDTH(Type::FLOAT)
    TRUNCATE_FIXED_WIDTH(Type::DOUBLE)
    TRUNCATE_FIXED_WIDTH(Type::DATE32)
    TRUNCATE_FIXED_WIDTH(Type::DATE64)
    TRUNCATE_FIXED_WIDTH(Type::TIME32)
    TRUNCATE_FIXED_WIDTH(Type::TIME64)
    TRUNCATE_FIXED_WIDTH(Type::TIMESTAMP)
    TRUNCATE_FIXED_WIDTH(Type::INTERVAL_MONTHS)
    TRUNCATE_FIXED_WIDTH(Type::INTERVAL_DAY_TIME)
    TRUNCATE_FIXED_WIDTH(Type::INTERVAL_MONTH_DAY_NANO)
    TRUNCATE_FIXED_WIDTH(Type::DURATION)
    TRUNCATE_FIXED_WIDTH(Type::DECIMAL128)
    TRUNCATE_FIXED_WIDTH(Type::DECIMAL256)
    TRUNCATE_FIXED_WIDTH(Type::FIXED_SIZE_BINARY)
    TRUNCATE_FIXED_WIDTH(Type::EXTENSION)  // extension reuses storage buffers

    // -----------------------------------------------------------------------
    // Variable-width: offsets (buffer 1) + data (buffer 2)
    // -----------------------------------------------------------------------
    case Type::STRING:
    case Type::BINARY: {
      // int32 offsets
      if (buffers.size() < 3) break;
      auto& offsets_buf = buffers[1];
      auto& data_buf = buffers[2];

      ARROW_ASSIGN_OR_RAISE(
          offsets_buf,
          GetZeroBasedValueOffsets<int32_t>(offsets_buf, offset, length, pool));

      int64_t total_data_bytes = 0;
      if (offsets_buf && length > 0) {
        const int32_t* off =
            reinterpret_cast<const int32_t*>(offsets_buf->data());
        total_data_bytes = off[length];
      }

      if (data_buf && NeedTruncate(0, data_buf.get(), total_data_bytes)) {
        data_buf = SliceBuffer(data_buf, 0,
                               std::min(static_cast<int64_t>(
                                            PaddedLength(total_data_bytes)),
                                        data_buf->size()));
      }
      break;
    }
    case Type::LARGE_STRING:
    case Type::LARGE_BINARY: {
      // int64 offsets
      if (buffers.size() < 3) break;
      auto& offsets_buf = buffers[1];
      auto& data_buf = buffers[2];

      ARROW_ASSIGN_OR_RAISE(
          offsets_buf,
          GetZeroBasedValueOffsets<int64_t>(offsets_buf, offset, length, pool));

      int64_t total_data_bytes = 0;
      if (offsets_buf && length > 0) {
        const int64_t* off =
            reinterpret_cast<const int64_t*>(offsets_buf->data());
        total_data_bytes = off[length];
      }

      if (data_buf && NeedTruncate(0, data_buf.get(), total_data_bytes)) {
        data_buf = SliceBuffer(data_buf, 0,
                               std::min(static_cast<int64_t>(
                                            PaddedLength(total_data_bytes)),
                                        data_buf->size()));
      }
      break;
    }

    // -----------------------------------------------------------------------
    // Variable-width list: offsets (buffer 1) + child array (child_data[0])
    // -----------------------------------------------------------------------
    case Type::LIST: {
      if (buffers.size() < 2 || child_data.empty()) break;

      auto& offsets_buf = buffers[1];
      ARROW_ASSIGN_OR_RAISE(
          offsets_buf,
          GetZeroBasedValueOffsets<int32_t>(offsets_buf, offset, length, pool));

      int32_t child_offset = 0;
      int32_t child_length = 0;
      if (offsets_buf && length > 0) {
        const int32_t* off =
            reinterpret_cast<const int32_t*>(offsets_buf->data());
        child_offset = off[0];
        child_length = off[length] - off[0];
      }

      auto child = child_data[0];
      auto child_adjusted = child->Copy();
      child_adjusted->offset += child_offset;
      child_adjusted->length = child_length;
      std::shared_ptr<ArrayData> truncated_child;
      RETURN_NOT_OK(
          TruncateBuffersImpl(child_adjusted, &truncated_child, pool));
      child_data[0] = std::move(truncated_child);
      break;
    }
    case Type::LARGE_LIST: {
      if (buffers.size() < 2 || child_data.empty()) break;

      auto& offsets_buf = buffers[1];
      ARROW_ASSIGN_OR_RAISE(
          offsets_buf,
          GetZeroBasedValueOffsets<int64_t>(offsets_buf, offset, length, pool));

      int64_t child_offset = 0;
      int64_t child_length = 0;
      if (offsets_buf && length > 0) {
        const int64_t* off =
            reinterpret_cast<const int64_t*>(offsets_buf->data());
        child_offset = off[0];
        child_length = off[length] - off[0];
      }

      auto child = child_data[0];
      auto child_adjusted = child->Copy();
      child_adjusted->offset += child_offset;
      child_adjusted->length = child_length;
      std::shared_ptr<ArrayData> truncated_child;
      RETURN_NOT_OK(
          TruncateBuffersImpl(child_adjusted, &truncated_child, pool));
      child_data[0] = std::move(truncated_child);
      break;
    }

    // -----------------------------------------------------------------------
    // Fixed-size list
    // -----------------------------------------------------------------------
    case Type::FIXED_SIZE_LIST: {
      if (child_data.empty()) break;
      const auto& list_type = checked_cast<const FixedSizeListType&>(*type);
      const int32_t list_size = list_type.list_size();

      auto child = child_data[0];
      auto child_adjusted = child->Copy();
      child_adjusted->offset += offset * list_size;
      child_adjusted->length = length * list_size;
      std::shared_ptr<ArrayData> truncated_child;
      RETURN_NOT_OK(
          TruncateBuffersImpl(child_adjusted, &truncated_child, pool));
      child_data[0] = std::move(truncated_child);
      break;
    }

    // -----------------------------------------------------------------------
    // Struct: just recurse into children
    // -----------------------------------------------------------------------
    case Type::STRUCT: {
      for (size_t i = 0; i < child_data.size(); ++i) {
        auto child = child_data[i];
        auto child_adjusted = child->Copy();
        child_adjusted->offset += offset;
        child_adjusted->length = length;
        std::shared_ptr<ArrayData> truncated;
        RETURN_NOT_OK(TruncateBuffersImpl(child_adjusted, &truncated, pool));
        child_data[i] = std::move(truncated);
      }
      break;
    }

    // -----------------------------------------------------------------------
    // Sparse union: type_codes buffer + children (no offsets buffer)
    // -----------------------------------------------------------------------
    case Type::SPARSE_UNION: {
      if (buffers.size() > 1 && buffers[1]) {
        ARROW_ASSIGN_OR_RAISE(
            buffers[1],
            TruncateFixedWidthBuffer(
                offset, length,
                static_cast<int32_t>(sizeof(int8_t)),
                buffers[1], pool));
      }
      for (size_t i = 0; i < child_data.size(); ++i) {
        auto child = child_data[i];
        auto child_adjusted = child->Copy();
        child_adjusted->offset += offset;
        child_adjusted->length = length;
        std::shared_ptr<ArrayData> truncated;
        RETURN_NOT_OK(TruncateBuffersImpl(child_adjusted, &truncated, pool));
        child_data[i] = std::move(truncated);
      }
      break;
    }

    // -----------------------------------------------------------------------
    // Dense union: type_codes buffer + offsets buffer + children
    // -----------------------------------------------------------------------
    case Type::DENSE_UNION: {
      if (buffers.size() > 1 && buffers[1]) {
        ARROW_ASSIGN_OR_RAISE(
            buffers[1],
            TruncateFixedWidthBuffer(
                offset, length,
                static_cast<int32_t>(sizeof(int8_t)),
                buffers[1], pool));
      }
      if (buffers.size() > 2 && buffers[2]) {
        ARROW_ASSIGN_OR_RAISE(
            buffers[2],
            TruncateFixedWidthBuffer(
                offset, length, static_cast<int32_t>(sizeof(int32_t)),
                buffers[2], pool));
      }
      // Children: per-child offsets are tricky for sliced dense unions.
      // For simplicity we only adjust offset/length to parent's offset/length;
      // IPC writer does a more involved rebase which we replicate below.
      if (buffers.size() > 2 && buffers[2] && offset != 0) {
        const int8_t* type_codes = reinterpret_cast<const int8_t*>(
            buffers[1]->data() + offset * sizeof(int8_t));
        const int32_t* value_offsets = reinterpret_cast<const int32_t*>(
            buffers[2]->data() + offset * sizeof(int32_t));

        const auto& union_type = checked_cast<const DenseUnionType&>(*type);
        int8_t max_code = 0;
        for (int8_t c : union_type.type_codes()) max_code = std::max(max_code, c);

        std::vector<int32_t> child_offsets(max_code + 1, -1);
        std::vector<int32_t> child_lengths(max_code + 1, 0);
        for (int64_t i = 0; i < length; ++i) {
          int8_t code = type_codes[i];
          if (child_offsets[code] == -1) {
            child_offsets[code] = value_offsets[i];
          }
          child_lengths[code] =
              std::max(child_lengths[code], value_offsets[i] + 1 - child_offsets[code]);
        }

        for (size_t i = 0; i < child_data.size(); ++i) {
          const int8_t code = union_type.type_codes()[i];
          auto child = child_data[i];
          auto child_adjusted = child->Copy();
          if (child_offsets[code] > 0) {
            child_adjusted->offset += child_offsets[code];
          }
          child_adjusted->length = child_lengths[code];
          std::shared_ptr<ArrayData> truncated;
          RETURN_NOT_OK(TruncateBuffersImpl(child_adjusted, &truncated, pool));
          child_data[i] = std::move(truncated);
        }
      } else {
        for (size_t i = 0; i < child_data.size(); ++i) {
          auto child = child_data[i];
          auto child_adjusted = child->Copy();
          child_adjusted->offset += offset;
          child_adjusted->length = length;
          std::shared_ptr<ArrayData> truncated;
          RETURN_NOT_OK(TruncateBuffersImpl(child_adjusted, &truncated, pool));
          child_data[i] = std::move(truncated);
        }
      }
      break;
    }

    // -----------------------------------------------------------------------
    // Dictionary: index array is child_data[0], dictionary is in data itself
    // -----------------------------------------------------------------------
    case Type::DICTIONARY: {
      // Truncate indices (child_data[0])
      if (!child_data.empty()) {
        auto child = child_data[0];
        auto child_adjusted = child->Copy();
        child_adjusted->offset += offset;
        child_adjusted->length = length;
        std::shared_ptr<ArrayData> truncated;
        RETURN_NOT_OK(TruncateBuffersImpl(child_adjusted, &truncated, pool));
        child_data[0] = std::move(truncated);
      }
      // NOTE: we intentionally do NOT truncate the dictionary values array
      // here — dictionaries are shared, and truncating them would break
      // equality semantics when multiple sliced DictionaryArrays reference
      // the same dictionary.  (That is what IPC writer does too; it writes
      // the dictionary separately and slices only the indices.)
      break;
    }

    // -----------------------------------------------------------------------
    // Run-end encoded
    // -----------------------------------------------------------------------
    case Type::RUN_END_ENCODED: {
      // buffers[0] may be the validity bitmap; child_data[0] is run_ends,
      // child_data[1] is values.  We truncate the logical view the same way
      // IPC writer does.
      if (child_data.size() >= 2) {
        // Truncate values by logical offset/length
        auto values = child_data[1];
        auto values_adjusted = values->Copy();
        values_adjusted->offset += offset;
        values_adjusted->length = length;
        std::shared_ptr<ArrayData> truncated_values;
        RETURN_NOT_OK(
            TruncateBuffersImpl(values_adjusted, &truncated_values, pool));
        child_data[1] = std::move(truncated_values);
      }
      break;
    }

    case Type::STRING_VIEW:
    case Type::BINARY_VIEW: {
      // views buffer (buffer 1) is fixed-size (16 bytes each)
      if (buffers.size() > 1 && buffers[1]) {
        ARROW_ASSIGN_OR_RAISE(
            buffers[1],
            TruncateFixedWidthBuffer(offset, length,
                                     static_cast<int32_t>(16),
                                     buffers[1], pool));
      }
      // variadic data buffers (buffers[2..]) are not truncated — they hold
      // all the inlined + external bytes and are tricky to size-truncate
      // without a full copy.  Leave as-is.
      break;
    }

    default:
      // Unknown type: nothing we can safely do, leave unchanged.
      break;
  }

  // Build the output.  Note: offset and length are preserved from the
  // original data — but the buffers/children/dictionary now point at
  // properly-truncated backing storage.
  // null_count: if we rebased the offset (e.g. via CopyBitmap or offsets
  // rebase), the null count of the truncated range is what we want.  We
  // compute it lazily by leaving null_count as kUnknownNullCount here so
  // that it gets recomputed on demand.  (IPC writer computes it explicitly
  // via the visitor, but for pickle we can afford to let it recompute.)
  auto result = ArrayData::Make(type,
                                /*length=*/length,
                                std::move(buffers),
                                std::move(child_data),
                                std::move(dictionary),
                                /*null_count=*/kUnknownNullCount,
                                /*offset=*/0);  // offset reset to 0 since we
                                                // already sliced everything!
  *out = std::move(result);
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<ArrayData>> TruncateBuffers(
    std::shared_ptr<ArrayData> data, MemoryPool* pool) {
  if (!pool) pool = default_memory_pool();
  std::shared_ptr<ArrayData> out;
  RETURN_NOT_OK(TruncateBuffersImpl(std::move(data), &out, pool));
  return out;
}

}  // namespace array
}  // namespace arrow

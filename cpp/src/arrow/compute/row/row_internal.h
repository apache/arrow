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
#pragma once

#include <cstdint>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/compute/light_array.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

/// Description of the data stored in a RowTable
struct ARROW_EXPORT RowTableMetadata {
  /// \brief True if there are no variable length columns in the table
  bool is_fixed_length;

  /// For a fixed-length binary row, common size of rows in bytes,
  /// rounded up to the multiple of alignment.
  ///
  /// For a varying-length binary, size of all encoded fixed-length key columns,
  /// including lengths of varying-length columns, rounded up to the multiple of string
  /// alignment.
  uint32_t fixed_length;

  /// Offset within a row to the array of 32-bit offsets within a row of
  /// ends of varbinary fields.
  /// Used only when the row is not fixed-length, zero for fixed-length row.
  /// There are N elements for N varbinary fields.
  /// Each element is the offset within a row of the first byte after
  /// the corresponding varbinary field bytes in that row.
  /// If varbinary fields begin at aligned addresses, than the end of the previous
  /// varbinary field needs to be rounded up according to the specified alignment
  /// to obtain the beginning of the next varbinary field.
  /// The first varbinary field starts at offset specified by fixed_length,
  /// which should already be aligned.
  uint32_t varbinary_end_array_offset;

  /// Fixed number of bytes per row that are used to encode null masks.
  /// Null masks indicate for a single row which of its columns are null.
  /// Nth bit in the sequence of bytes assigned to a row represents null
  /// information for Nth field according to the order in which they are encoded.
  int null_masks_bytes_per_row;

  /// Power of 2. Every row will start at an offset aligned to that number of bytes.
  int row_alignment;

  /// Power of 2. Must be no greater than row alignment.
  /// Every non-power-of-2 binary field and every varbinary field bytes
  /// will start aligned to that number of bytes.
  int string_alignment;

  /// Metadata of encoded columns in their original order.
  std::vector<KeyColumnMetadata> column_metadatas;

  /// Order in which fields are encoded.
  std::vector<uint32_t> column_order;

  /// Offsets within a row to fields in their encoding order.
  std::vector<uint32_t> column_offsets;

  /// Rounding up offset to the nearest multiple of alignment value.
  /// Alignment must be a power of 2.
  static inline uint32_t padding_for_alignment(uint32_t offset, int required_alignment) {
    ARROW_DCHECK(ARROW_POPCOUNT64(required_alignment) == 1);
    return static_cast<uint32_t>((-static_cast<int32_t>(offset)) &
                                 (required_alignment - 1));
  }

  /// Rounding up offset to the beginning of next column,
  /// choosing required alignment based on the data type of that column.
  static inline uint32_t padding_for_alignment(uint32_t offset, int string_alignment,
                                               const KeyColumnMetadata& col_metadata) {
    if (!col_metadata.is_fixed_length ||
        ARROW_POPCOUNT64(col_metadata.fixed_length) <= 1) {
      return 0;
    } else {
      return padding_for_alignment(offset, string_alignment);
    }
  }

  /// Returns an array of offsets within a row of ends of varbinary fields.
  inline const uint32_t* varbinary_end_array(const uint8_t* row) const {
    ARROW_DCHECK(!is_fixed_length);
    return reinterpret_cast<const uint32_t*>(row + varbinary_end_array_offset);
  }

  /// \brief An array of mutable offsets within a row of ends of varbinary fields.
  inline uint32_t* varbinary_end_array(uint8_t* row) const {
    ARROW_DCHECK(!is_fixed_length);
    return reinterpret_cast<uint32_t*>(row + varbinary_end_array_offset);
  }

  /// Returns the offset within the row and length of the first varbinary field.
  inline void first_varbinary_offset_and_length(const uint8_t* row, uint32_t* offset,
                                                uint32_t* length) const {
    ARROW_DCHECK(!is_fixed_length);
    *offset = fixed_length;
    *length = varbinary_end_array(row)[0] - fixed_length;
  }

  /// Returns the offset within the row and length of the second and further varbinary
  /// fields.
  inline void nth_varbinary_offset_and_length(const uint8_t* row, int varbinary_id,
                                              uint32_t* out_offset,
                                              uint32_t* out_length) const {
    ARROW_DCHECK(!is_fixed_length);
    ARROW_DCHECK(varbinary_id > 0);
    const uint32_t* varbinary_end = varbinary_end_array(row);
    uint32_t offset = varbinary_end[varbinary_id - 1];
    offset += padding_for_alignment(offset, string_alignment);
    *out_offset = offset;
    *out_length = varbinary_end[varbinary_id] - offset;
  }

  uint32_t encoded_field_order(uint32_t icol) const { return column_order[icol]; }

  uint32_t encoded_field_offset(uint32_t icol) const { return column_offsets[icol]; }

  uint32_t num_cols() const { return static_cast<uint32_t>(column_metadatas.size()); }

  uint32_t num_varbinary_cols() const;

  /// \brief Populate this instance to describe `cols` with the given alignment
  void FromColumnMetadataVector(const std::vector<KeyColumnMetadata>& cols,
                                int in_row_alignment, int in_string_alignment);

  /// \brief True if `other` has the same number of columns
  ///   and each column has the same width (two variable length
  ///   columns are considered to have the same width)
  bool is_compatible(const RowTableMetadata& other) const;
};

/// \brief A table of data stored in row-major order
///
/// Can only store non-nested data types
///
/// Can store both fixed-size data types and variable-length data types
///
/// The row table is not safe
class ARROW_EXPORT RowTableImpl {
 public:
  RowTableImpl();
  /// \brief Initialize a row array for use
  ///
  /// This must be called before any other method
  Status Init(MemoryPool* pool, const RowTableMetadata& metadata);
  /// \brief Clear all rows from the table
  ///
  /// Does not shrink buffers
  void Clean();
  /// \brief Add empty rows
  /// \param num_rows_to_append The number of empty rows to append
  /// \param num_extra_bytes_to_append For tables storing variable-length data this
  ///     should be a guess of how many data bytes will be needed to populate the
  ///     data.  This is ignored if there are no variable-length columns
  Status AppendEmpty(uint32_t num_rows_to_append, uint32_t num_extra_bytes_to_append);
  /// \brief Append rows from a source table
  /// \param from The table to append from
  /// \param num_rows_to_append The number of rows to append
  /// \param source_row_ids Indices (into `from`) of the desired rows
  Status AppendSelectionFrom(const RowTableImpl& from, uint32_t num_rows_to_append,
                             const uint16_t* source_row_ids);
  /// \brief Metadata describing the data stored in this table
  const RowTableMetadata& metadata() const { return metadata_; }
  /// \brief The number of rows stored in the table
  int64_t length() const { return num_rows_; }
  // Accessors into the table's buffers
  const uint8_t* data(int i) const {
    ARROW_DCHECK(i >= 0 && i < kMaxBuffers);
    return buffers_[i];
  }
  uint8_t* mutable_data(int i) {
    ARROW_DCHECK(i >= 0 && i < kMaxBuffers);
    return buffers_[i];
  }
  const uint32_t* offsets() const { return reinterpret_cast<const uint32_t*>(data(1)); }
  uint32_t* mutable_offsets() { return reinterpret_cast<uint32_t*>(mutable_data(1)); }
  const uint8_t* null_masks() const { return null_masks_->data(); }
  uint8_t* null_masks() { return null_masks_->mutable_data(); }

  /// \brief True if there is a null value anywhere in the table
  ///
  /// This calculation is memoized based on the number of rows and assumes
  /// that values are only appended (and not modified in place) between
  /// successive calls
  bool has_any_nulls(const LightContext* ctx) const;

 private:
  Status ResizeFixedLengthBuffers(int64_t num_extra_rows);
  Status ResizeOptionalVaryingLengthBuffer(int64_t num_extra_bytes);

  // Helper functions to determine the number of bytes needed for each
  // buffer given a number of rows.
  int64_t size_null_masks(int64_t num_rows) const;
  int64_t size_offsets(int64_t num_rows) const;
  int64_t size_rows_fixed_length(int64_t num_rows) const;
  int64_t size_rows_varying_length(int64_t num_bytes) const;

  // Called after resize to fix pointers
  void UpdateBufferPointers();

  // The arrays in `buffers_` need to be padded so that
  // vectorized operations can operate in blocks without
  // worrying about tails
  static constexpr int64_t kPaddingForVectors = 64;
  MemoryPool* pool_;
  RowTableMetadata metadata_;
  // Buffers can only expand during lifetime and never shrink.
  std::unique_ptr<ResizableBuffer> null_masks_;
  // Only used if the table has variable-length columns
  // Stores the offsets into the binary data (which is stored
  // after all the fixed-sized fields)
  std::unique_ptr<ResizableBuffer> offsets_;
  // Stores the fixed-length parts of the rows
  std::unique_ptr<ResizableBuffer> rows_;
  static constexpr int kMaxBuffers = 3;
  uint8_t* buffers_[kMaxBuffers];
  // The number of rows in the table
  int64_t num_rows_;
  // The number of rows that can be stored in the table without resizing
  int64_t rows_capacity_;
  // The number of bytes that can be stored in the table without resizing
  int64_t bytes_capacity_;

  // Mutable to allow lazy evaluation
  mutable int64_t num_rows_for_has_any_nulls_;
  mutable bool has_any_nulls_;
};

}  // namespace compute
}  // namespace arrow

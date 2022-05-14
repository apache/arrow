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

#include "arrow/compute/row/compare_internal.h"
#include "arrow/compute/row/encode_internal.h"
#include "arrow/compute/row/row_internal.h"

#include <memory.h>

#include <algorithm>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace compute {

void RowTableEncoder::Init(const std::vector<KeyColumnMetadata>& cols, int row_alignment,
                           int string_alignment) {
  row_metadata_.FromColumnMetadataVector(cols, row_alignment, string_alignment);
  uint32_t num_cols = row_metadata_.num_cols();
  uint32_t num_varbinary_cols = row_metadata_.num_varbinary_cols();
  batch_all_cols_.resize(num_cols);
  batch_varbinary_cols_.resize(num_varbinary_cols);
  batch_varbinary_cols_base_offsets_.resize(num_varbinary_cols);
}

void RowTableEncoder::PrepareKeyColumnArrays(int64_t start_row, int64_t num_rows,
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

void RowTableEncoder::DecodeFixedLengthBuffers(int64_t start_row_input,
                                               int64_t start_row_output, int64_t num_rows,
                                               const RowTableImpl& rows,
                                               std::vector<KeyColumnArray>* cols,
                                               int64_t hardware_flags,
                                               util::TempVectorStack* temp_stack) {
  // Prepare column array vectors
  PrepareKeyColumnArrays(start_row_output, num_rows, *cols);

  LightContext ctx;
  ctx.hardware_flags = hardware_flags;
  ctx.stack = temp_stack;

  // Create two temp vectors with 16-bit elements
  auto temp_buffer_holder_A =
      util::TempVectorHolder<uint16_t>(ctx.stack, static_cast<uint32_t>(num_rows));
  auto temp_buffer_A = KeyColumnArray(
      KeyColumnMetadata(true, sizeof(uint16_t)), num_rows, nullptr,
      reinterpret_cast<uint8_t*>(temp_buffer_holder_A.mutable_data()), nullptr);
  auto temp_buffer_holder_B =
      util::TempVectorHolder<uint16_t>(ctx.stack, static_cast<uint32_t>(num_rows));
  auto temp_buffer_B = KeyColumnArray(
      KeyColumnMetadata(true, sizeof(uint16_t)), num_rows, nullptr,
      reinterpret_cast<uint8_t*>(temp_buffer_holder_B.mutable_data()), nullptr);

  bool is_row_fixed_length = row_metadata_.is_fixed_length;
  if (!is_row_fixed_length) {
    EncoderOffsets::Decode(static_cast<uint32_t>(start_row_input),
                           static_cast<uint32_t>(num_rows), rows, &batch_varbinary_cols_,
                           batch_varbinary_cols_base_offsets_, &ctx);
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
                            &ctx, &temp_buffer_A);
      i += 1;
    } else {
      EncoderBinaryPair::Decode(
          static_cast<uint32_t>(start_row_input), static_cast<uint32_t>(num_rows),
          row_metadata_.column_offsets[i], rows, &batch_all_cols_[i],
          &batch_all_cols_[i + 1], &ctx, &temp_buffer_A, &temp_buffer_B);
      i += 2;
    }
  }

  // Process nulls
  EncoderNulls::Decode(static_cast<uint32_t>(start_row_input),
                       static_cast<uint32_t>(num_rows), rows, &batch_all_cols_);
}

void RowTableEncoder::DecodeVaryingLengthBuffers(
    int64_t start_row_input, int64_t start_row_output, int64_t num_rows,
    const RowTableImpl& rows, std::vector<KeyColumnArray>* cols, int64_t hardware_flags,
    util::TempVectorStack* temp_stack) {
  // Prepare column array vectors
  PrepareKeyColumnArrays(start_row_output, num_rows, *cols);

  LightContext ctx;
  ctx.hardware_flags = hardware_flags;
  ctx.stack = temp_stack;

  bool is_row_fixed_length = row_metadata_.is_fixed_length;
  if (!is_row_fixed_length) {
    for (size_t i = 0; i < batch_varbinary_cols_.size(); ++i) {
      // Memcpy varbinary fields into precomputed in the previous step
      // positions in the output row buffer.
      EncoderVarBinary::Decode(static_cast<uint32_t>(start_row_input),
                               static_cast<uint32_t>(num_rows), static_cast<uint32_t>(i),
                               rows, &batch_varbinary_cols_[i], &ctx);
    }
  }
}

void RowTableEncoder::PrepareEncodeSelected(int64_t start_row, int64_t num_rows,
                                            const std::vector<KeyColumnArray>& cols) {
  // Prepare column array vectors
  PrepareKeyColumnArrays(start_row, num_rows, cols);
}

Status RowTableEncoder::EncodeSelected(RowTableImpl* rows, uint32_t num_selected,
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

Status RowTable::InitIfNeeded(MemoryPool* pool, const RowTableMetadata& row_metadata) {
  if (is_initialized_) {
    return Status::OK();
  }
  encoder_.Init(row_metadata.column_metadatas, sizeof(uint64_t), sizeof(uint64_t));
  RETURN_NOT_OK(rows_temp_.Init(pool, row_metadata));
  RETURN_NOT_OK(rows_.Init(pool, row_metadata));
  is_initialized_ = true;
  return Status::OK();
}

Status RowTable::InitIfNeeded(MemoryPool* pool, const ExecBatch& batch) {
  if (is_initialized_) {
    return Status::OK();
  }
  std::vector<KeyColumnMetadata> column_metadatas;
  RETURN_NOT_OK(ColumnMetadatasFromExecBatch(batch, &column_metadatas));
  RowTableMetadata row_metadata;
  row_metadata.FromColumnMetadataVector(column_metadatas, sizeof(uint64_t),
                                        sizeof(uint64_t));

  return InitIfNeeded(pool, row_metadata);
}

Status RowTable::AppendBatchSelection(MemoryPool* pool, const ExecBatch& batch,
                                      int begin_row_id, int end_row_id, int num_row_ids,
                                      const uint16_t* row_ids,
                                      std::vector<KeyColumnArray>& temp_column_arrays) {
  RETURN_NOT_OK(InitIfNeeded(pool, batch));
  RETURN_NOT_OK(ColumnArraysFromExecBatch(batch, begin_row_id, end_row_id - begin_row_id,
                                          &temp_column_arrays));
  encoder_.PrepareEncodeSelected(
      /*start_row=*/0, end_row_id - begin_row_id, temp_column_arrays);
  RETURN_NOT_OK(encoder_.EncodeSelected(&rows_temp_, num_row_ids, row_ids));
  RETURN_NOT_OK(rows_.AppendSelectionFrom(rows_temp_, num_row_ids, nullptr));
  return Status::OK();
}

void RowTable::Compare(const ExecBatch& batch, int begin_row_id, int end_row_id,
                       int num_selected, const uint16_t* batch_selection_maybe_null,
                       const uint32_t* array_row_ids, uint32_t* out_num_not_equal,
                       uint16_t* out_not_equal_selection, int64_t hardware_flags,
                       util::TempVectorStack* temp_stack,
                       std::vector<KeyColumnArray>& temp_column_arrays,
                       uint8_t* out_match_bitvector_maybe_null) {
  Status status = ColumnArraysFromExecBatch(
      batch, begin_row_id, end_row_id - begin_row_id, &temp_column_arrays);
  ARROW_DCHECK(status.ok());

  LightContext ctx;
  ctx.hardware_flags = hardware_flags;
  ctx.stack = temp_stack;
  KeyCompare::CompareColumnsToRows(
      num_selected, batch_selection_maybe_null, array_row_ids, &ctx, out_num_not_equal,
      out_not_equal_selection, temp_column_arrays, rows_,
      /*are_cols_in_encoding_order=*/false, out_match_bitvector_maybe_null);
}

Status RowTable::DecodeSelected(ResizableArrayData* output, int column_id,
                                int num_rows_to_append, const uint32_t* row_ids,
                                MemoryPool* pool) const {
  int num_rows_before = output->num_rows();
  RETURN_NOT_OK(output->ResizeFixedLengthBuffers(num_rows_before + num_rows_to_append));

  // Both input (RowTableImpl) and output (ResizableArrayData) have buffers with
  // extra bytes added at the end to avoid buffer overruns when using wide load
  // instructions.
  //

  ARROW_ASSIGN_OR_RAISE(KeyColumnMetadata column_metadata, output->column_metadata());

  if (column_metadata.is_fixed_length) {
    uint32_t fixed_length = column_metadata.fixed_length;
    switch (fixed_length) {
      case 0:
        RowArrayAccessor::Visit(rows_, column_id, num_rows_to_append, row_ids,
                                [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
                                  bit_util::SetBitTo(output->mutable_data(1),
                                                     num_rows_before + i, *ptr != 0);
                                });
        break;
      case 1:
        RowArrayAccessor::Visit(rows_, column_id, num_rows_to_append, row_ids,
                                [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
                                  output->mutable_data(1)[num_rows_before + i] = *ptr;
                                });
        break;
      case 2:
        RowArrayAccessor::Visit(
            rows_, column_id, num_rows_to_append, row_ids,
            [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
              reinterpret_cast<uint16_t*>(output->mutable_data(1))[num_rows_before + i] =
                  *reinterpret_cast<const uint16_t*>(ptr);
            });
        break;
      case 4:
        RowArrayAccessor::Visit(
            rows_, column_id, num_rows_to_append, row_ids,
            [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
              reinterpret_cast<uint32_t*>(output->mutable_data(1))[num_rows_before + i] =
                  *reinterpret_cast<const uint32_t*>(ptr);
            });
        break;
      case 8:
        RowArrayAccessor::Visit(
            rows_, column_id, num_rows_to_append, row_ids,
            [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
              reinterpret_cast<uint64_t*>(output->mutable_data(1))[num_rows_before + i] =
                  *reinterpret_cast<const uint64_t*>(ptr);
            });
        break;
      default:
        RowArrayAccessor::Visit(
            rows_, column_id, num_rows_to_append, row_ids,
            [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
              uint64_t* dst = reinterpret_cast<uint64_t*>(
                  output->mutable_data(1) + num_bytes * (num_rows_before + i));
              const uint64_t* src = reinterpret_cast<const uint64_t*>(ptr);
              for (uint32_t word_id = 0;
                   word_id < bit_util::CeilDiv(num_bytes, sizeof(uint64_t)); ++word_id) {
                util::SafeStore<uint64_t>(dst + word_id, util::SafeLoad(src + word_id));
              }
            });
        break;
    }
  } else {
    uint32_t* offsets =
        reinterpret_cast<uint32_t*>(output->mutable_data(1)) + num_rows_before;
    uint32_t sum = num_rows_before == 0 ? 0 : offsets[0];
    RowArrayAccessor::Visit(
        rows_, column_id, num_rows_to_append, row_ids,
        [&](int i, const uint8_t* ptr, uint32_t num_bytes) { offsets[i] = num_bytes; });
    for (int i = 0; i < num_rows_to_append; ++i) {
      uint32_t length = offsets[i];
      offsets[i] = sum;
      sum += length;
    }
    offsets[num_rows_to_append] = sum;
    RETURN_NOT_OK(output->ResizeVaryingLengthBuffer());
    RowArrayAccessor::Visit(
        rows_, column_id, num_rows_to_append, row_ids,
        [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
          uint64_t* dst = reinterpret_cast<uint64_t*>(
              output->mutable_data(2) +
              reinterpret_cast<const uint32_t*>(
                  output->mutable_data(1))[num_rows_before + i]);
          const uint64_t* src = reinterpret_cast<const uint64_t*>(ptr);
          for (uint32_t word_id = 0;
               word_id < bit_util::CeilDiv(num_bytes, sizeof(uint64_t)); ++word_id) {
            util::SafeStore<uint64_t>(dst + word_id, util::SafeLoad(src + word_id));
          }
        });
  }

  // Process nulls
  //
  RowArrayAccessor::VisitNulls(
      rows_, column_id, num_rows_to_append, row_ids, [&](int i, uint8_t value) {
        bit_util::SetBitTo(output->mutable_data(0), num_rows_before + i, value == 0);
      });

  return Status::OK();
}

Status RowArrayMerge::PrepareForMerge(RowTable* target,
                                      const std::vector<RowTable*>& sources,
                                      std::vector<int64_t>* first_target_row_id,
                                      MemoryPool* pool) {
  ARROW_DCHECK(!sources.empty());

  ARROW_DCHECK(sources[0]->is_initialized_);
  const RowTableMetadata& metadata = sources[0]->rows_.metadata();
  ARROW_DCHECK(!target->is_initialized_);
  RETURN_NOT_OK(target->InitIfNeeded(pool, metadata));

  // Sum the number of rows from all input sources and calculate their total
  // size.
  //
  int64_t num_rows = 0;
  int64_t num_bytes = 0;
  first_target_row_id->resize(sources.size() + 1);
  for (size_t i = 0; i < sources.size(); ++i) {
    // All input sources must be initialized and have the same row format.
    //
    ARROW_DCHECK(sources[i]->is_initialized_);
    ARROW_DCHECK(metadata.is_compatible(sources[i]->rows_.metadata()));
    (*first_target_row_id)[i] = num_rows;
    num_rows += sources[i]->rows_.length();
    if (!metadata.is_fixed_length) {
      num_bytes += sources[i]->rows_.offsets()[sources[i]->rows_.length()];
    }
  }
  (*first_target_row_id)[sources.size()] = num_rows;

  // Allocate target memory
  //
  target->rows_.Clean();
  RETURN_NOT_OK(target->rows_.AppendEmpty(static_cast<uint32_t>(num_rows),
                                          static_cast<uint32_t>(num_bytes)));

  // In case of varying length rows,
  // initialize the first row offset for each range of rows corresponding to a
  // single source.
  //
  if (!metadata.is_fixed_length) {
    num_rows = 0;
    num_bytes = 0;
    for (size_t i = 0; i < sources.size(); ++i) {
      target->rows_.mutable_offsets()[num_rows] = static_cast<uint32_t>(num_bytes);
      num_rows += sources[i]->rows_.length();
      num_bytes += sources[i]->rows_.offsets()[sources[i]->rows_.length()];
    }
    target->rows_.mutable_offsets()[num_rows] = static_cast<uint32_t>(num_bytes);
  }

  return Status::OK();
}

void RowArrayMerge::MergeSingle(RowTable* target, const RowTable& source,
                                int64_t first_target_row_id,
                                const int64_t* source_rows_permutation) {
  // Source and target must:
  // - be initialized
  // - use the same row format
  // - use 64-bit alignment
  //
  ARROW_DCHECK(source.is_initialized_ && target->is_initialized_);
  ARROW_DCHECK(target->rows_.metadata().is_compatible(source.rows_.metadata()));
  ARROW_DCHECK(target->rows_.metadata().row_alignment == sizeof(uint64_t));

  if (target->rows_.metadata().is_fixed_length) {
    CopyFixedLength(&target->rows_, source.rows_, first_target_row_id,
                    source_rows_permutation);
  } else {
    CopyVaryingLength(&target->rows_, source.rows_, first_target_row_id,
                      target->rows_.offsets()[first_target_row_id],
                      source_rows_permutation);
  }
  CopyNulls(&target->rows_, source.rows_, first_target_row_id, source_rows_permutation);
}

void RowArrayMerge::CopyFixedLength(RowTableImpl* target, const RowTableImpl& source,
                                    int64_t first_target_row_id,
                                    const int64_t* source_rows_permutation) {
  int64_t num_source_rows = source.length();

  int64_t fixed_length = target->metadata().fixed_length;

  // Permutation of source rows is optional. Without permutation all that is
  // needed is memcpy.
  //
  if (!source_rows_permutation) {
    memcpy(target->mutable_data(1) + fixed_length * first_target_row_id, source.data(1),
           fixed_length * num_source_rows);
  } else {
    // Row length must be a multiple of 64-bits due to enforced alignment.
    // Loop for each output row copying a fixed number of 64-bit words.
    //
    ARROW_DCHECK(fixed_length % sizeof(uint64_t) == 0);

    int64_t num_words_per_row = fixed_length / sizeof(uint64_t);
    for (int64_t i = 0; i < num_source_rows; ++i) {
      int64_t source_row_id = source_rows_permutation[i];
      const uint64_t* source_row_ptr = reinterpret_cast<const uint64_t*>(
          source.data(1) + fixed_length * source_row_id);
      uint64_t* target_row_ptr = reinterpret_cast<uint64_t*>(
          target->mutable_data(1) + fixed_length * (first_target_row_id + i));

      for (int64_t word = 0; word < num_words_per_row; ++word) {
        target_row_ptr[word] = source_row_ptr[word];
      }
    }
  }
}

void RowArrayMerge::CopyVaryingLength(RowTableImpl* target, const RowTableImpl& source,
                                      int64_t first_target_row_id,
                                      int64_t first_target_row_offset,
                                      const int64_t* source_rows_permutation) {
  int64_t num_source_rows = source.length();
  uint32_t* target_offsets = target->mutable_offsets();
  const uint32_t* source_offsets = source.offsets();

  // Permutation of source rows is optional.
  //
  if (!source_rows_permutation) {
    int64_t target_row_offset = first_target_row_offset;
    for (int64_t i = 0; i < num_source_rows; ++i) {
      target_offsets[first_target_row_id + i] = static_cast<uint32_t>(target_row_offset);
      target_row_offset += source_offsets[i + 1] - source_offsets[i];
    }
    // We purposefully skip outputting of N+1 offset, to allow concurrent
    // copies of rows done to adjacent ranges in target array.
    // It should have already been initialized during preparation for merge.
    //

    // We can simply memcpy bytes of rows if their order has not changed.
    //
    memcpy(target->mutable_data(2) + target_offsets[first_target_row_id], source.data(2),
           source_offsets[num_source_rows] - source_offsets[0]);
  } else {
    int64_t target_row_offset = first_target_row_offset;
    uint64_t* target_row_ptr =
        reinterpret_cast<uint64_t*>(target->mutable_data(2) + target_row_offset);
    for (int64_t i = 0; i < num_source_rows; ++i) {
      int64_t source_row_id = source_rows_permutation[i];
      const uint64_t* source_row_ptr = reinterpret_cast<const uint64_t*>(
          source.data(2) + source_offsets[source_row_id]);
      uint32_t length = source_offsets[source_row_id + 1] - source_offsets[source_row_id];

      // Rows should be 64-bit aligned.
      // In that case we can copy them using a sequence of 64-bit read/writes.
      //
      ARROW_DCHECK(length % sizeof(uint64_t) == 0);

      for (uint32_t word = 0; word < length / sizeof(uint64_t); ++word) {
        *target_row_ptr++ = *source_row_ptr++;
      }

      target_offsets[first_target_row_id + i] = static_cast<uint32_t>(target_row_offset);
      target_row_offset += length;
    }
  }
}

void RowArrayMerge::CopyNulls(RowTableImpl* target, const RowTableImpl& source,
                              int64_t first_target_row_id,
                              const int64_t* source_rows_permutation) {
  int64_t num_source_rows = source.length();
  int num_bytes_per_row = target->metadata().null_masks_bytes_per_row;
  uint8_t* target_nulls = target->null_masks() + num_bytes_per_row * first_target_row_id;
  if (!source_rows_permutation) {
    memcpy(target_nulls, source.null_masks(), num_bytes_per_row * num_source_rows);
  } else {
    for (int64_t i = 0; i < num_source_rows; ++i) {
      int64_t source_row_id = source_rows_permutation[i];
      const uint8_t* source_nulls =
          source.null_masks() + num_bytes_per_row * source_row_id;
      for (int64_t byte = 0; byte < num_bytes_per_row; ++byte) {
        *target_nulls++ = *source_nulls++;
      }
    }
  }
}

}  // namespace compute
}  // namespace arrow

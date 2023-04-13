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

#include <sys/stat.h>
#include <algorithm>  // std::upper_bound
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include "arrow/acero/hash_join.h"
#include "arrow/acero/swiss_join_internal.h"
#include "arrow/acero/util.h"
#include "arrow/array/util.h"  // MakeArrayFromScalar
#include "arrow/compute/kernels/row_encoder_internal.h"
#include "arrow/compute/key_hash.h"
#include "arrow/compute/row/compare_internal.h"
#include "arrow/compute/row/encode_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using compute::ColumnMetadataFromDataType;
using compute::Hashing32;
using compute::KeyCompare;
using compute::LightContext;
using compute::SwissTable;

namespace acero {

int RowArrayAccessor::VarbinaryColumnId(const RowTableMetadata& row_metadata,
                                        int column_id) {
  ARROW_DCHECK(row_metadata.num_cols() > static_cast<uint32_t>(column_id));
  ARROW_DCHECK(!row_metadata.is_fixed_length);
  ARROW_DCHECK(!row_metadata.column_metadatas[column_id].is_fixed_length);

  int varbinary_column_id = 0;
  for (int i = 0; i < column_id; ++i) {
    if (!row_metadata.column_metadatas[i].is_fixed_length) {
      ++varbinary_column_id;
    }
  }
  return varbinary_column_id;
}

int RowArrayAccessor::NumRowsToSkip(const RowTableImpl& rows, int column_id, int num_rows,
                                    const uint32_t* row_ids, int num_tail_bytes_to_skip) {
  uint32_t num_bytes_skipped = 0;
  int num_rows_left = num_rows;

  bool is_fixed_length_column =
      rows.metadata().column_metadatas[column_id].is_fixed_length;

  if (!is_fixed_length_column) {
    // Varying length column
    //
    int varbinary_column_id = VarbinaryColumnId(rows.metadata(), column_id);

    while (num_rows_left > 0 &&
           num_bytes_skipped < static_cast<uint32_t>(num_tail_bytes_to_skip)) {
      // Find the pointer to the last requested row
      //
      uint32_t last_row_id = row_ids[num_rows_left - 1];
      const uint8_t* row_ptr = rows.data(2) + rows.offsets()[last_row_id];

      // Find the length of the requested varying length field in that row
      //
      uint32_t field_offset_within_row, field_length;
      if (varbinary_column_id == 0) {
        rows.metadata().first_varbinary_offset_and_length(
            row_ptr, &field_offset_within_row, &field_length);
      } else {
        rows.metadata().nth_varbinary_offset_and_length(
            row_ptr, varbinary_column_id, &field_offset_within_row, &field_length);
      }

      num_bytes_skipped += field_length;
      --num_rows_left;
    }
  } else {
    // Fixed length column
    //
    uint32_t field_length = rows.metadata().column_metadatas[column_id].fixed_length;
    uint32_t num_bytes_skipped = 0;
    while (num_rows_left > 0 &&
           num_bytes_skipped < static_cast<uint32_t>(num_tail_bytes_to_skip)) {
      num_bytes_skipped += field_length;
      --num_rows_left;
    }
  }

  return num_rows - num_rows_left;
}

template <class PROCESS_VALUE_FN>
void RowArrayAccessor::Visit(const RowTableImpl& rows, int column_id, int num_rows,
                             const uint32_t* row_ids, PROCESS_VALUE_FN process_value_fn) {
  bool is_fixed_length_column =
      rows.metadata().column_metadatas[column_id].is_fixed_length;

  // There are 4 cases, each requiring different steps:
  // 1. Varying length column that is the first varying length column in a row
  // 2. Varying length column that is not the first varying length column in a
  // row
  // 3. Fixed length column in a fixed length row
  // 4. Fixed length column in a varying length row

  if (!is_fixed_length_column) {
    int varbinary_column_id = VarbinaryColumnId(rows.metadata(), column_id);
    const uint8_t* row_ptr_base = rows.data(2);
    const uint32_t* row_offsets = rows.offsets();
    uint32_t field_offset_within_row, field_length;

    if (varbinary_column_id == 0) {
      // Case 1: This is the first varbinary column
      //
      for (int i = 0; i < num_rows; ++i) {
        uint32_t row_id = row_ids[i];
        const uint8_t* row_ptr = row_ptr_base + row_offsets[row_id];
        rows.metadata().first_varbinary_offset_and_length(
            row_ptr, &field_offset_within_row, &field_length);
        process_value_fn(i, row_ptr + field_offset_within_row, field_length);
      }
    } else {
      // Case 2: This is second or later varbinary column
      //
      for (int i = 0; i < num_rows; ++i) {
        uint32_t row_id = row_ids[i];
        const uint8_t* row_ptr = row_ptr_base + row_offsets[row_id];
        rows.metadata().nth_varbinary_offset_and_length(
            row_ptr, varbinary_column_id, &field_offset_within_row, &field_length);
        process_value_fn(i, row_ptr + field_offset_within_row, field_length);
      }
    }
  }

  if (is_fixed_length_column) {
    uint32_t field_offset_within_row = rows.metadata().encoded_field_offset(
        rows.metadata().pos_after_encoding(column_id));
    uint32_t field_length = rows.metadata().column_metadatas[column_id].fixed_length;
    // Bit column is encoded as a single byte
    //
    if (field_length == 0) {
      field_length = 1;
    }
    uint32_t row_length = rows.metadata().fixed_length;

    bool is_fixed_length_row = rows.metadata().is_fixed_length;
    if (is_fixed_length_row) {
      // Case 3: This is a fixed length column in a fixed length row
      //
      const uint8_t* row_ptr_base = rows.data(1) + field_offset_within_row;
      for (int i = 0; i < num_rows; ++i) {
        uint32_t row_id = row_ids[i];
        const uint8_t* row_ptr = row_ptr_base + row_length * row_id;
        process_value_fn(i, row_ptr, field_length);
      }
    } else {
      // Case 4: This is a fixed length column in a varying length row
      //
      const uint8_t* row_ptr_base = rows.data(2) + field_offset_within_row;
      const uint32_t* row_offsets = rows.offsets();
      for (int i = 0; i < num_rows; ++i) {
        uint32_t row_id = row_ids[i];
        const uint8_t* row_ptr = row_ptr_base + row_offsets[row_id];
        process_value_fn(i, row_ptr, field_length);
      }
    }
  }
}

template <class PROCESS_VALUE_FN>
void RowArrayAccessor::VisitNulls(const RowTableImpl& rows, int column_id, int num_rows,
                                  const uint32_t* row_ids,
                                  PROCESS_VALUE_FN process_value_fn) {
  const uint8_t* null_masks = rows.null_masks();
  uint32_t null_mask_num_bytes = rows.metadata().null_masks_bytes_per_row;
  uint32_t pos_after_encoding = rows.metadata().pos_after_encoding(column_id);
  for (int i = 0; i < num_rows; ++i) {
    uint32_t row_id = row_ids[i];
    int64_t bit_id = row_id * null_mask_num_bytes * 8 + pos_after_encoding;
    process_value_fn(i, bit_util::GetBit(null_masks, bit_id) ? 0xff : 0);
  }
}

Status RowArray::InitIfNeeded(MemoryPool* pool, const RowTableMetadata& row_metadata) {
  if (is_initialized_) {
    return Status::OK();
  }
  encoder_.Init(row_metadata.column_metadatas, sizeof(uint64_t), sizeof(uint64_t));
  RETURN_NOT_OK(rows_temp_.Init(pool, row_metadata));
  RETURN_NOT_OK(rows_.Init(pool, row_metadata));
  is_initialized_ = true;
  return Status::OK();
}

Status RowArray::InitIfNeeded(MemoryPool* pool, const ExecBatch& batch) {
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

Status RowArray::AppendBatchSelection(MemoryPool* pool, const ExecBatch& batch,
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

void RowArray::Compare(const ExecBatch& batch, int begin_row_id, int end_row_id,
                       int num_selected, const uint16_t* batch_selection_maybe_null,
                       const uint32_t* array_row_ids, uint32_t* out_num_not_equal,
                       uint16_t* out_not_equal_selection, int64_t hardware_flags,
                       arrow::util::TempVectorStack* temp_stack,
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

Status RowArray::DecodeSelected(ResizableArrayData* output, int column_id,
                                int num_rows_to_append, const uint32_t* row_ids,
                                MemoryPool* pool) const {
  int num_rows_before = output->num_rows();
  RETURN_NOT_OK(output->ResizeFixedLengthBuffers(num_rows_before + num_rows_to_append));

  // Both input (KeyRowArray) and output (ResizableArrayData) have buffers with
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
                arrow::util::SafeStore<uint64_t>(dst + word_id,
                                                 arrow::util::SafeLoad(src + word_id));
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
            arrow::util::SafeStore<uint64_t>(dst + word_id,
                                             arrow::util::SafeLoad(src + word_id));
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

void RowArray::DebugPrintToFile(const char* filename, bool print_sorted) const {
  FILE* fout;
#if defined(_MSC_VER) && _MSC_VER >= 1400
  fopen_s(&fout, filename, "wt");
#else
  fout = fopen(filename, "wt");
#endif
  if (!fout) {
    return;
  }

  for (int64_t row_id = 0; row_id < rows_.length(); ++row_id) {
    for (uint32_t column_id = 0; column_id < rows_.metadata().num_cols(); ++column_id) {
      bool is_null;
      uint32_t row_id_cast = static_cast<uint32_t>(row_id);
      RowArrayAccessor::VisitNulls(rows_, column_id, 1, &row_id_cast,
                                   [&](int i, uint8_t value) { is_null = (value != 0); });
      if (is_null) {
        fprintf(fout, "null");
      } else {
        RowArrayAccessor::Visit(rows_, column_id, 1, &row_id_cast,
                                [&](int i, const uint8_t* ptr, uint32_t num_bytes) {
                                  fprintf(fout, "\"");
                                  for (uint32_t ibyte = 0; ibyte < num_bytes; ++ibyte) {
                                    fprintf(fout, "%02x", ptr[ibyte]);
                                  }
                                  fprintf(fout, "\"");
                                });
      }
      fprintf(fout, "\t");
    }
    fprintf(fout, "\n");
  }
  fclose(fout);

  if (print_sorted) {
    struct stat sb;
    if (stat(filename, &sb) == -1) {
      ARROW_DCHECK(false);
      return;
    }
    std::vector<char> buffer;
    buffer.resize(sb.st_size);
    std::vector<std::string> lines;
    FILE* fin;
#if defined(_MSC_VER) && _MSC_VER >= 1400
    fopen_s(&fin, filename, "rt");
#else
    fin = fopen(filename, "rt");
#endif
    if (!fin) {
      return;
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), fin)) {
      lines.push_back(std::string(buffer.data()));
    }
    fclose(fin);
    std::sort(lines.begin(), lines.end());
    FILE* fout2;
#if defined(_MSC_VER) && _MSC_VER >= 1400
    fopen_s(&fout2, filename, "wt");
#else
    fout2 = fopen(filename, "wt");
#endif
    if (!fout2) {
      return;
    }
    for (size_t i = 0; i < lines.size(); ++i) {
      fprintf(fout2, "%s\n", lines[i].c_str());
    }
    fclose(fout2);
  }
}

Status RowArrayMerge::PrepareForMerge(RowArray* target,
                                      const std::vector<RowArray*>& sources,
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
  if (first_target_row_id) {
    first_target_row_id->resize(sources.size() + 1);
  }
  for (size_t i = 0; i < sources.size(); ++i) {
    // All input sources must be initialized and have the same row format.
    //
    ARROW_DCHECK(sources[i]->is_initialized_);
    ARROW_DCHECK(metadata.is_compatible(sources[i]->rows_.metadata()));
    if (first_target_row_id) {
      (*first_target_row_id)[i] = num_rows;
    }
    num_rows += sources[i]->rows_.length();
    if (!metadata.is_fixed_length) {
      num_bytes += sources[i]->rows_.offsets()[sources[i]->rows_.length()];
    }
  }
  if (first_target_row_id) {
    (*first_target_row_id)[sources.size()] = num_rows;
  }

  if (num_bytes > std::numeric_limits<uint32_t>::max()) {
    return Status::Invalid(
        "There are more than 2^32 bytes of key data.  Acero cannot "
        "process a join of this magnitude");
  }

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

void RowArrayMerge::MergeSingle(RowArray* target, const RowArray& source,
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

Status SwissTableMerge::PrepareForMerge(SwissTable* target,
                                        const std::vector<SwissTable*>& sources,
                                        std::vector<uint32_t>* first_target_group_id,
                                        MemoryPool* pool) {
  ARROW_DCHECK(!sources.empty());

  // Each source should correspond to a range of hashes.
  // A row belongs to a source with index determined by K highest bits of hash.
  // That means that the number of sources must be a power of 2.
  //
  int log_num_sources = bit_util::Log2(sources.size());
  ARROW_DCHECK((1 << log_num_sources) == static_cast<int>(sources.size()));

  // Determine the number of blocks in the target table.
  // We will use max of numbers of blocks in any of the sources multiplied by
  // the number of sources.
  //
  int log_blocks_max = 1;
  for (size_t i = 0; i < sources.size(); ++i) {
    log_blocks_max = std::max(log_blocks_max, sources[i]->log_blocks());
  }
  int log_blocks = log_num_sources + log_blocks_max;

  // Allocate target blocks and mark all slots as empty
  //
  // We will skip allocating the array of hash values in target table.
  // Target will be used in read-only mode and that array is only needed when
  // resizing table which may occur only after new inserts.
  //
  RETURN_NOT_OK(target->init(sources[0]->hardware_flags(), pool, log_blocks,
                             /*no_hash_array=*/true));

  // Calculate and output the first group id index for each source.
  //
  if (first_target_group_id) {
    uint32_t num_groups = 0;
    first_target_group_id->resize(sources.size());
    for (size_t i = 0; i < sources.size(); ++i) {
      (*first_target_group_id)[i] = num_groups;
      num_groups += sources[i]->num_inserted();
    }
    target->num_inserted(num_groups);
  }

  return Status::OK();
}

void SwissTableMerge::MergePartition(SwissTable* target, const SwissTable* source,
                                     uint32_t partition_id, int num_partition_bits,
                                     uint32_t base_group_id,
                                     std::vector<uint32_t>* overflow_group_ids,
                                     std::vector<uint32_t>* overflow_hashes) {
  // Prepare parameters needed for scanning full slots in source.
  //
  int source_group_id_bits =
      SwissTable::num_groupid_bits_from_log_blocks(source->log_blocks());
  uint64_t source_group_id_mask = ~0ULL >> (64 - source_group_id_bits);
  int64_t source_block_bytes = source_group_id_bits + 8;
  ARROW_DCHECK(source_block_bytes % sizeof(uint64_t) == 0);

  // Compute index of the last block in target that corresponds to the given
  // partition.
  //
  ARROW_DCHECK(num_partition_bits <= target->log_blocks());
  int64_t target_max_block_id =
      ((partition_id + 1) << (target->log_blocks() - num_partition_bits)) - 1;

  overflow_group_ids->clear();
  overflow_hashes->clear();

  // For each source block...
  int64_t source_blocks = 1LL << source->log_blocks();
  for (int64_t block_id = 0; block_id < source_blocks; ++block_id) {
    uint8_t* block_bytes = source->blocks() + block_id * source_block_bytes;
    uint64_t block = *reinterpret_cast<const uint64_t*>(block_bytes);

    // For each non-empty source slot...
    constexpr uint64_t kHighBitOfEachByte = 0x8080808080808080ULL;
    constexpr int kSlotsPerBlock = 8;
    int num_full_slots =
        kSlotsPerBlock - static_cast<int>(ARROW_POPCOUNT64(block & kHighBitOfEachByte));
    for (int local_slot_id = 0; local_slot_id < num_full_slots; ++local_slot_id) {
      // Read group id and hash for this slot.
      //
      uint64_t group_id =
          source->extract_group_id(block_bytes, local_slot_id, source_group_id_mask);
      int64_t global_slot_id = block_id * kSlotsPerBlock + local_slot_id;
      uint32_t hash = source->hashes()[global_slot_id];
      // Insert partition id into the highest bits of hash, shifting the
      // remaining hash bits right.
      //
      hash >>= num_partition_bits;
      hash |= (partition_id << (SwissTable::bits_hash_ - 1 - num_partition_bits) << 1);
      // Add base group id
      //
      group_id += base_group_id;

      // Insert new entry into target. Store in overflow vectors if not
      // successful.
      //
      bool was_inserted = InsertNewGroup(target, group_id, hash, target_max_block_id);
      if (!was_inserted) {
        overflow_group_ids->push_back(static_cast<uint32_t>(group_id));
        overflow_hashes->push_back(hash);
      }
    }
  }
}

inline bool SwissTableMerge::InsertNewGroup(SwissTable* target, uint64_t group_id,
                                            uint32_t hash, int64_t max_block_id) {
  // Load the first block to visit for this hash
  //
  int64_t block_id = hash >> (SwissTable::bits_hash_ - target->log_blocks());
  int64_t block_id_mask = ((1LL << target->log_blocks()) - 1);
  int num_group_id_bits =
      SwissTable::num_groupid_bits_from_log_blocks(target->log_blocks());
  int64_t num_block_bytes = num_group_id_bits + sizeof(uint64_t);
  ARROW_DCHECK(num_block_bytes % sizeof(uint64_t) == 0);
  uint8_t* block_bytes = target->blocks() + block_id * num_block_bytes;
  uint64_t block = *reinterpret_cast<const uint64_t*>(block_bytes);

  // Search for the first block with empty slots.
  // Stop after reaching max block id.
  //
  constexpr uint64_t kHighBitOfEachByte = 0x8080808080808080ULL;
  while ((block & kHighBitOfEachByte) == 0 && block_id < max_block_id) {
    block_id = (block_id + 1) & block_id_mask;
    block_bytes = target->blocks() + block_id * num_block_bytes;
    block = *reinterpret_cast<const uint64_t*>(block_bytes);
  }
  if ((block & kHighBitOfEachByte) == 0) {
    return false;
  }
  constexpr int kSlotsPerBlock = 8;
  int local_slot_id =
      kSlotsPerBlock - static_cast<int>(ARROW_POPCOUNT64(block & kHighBitOfEachByte));
  int64_t global_slot_id = block_id * kSlotsPerBlock + local_slot_id;
  target->insert_into_empty_slot(static_cast<uint32_t>(global_slot_id), hash,
                                 static_cast<uint32_t>(group_id));
  return true;
}

void SwissTableMerge::InsertNewGroups(SwissTable* target,
                                      const std::vector<uint32_t>& group_ids,
                                      const std::vector<uint32_t>& hashes) {
  int64_t num_blocks = 1LL << target->log_blocks();
  for (size_t i = 0; i < group_ids.size(); ++i) {
    std::ignore = InsertNewGroup(target, group_ids[i], hashes[i], num_blocks);
  }
}

SwissTableWithKeys::Input::Input(const ExecBatch* in_batch, int in_batch_start_row,
                                 int in_batch_end_row,
                                 arrow::util::TempVectorStack* in_temp_stack,
                                 std::vector<KeyColumnArray>* in_temp_column_arrays)
    : batch(in_batch),
      batch_start_row(in_batch_start_row),
      batch_end_row(in_batch_end_row),
      num_selected(0),
      selection_maybe_null(nullptr),
      temp_stack(in_temp_stack),
      temp_column_arrays(in_temp_column_arrays),
      temp_group_ids(nullptr) {}

SwissTableWithKeys::Input::Input(const ExecBatch* in_batch,
                                 arrow::util::TempVectorStack* in_temp_stack,
                                 std::vector<KeyColumnArray>* in_temp_column_arrays)
    : batch(in_batch),
      batch_start_row(0),
      batch_end_row(static_cast<int>(in_batch->length)),
      num_selected(0),
      selection_maybe_null(nullptr),
      temp_stack(in_temp_stack),
      temp_column_arrays(in_temp_column_arrays),
      temp_group_ids(nullptr) {}

SwissTableWithKeys::Input::Input(const ExecBatch* in_batch, int in_num_selected,
                                 const uint16_t* in_selection,
                                 arrow::util::TempVectorStack* in_temp_stack,
                                 std::vector<KeyColumnArray>* in_temp_column_arrays,
                                 std::vector<uint32_t>* in_temp_group_ids)
    : batch(in_batch),
      batch_start_row(0),
      batch_end_row(static_cast<int>(in_batch->length)),
      num_selected(in_num_selected),
      selection_maybe_null(in_selection),
      temp_stack(in_temp_stack),
      temp_column_arrays(in_temp_column_arrays),
      temp_group_ids(in_temp_group_ids) {}

SwissTableWithKeys::Input::Input(const Input& base, int num_rows_to_skip,
                                 int num_rows_to_include)
    : batch(base.batch),
      temp_stack(base.temp_stack),
      temp_column_arrays(base.temp_column_arrays),
      temp_group_ids(base.temp_group_ids) {
  if (base.selection_maybe_null) {
    batch_start_row = 0;
    batch_end_row = static_cast<int>(batch->length);
    ARROW_DCHECK(num_rows_to_skip + num_rows_to_include <= base.num_selected);
    num_selected = num_rows_to_include;
    selection_maybe_null = base.selection_maybe_null + num_rows_to_skip;
  } else {
    ARROW_DCHECK(base.batch_start_row + num_rows_to_skip + num_rows_to_include <=
                 base.batch_end_row);
    batch_start_row = base.batch_start_row + num_rows_to_skip;
    batch_end_row = base.batch_start_row + num_rows_to_skip + num_rows_to_include;
    num_selected = 0;
    selection_maybe_null = nullptr;
  }
}

Status SwissTableWithKeys::Init(int64_t hardware_flags, MemoryPool* pool) {
  InitCallbacks();
  return swiss_table_.init(hardware_flags, pool);
}

void SwissTableWithKeys::EqualCallback(int num_keys, const uint16_t* selection_maybe_null,
                                       const uint32_t* group_ids,
                                       uint32_t* out_num_keys_mismatch,
                                       uint16_t* out_selection_mismatch,
                                       void* callback_ctx) {
  if (num_keys == 0) {
    *out_num_keys_mismatch = 0;
    return;
  }

  ARROW_DCHECK(num_keys <= swiss_table_.minibatch_size());

  Input* in = reinterpret_cast<Input*>(callback_ctx);

  int64_t hardware_flags = swiss_table_.hardware_flags();

  int batch_start_to_use;
  int batch_end_to_use;
  const uint16_t* selection_to_use;
  const uint32_t* group_ids_to_use;

  if (in->selection_maybe_null) {
    auto selection_to_use_buf =
        arrow::util::TempVectorHolder<uint16_t>(in->temp_stack, num_keys);
    ARROW_DCHECK(in->temp_group_ids);
    in->temp_group_ids->resize(in->batch->length);

    if (selection_maybe_null) {
      for (int i = 0; i < num_keys; ++i) {
        uint16_t local_row_id = selection_maybe_null[i];
        uint16_t global_row_id = in->selection_maybe_null[local_row_id];
        selection_to_use_buf.mutable_data()[i] = global_row_id;
        (*in->temp_group_ids)[global_row_id] = group_ids[local_row_id];
      }
      selection_to_use = selection_to_use_buf.mutable_data();
    } else {
      for (int i = 0; i < num_keys; ++i) {
        uint16_t global_row_id = in->selection_maybe_null[i];
        (*in->temp_group_ids)[global_row_id] = group_ids[i];
      }
      selection_to_use = in->selection_maybe_null;
    }
    batch_start_to_use = 0;
    batch_end_to_use = static_cast<int>(in->batch->length);
    group_ids_to_use = in->temp_group_ids->data();

    auto match_bitvector_buf =
        arrow::util::TempVectorHolder<uint8_t>(in->temp_stack, num_keys);
    uint8_t* match_bitvector = match_bitvector_buf.mutable_data();

    keys_.Compare(*in->batch, batch_start_to_use, batch_end_to_use, num_keys,
                  selection_to_use, group_ids_to_use, nullptr, nullptr, hardware_flags,
                  in->temp_stack, *in->temp_column_arrays, match_bitvector);

    if (selection_maybe_null) {
      int num_keys_mismatch = 0;
      arrow::util::bit_util::bits_filter_indexes(
          0, hardware_flags, num_keys, match_bitvector, selection_maybe_null,
          &num_keys_mismatch, out_selection_mismatch);
      *out_num_keys_mismatch = num_keys_mismatch;
    } else {
      int num_keys_mismatch = 0;
      arrow::util::bit_util::bits_to_indexes(0, hardware_flags, num_keys, match_bitvector,
                                             &num_keys_mismatch, out_selection_mismatch);
      *out_num_keys_mismatch = num_keys_mismatch;
    }

  } else {
    batch_start_to_use = in->batch_start_row;
    batch_end_to_use = in->batch_end_row;
    selection_to_use = selection_maybe_null;
    group_ids_to_use = group_ids;
    keys_.Compare(*in->batch, batch_start_to_use, batch_end_to_use, num_keys,
                  selection_to_use, group_ids_to_use, out_num_keys_mismatch,
                  out_selection_mismatch, hardware_flags, in->temp_stack,
                  *in->temp_column_arrays);
  }
}

Status SwissTableWithKeys::AppendCallback(int num_keys, const uint16_t* selection,
                                          void* callback_ctx) {
  ARROW_DCHECK(num_keys <= swiss_table_.minibatch_size());
  ARROW_DCHECK(selection);

  Input* in = reinterpret_cast<Input*>(callback_ctx);

  int batch_start_to_use;
  int batch_end_to_use;
  const uint16_t* selection_to_use;

  if (in->selection_maybe_null) {
    auto selection_to_use_buf =
        arrow::util::TempVectorHolder<uint16_t>(in->temp_stack, num_keys);
    for (int i = 0; i < num_keys; ++i) {
      selection_to_use_buf.mutable_data()[i] = in->selection_maybe_null[selection[i]];
    }
    batch_start_to_use = 0;
    batch_end_to_use = static_cast<int>(in->batch->length);
    selection_to_use = selection_to_use_buf.mutable_data();

    return keys_.AppendBatchSelection(swiss_table_.pool(), *in->batch, batch_start_to_use,
                                      batch_end_to_use, num_keys, selection_to_use,
                                      *in->temp_column_arrays);
  } else {
    batch_start_to_use = in->batch_start_row;
    batch_end_to_use = in->batch_end_row;
    selection_to_use = selection;

    return keys_.AppendBatchSelection(swiss_table_.pool(), *in->batch, batch_start_to_use,
                                      batch_end_to_use, num_keys, selection_to_use,
                                      *in->temp_column_arrays);
  }
}

void SwissTableWithKeys::InitCallbacks() {
  equal_impl_ = [&](int num_keys, const uint16_t* selection_maybe_null,
                    const uint32_t* group_ids, uint32_t* out_num_keys_mismatch,
                    uint16_t* out_selection_mismatch, void* callback_ctx) {
    EqualCallback(num_keys, selection_maybe_null, group_ids, out_num_keys_mismatch,
                  out_selection_mismatch, callback_ctx);
  };
  append_impl_ = [&](int num_keys, const uint16_t* selection, void* callback_ctx) {
    return AppendCallback(num_keys, selection, callback_ctx);
  };
}

void SwissTableWithKeys::Hash(Input* input, uint32_t* hashes, int64_t hardware_flags) {
  // Hashing does not support selection of rows
  //
  ARROW_DCHECK(input->selection_maybe_null == nullptr);

  Status status =
      Hashing32::HashBatch(*input->batch, hashes, *input->temp_column_arrays,
                           hardware_flags, input->temp_stack, input->batch_start_row,
                           input->batch_end_row - input->batch_start_row);
  ARROW_DCHECK(status.ok());
}

void SwissTableWithKeys::MapReadOnly(Input* input, const uint32_t* hashes,
                                     uint8_t* match_bitvector, uint32_t* key_ids) {
  std::ignore = Map(input, /*insert_missing=*/false, hashes, match_bitvector, key_ids);
}

Status SwissTableWithKeys::MapWithInserts(Input* input, const uint32_t* hashes,
                                          uint32_t* key_ids) {
  return Map(input, /*insert_missing=*/true, hashes, nullptr, key_ids);
}

Status SwissTableWithKeys::Map(Input* input, bool insert_missing, const uint32_t* hashes,
                               uint8_t* match_bitvector_maybe_null, uint32_t* key_ids) {
  arrow::util::TempVectorStack* temp_stack = input->temp_stack;

  // Split into smaller mini-batches
  //
  int minibatch_size = swiss_table_.minibatch_size();
  int num_rows_to_process = input->selection_maybe_null
                                ? input->num_selected
                                : input->batch_end_row - input->batch_start_row;
  auto hashes_buf = arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
  auto match_bitvector_buf = arrow::util::TempVectorHolder<uint8_t>(
      temp_stack,
      static_cast<uint32_t>(bit_util::BytesForBits(minibatch_size)) + sizeof(uint64_t));
  for (int minibatch_start = 0; minibatch_start < num_rows_to_process;) {
    int minibatch_size_next =
        std::min(minibatch_size, num_rows_to_process - minibatch_start);

    // Prepare updated input buffers that represent the current minibatch.
    //
    Input minibatch_input(*input, minibatch_start, minibatch_size_next);
    uint8_t* minibatch_match_bitvector =
        insert_missing ? match_bitvector_buf.mutable_data()
                       : match_bitvector_maybe_null + minibatch_start / 8;
    const uint32_t* minibatch_hashes;
    if (input->selection_maybe_null) {
      minibatch_hashes = hashes_buf.mutable_data();
      for (int i = 0; i < minibatch_size_next; ++i) {
        hashes_buf.mutable_data()[i] = hashes[minibatch_input.selection_maybe_null[i]];
      }
    } else {
      minibatch_hashes = hashes + minibatch_start;
    }
    uint32_t* minibatch_key_ids = key_ids + minibatch_start;

    // Lookup existing keys.
    {
      auto slots =
          arrow::util::TempVectorHolder<uint8_t>(temp_stack, minibatch_size_next);
      swiss_table_.early_filter(minibatch_size_next, minibatch_hashes,
                                minibatch_match_bitvector, slots.mutable_data());
      swiss_table_.find(minibatch_size_next, minibatch_hashes, minibatch_match_bitvector,
                        slots.mutable_data(), minibatch_key_ids, temp_stack, equal_impl_,
                        &minibatch_input);
    }

    // Perform inserts of missing keys if required.
    //
    if (insert_missing) {
      auto ids_buf =
          arrow::util::TempVectorHolder<uint16_t>(temp_stack, minibatch_size_next);
      int num_ids;
      arrow::util::bit_util::bits_to_indexes(
          0, swiss_table_.hardware_flags(), minibatch_size_next,
          minibatch_match_bitvector, &num_ids, ids_buf.mutable_data());

      RETURN_NOT_OK(swiss_table_.map_new_keys(
          num_ids, ids_buf.mutable_data(), minibatch_hashes, minibatch_key_ids,
          temp_stack, equal_impl_, append_impl_, &minibatch_input));
    }

    minibatch_start += minibatch_size_next;
  }

  return Status::OK();
}

uint8_t* SwissTableForJoin::local_has_match(int64_t thread_id) {
  int64_t num_rows_hash_table = num_rows();
  if (num_rows_hash_table == 0) {
    return nullptr;
  }

  ThreadLocalState& local_state = local_states_[thread_id];
  if (local_state.has_match.empty() && num_rows_hash_table > 0) {
    local_state.has_match.resize(bit_util::BytesForBits(num_rows_hash_table) +
                                 sizeof(uint64_t));
    memset(local_state.has_match.data(), 0, bit_util::BytesForBits(num_rows_hash_table));
  }

  return local_states_[thread_id].has_match.data();
}

void SwissTableForJoin::UpdateHasMatchForKeys(int64_t thread_id, int num_ids,
                                              const uint32_t* key_ids) {
  uint8_t* bit_vector = local_has_match(thread_id);
  if (num_ids == 0 || !bit_vector) {
    return;
  }
  for (int i = 0; i < num_ids; ++i) {
    // Mark row in hash table as having a match
    //
    bit_util::SetBit(bit_vector, key_ids[i]);
  }
}

void SwissTableForJoin::MergeHasMatch() {
  int64_t num_rows_hash_table = num_rows();
  if (num_rows_hash_table == 0) {
    return;
  }

  has_match_.resize(bit_util::BytesForBits(num_rows_hash_table) + sizeof(uint64_t));
  memset(has_match_.data(), 0, bit_util::BytesForBits(num_rows_hash_table));

  for (size_t tid = 0; tid < local_states_.size(); ++tid) {
    if (!local_states_[tid].has_match.empty()) {
      arrow::internal::BitmapOr(has_match_.data(), 0, local_states_[tid].has_match.data(),
                                0, num_rows_hash_table, 0, has_match_.data());
    }
  }
}

uint32_t SwissTableForJoin::payload_id_to_key_id(uint32_t payload_id) const {
  if (no_duplicate_keys_) {
    return payload_id;
  }
  int64_t num_entries = num_keys();
  const uint32_t* entries = key_to_payload();
  ARROW_DCHECK(entries);
  ARROW_DCHECK(entries[num_entries] > payload_id);
  const uint32_t* first_greater =
      std::upper_bound(entries, entries + num_entries + 1, payload_id);
  ARROW_DCHECK(first_greater > entries);
  return static_cast<uint32_t>(first_greater - entries) - 1;
}

void SwissTableForJoin::payload_ids_to_key_ids(int num_rows, const uint32_t* payload_ids,
                                               uint32_t* key_ids) const {
  if (num_rows == 0) {
    return;
  }
  if (no_duplicate_keys_) {
    memcpy(key_ids, payload_ids, num_rows * sizeof(uint32_t));
    return;
  }

  const uint32_t* entries = key_to_payload();
  uint32_t key_id = payload_id_to_key_id(payload_ids[0]);
  key_ids[0] = key_id;
  for (int i = 1; i < num_rows; ++i) {
    ARROW_DCHECK(payload_ids[i] > payload_ids[i - 1]);
    while (entries[key_id + 1] <= payload_ids[i]) {
      ++key_id;
      ARROW_DCHECK(key_id < num_keys());
    }
    key_ids[i] = key_id;
  }
}

Status SwissTableForJoinBuild::Init(SwissTableForJoin* target, int dop, int64_t num_rows,
                                    bool reject_duplicate_keys, bool no_payload,
                                    const std::vector<KeyColumnMetadata>& key_types,
                                    const std::vector<KeyColumnMetadata>& payload_types,
                                    MemoryPool* pool, int64_t hardware_flags) {
  target_ = target;
  dop_ = dop;
  num_rows_ = num_rows;

  // Make sure that we do not use many partitions if there are not enough rows.
  //
  constexpr int64_t min_num_rows_per_prtn = 1 << 18;
  log_num_prtns_ =
      std::min(bit_util::Log2(dop_),
               bit_util::Log2(bit_util::CeilDiv(num_rows, min_num_rows_per_prtn)));
  num_prtns_ = 1 << log_num_prtns_;

  reject_duplicate_keys_ = reject_duplicate_keys;
  no_payload_ = no_payload;
  pool_ = pool;
  hardware_flags_ = hardware_flags;

  prtn_states_.resize(num_prtns_);
  thread_states_.resize(dop_);
  prtn_locks_.Init(dop_, num_prtns_);

  RowTableMetadata key_row_metadata;
  key_row_metadata.FromColumnMetadataVector(key_types,
                                            /*row_alignment=*/sizeof(uint64_t),
                                            /*string_alignment=*/sizeof(uint64_t));
  RowTableMetadata payload_row_metadata;
  payload_row_metadata.FromColumnMetadataVector(payload_types,
                                                /*row_alignment=*/sizeof(uint64_t),
                                                /*string_alignment=*/sizeof(uint64_t));

  for (int i = 0; i < num_prtns_; ++i) {
    PartitionState& prtn_state = prtn_states_[i];
    RETURN_NOT_OK(prtn_state.keys.Init(hardware_flags_, pool_));
    RETURN_NOT_OK(prtn_state.keys.keys()->InitIfNeeded(pool, key_row_metadata));
    RETURN_NOT_OK(prtn_state.payloads.InitIfNeeded(pool, payload_row_metadata));
  }

  target_->dop_ = dop_;
  target_->local_states_.resize(dop_);
  target_->no_payload_columns_ = no_payload;
  target_->no_duplicate_keys_ = reject_duplicate_keys;
  target_->map_.InitCallbacks();

  return Status::OK();
}

Status SwissTableForJoinBuild::PushNextBatch(int64_t thread_id,
                                             const ExecBatch& key_batch,
                                             const ExecBatch* payload_batch_maybe_null,
                                             arrow::util::TempVectorStack* temp_stack) {
  ARROW_DCHECK(thread_id < dop_);
  ThreadState& locals = thread_states_[thread_id];

  // Compute hash
  //
  locals.batch_hashes.resize(key_batch.length);
  RETURN_NOT_OK(Hashing32::HashBatch(
      key_batch, locals.batch_hashes.data(), locals.temp_column_arrays, hardware_flags_,
      temp_stack, /*start_row=*/0, static_cast<int>(key_batch.length)));

  // Partition on hash
  //
  locals.batch_prtn_row_ids.resize(locals.batch_hashes.size());
  locals.batch_prtn_ranges.resize(num_prtns_ + 1);
  int num_rows = static_cast<int>(locals.batch_hashes.size());
  if (num_prtns_ == 1) {
    // We treat single partition case separately to avoid extra checks in row
    // partitioning implementation for general case.
    //
    locals.batch_prtn_ranges[0] = 0;
    locals.batch_prtn_ranges[1] = num_rows;
    for (int i = 0; i < num_rows; ++i) {
      locals.batch_prtn_row_ids[i] = i;
    }
  } else {
    PartitionSort::Eval(
        static_cast<int>(locals.batch_hashes.size()), num_prtns_,
        locals.batch_prtn_ranges.data(),
        [this, &locals](int64_t i) {
          // SwissTable uses the highest bits of the hash for block index.
          // We want each partition to correspond to a range of block indices,
          // so we also partition on the highest bits of the hash.
          //
          return locals.batch_hashes[i] >> (31 - log_num_prtns_) >> 1;
        },
        [&locals](int64_t i, int pos) {
          locals.batch_prtn_row_ids[pos] = static_cast<uint16_t>(i);
        });
  }

  // Update hashes, shifting left to get rid of the bits that were already used
  // for partitioning.
  //
  for (size_t i = 0; i < locals.batch_hashes.size(); ++i) {
    locals.batch_hashes[i] <<= log_num_prtns_;
  }

  // For each partition:
  // - map keys to unique integers using (this partition's) hash table
  // - append payloads (if present) to (this partition's) row array
  //
  locals.temp_prtn_ids.resize(num_prtns_);

  RETURN_NOT_OK(prtn_locks_.ForEachPartition(
      thread_id, locals.temp_prtn_ids.data(),
      /*is_prtn_empty_fn=*/
      [&](int prtn_id) {
        return locals.batch_prtn_ranges[prtn_id + 1] == locals.batch_prtn_ranges[prtn_id];
      },
      /*process_prtn_fn=*/
      [&](int prtn_id) {
        return ProcessPartition(thread_id, key_batch, payload_batch_maybe_null,
                                temp_stack, prtn_id);
      }));

  return Status::OK();
}

Status SwissTableForJoinBuild::ProcessPartition(int64_t thread_id,
                                                const ExecBatch& key_batch,
                                                const ExecBatch* payload_batch_maybe_null,
                                                arrow::util::TempVectorStack* temp_stack,
                                                int prtn_id) {
  ARROW_DCHECK(thread_id < dop_);
  ThreadState& locals = thread_states_[thread_id];

  int num_rows_new =
      locals.batch_prtn_ranges[prtn_id + 1] - locals.batch_prtn_ranges[prtn_id];
  const uint16_t* row_ids =
      locals.batch_prtn_row_ids.data() + locals.batch_prtn_ranges[prtn_id];
  PartitionState& prtn_state = prtn_states_[prtn_id];
  size_t num_rows_before = prtn_state.key_ids.size();
  // Insert new keys into hash table associated with the current partition
  // and map existing keys to integer ids.
  //
  prtn_state.key_ids.resize(num_rows_before + num_rows_new);
  SwissTableWithKeys::Input input(&key_batch, num_rows_new, row_ids, temp_stack,
                                  &locals.temp_column_arrays, &locals.temp_group_ids);
  RETURN_NOT_OK(prtn_state.keys.MapWithInserts(
      &input, locals.batch_hashes.data(), prtn_state.key_ids.data() + num_rows_before));
  // Append input batch rows from current partition to an array of payload
  // rows for this partition.
  //
  // The order of payloads is the same as the order of key ids accumulated
  // in a vector (we will use the vector of key ids later on to sort
  // payload on key ids before merging into the final row array).
  //
  if (!no_payload_) {
    ARROW_DCHECK(payload_batch_maybe_null);
    RETURN_NOT_OK(prtn_state.payloads.AppendBatchSelection(
        pool_, *payload_batch_maybe_null, 0,
        static_cast<int>(payload_batch_maybe_null->length), num_rows_new, row_ids,
        locals.temp_column_arrays));
  }
  // We do not need to keep track of key ids if we reject rows with
  // duplicate keys.
  //
  if (reject_duplicate_keys_) {
    prtn_state.key_ids.clear();
  }
  return Status::OK();
}

Status SwissTableForJoinBuild::PreparePrtnMerge() {
  // There are 4 data structures that require partition merging:
  // 1. array of key rows
  // 2. SwissTable
  // 3. array of payload rows (only when no_payload_ is false)
  // 4. mapping from key id to first payload id (only when
  // reject_duplicate_keys_ is false and there are duplicate keys)
  //

  // 1. Array of key rows:
  //
  std::vector<RowArray*> partition_keys;
  partition_keys.resize(num_prtns_);
  for (int i = 0; i < num_prtns_; ++i) {
    partition_keys[i] = prtn_states_[i].keys.keys();
  }
  RETURN_NOT_OK(RowArrayMerge::PrepareForMerge(target_->map_.keys(), partition_keys,
                                               &partition_keys_first_row_id_, pool_));

  // 2. SwissTable:
  //
  std::vector<SwissTable*> partition_tables;
  partition_tables.resize(num_prtns_);
  for (int i = 0; i < num_prtns_; ++i) {
    partition_tables[i] = prtn_states_[i].keys.swiss_table();
  }
  std::vector<uint32_t> partition_first_group_id;
  RETURN_NOT_OK(SwissTableMerge::PrepareForMerge(
      target_->map_.swiss_table(), partition_tables, &partition_first_group_id, pool_));

  // 3. Array of payload rows:
  //
  if (!no_payload_) {
    std::vector<RowArray*> partition_payloads;
    partition_payloads.resize(num_prtns_);
    for (int i = 0; i < num_prtns_; ++i) {
      partition_payloads[i] = &prtn_states_[i].payloads;
    }
    RETURN_NOT_OK(RowArrayMerge::PrepareForMerge(&target_->payloads_, partition_payloads,
                                                 &partition_payloads_first_row_id_,
                                                 pool_));
  }

  // Check if we have duplicate keys
  //
  int64_t num_keys = partition_keys_first_row_id_[num_prtns_];
  int64_t num_rows = 0;
  for (int i = 0; i < num_prtns_; ++i) {
    num_rows += static_cast<int64_t>(prtn_states_[i].key_ids.size());
  }
  bool no_duplicate_keys = reject_duplicate_keys_ || num_keys == num_rows;

  // 4. Mapping from key id to first payload id:
  //
  target_->no_duplicate_keys_ = no_duplicate_keys;
  if (!no_duplicate_keys) {
    target_->row_offset_for_key_.resize(num_keys + 1);
    int64_t num_rows = 0;
    for (int i = 0; i < num_prtns_; ++i) {
      int64_t first_key = partition_keys_first_row_id_[i];
      target_->row_offset_for_key_[first_key] = static_cast<uint32_t>(num_rows);
      num_rows += static_cast<int64_t>(prtn_states_[i].key_ids.size());
    }
    target_->row_offset_for_key_[num_keys] = static_cast<uint32_t>(num_rows);
  }

  return Status::OK();
}

void SwissTableForJoinBuild::PrtnMerge(int prtn_id) {
  PartitionState& prtn_state = prtn_states_[prtn_id];

  // There are 4 data structures that require partition merging:
  // 1. array of key rows
  // 2. SwissTable
  // 3. mapping from key id to first payload id (only when
  // reject_duplicate_keys_ is false and there are duplicate keys)
  // 4. array of payload rows (only when no_payload_ is false)
  //

  // 1. Array of key rows:
  //
  RowArrayMerge::MergeSingle(target_->map_.keys(), *prtn_state.keys.keys(),
                             partition_keys_first_row_id_[prtn_id],
                             /*source_rows_permutation=*/nullptr);

  // 2. SwissTable:
  //
  SwissTableMerge::MergePartition(
      target_->map_.swiss_table(), prtn_state.keys.swiss_table(), prtn_id, log_num_prtns_,
      static_cast<uint32_t>(partition_keys_first_row_id_[prtn_id]),
      &prtn_state.overflow_key_ids, &prtn_state.overflow_hashes);

  std::vector<int64_t> source_payload_ids;

  // 3. mapping from key id to first payload id
  //
  if (!target_->no_duplicate_keys_) {
    // Count for each local (within partition) key id how many times it appears
    // in input rows.
    //
    // For convenience, we use an array in merged hash table mapping key ids to
    // first payload ids to collect the counters.
    //
    int64_t first_key = partition_keys_first_row_id_[prtn_id];
    int64_t num_keys = partition_keys_first_row_id_[prtn_id + 1] - first_key;
    uint32_t* counters = target_->row_offset_for_key_.data() + first_key;
    uint32_t first_payload = counters[0];
    for (int64_t i = 0; i < num_keys; ++i) {
      counters[i] = 0;
    }
    for (size_t i = 0; i < prtn_state.key_ids.size(); ++i) {
      uint32_t key_id = prtn_state.key_ids[i];
      ++counters[key_id];
    }

    if (!no_payload_) {
      // Count sort payloads on key id
      //
      // Start by computing inclusive cummulative sum of counters.
      //
      uint32_t sum = 0;
      for (int64_t i = 0; i < num_keys; ++i) {
        sum += counters[i];
        counters[i] = sum;
      }
      // Now use cummulative sum of counters to obtain the target position in
      // the sorted order for each row. At the end of this process the counters
      // will contain exclusive cummulative sum (instead of inclusive that is
      // there at the beginning).
      //
      source_payload_ids.resize(prtn_state.key_ids.size());
      for (size_t i = 0; i < prtn_state.key_ids.size(); ++i) {
        uint32_t key_id = prtn_state.key_ids[i];
        int64_t position = --counters[key_id];
        source_payload_ids[position] = static_cast<int64_t>(i);
      }
      // Add base payload id to all of the counters.
      //
      for (int64_t i = 0; i < num_keys; ++i) {
        counters[i] += first_payload;
      }
    } else {
      // When there is no payload to process, we just need to compute exclusive
      // cummulative sum of counters and add the base payload id to all of them.
      //
      uint32_t sum = 0;
      for (int64_t i = 0; i < num_keys; ++i) {
        uint32_t sum_next = sum + counters[i];
        counters[i] = sum + first_payload;
        sum = sum_next;
      }
    }
  }

  // 4. Array of payload rows:
  //
  if (!no_payload_) {
    // If there are duplicate keys, then we have already initialized permutation
    // of payloads for this partition.
    //
    if (target_->no_duplicate_keys_) {
      source_payload_ids.resize(prtn_state.key_ids.size());
      for (size_t i = 0; i < prtn_state.key_ids.size(); ++i) {
        uint32_t key_id = prtn_state.key_ids[i];
        source_payload_ids[key_id] = static_cast<int64_t>(i);
      }
    }
    // Merge partition payloads into target array using the permutation.
    //
    RowArrayMerge::MergeSingle(&target_->payloads_, prtn_state.payloads,
                               partition_payloads_first_row_id_[prtn_id],
                               source_payload_ids.data());
  }
}

void SwissTableForJoinBuild::FinishPrtnMerge(arrow::util::TempVectorStack* temp_stack) {
  // Process overflow key ids
  //
  for (int prtn_id = 0; prtn_id < num_prtns_; ++prtn_id) {
    SwissTableMerge::InsertNewGroups(target_->map_.swiss_table(),
                                     prtn_states_[prtn_id].overflow_key_ids,
                                     prtn_states_[prtn_id].overflow_hashes);
  }

  // Calculate whether we have nulls in hash table keys
  // (it is lazily evaluated but since we will be accessing it from multiple
  // threads we need to make sure that the value gets calculated here).
  //
  LightContext ctx;
  ctx.hardware_flags = hardware_flags_;
  ctx.stack = temp_stack;
  std::ignore = target_->map_.keys()->rows_.has_any_nulls(&ctx);
}

void JoinResultMaterialize::Init(MemoryPool* pool,
                                 const HashJoinProjectionMaps* probe_schemas,
                                 const HashJoinProjectionMaps* build_schemas) {
  pool_ = pool;
  probe_schemas_ = probe_schemas;
  build_schemas_ = build_schemas;
  num_rows_ = 0;
  null_ranges_.clear();
  num_produced_batches_ = 0;

  // Initialize mapping of columns from output batch column index to key and
  // payload batch column index.
  //
  probe_output_to_key_and_payload_.resize(
      probe_schemas_->num_cols(HashJoinProjection::OUTPUT));
  int num_key_cols = probe_schemas_->num_cols(HashJoinProjection::KEY);
  auto to_key = probe_schemas_->map(HashJoinProjection::OUTPUT, HashJoinProjection::KEY);
  auto to_payload =
      probe_schemas_->map(HashJoinProjection::OUTPUT, HashJoinProjection::PAYLOAD);
  for (int i = 0; static_cast<size_t>(i) < probe_output_to_key_and_payload_.size(); ++i) {
    probe_output_to_key_and_payload_[i] =
        to_key.get(i) == SchemaProjectionMap::kMissingField
            ? to_payload.get(i) + num_key_cols
            : to_key.get(i);
  }
}

void JoinResultMaterialize::SetBuildSide(const RowArray* build_keys,
                                         const RowArray* build_payloads,
                                         bool payload_id_same_as_key_id) {
  build_keys_ = build_keys;
  build_payloads_ = build_payloads;
  payload_id_same_as_key_id_ = payload_id_same_as_key_id;
}

bool JoinResultMaterialize::HasProbeOutput() const {
  return probe_schemas_->num_cols(HashJoinProjection::OUTPUT) > 0;
}

bool JoinResultMaterialize::HasBuildKeyOutput() const {
  auto to_key = build_schemas_->map(HashJoinProjection::OUTPUT, HashJoinProjection::KEY);
  for (int i = 0; i < build_schemas_->num_cols(HashJoinProjection::OUTPUT); ++i) {
    if (to_key.get(i) != SchemaProjectionMap::kMissingField) {
      return true;
    }
  }
  return false;
}

bool JoinResultMaterialize::HasBuildPayloadOutput() const {
  auto to_payload =
      build_schemas_->map(HashJoinProjection::OUTPUT, HashJoinProjection::PAYLOAD);
  for (int i = 0; i < build_schemas_->num_cols(HashJoinProjection::OUTPUT); ++i) {
    if (to_payload.get(i) != SchemaProjectionMap::kMissingField) {
      return true;
    }
  }
  return false;
}

bool JoinResultMaterialize::NeedsKeyId() const {
  return HasBuildKeyOutput() || (HasBuildPayloadOutput() && payload_id_same_as_key_id_);
}

bool JoinResultMaterialize::NeedsPayloadId() const {
  return HasBuildPayloadOutput() && !payload_id_same_as_key_id_;
}

Status JoinResultMaterialize::AppendProbeOnly(const ExecBatch& key_and_payload,
                                              int num_rows_to_append,
                                              const uint16_t* row_ids,
                                              int* num_rows_appended) {
  num_rows_to_append =
      std::min(ExecBatchBuilder::num_rows_max() - num_rows_, num_rows_to_append);
  if (HasProbeOutput()) {
    RETURN_NOT_OK(batch_builder_.AppendSelected(
        pool_, key_and_payload, num_rows_to_append, row_ids,
        static_cast<int>(probe_output_to_key_and_payload_.size()),
        probe_output_to_key_and_payload_.data()));
  }
  if (!null_ranges_.empty() &&
      null_ranges_.back().first + null_ranges_.back().second == num_rows_) {
    // We can extend the last range of null rows on build side.
    //
    null_ranges_.back().second += num_rows_to_append;
  } else {
    null_ranges_.push_back(
        std::make_pair(static_cast<int>(num_rows_), num_rows_to_append));
  }
  num_rows_ += num_rows_to_append;
  *num_rows_appended = num_rows_to_append;
  return Status::OK();
}

Status JoinResultMaterialize::AppendBuildOnly(int num_rows_to_append,
                                              const uint32_t* key_ids,
                                              const uint32_t* payload_ids,
                                              int* num_rows_appended) {
  num_rows_to_append =
      std::min(ExecBatchBuilder::num_rows_max() - num_rows_, num_rows_to_append);
  if (HasProbeOutput()) {
    RETURN_NOT_OK(batch_builder_.AppendNulls(
        pool_, probe_schemas_->data_types(HashJoinProjection::OUTPUT),
        num_rows_to_append));
  }
  if (NeedsKeyId()) {
    ARROW_DCHECK(key_ids != nullptr);
    key_ids_.resize(num_rows_ + num_rows_to_append);
    memcpy(key_ids_.data() + num_rows_, key_ids, num_rows_to_append * sizeof(uint32_t));
  }
  if (NeedsPayloadId()) {
    ARROW_DCHECK(payload_ids != nullptr);
    payload_ids_.resize(num_rows_ + num_rows_to_append);
    memcpy(payload_ids_.data() + num_rows_, payload_ids,
           num_rows_to_append * sizeof(uint32_t));
  }
  num_rows_ += num_rows_to_append;
  *num_rows_appended = num_rows_to_append;
  return Status::OK();
}

Status JoinResultMaterialize::Append(const ExecBatch& key_and_payload,
                                     int num_rows_to_append, const uint16_t* row_ids,
                                     const uint32_t* key_ids, const uint32_t* payload_ids,
                                     int* num_rows_appended) {
  num_rows_to_append =
      std::min(ExecBatchBuilder::num_rows_max() - num_rows_, num_rows_to_append);
  if (HasProbeOutput()) {
    RETURN_NOT_OK(batch_builder_.AppendSelected(
        pool_, key_and_payload, num_rows_to_append, row_ids,
        static_cast<int>(probe_output_to_key_and_payload_.size()),
        probe_output_to_key_and_payload_.data()));
  }
  if (NeedsKeyId()) {
    ARROW_DCHECK(key_ids != nullptr);
    key_ids_.resize(num_rows_ + num_rows_to_append);
    memcpy(key_ids_.data() + num_rows_, key_ids, num_rows_to_append * sizeof(uint32_t));
  }
  if (NeedsPayloadId()) {
    ARROW_DCHECK(payload_ids != nullptr);
    payload_ids_.resize(num_rows_ + num_rows_to_append);
    memcpy(payload_ids_.data() + num_rows_, payload_ids,
           num_rows_to_append * sizeof(uint32_t));
  }
  num_rows_ += num_rows_to_append;
  *num_rows_appended = num_rows_to_append;
  return Status::OK();
}

Result<std::shared_ptr<ArrayData>> JoinResultMaterialize::FlushBuildColumn(
    const std::shared_ptr<DataType>& data_type, const RowArray* row_array, int column_id,
    uint32_t* row_ids) {
  ResizableArrayData output;
  output.Init(data_type, pool_, bit_util::Log2(num_rows_));

  for (size_t i = 0; i <= null_ranges_.size(); ++i) {
    int row_id_begin =
        i == 0 ? 0 : null_ranges_[i - 1].first + null_ranges_[i - 1].second;
    int row_id_end = i == null_ranges_.size() ? num_rows_ : null_ranges_[i].first;
    if (row_id_end > row_id_begin) {
      RETURN_NOT_OK(row_array->DecodeSelected(
          &output, column_id, row_id_end - row_id_begin, row_ids + row_id_begin, pool_));
    }
    int num_nulls = i == null_ranges_.size() ? 0 : null_ranges_[i].second;
    if (num_nulls > 0) {
      RETURN_NOT_OK(ExecBatchBuilder::AppendNulls(data_type, output, num_nulls, pool_));
    }
  }

  return output.array_data();
}

Status JoinResultMaterialize::Flush(ExecBatch* out) {
  if (num_rows_ == 0) {
    return Status::OK();
  }

  out->length = num_rows_;
  out->values.clear();

  int num_probe_cols = probe_schemas_->num_cols(HashJoinProjection::OUTPUT);
  int num_build_cols = build_schemas_->num_cols(HashJoinProjection::OUTPUT);
  out->values.resize(num_probe_cols + num_build_cols);

  if (HasProbeOutput()) {
    ExecBatch probe_batch = batch_builder_.Flush();
    ARROW_DCHECK(static_cast<int>(probe_batch.values.size()) == num_probe_cols);
    for (size_t i = 0; i < probe_batch.values.size(); ++i) {
      out->values[i] = std::move(probe_batch.values[i]);
    }
  }
  auto to_key = build_schemas_->map(HashJoinProjection::OUTPUT, HashJoinProjection::KEY);
  auto to_payload =
      build_schemas_->map(HashJoinProjection::OUTPUT, HashJoinProjection::PAYLOAD);
  for (int i = 0; i < num_build_cols; ++i) {
    if (to_key.get(i) != SchemaProjectionMap::kMissingField) {
      std::shared_ptr<ArrayData> column;
      ARROW_ASSIGN_OR_RAISE(
          column,
          FlushBuildColumn(build_schemas_->data_type(HashJoinProjection::OUTPUT, i),
                           build_keys_, to_key.get(i), key_ids_.data()));
      out->values[num_probe_cols + i] = std::move(column);
    } else if (to_payload.get(i) != SchemaProjectionMap::kMissingField) {
      std::shared_ptr<ArrayData> column;
      ARROW_ASSIGN_OR_RAISE(
          column,
          FlushBuildColumn(
              build_schemas_->data_type(HashJoinProjection::OUTPUT, i), build_payloads_,
              to_payload.get(i),
              payload_id_same_as_key_id_ ? key_ids_.data() : payload_ids_.data()));
      out->values[num_probe_cols + i] = std::move(column);
    } else {
      ARROW_DCHECK(false);
    }
  }

  num_rows_ = 0;
  key_ids_.clear();
  payload_ids_.clear();
  null_ranges_.clear();

  ++num_produced_batches_;

  return Status::OK();
}

void JoinNullFilter::Filter(const ExecBatch& key_batch, int batch_start_row,
                            int num_batch_rows, const std::vector<JoinKeyCmp>& cmp,
                            bool* all_valid, bool and_with_input,
                            uint8_t* inout_bit_vector) {
  // AND together validity vectors for columns that use equality comparison.
  //
  bool is_output_initialized = and_with_input;
  for (size_t i = 0; i < cmp.size(); ++i) {
    // No null filtering if null == null is true
    //
    if (cmp[i] != JoinKeyCmp::EQ) {
      continue;
    }

    // No null filtering when there are no nulls
    //
    const Datum& data = key_batch.values[i];
    ARROW_DCHECK(data.is_array());
    const std::shared_ptr<ArrayData>& array_data = data.array();
    if (!array_data->buffers[0]) {
      continue;
    }

    const uint8_t* non_null_buffer = array_data->buffers[0]->data();
    int64_t offset = array_data->offset + batch_start_row;

    // Filter out nulls for this column
    //
    if (!is_output_initialized) {
      memset(inout_bit_vector, 0xff, bit_util::BytesForBits(num_batch_rows));
      is_output_initialized = true;
    }
    arrow::internal::BitmapAnd(inout_bit_vector, 0, non_null_buffer, offset,
                               num_batch_rows, 0, inout_bit_vector);
  }
  *all_valid = !is_output_initialized;
}

void JoinMatchIterator::SetLookupResult(int num_batch_rows, int start_batch_row,
                                        const uint8_t* batch_has_match,
                                        const uint32_t* key_ids, bool no_duplicate_keys,
                                        const uint32_t* key_to_payload) {
  num_batch_rows_ = num_batch_rows;
  start_batch_row_ = start_batch_row;
  batch_has_match_ = batch_has_match;
  key_ids_ = key_ids;

  no_duplicate_keys_ = no_duplicate_keys;
  key_to_payload_ = key_to_payload;

  current_row_ = 0;
  current_match_for_row_ = 0;
}

bool JoinMatchIterator::GetNextBatch(int num_rows_max, int* out_num_rows,
                                     uint16_t* batch_row_ids, uint32_t* key_ids,
                                     uint32_t* payload_ids) {
  *out_num_rows = 0;

  if (no_duplicate_keys_) {
    // When every input key can have at most one match,
    // then we only need to filter according to has match bit vector.
    //
    // We stop when either we produce a full batch or when we reach the end of
    // matches to output.
    //
    while (current_row_ < num_batch_rows_ && *out_num_rows < num_rows_max) {
      batch_row_ids[*out_num_rows] = start_batch_row_ + current_row_;
      key_ids[*out_num_rows] = payload_ids[*out_num_rows] = key_ids_[current_row_];
      (*out_num_rows) += bit_util::GetBit(batch_has_match_, current_row_) ? 1 : 0;
      ++current_row_;
    }
  } else {
    // When every input key can have zero, one or many matches,
    // then we need to filter out ones with no match and
    // iterate over all matches for the remaining ones.
    //
    // We stop when either we produce a full batch or when we reach the end of
    // matches to output.
    //
    while (current_row_ < num_batch_rows_ && *out_num_rows < num_rows_max) {
      if (!bit_util::GetBit(batch_has_match_, current_row_)) {
        ++current_row_;
        current_match_for_row_ = 0;
        continue;
      }
      uint32_t base_payload_id = key_to_payload_[key_ids_[current_row_]];

      // Total number of matches for the currently selected input row
      //
      int num_matches_total =
          key_to_payload_[key_ids_[current_row_] + 1] - base_payload_id;

      // Number of remaining matches for the currently selected input row
      //
      int num_matches_left = num_matches_total - current_match_for_row_;

      // Number of matches for the currently selected input row that will fit
      // into the next batch
      //
      int num_matches_next = std::min(num_matches_left, num_rows_max - *out_num_rows);

      for (int imatch = 0; imatch < num_matches_next; ++imatch) {
        batch_row_ids[*out_num_rows] = start_batch_row_ + current_row_;
        key_ids[*out_num_rows] = key_ids_[current_row_];
        payload_ids[*out_num_rows] = base_payload_id + current_match_for_row_ + imatch;
        ++(*out_num_rows);
      }
      current_match_for_row_ += num_matches_next;

      if (current_match_for_row_ == num_matches_total) {
        ++current_row_;
        current_match_for_row_ = 0;
      }
    }
  }

  return (*out_num_rows) > 0;
}

void JoinProbeProcessor::Init(int num_key_columns, JoinType join_type,
                              SwissTableForJoin* hash_table,
                              std::vector<JoinResultMaterialize*> materialize,
                              const std::vector<JoinKeyCmp>* cmp,
                              OutputBatchFn output_batch_fn) {
  num_key_columns_ = num_key_columns;
  join_type_ = join_type;
  hash_table_ = hash_table;
  materialize_.resize(materialize.size());
  for (size_t i = 0; i < materialize.size(); ++i) {
    materialize_[i] = materialize[i];
  }
  cmp_ = cmp;
  output_batch_fn_ = output_batch_fn;
}

Status JoinProbeProcessor::OnNextBatch(int64_t thread_id,
                                       const ExecBatch& keypayload_batch,
                                       arrow::util::TempVectorStack* temp_stack,
                                       std::vector<KeyColumnArray>* temp_column_arrays) {
  const SwissTable* swiss_table = hash_table_->keys()->swiss_table();
  int64_t hardware_flags = swiss_table->hardware_flags();
  int minibatch_size = swiss_table->minibatch_size();
  int num_rows = static_cast<int>(keypayload_batch.length);

  ExecBatch key_batch({}, keypayload_batch.length);
  key_batch.values.resize(num_key_columns_);
  for (int i = 0; i < num_key_columns_; ++i) {
    key_batch.values[i] = keypayload_batch.values[i];
  }

  // Break into mini-batches
  //
  // Start by allocating mini-batch buffers
  //
  auto hashes_buf = arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
  auto match_bitvector_buf = arrow::util::TempVectorHolder<uint8_t>(
      temp_stack, static_cast<uint32_t>(bit_util::BytesForBits(minibatch_size)));
  auto key_ids_buf = arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
  auto materialize_batch_ids_buf =
      arrow::util::TempVectorHolder<uint16_t>(temp_stack, minibatch_size);
  auto materialize_key_ids_buf =
      arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
  auto materialize_payload_ids_buf =
      arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);

  for (int minibatch_start = 0; minibatch_start < num_rows;) {
    uint32_t minibatch_size_next = std::min(minibatch_size, num_rows - minibatch_start);

    SwissTableWithKeys::Input input(&key_batch, minibatch_start,
                                    minibatch_start + minibatch_size_next, temp_stack,
                                    temp_column_arrays);
    hash_table_->keys()->Hash(&input, hashes_buf.mutable_data(), hardware_flags);
    hash_table_->keys()->MapReadOnly(&input, hashes_buf.mutable_data(),
                                     match_bitvector_buf.mutable_data(),
                                     key_ids_buf.mutable_data());

    // AND bit vector with null key filter for join
    //
    bool ignored;
    JoinNullFilter::Filter(key_batch, minibatch_start, minibatch_size_next, *cmp_,
                           &ignored,
                           /*and_with_input=*/true, match_bitvector_buf.mutable_data());
    // Semi-joins
    //
    if (join_type_ == JoinType::LEFT_SEMI || join_type_ == JoinType::LEFT_ANTI ||
        join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
      int num_passing_ids = 0;
      arrow::util::bit_util::bits_to_indexes(
          (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags,
          minibatch_size_next, match_bitvector_buf.mutable_data(), &num_passing_ids,
          materialize_batch_ids_buf.mutable_data());

      // For right-semi, right-anti joins: update has-match flags for the rows
      // in hash table.
      //
      if (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
        for (int i = 0; i < num_passing_ids; ++i) {
          uint16_t id = materialize_batch_ids_buf.mutable_data()[i];
          key_ids_buf.mutable_data()[i] = key_ids_buf.mutable_data()[id];
        }
        hash_table_->UpdateHasMatchForKeys(thread_id, num_passing_ids,
                                           key_ids_buf.mutable_data());
      } else {
        // For left-semi, left-anti joins: call materialize using match
        // bit-vector.
        //

        // Add base batch row index.
        //
        for (int i = 0; i < num_passing_ids; ++i) {
          materialize_batch_ids_buf.mutable_data()[i] +=
              static_cast<uint16_t>(minibatch_start);
        }

        RETURN_NOT_OK(materialize_[thread_id]->AppendProbeOnly(
            keypayload_batch, num_passing_ids, materialize_batch_ids_buf.mutable_data(),
            [&](ExecBatch batch) {
              return output_batch_fn_(thread_id, std::move(batch));
            }));
      }
    } else {
      // We need to output matching pairs of rows from both sides of the join.
      // Since every hash table lookup for an input row might have multiple
      // matches we use a helper class that implements enumerating all of them.
      //
      bool no_duplicate_keys = (hash_table_->key_to_payload() == nullptr);
      bool no_payload_columns = (hash_table_->payloads() == nullptr);
      JoinMatchIterator match_iterator;
      match_iterator.SetLookupResult(
          minibatch_size_next, minibatch_start, match_bitvector_buf.mutable_data(),
          key_ids_buf.mutable_data(), no_duplicate_keys, hash_table_->key_to_payload());
      int num_matches_next;
      while (match_iterator.GetNextBatch(minibatch_size, &num_matches_next,
                                         materialize_batch_ids_buf.mutable_data(),
                                         materialize_key_ids_buf.mutable_data(),
                                         materialize_payload_ids_buf.mutable_data())) {
        const uint16_t* materialize_batch_ids = materialize_batch_ids_buf.mutable_data();
        const uint32_t* materialize_key_ids = materialize_key_ids_buf.mutable_data();
        const uint32_t* materialize_payload_ids =
            no_duplicate_keys || no_payload_columns
                ? materialize_key_ids_buf.mutable_data()
                : materialize_payload_ids_buf.mutable_data();

        // For right-outer, full-outer joins we need to update has-match flags
        // for the rows in hash table.
        //
        if (join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
          hash_table_->UpdateHasMatchForKeys(thread_id, num_matches_next,
                                             materialize_key_ids);
        }

        // Call materialize for resulting id tuples pointing to matching pairs
        // of rows.
        //
        RETURN_NOT_OK(materialize_[thread_id]->Append(
            keypayload_batch, num_matches_next, materialize_batch_ids,
            materialize_key_ids, materialize_payload_ids, [&](ExecBatch batch) {
              return output_batch_fn_(thread_id, std::move(batch));
            }));
      }

      // For left-outer and full-outer joins output non-matches.
      //
      // Call materialize. Nulls will be output in all columns that come from
      // the other side of the join.
      //
      if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
        int num_passing_ids = 0;
        arrow::util::bit_util::bits_to_indexes(
            /*bit_to_search=*/0, hardware_flags, minibatch_size_next,
            match_bitvector_buf.mutable_data(), &num_passing_ids,
            materialize_batch_ids_buf.mutable_data());

        // Add base batch row index.
        //
        for (int i = 0; i < num_passing_ids; ++i) {
          materialize_batch_ids_buf.mutable_data()[i] +=
              static_cast<uint16_t>(minibatch_start);
        }

        RETURN_NOT_OK(materialize_[thread_id]->AppendProbeOnly(
            keypayload_batch, num_passing_ids, materialize_batch_ids_buf.mutable_data(),
            [&](ExecBatch batch) {
              return output_batch_fn_(thread_id, std::move(batch));
            }));
      }
    }

    minibatch_start += minibatch_size_next;
  }

  return Status::OK();
}

Status JoinProbeProcessor::OnFinished() {
  // Flush all instances of materialize that have non-zero accumulated output
  // rows.
  //
  for (size_t i = 0; i < materialize_.size(); ++i) {
    JoinResultMaterialize& materialize = *materialize_[i];
    RETURN_NOT_OK(materialize.Flush(
        [&](ExecBatch batch) { return output_batch_fn_(i, std::move(batch)); }));
  }

  return Status::OK();
}

class SwissJoin : public HashJoinImpl {
 public:
  Status Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
              const HashJoinProjectionMaps* proj_map_left,
              const HashJoinProjectionMaps* proj_map_right,
              std::vector<JoinKeyCmp> key_cmp, Expression filter,
              RegisterTaskGroupCallback register_task_group_callback,
              StartTaskGroupCallback start_task_group_callback,
              OutputBatchCallback output_batch_callback,
              FinishedCallback finished_callback) override {
    START_COMPUTE_SPAN(span_, "SwissJoinImpl",
                       {{"detail", filter.ToString()},
                        {"join.kind", arrow::acero::ToString(join_type)},
                        {"join.threads", static_cast<uint32_t>(num_threads)}});

    num_threads_ = static_cast<int>(num_threads);
    ctx_ = ctx;
    hardware_flags_ = ctx->cpu_info()->hardware_flags();
    pool_ = ctx->memory_pool();

    join_type_ = join_type;
    key_cmp_.resize(key_cmp.size());
    for (size_t i = 0; i < key_cmp.size(); ++i) {
      key_cmp_[i] = key_cmp[i];
    }

    schema_[0] = proj_map_left;
    schema_[1] = proj_map_right;

    register_task_group_callback_ = std::move(register_task_group_callback);
    start_task_group_callback_ = std::move(start_task_group_callback);
    output_batch_callback_ = std::move(output_batch_callback);
    finished_callback_ = std::move(finished_callback);

    hash_table_ready_.store(false);
    cancelled_.store(false);
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      left_side_finished_ = false;
      right_side_finished_ = false;
      error_status_ = Status::OK();
    }

    local_states_.resize(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
      local_states_[i].hash_table_ready = false;
      local_states_[i].num_output_batches = 0;
      local_states_[i].materialize.Init(pool_, proj_map_left, proj_map_right);
    }

    std::vector<JoinResultMaterialize*> materialize;
    materialize.resize(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
      materialize[i] = &local_states_[i].materialize;
    }

    probe_processor_.Init(proj_map_left->num_cols(HashJoinProjection::KEY), join_type_,
                          &hash_table_, materialize, &key_cmp_, output_batch_callback_);

    InitTaskGroups();

    return Status::OK();
  }

  void InitTaskGroups() {
    task_group_build_ = register_task_group_callback_(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return BuildTask(thread_index, task_id);
        },
        [this](size_t thread_index) -> Status { return BuildFinished(thread_index); });
    task_group_merge_ = register_task_group_callback_(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return MergeTask(thread_index, task_id);
        },
        [this](size_t thread_index) -> Status { return MergeFinished(thread_index); });
    task_group_scan_ = register_task_group_callback_(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return ScanTask(thread_index, task_id);
        },
        [this](size_t thread_index) -> Status { return ScanFinished(thread_index); });
  }

  Status ProbeSingleBatch(size_t thread_index, ExecBatch batch) override {
    if (IsCancelled()) {
      return status();
    }

    if (!local_states_[thread_index].hash_table_ready) {
      local_states_[thread_index].hash_table_ready = hash_table_ready_.load();
    }
    ARROW_DCHECK(local_states_[thread_index].hash_table_ready);

    ExecBatch keypayload_batch;
    ARROW_ASSIGN_OR_RAISE(keypayload_batch, KeyPayloadFromInput(/*side=*/0, &batch));
    ARROW_ASSIGN_OR_RAISE(arrow::util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_index));

    return CancelIfNotOK(
        probe_processor_.OnNextBatch(thread_index, keypayload_batch, temp_stack,
                                     &local_states_[thread_index].temp_column_arrays));
  }

  Status ProbingFinished(size_t thread_index) override {
    if (IsCancelled()) {
      return status();
    }

    return CancelIfNotOK(StartScanHashTable(static_cast<int64_t>(thread_index)));
  }

  Status BuildHashTable(size_t thread_id, AccumulationQueue batches,
                        BuildFinishedCallback on_finished) override {
    if (IsCancelled()) {
      return status();
    }

    build_side_batches_ = std::move(batches);
    build_finished_callback_ = on_finished;

    return CancelIfNotOK(StartBuildHashTable(static_cast<int64_t>(thread_id)));
  }

  void Abort(AbortContinuationImpl pos_abort_callback) override {
    EVENT(span_, "Abort");
    END_SPAN(span_);
    std::ignore = CancelIfNotOK(Status::Cancelled("Hash Join Cancelled"));
    pos_abort_callback();
  }

  std::string ToString() const override { return "SwissJoin"; }

 private:
  Status StartBuildHashTable(int64_t thread_id) {
    // Initialize build class instance
    //
    const HashJoinProjectionMaps* schema = schema_[1];
    bool reject_duplicate_keys =
        join_type_ == JoinType::LEFT_SEMI || join_type_ == JoinType::LEFT_ANTI;
    bool no_payload =
        reject_duplicate_keys || schema->num_cols(HashJoinProjection::PAYLOAD) == 0;

    std::vector<KeyColumnMetadata> key_types;
    for (int i = 0; i < schema->num_cols(HashJoinProjection::KEY); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          KeyColumnMetadata metadata,
          ColumnMetadataFromDataType(schema->data_type(HashJoinProjection::KEY, i)));
      key_types.push_back(metadata);
    }
    std::vector<KeyColumnMetadata> payload_types;
    for (int i = 0; i < schema->num_cols(HashJoinProjection::PAYLOAD); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          KeyColumnMetadata metadata,
          ColumnMetadataFromDataType(schema->data_type(HashJoinProjection::PAYLOAD, i)));
      payload_types.push_back(metadata);
    }
    RETURN_NOT_OK(CancelIfNotOK(hash_table_build_.Init(
        &hash_table_, num_threads_, build_side_batches_.row_count(),
        reject_duplicate_keys, no_payload, key_types, payload_types, pool_,
        hardware_flags_)));

    // Process all input batches
    //
    return CancelIfNotOK(
        start_task_group_callback_(task_group_build_, build_side_batches_.batch_count()));
  }

  Status BuildTask(size_t thread_id, int64_t batch_id) {
    if (IsCancelled()) {
      return Status::OK();
    }

    const HashJoinProjectionMaps* schema = schema_[1];
    bool no_payload = hash_table_build_.no_payload();

    ExecBatch input_batch;
    ARROW_ASSIGN_OR_RAISE(
        input_batch, KeyPayloadFromInput(/*side=*/1, &build_side_batches_[batch_id]));

    if (input_batch.length == 0) {
      return Status::OK();
    }

    // Split batch into key batch and optional payload batch
    //
    // Input batch is key-payload batch (key columns followed by payload
    // columns). We split it into two separate batches.
    //
    // TODO: Change SwissTableForJoinBuild interface to use key-payload
    // batch instead to avoid this operation, which involves increasing
    // shared pointer ref counts.
    //
    ExecBatch key_batch({}, input_batch.length);
    key_batch.values.resize(schema->num_cols(HashJoinProjection::KEY));
    for (size_t icol = 0; icol < key_batch.values.size(); ++icol) {
      key_batch.values[icol] = input_batch.values[icol];
    }
    ExecBatch payload_batch({}, input_batch.length);

    if (!no_payload) {
      payload_batch.values.resize(schema->num_cols(HashJoinProjection::PAYLOAD));
      for (size_t icol = 0; icol < payload_batch.values.size(); ++icol) {
        payload_batch.values[icol] =
            input_batch.values[schema->num_cols(HashJoinProjection::KEY) + icol];
      }
    }
    ARROW_ASSIGN_OR_RAISE(arrow::util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_id));
    RETURN_NOT_OK(CancelIfNotOK(hash_table_build_.PushNextBatch(
        static_cast<int64_t>(thread_id), key_batch, no_payload ? nullptr : &payload_batch,
        temp_stack)));

    // Release input batch
    //
    input_batch.values.clear();

    return Status::OK();
  }

  Status BuildFinished(size_t thread_id) {
    RETURN_NOT_OK(status());

    build_side_batches_.Clear();

    // On a single thread prepare for merging partitions of the resulting hash
    // table.
    //
    RETURN_NOT_OK(CancelIfNotOK(hash_table_build_.PreparePrtnMerge()));
    return CancelIfNotOK(
        start_task_group_callback_(task_group_merge_, hash_table_build_.num_prtns()));
  }

  Status MergeTask(size_t /*thread_id*/, int64_t prtn_id) {
    if (IsCancelled()) {
      return Status::OK();
    }
    hash_table_build_.PrtnMerge(static_cast<int>(prtn_id));
    return Status::OK();
  }

  Status MergeFinished(size_t thread_id) {
    RETURN_NOT_OK(status());
    ARROW_ASSIGN_OR_RAISE(arrow::util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_id));
    hash_table_build_.FinishPrtnMerge(temp_stack);
    return CancelIfNotOK(OnBuildHashTableFinished(static_cast<int64_t>(thread_id)));
  }

  Status OnBuildHashTableFinished(int64_t thread_id) {
    if (IsCancelled()) {
      return status();
    }

    for (int i = 0; i < num_threads_; ++i) {
      local_states_[i].materialize.SetBuildSide(hash_table_.keys()->keys(),
                                                hash_table_.payloads(),
                                                hash_table_.key_to_payload() == nullptr);
    }
    hash_table_ready_.store(true);

    return build_finished_callback_(thread_id);
  }

  Status StartScanHashTable(int64_t thread_id) {
    if (IsCancelled()) {
      return status();
    }

    bool need_to_scan =
        (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI ||
         join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER);

    if (need_to_scan) {
      hash_table_.MergeHasMatch();
      int64_t num_tasks = bit_util::CeilDiv(hash_table_.num_rows(), kNumRowsPerScanTask);

      return CancelIfNotOK(start_task_group_callback_(task_group_scan_, num_tasks));
    } else {
      return CancelIfNotOK(OnScanHashTableFinished());
    }
  }

  Status ScanTask(size_t thread_id, int64_t task_id) {
    if (IsCancelled()) {
      return Status::OK();
    }

    // Should we output matches or non-matches?
    //
    bool bit_to_output = (join_type_ == JoinType::RIGHT_SEMI);

    int64_t start_row = task_id * kNumRowsPerScanTask;
    int64_t end_row =
        std::min((task_id + 1) * kNumRowsPerScanTask, hash_table_.num_rows());
    // Get thread index and related temp vector stack
    //
    ARROW_ASSIGN_OR_RAISE(arrow::util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_id));

    // Split into mini-batches
    //
    auto payload_ids_buf = arrow::util::TempVectorHolder<uint32_t>(
        temp_stack, arrow::util::MiniBatch::kMiniBatchLength);
    auto key_ids_buf = arrow::util::TempVectorHolder<uint32_t>(
        temp_stack, arrow::util::MiniBatch::kMiniBatchLength);
    auto selection_buf = arrow::util::TempVectorHolder<uint16_t>(
        temp_stack, arrow::util::MiniBatch::kMiniBatchLength);
    for (int64_t mini_batch_start = start_row; mini_batch_start < end_row;) {
      // Compute the size of the next mini-batch
      //
      int64_t mini_batch_size_next =
          std::min(end_row - mini_batch_start,
                   static_cast<int64_t>(arrow::util::MiniBatch::kMiniBatchLength));

      // Get the list of key and payload ids from this mini-batch to output.
      //
      uint32_t first_key_id =
          hash_table_.payload_id_to_key_id(static_cast<uint32_t>(mini_batch_start));
      uint32_t last_key_id = hash_table_.payload_id_to_key_id(
          static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1));
      int num_output_rows = 0;
      for (uint32_t key_id = first_key_id; key_id <= last_key_id; ++key_id) {
        if (bit_util::GetBit(hash_table_.has_match(), key_id) == bit_to_output) {
          uint32_t first_payload_for_key =
              std::max(static_cast<uint32_t>(mini_batch_start),
                       hash_table_.key_to_payload() ? hash_table_.key_to_payload()[key_id]
                                                    : key_id);
          uint32_t last_payload_for_key = std::min(
              static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1),
              hash_table_.key_to_payload() ? hash_table_.key_to_payload()[key_id + 1] - 1
                                           : key_id);
          uint32_t num_payloads_for_key =
              last_payload_for_key - first_payload_for_key + 1;
          for (uint32_t i = 0; i < num_payloads_for_key; ++i) {
            key_ids_buf.mutable_data()[num_output_rows + i] = key_id;
            payload_ids_buf.mutable_data()[num_output_rows + i] =
                first_payload_for_key + i;
          }
          num_output_rows += num_payloads_for_key;
        }
      }

      if (num_output_rows > 0) {
        // Materialize (and output whenever buffers get full) hash table
        // values according to the generated list of ids.
        //
        Status status = local_states_[thread_id].materialize.AppendBuildOnly(
            num_output_rows, key_ids_buf.mutable_data(), payload_ids_buf.mutable_data(),
            [&](ExecBatch batch) {
              return output_batch_callback_(static_cast<int64_t>(thread_id),
                                            std::move(batch));
            });
        RETURN_NOT_OK(CancelIfNotOK(status));
        if (!status.ok()) {
          break;
        }
      }
      mini_batch_start += mini_batch_size_next;
    }

    return Status::OK();
  }

  Status ScanFinished(size_t thread_id) {
    if (IsCancelled()) {
      return status();
    }

    return CancelIfNotOK(OnScanHashTableFinished());
  }

  Status OnScanHashTableFinished() {
    if (IsCancelled()) {
      return status();
    }
    END_SPAN(span_);

    // Flush all instances of materialize that have non-zero accumulated output
    // rows.
    //
    RETURN_NOT_OK(CancelIfNotOK(probe_processor_.OnFinished()));

    int64_t num_produced_batches = 0;
    for (size_t i = 0; i < local_states_.size(); ++i) {
      JoinResultMaterialize& materialize = local_states_[i].materialize;
      num_produced_batches += materialize.num_produced_batches();
    }

    return finished_callback_(num_produced_batches);
  }

  Result<ExecBatch> KeyPayloadFromInput(int side, ExecBatch* input) {
    ExecBatch projected({}, input->length);
    int num_key_cols = schema_[side]->num_cols(HashJoinProjection::KEY);
    int num_payload_cols = schema_[side]->num_cols(HashJoinProjection::PAYLOAD);
    projected.values.resize(num_key_cols + num_payload_cols);

    auto key_to_input =
        schema_[side]->map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
    for (int icol = 0; icol < num_key_cols; ++icol) {
      const Datum& value_in = input->values[key_to_input.get(icol)];
      if (value_in.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            projected.values[icol],
            MakeArrayFromScalar(*value_in.scalar(), projected.length, pool_));
      } else {
        projected.values[icol] = value_in;
      }
    }
    auto payload_to_input =
        schema_[side]->map(HashJoinProjection::PAYLOAD, HashJoinProjection::INPUT);
    for (int icol = 0; icol < num_payload_cols; ++icol) {
      const Datum& value_in = input->values[payload_to_input.get(icol)];
      if (value_in.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            projected.values[num_key_cols + icol],
            MakeArrayFromScalar(*value_in.scalar(), projected.length, pool_));
      } else {
        projected.values[num_key_cols + icol] = value_in;
      }
    }

    return projected;
  }

  bool IsCancelled() { return cancelled_.load(); }

  Status status() {
    if (IsCancelled()) {
      std::lock_guard<std::mutex> lock(state_mutex_);
      return error_status_;
    }
    return Status::OK();
  }

  Status CancelIfNotOK(Status status) {
    if (!status.ok()) {
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        // Only update the status for the first error encountered.
        //
        if (error_status_.ok()) {
          error_status_ = status;
        }
      }
      cancelled_.store(true);
    }
    return status;
  }

  static constexpr int kNumRowsPerScanTask = 512 * 1024;

  QueryContext* ctx_;
  int64_t hardware_flags_;
  MemoryPool* pool_;
  int num_threads_;
  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  const HashJoinProjectionMaps* schema_[2];

  // Task scheduling
  int task_group_build_;
  int task_group_merge_;
  int task_group_scan_;

  // Callbacks
  RegisterTaskGroupCallback register_task_group_callback_;
  StartTaskGroupCallback start_task_group_callback_;
  OutputBatchCallback output_batch_callback_;
  BuildFinishedCallback build_finished_callback_;
  FinishedCallback finished_callback_;

  struct ThreadLocalState {
    JoinResultMaterialize materialize;
    std::vector<KeyColumnArray> temp_column_arrays;
    int64_t num_output_batches;
    bool hash_table_ready;
  };
  std::vector<ThreadLocalState> local_states_;

  SwissTableForJoin hash_table_;
  JoinProbeProcessor probe_processor_;
  SwissTableForJoinBuild hash_table_build_;
  AccumulationQueue build_side_batches_;

  // Atomic state flags.
  // These flags are kept outside of mutex, since they can be queried for every
  // batch.
  //
  // The other flags that follow them, protected by mutex, will be queried or
  // updated only a fixed number of times during entire join processing.
  //
  std::atomic<bool> hash_table_ready_;
  std::atomic<bool> cancelled_;

  // Mutex protecting state flags.
  //
  std::mutex state_mutex_;

  // Mutex protected state flags.
  //
  bool left_side_finished_;
  bool right_side_finished_;
  Status error_status_;
};

Result<std::unique_ptr<HashJoinImpl>> HashJoinImpl::MakeSwiss() {
  std::unique_ptr<HashJoinImpl> impl{new SwissJoin()};
  return std::move(impl);
}

}  // namespace acero
}  // namespace arrow

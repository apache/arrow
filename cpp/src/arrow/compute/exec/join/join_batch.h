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
#include <memory>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/join/join_filter.h"
#include "arrow/compute/exec/join/join_schema.h"
#include "arrow/compute/exec/join/join_type.h"
#include "arrow/compute/exec/key_encode.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/bit_util.h"

/*
  This file implements operations on exec batches related to hash join processing, such
as:
- moving selected rows and columns from an input batch or a hash table to an output batch
- hash join related projections (hash value)
- hash join related filtering (early filter)
- accumulating filter results and join outputs
*/

namespace arrow {
namespace compute {

// Local context that is provided by a thread when executing batch or KeyRowArray
// shuffle operations.
//
struct Shuffle_ThreadLocal {
  Shuffle_ThreadLocal(int64_t in_hardware_flags, util::TempVectorStack* in_temp_stack,
                      int in_minibatch_size)
      : hardware_flags(in_hardware_flags),
        temp_stack(in_temp_stack),
        minibatch_size(in_minibatch_size) {}
  int64_t hardware_flags;
  // For simple and fast allocation of temporary vectors
  util::TempVectorStack* temp_stack;
  // Size of a batch to use with temp_stack allocations, related to
  // the total size of memory owned by it
  int minibatch_size;
};

// Description of output buffers to use for a single column in a shuffle operation.
//
struct ShuffleOutputDesc {
  ShuffleOutputDesc(std::vector<std::shared_ptr<ResizableBuffer>>& in_buffers,
                    int64_t in_length, bool in_has_nulls);
  Status ResizeBufferNonNull(int num_new_rows);
  Status ResizeBufferFixedLen(int num_new_rows,
                              const KeyEncoder::KeyColumnMetadata& metadata);
  Status ResizeBufferVarLen(int num_new_rows);

  ResizableBuffer* buffer[3];
  int64_t offset;
  bool has_nulls;
};

// Write to output buffers a sequence of input batch fields from a single column
// specified by a sequence of row ids.
// The mapping from output rows to input rows can be
// arbitrary, and does not have to be monotonic or injective.
// Includes special handling for a single contiguous range of row ids.
//
class BatchShuffle {
 public:
  struct ShuffleInputDesc {
    ShuffleInputDesc(const uint8_t* non_null_buf, const uint8_t* fixed_len_buf,
                     const uint8_t* var_len_buf, int in_num_rows, int in_start_row,
                     const uint16_t* in_opt_row_ids,
                     const KeyEncoder::KeyColumnMetadata& in_metadata);
    const uint8_t* buffer[3];
    int num_rows;
    const uint16_t* opt_row_ids;
    int64_t offset;
    KeyEncoder::KeyColumnMetadata metadata;
  };
  static Status Shuffle(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                        Shuffle_ThreadLocal& ctx, bool* out_has_nulls);

 private:
  static Status ShuffleNull(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                            Shuffle_ThreadLocal& ctx, bool* out_has_nulls);
  static void ShuffleBit(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                         Shuffle_ThreadLocal& ctx);
  template <typename T>
  static void ShuffleInteger(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
  static void ShuffleBinary(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
  static void ShuffleOffset(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
  static void ShuffleVarBinary(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
};

// Write to output buffers fields from a single column of rows encoded in KeyRowArray
// according to specified sequence of row ids.
//
class KeyRowArrayShuffle {
 public:
  struct ShuffleInputDesc {
    ShuffleInputDesc(const KeyEncoder::KeyRowArray& in_rows, int in_column_id,
                     int in_num_rows, const key_id_type* in_row_ids);
    const KeyEncoder::KeyRowArray* rows;
    int column_id;
    int num_rows;
    const key_id_type* row_ids;
    // Precomputed info for accessing this column's data inside encoded rows
    //
    KeyEncoder::KeyColumnMetadata metadata;
    int null_bit_id;
    int varbinary_id;
    uint32_t offset_within_row;
  };

  static Status Shuffle(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                        Shuffle_ThreadLocal& ctx, bool* out_has_nulls);

 private:
  static Status ShuffleNull(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                            Shuffle_ThreadLocal& ctx, bool* out_has_nulls);
  static void ShuffleBit(ShuffleOutputDesc& output, const ShuffleInputDesc& input,
                         Shuffle_ThreadLocal& ctx);
  template <typename T>
  static void ShuffleInteger(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
  static void ShuffleBinary(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
  static void ShuffleOffset(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
  static void ShuffleVarBinary(ShuffleOutputDesc& output, const ShuffleInputDesc& input);
};

struct JoinHashTable_ThreadLocal;

// Wrapper around ExecBatch that carries computed hash with it after it is evaluated on
// demand for the first time.
//
struct BatchWithJoinData {
  BatchWithJoinData() = default;
  BatchWithJoinData(int in_join_side, const ExecBatch& in_batch);

  Status ComputeHashIfMissing(MemoryPool* pool, JoinColumnMapper* schema_mgr,
                              JoinHashTable_ThreadLocal* locals);
  Status Encode(int64_t start_row, int64_t num_rows, KeyEncoder& encoder,
                KeyEncoder::KeyRowArray& rows, JoinColumnMapper* schema_mgr,
                JoinSchemaHandle batch_schema, JoinSchemaHandle output_schema) const;

  int join_side;
  ExecBatch batch;
  std::shared_ptr<ResizableBuffer> hashes;
};

// Handles accumulation of rows in output buffers up to the provided batch size before
// producing an exec batch.
//
class BatchAccumulation {
 public:
  Status Init(JoinSchemaHandle schema, JoinColumnMapper* schema_mgr,
              int64_t max_batch_size, MemoryPool* pool);
  ShuffleOutputDesc GetColumn(int column_id);
  ResizableBuffer* GetHashes() { return hashes_.get(); }
  Result<std::unique_ptr<ExecBatch>> MakeBatch();
  Result<std::unique_ptr<BatchWithJoinData>> MakeBatchWithJoinData();

  void SetHasNulls(int column_id) { output_buffer_has_nulls_[column_id] = true; }
  void SetHasHashes() { has_hashes_ = true; }
  bool is_empty() const { return output_length_ == 0; }
  int64_t space_left() const { return max_batch_size_ - output_length_; }
  int64_t length() const { return output_length_; }
  void IncreaseLength(int64_t delta) { output_length_ += delta; }

 private:
  Status AllocateEmptyBuffers();

  MemoryPool* pool_;
  JoinSchemaHandle schema_;
  int64_t max_batch_size_;
  JoinColumnMapper* schema_mgr_;
  std::vector<std::vector<std::shared_ptr<ResizableBuffer>>> output_buffers_;
  std::vector<bool> output_buffer_has_nulls_;
  bool has_hashes_;
  std::shared_ptr<ResizableBuffer> hashes_;
  int64_t output_length_;
};

// Assembles output exec batches based on the inputs from an exec batch on one side of the
// join and a hash table on the other side of the join.
// Rows of output batch represent a combination of two rows, one from each side of the
// join, specified by given row ids. Missing row ids are used in outer joins and mean that
// nulls will be used in place of a row columns from one of the sides.
//
class BatchJoinAssembler {
 public:
  Status Init(MemoryPool* pool, int64_t max_batch_size, JoinColumnMapper* schema_mgr);
  void BindSourceBatch(int side, const ExecBatch* batch);
  void BindSourceHashTable(int side, const KeyEncoder::KeyRowArray* keys,
                           const KeyEncoder::KeyRowArray* payload);

  // Returns null as output batch if the resulting number of rows in accumulation buffers
  // is less than max batch size.
  // Missing batch row ids when batch is present mean a
  // sequence of consecutive row ids.
  Result<std::unique_ptr<ExecBatch>> Push(Shuffle_ThreadLocal& ctx, int num_rows,
                                          int hash_table_side, bool is_batch_present,
                                          bool is_hash_table_present, int batch_start_row,
                                          const uint16_t* opt_batch_row_ids,
                                          const key_id_type* opt_key_ids,
                                          const key_id_type* opt_payload_ids,
                                          int* out_num_rows_processed);

  Result<std::unique_ptr<ExecBatch>> Flush();

 private:
  Status AppendNulls(ShuffleOutputDesc& output,
                     const KeyEncoder::KeyColumnMetadata& metadata, int num_rows);

  int bound_batch_side_;
  int bound_hash_table_side_;
  const ExecBatch* bound_batch_;
  const KeyEncoder::KeyRowArray* bound_keys_;
  const KeyEncoder::KeyRowArray* bound_payload_;
  JoinColumnMapper* schema_mgr_;
  BatchAccumulation output_buffers_;
};

// Evaluation of early filter (a cheap hash-based filter that allows for false positives
// but not false negatives). Also takes care of filtering rows that do not have a match in
// case of a) empty hash table, b) when nulls appear in key columns and null is not equal
// to null.
class BatchEarlyFilterEval {
  void Init(int join_side, MemoryPool* pool, JoinColumnMapper* schema_mgr);
  void SetFilter(bool is_other_side_empty,
                 const std::vector<bool>& null_field_means_no_match,
                 const ApproximateMembershipTest* hash_based_filter);
  Status EvalFilter(BatchWithJoinData& batch, int64_t start_row, int64_t num_rows,
                    uint8_t* filter_bit_vector, JoinHashTable_ThreadLocal* locals);
  Result<std::unique_ptr<BatchWithJoinData>> FilterBatch(const BatchWithJoinData& batch,
                                                         int64_t start, int64_t num_rows,
                                                         const uint8_t* filter_bit_vector,
                                                         int* out_num_rows_processed);
  Result<std::unique_ptr<ExecBatch>> Flush();

 private:
  void EvalNullFilter(const ExecBatch& batch, int64_t start_row, int64_t num_rows,
                      uint8_t* bit_vector_to_update, JoinHashTable_ThreadLocal* locals);
  int side_;
  MemoryPool* pool_;
  JoinColumnMapper* schema_mgr_;

  BatchAccumulation output_buffers_;

  // Filter information
  //
  bool is_other_side_empty_;
  std::vector<bool> null_field_means_no_match_;
  const ApproximateMembershipTest* hash_based_filter_;
};

// Instances of classes that due to accumulation of rows are not
// thread-safe and therefore need a copy per thread.
//
class JoinBatch_ThreadLocal {
  // Output assembler is shared by both sides of the join
  BatchJoinAssembler assembler;
  // One filter for each side of the join
  BatchEarlyFilterEval early_filter[2];
};

}  // namespace compute
}  // namespace arrow
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

#include "arrow/compute/exec/join/join_schema.h"
#include "arrow/compute/exec/join/join_type.h"
#include "arrow/compute/exec/key_encode.h"
#include "arrow/compute/exec/key_map.h"
#include "arrow/compute/exec/util.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"

/*
  This file implements hash table access for joins.
  Hash table build is single-threaded. TODO: implement parallel version.
  Hash table lookups can be done concurrently (hash table is constant after build has
  finished). In order to do that, a thread local context for storage of temporary mutable
  buffers needs to be provided.
*/

namespace arrow {
namespace compute {

// Thread local context to be used as a scratch space during access to hash
// table. Allows for concurrent read-only lookups.
//
struct JoinHashTable_ThreadLocal {
  Status Init(MemoryPool* in_pool, int64_t in_hardware_flags,
              JoinColumnMapper* schema_mgr);

  static constexpr int log_minibatch_size = 10;
  static constexpr int minibatch_size = 1 << log_minibatch_size;
  MemoryPool* pool;
  util::TempVectorStack stack;
  KeyEncoder::KeyEncoderContext encoder_ctx;
  KeyEncoder key_encoder;
  KeyEncoder::KeyRowArray keys_minibatch;
};

struct BatchWithJoinData;

// Represents a hash table and related data created for one of the sides of the hash join.
// Here is a list of child structures included in it:
// - SwissTable for hash-based search of matching key candidates
// - KeyRowArray storing only key columns for all inserted rows in a row-oriented way
// - optional KeyRowArray storing payload columns for all inserted rows;
// note: when multiple rows with the same key are inserted into a hash table,
// KeyRowArray for keys will only contain one copy of key shared by all inserted rows,
// while payload KeyRowArray will store one row for each input row
// - cummulative sum of row multiplicities for all keys;
// this is used to enumerate all matching rows in a hash table for a given key;
// the enumerated rows will be stored next to each other in payload KeyRowArray
// - byte vector with one element per inserted key for marking keys with matches;
// used in order to implement right outer and full outer joins.
//
class JoinHashTable {
 public:
  Status Init(MemoryPool* pool);
  Status Build(int hash_table_side, std::vector<BatchWithJoinData>& batches,
               JoinColumnMapper* schema_mgr, JoinHashTable_ThreadLocal* locals);
  Status Find(BatchWithJoinData& batch, int start_row, int num_rows,
              JoinColumnMapper* schema_mgr, JoinHashTable_ThreadLocal* locals,
              key_id_type* out_key_ids, uint8_t* out_found_bitvector);
  const KeyEncoder::KeyRowArray& keys() const { return keys_; }
  const KeyEncoder::KeyRowArray& payloads() const { return payloads_; }
  const key_id_type* key_to_payload() const { return key_to_payload_.data(); }
  const uint8_t* has_match() const { return has_match_.data(); }

 private:
  void Equal(int num_keys, const uint16_t* selection /* may be null */,
             const uint32_t* group_ids, uint32_t* out_num_keys_mismatch,
             uint16_t* out_selection_mismatch, void* callback_ctx) const;
  Status Append(int num_keys, const uint16_t* selection, void* callback_ctx);
  Status BucketSort(KeyEncoder::KeyRowArray& output, const KeyEncoder::KeyRowArray& input,
                    int64_t num_keys, std::vector<key_id_type>& key_ids,
                    std::vector<key_id_type>& key_to_payload,
                    JoinHashTable_ThreadLocal* locals);

  MemoryPool* pool_;
  SwissTable map_;
  int hash_table_side_;
  KeyEncoder::KeyRowArray keys_;
  KeyEncoder::KeyRowArray payloads_;
  std::vector<key_id_type> key_to_payload_;
  std::vector<uint8_t> has_match_;
};

}  // namespace compute
}  // namespace arrow
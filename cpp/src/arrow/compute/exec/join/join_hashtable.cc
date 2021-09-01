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

#include "arrow/compute/exec/join/join_hashtable.h"

#include "arrow/compute/exec/join/join_batch.h"
#include "arrow/compute/exec/key_compare.h"

namespace arrow {
namespace compute {

constexpr int JoinHashTable_ThreadLocal::log_minibatch_size;
constexpr int JoinHashTable_ThreadLocal::minibatch_size;

Status JoinHashTable_ThreadLocal::Init(MemoryPool* in_pool, int64_t in_hardware_flags,
                                       JoinColumnMapper* schema_mgr) {
  pool = in_pool;
  RETURN_NOT_OK(stack.Init(pool, 64 * minibatch_size));
  encoder_ctx.hardware_flags = in_hardware_flags;
  encoder_ctx.stack = &stack;

  // Key columns encoding
  //
  std::vector<KeyEncoder::KeyColumnMetadata> col_metadata;
  for (int i = 0; i < schema_mgr->num_cols(JoinSchemaHandle::FIRST_KEY); ++i) {
    col_metadata.push_back(schema_mgr->data_type(JoinSchemaHandle::FIRST_KEY, i));
  }
  key_encoder.Init(col_metadata, &encoder_ctx,
                   /* row_alignment = */ sizeof(uint64_t),
                   /* string_alignment = */ sizeof(uint64_t));
  RETURN_NOT_OK(keys_minibatch.Init(pool, key_encoder.row_metadata()));

  return Status::OK();
}

Status JoinHashTable::Init(MemoryPool* pool) {
  pool_ = pool;
  RETURN_NOT_OK(map_.init(
      pool,
      [this](int num_keys, const uint16_t* selection /* may be null */,
             const uint32_t* group_ids, uint32_t* out_num_keys_mismatch,
             uint16_t* out_selection_mismatch, void* callback_ctx) {
        this->Equal(num_keys, selection, group_ids, out_num_keys_mismatch,
                    out_selection_mismatch, callback_ctx);
      },
      [this](int num_keys, const uint16_t* selection, void* callback_ctx) -> Status {
        return Append(num_keys, selection, callback_ctx);
      }));
  return Status::OK();
}

Status JoinHashTable::Build(int hash_table_side, std::vector<BatchWithJoinData>& batches,
                            JoinColumnMapper* schema_mgr,
                            JoinHashTable_ThreadLocal* locals) {
  hash_table_side_ = hash_table_side;
  RETURN_NOT_OK(keys_.Init(pool_, locals->key_encoder.row_metadata()));
  KeyEncoder payload_encoder;
  {
    std::vector<KeyEncoder::KeyColumnMetadata> col_metadata;
    JoinSchemaHandle handle = hash_table_side == 0 ? JoinSchemaHandle::FIRST_PAYLOAD
                                                   : JoinSchemaHandle::SECOND_PAYLOAD;
    for (int i = 0; i < schema_mgr->num_cols(handle); ++i) {
      col_metadata.push_back(schema_mgr->data_type(handle, i));
    }
    payload_encoder.Init(col_metadata, &locals->encoder_ctx,
                         /* row_alignment = */ sizeof(uint64_t),
                         /* string_alignment = */ sizeof(uint64_t));
  }
  KeyEncoder::KeyRowArray original_payloads, minibatch_payloads;
  RETURN_NOT_OK(original_payloads.Init(pool_, payload_encoder.row_metadata()));
  RETURN_NOT_OK(minibatch_payloads.Init(pool_, payload_encoder.row_metadata()));

  std::vector<key_id_type> key_ids;

  // TODO: handle the case when there are no payload columns

  std::vector<uint16_t> sequence(locals->minibatch_size);
  for (int i = 0; i < locals->minibatch_size; ++i) {
    sequence[i] = i;
  }

  // For each batch...
  //
  for (size_t ibatch = 0; ibatch < batches.size(); ++ibatch) {
    RETURN_NOT_OK(batches[ibatch].ComputeHashIfMissing(pool_, schema_mgr, locals));
    // Break batch into minibatches
    //
    for (int64_t start = 0; start < batches[ibatch].batch.length;
         start += locals->minibatch_size) {
      int next_minibatch_size = std::min(
          static_cast<int>(batches[ibatch].batch.length - start), locals->minibatch_size);

      size_t row_id = key_ids.size();
      key_ids.resize(row_id + next_minibatch_size);

      // Encode keys
      //
      RETURN_NOT_OK(
          batches[ibatch].Encode(start, next_minibatch_size, locals->key_encoder,
                                 locals->keys_minibatch, schema_mgr,
                                 hash_table_side == 0 ? JoinSchemaHandle::FIRST_INPUT
                                                      : JoinSchemaHandle::SECOND_INPUT,
                                 hash_table_side == 0 ? JoinSchemaHandle::FIRST_KEY
                                                      : JoinSchemaHandle::SECOND_KEY));

      // Encode payloads
      //
      // TODO: append to original_payloads instead of making a copy from
      // minibatch_payloads to original_payloads
      //
      RETURN_NOT_OK(batches[ibatch].Encode(
          start, next_minibatch_size, payload_encoder, minibatch_payloads, schema_mgr,
          hash_table_side == 0 ? JoinSchemaHandle::FIRST_INPUT
                               : JoinSchemaHandle::SECOND_INPUT,
          hash_table_side == 0 ? JoinSchemaHandle::FIRST_PAYLOAD
                               : JoinSchemaHandle::SECOND_PAYLOAD));
      RETURN_NOT_OK(original_payloads.AppendSelectionFrom(
          minibatch_payloads, next_minibatch_size, sequence.data()));

      SwissTable_ThreadLocal map_ctx(locals->encoder_ctx.hardware_flags, &locals->stack,
                                     locals->log_minibatch_size, locals);

      auto match_bitvector = util::TempVectorHolder<uint8_t>(
          &locals->stack,
          static_cast<uint32_t>(BitUtil::BytesForBits(next_minibatch_size)));

      const uint32_t* hashes =
          reinterpret_cast<const uint32_t*>(batches[ibatch].hashes->data()) + start;
      {
        auto local_slots =
            util::TempVectorHolder<uint8_t>(&locals->stack, next_minibatch_size);
        map_.early_filter(locals->encoder_ctx.hardware_flags, next_minibatch_size, hashes,
                          match_bitvector.mutable_data(), local_slots.mutable_data());
        map_.find(next_minibatch_size, hashes, match_bitvector.mutable_data(),
                  local_slots.mutable_data(), key_ids.data() + row_id, &map_ctx);
      }
      auto ids = util::TempVectorHolder<uint16_t>(&locals->stack, next_minibatch_size);
      int num_ids;
      util::BitUtil::bits_to_indexes(0, locals->encoder_ctx.hardware_flags,
                                     next_minibatch_size, match_bitvector.mutable_data(),
                                     &num_ids, ids.mutable_data());
      RETURN_NOT_OK(map_.map_new_keys(num_ids, ids.mutable_data(), hashes,
                                      key_ids.data() + row_id, &map_ctx));
    }
  }

  // Bucket sort payloads on key_id
  //
  RETURN_NOT_OK(BucketSort(payloads_, original_payloads, map_.num_groups(), key_ids,
                           key_to_payload_, locals));

  // TODO: allocate memory on the first access
  has_match_.resize(map_.num_groups());
  memset(has_match_.data(), 0, map_.num_groups());

  return Status::OK();
}

Status JoinHashTable::Find(BatchWithJoinData& batch, int start_row, int num_rows,
                           JoinColumnMapper* schema_mgr,
                           JoinHashTable_ThreadLocal* locals, key_id_type* out_key_ids,
                           uint8_t* out_found_bitvector) {
  RETURN_NOT_OK(batch.ComputeHashIfMissing(pool_, schema_mgr, locals));

  // Encode keys
  //
  RETURN_NOT_OK(batch.Encode(start_row, num_rows, locals->key_encoder,
                             locals->keys_minibatch, schema_mgr,
                             hash_table_side_ == 0 ? JoinSchemaHandle::FIRST_INPUT
                                                   : JoinSchemaHandle::SECOND_INPUT,
                             hash_table_side_ == 0 ? JoinSchemaHandle::FIRST_KEY
                                                   : JoinSchemaHandle::SECOND_KEY));

  {
    const uint32_t* hashes =
        reinterpret_cast<const uint32_t*>(batch.hashes->data()) + start_row;
    auto local_slots = util::TempVectorHolder<uint8_t>(&locals->stack, num_rows);
    map_.early_filter(locals->encoder_ctx.hardware_flags, num_rows, hashes,
                      out_found_bitvector, local_slots.mutable_data());
    SwissTable_ThreadLocal map_ctx(locals->encoder_ctx.hardware_flags, &locals->stack,
                                   locals->log_minibatch_size, locals);
    map_.find(num_rows, hashes, out_found_bitvector, local_slots.mutable_data(),
              out_key_ids, &map_ctx);
  }
  return Status::OK();
}

void JoinHashTable::Equal(int num_keys, const uint16_t* selection /* may be null */,
                          const uint32_t* group_ids, uint32_t* out_num_keys_mismatch,
                          uint16_t* out_selection_mismatch, void* callback_ctx) const {
  JoinHashTable_ThreadLocal* locals =
      reinterpret_cast<JoinHashTable_ThreadLocal*>(callback_ctx);
  arrow::compute::KeyCompare::CompareRows(
      num_keys, selection, group_ids, &locals->encoder_ctx, out_num_keys_mismatch,
      out_selection_mismatch, locals->keys_minibatch, keys_);
}

Status JoinHashTable::Append(int num_keys, const uint16_t* selection,
                             void* callback_ctx) {
  JoinHashTable_ThreadLocal* locals =
      reinterpret_cast<JoinHashTable_ThreadLocal*>(callback_ctx);
  return keys_.AppendSelectionFrom(locals->keys_minibatch, num_keys, selection);
}

Status JoinHashTable::BucketSort(KeyEncoder::KeyRowArray& output,
                                 const KeyEncoder::KeyRowArray& input, int64_t num_keys,
                                 std::vector<key_id_type>& key_ids,
                                 std::vector<key_id_type>& key_to_payload,
                                 JoinHashTable_ThreadLocal* locals) {
  if (num_keys == 0) {
    return Status::OK();
  }
  // Reorder key_ids while updating key_to_payload
  //
  std::vector<key_id_type> gather_ids;
  gather_ids.resize(key_ids.size());
  key_to_payload.clear();
  key_to_payload.resize(num_keys + 1);
  for (size_t i = 0; i < key_to_payload.size(); ++i) {
    key_to_payload[i] = 0;
  }
  for (size_t i = 0; i < key_ids.size(); ++i) {
    ++key_to_payload[key_ids[i]];
  }
  key_id_type sum = 0;
  for (int64_t i = 0; i < num_keys; ++i) {
    sum += key_to_payload[i];
  }
  key_to_payload[num_keys] = key_to_payload[num_keys - 1];
  for (size_t i = 0; i < key_ids.size(); ++i) {
    gather_ids[--key_to_payload[key_ids[i]]] = static_cast<uint32_t>(i);
  }
  key_ids.clear();

  for (size_t start = 0; start < gather_ids.size(); start += locals->minibatch_size) {
    int64_t next_minibatch_size =
        std::min(gather_ids.size() - start, static_cast<size_t>(locals->minibatch_size));
    RETURN_NOT_OK(output.AppendSelectionFrom(
        input, static_cast<uint32_t>(next_minibatch_size), gather_ids.data() + start));
  }

  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
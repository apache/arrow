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

#include "arrow/acero/hash_join_dict.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using compute::ExecSpan;
using compute::internal::KeyEncoder;
using compute::internal::RowEncoder;

namespace acero {

bool HashJoinDictUtil::KeyDataTypesValid(
    const std::shared_ptr<DataType>& probe_data_type,
    const std::shared_ptr<DataType>& build_data_type) {
  bool l_is_dict = (probe_data_type->id() == Type::DICTIONARY);
  bool r_is_dict = (build_data_type->id() == Type::DICTIONARY);
  DataType* l_type;
  if (l_is_dict) {
    const auto& dict_type = checked_cast<const DictionaryType&>(*probe_data_type);
    l_type = dict_type.value_type().get();
  } else {
    l_type = probe_data_type.get();
  }
  DataType* r_type;
  if (r_is_dict) {
    const auto& dict_type = checked_cast<const DictionaryType&>(*build_data_type);
    r_type = dict_type.value_type().get();
  } else {
    r_type = build_data_type.get();
  }
  return l_type->Equals(*r_type);
}

Result<std::shared_ptr<ArrayData>> HashJoinDictUtil::IndexRemapUsingLUT(
    ExecContext* ctx, const Datum& indices, int64_t batch_length,
    const std::shared_ptr<ArrayData>& map_array,
    const std::shared_ptr<DataType>& data_type) {
  ARROW_DCHECK(indices.is_array() || indices.is_scalar());

  const uint8_t* map_non_nulls = map_array->buffers[0]->data();
  const int32_t* map = reinterpret_cast<const int32_t*>(map_array->buffers[1]->data());

  ARROW_DCHECK(data_type->id() == Type::DICTIONARY);
  const auto& dict_type = checked_cast<const DictionaryType&>(*data_type);

  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<ArrayData> result,
      ConvertToInt32(dict_type.index_type(), indices, batch_length, ctx));

  uint8_t* nns = result->buffers[0]->mutable_data();
  int32_t* ids = reinterpret_cast<int32_t*>(result->buffers[1]->mutable_data());
  for (int64_t i = 0; i < batch_length; ++i) {
    bool is_null = !bit_util::GetBit(nns, i);
    if (is_null) {
      ids[i] = kNullId;
    } else {
      ARROW_DCHECK(ids[i] >= 0 && ids[i] < map_array->length);
      if (!bit_util::GetBit(map_non_nulls, ids[i])) {
        bit_util::ClearBit(nns, i);
        ids[i] = kNullId;
      } else {
        ids[i] = map[ids[i]];
      }
    }
  }

  return result;
}

namespace {
template <typename FROM, typename TO>
static Result<std::shared_ptr<ArrayData>> ConvertImp(
    const std::shared_ptr<DataType>& to_type, const Datum& input, int64_t batch_length,
    ExecContext* ctx) {
  ARROW_DCHECK(input.is_array() || input.is_scalar());
  bool is_scalar = input.is_scalar();

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> to_buf,
                        AllocateBuffer(batch_length * sizeof(TO), ctx->memory_pool()));
  TO* to = reinterpret_cast<TO*>(to_buf->mutable_data());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> to_nn_buf,
                        AllocateBitmap(batch_length, ctx->memory_pool()));
  uint8_t* to_nn = to_nn_buf->mutable_data();
  memset(to_nn, 0xff, bit_util::BytesForBits(batch_length));

  if (!is_scalar) {
    const ArrayData& arr = *input.array();
    const FROM* from = arr.GetValues<FROM>(1);
    DCHECK_EQ(arr.length, batch_length);

    for (int64_t i = 0; i < arr.length; ++i) {
      to[i] = static_cast<TO>(from[i]);
      // Make sure we did not lose information during cast
      ARROW_DCHECK(static_cast<FROM>(to[i]) == from[i]);

      bool is_null = (arr.buffers[0] != NULLPTR) &&
                     !bit_util::GetBit(arr.buffers[0]->data(), arr.offset + i);
      if (is_null) {
        bit_util::ClearBit(to_nn, i);
      }
    }

    // Pass null buffer unchanged
    return ArrayData::Make(to_type, arr.length,
                           {std::move(to_nn_buf), std::move(to_buf)});
  } else {
    const auto& scalar = input.scalar_as<arrow::internal::PrimitiveScalarBase>();
    if (scalar.is_valid) {
      const std::string_view data = scalar.view();
      DCHECK_EQ(data.size(), sizeof(FROM));
      const FROM from = *reinterpret_cast<const FROM*>(data.data());
      const TO to_value = static_cast<TO>(from);
      // Make sure we did not lose information during cast
      ARROW_DCHECK(static_cast<FROM>(to_value) == from);

      for (int64_t i = 0; i < batch_length; ++i) {
        to[i] = to_value;
      }

      memset(to_nn, 0xff, bit_util::BytesForBits(batch_length));
      return ArrayData::Make(to_type, batch_length,
                             {std::move(to_nn_buf), std::move(to_buf)});
    } else {
      memset(to_nn, 0, bit_util::BytesForBits(batch_length));
      return ArrayData::Make(to_type, batch_length,
                             {std::move(to_nn_buf), std::move(to_buf)});
    }
  }
}
}  // namespace

Result<std::shared_ptr<ArrayData>> HashJoinDictUtil::ConvertToInt32(
    const std::shared_ptr<DataType>& from_type, const Datum& input, int64_t batch_length,
    ExecContext* ctx) {
  switch (from_type->id()) {
    case Type::UINT8:
      return ConvertImp<uint8_t, int32_t>(int32(), input, batch_length, ctx);
    case Type::INT8:
      return ConvertImp<int8_t, int32_t>(int32(), input, batch_length, ctx);
    case Type::UINT16:
      return ConvertImp<uint16_t, int32_t>(int32(), input, batch_length, ctx);
    case Type::INT16:
      return ConvertImp<int16_t, int32_t>(int32(), input, batch_length, ctx);
    case Type::UINT32:
      return ConvertImp<uint32_t, int32_t>(int32(), input, batch_length, ctx);
    case Type::INT32:
      return ConvertImp<int32_t, int32_t>(int32(), input, batch_length, ctx);
    case Type::UINT64:
      return ConvertImp<uint64_t, int32_t>(int32(), input, batch_length, ctx);
    case Type::INT64:
      return ConvertImp<int64_t, int32_t>(int32(), input, batch_length, ctx);
    default:
      ARROW_DCHECK(false);
      return nullptr;
  }
}

Result<std::shared_ptr<ArrayData>> HashJoinDictUtil::ConvertFromInt32(
    const std::shared_ptr<DataType>& to_type, const Datum& input, int64_t batch_length,
    ExecContext* ctx) {
  switch (to_type->id()) {
    case Type::UINT8:
      return ConvertImp<int32_t, uint8_t>(to_type, input, batch_length, ctx);
    case Type::INT8:
      return ConvertImp<int32_t, int8_t>(to_type, input, batch_length, ctx);
    case Type::UINT16:
      return ConvertImp<int32_t, uint16_t>(to_type, input, batch_length, ctx);
    case Type::INT16:
      return ConvertImp<int32_t, int16_t>(to_type, input, batch_length, ctx);
    case Type::UINT32:
      return ConvertImp<int32_t, uint32_t>(to_type, input, batch_length, ctx);
    case Type::INT32:
      return ConvertImp<int32_t, int32_t>(to_type, input, batch_length, ctx);
    case Type::UINT64:
      return ConvertImp<int32_t, uint64_t>(to_type, input, batch_length, ctx);
    case Type::INT64:
      return ConvertImp<int32_t, int64_t>(to_type, input, batch_length, ctx);
    default:
      ARROW_DCHECK(false);
      return nullptr;
  }
}

std::shared_ptr<Array> HashJoinDictUtil::ExtractDictionary(const Datum& data) {
  return data.is_array() ? MakeArray(data.array()->dictionary)
                         : data.scalar_as<DictionaryScalar>().value.dictionary;
}

Status HashJoinDictBuild::Init(ExecContext* ctx, std::shared_ptr<Array> dictionary,
                               std::shared_ptr<DataType> index_type,
                               std::shared_ptr<DataType> value_type) {
  index_type_ = std::move(index_type);
  value_type_ = std::move(value_type);
  hash_table_.clear();

  if (!dictionary) {
    ARROW_ASSIGN_OR_RAISE(auto dict, MakeArrayOfNull(value_type_, 0));
    unified_dictionary_ = dict->data();
    return Status::OK();
  }

  dictionary_ = dictionary;

  // Initialize encoder
  RowEncoder encoder;
  std::vector<TypeHolder> encoder_types;
  encoder_types.emplace_back(value_type_);
  encoder.Init(encoder_types, ctx);

  // Encode all dictionary values
  int64_t length = dictionary->data()->length;
  if (length >= std::numeric_limits<int32_t>::max()) {
    return Status::Invalid(
        "Dictionary length in hash join must fit into signed 32-bit integer.");
  }
  RETURN_NOT_OK(encoder.EncodeAndAppend(ExecSpan({*dictionary->data()}, length)));

  std::vector<int32_t> entries_to_take;

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> non_nulls_buf,
                        AllocateBitmap(length, ctx->memory_pool()));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> ids_buf,
                        AllocateBuffer(length * sizeof(int32_t), ctx->memory_pool()));
  uint8_t* non_nulls = non_nulls_buf->mutable_data();
  int32_t* ids = reinterpret_cast<int32_t*>(ids_buf->mutable_data());
  memset(non_nulls, 0xff, bit_util::BytesForBits(length));

  int32_t num_entries = 0;
  for (int64_t i = 0; i < length; ++i) {
    std::string str = encoder.encoded_row(static_cast<int32_t>(i));

    // Do not insert null values into resulting dictionary.
    // Null values will always be represented as null not an id pointing to a
    // dictionary entry for null.
    //
    if (KeyEncoder::IsNull(reinterpret_cast<const uint8_t*>(str.data()))) {
      ids[i] = HashJoinDictUtil::kNullId;
      bit_util::ClearBit(non_nulls, i);
      continue;
    }

    auto iter = hash_table_.find(str);
    if (iter == hash_table_.end()) {
      hash_table_.insert(std::make_pair(str, num_entries));
      ids[i] = num_entries;
      entries_to_take.push_back(static_cast<int32_t>(i));
      ++num_entries;
    } else {
      ids[i] = iter->second;
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto out, encoder.Decode(num_entries, entries_to_take.data()));

  unified_dictionary_ = out[0].array();
  remapped_ids_ = ArrayData::Make(DataTypeAfterRemapping(), length,
                                  {std::move(non_nulls_buf), std::move(ids_buf)});

  return Status::OK();
}

Result<std::shared_ptr<ArrayData>> HashJoinDictBuild::RemapInputValues(
    ExecContext* ctx, const Datum& values, int64_t batch_length) const {
  // Initialize encoder
  //
  RowEncoder encoder;
  std::vector<TypeHolder> encoder_types = {value_type_};
  encoder.Init(encoder_types, ctx);

  // Encode all
  //
  ARROW_DCHECK(values.is_array() || values.is_scalar());
  bool is_scalar = values.is_scalar();
  int64_t encoded_length = is_scalar ? 1 : batch_length;
  ExecBatch batch({values}, encoded_length);
  RETURN_NOT_OK(encoder.EncodeAndAppend(ExecSpan(batch)));

  // Allocate output buffers
  //
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> non_nulls_buf,
                        AllocateBitmap(batch_length, ctx->memory_pool()));
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Buffer> ids_buf,
      AllocateBuffer(batch_length * sizeof(int32_t), ctx->memory_pool()));
  uint8_t* non_nulls = non_nulls_buf->mutable_data();
  int32_t* ids = reinterpret_cast<int32_t*>(ids_buf->mutable_data());
  memset(non_nulls, 0xff, bit_util::BytesForBits(batch_length));

  // Populate output buffers (for scalar only the first entry is populated)
  //
  for (int64_t i = 0; i < encoded_length; ++i) {
    std::string str = encoder.encoded_row(static_cast<int32_t>(i));
    if (KeyEncoder::IsNull(reinterpret_cast<const uint8_t*>(str.data()))) {
      // Map nulls to nulls
      bit_util::ClearBit(non_nulls, i);
      ids[i] = HashJoinDictUtil::kNullId;
    } else {
      auto iter = hash_table_.find(str);
      if (iter == hash_table_.end()) {
        ids[i] = HashJoinDictUtil::kMissingValueId;
      } else {
        ids[i] = iter->second;
      }
    }
  }

  // Generate array of repeated values for scalar input
  //
  if (is_scalar) {
    if (!bit_util::GetBit(non_nulls, 0)) {
      memset(non_nulls, 0, bit_util::BytesForBits(batch_length));
    }
    for (int64_t i = 1; i < batch_length; ++i) {
      ids[i] = ids[0];
    }
  }

  return ArrayData::Make(DataTypeAfterRemapping(), batch_length,
                         {std::move(non_nulls_buf), std::move(ids_buf)});
}

Result<std::shared_ptr<ArrayData>> HashJoinDictBuild::RemapInput(
    ExecContext* ctx, const Datum& indices, int64_t batch_length,
    const std::shared_ptr<DataType>& data_type) const {
  auto dict = HashJoinDictUtil::ExtractDictionary(indices);

  if (!dictionary_->Equals(dict)) {
    return Status::NotImplemented("Unifying differing dictionaries");
  }

  return HashJoinDictUtil::IndexRemapUsingLUT(ctx, indices, batch_length, remapped_ids_,
                                              data_type);
}

Result<std::shared_ptr<ArrayData>> HashJoinDictBuild::RemapOutput(
    const ArrayData& indices32Bit, ExecContext* ctx) const {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> indices,
                        HashJoinDictUtil::ConvertFromInt32(
                            index_type_, Datum(indices32Bit), indices32Bit.length, ctx));

  auto type = std::make_shared<DictionaryType>(index_type_, value_type_);
  return ArrayData::Make(type, indices->length, indices->buffers, {},
                         unified_dictionary_);
}

void HashJoinDictBuild::CleanUp() {
  index_type_.reset();
  value_type_.reset();
  hash_table_.clear();
  remapped_ids_.reset();
  unified_dictionary_.reset();
}

bool HashJoinDictProbe::KeyNeedsProcessing(
    const std::shared_ptr<DataType>& probe_data_type,
    const std::shared_ptr<DataType>& build_data_type) {
  bool l_is_dict = (probe_data_type->id() == Type::DICTIONARY);
  bool r_is_dict = (build_data_type->id() == Type::DICTIONARY);
  return l_is_dict || r_is_dict;
}

std::shared_ptr<DataType> HashJoinDictProbe::DataTypeAfterRemapping(
    const std::shared_ptr<DataType>& build_data_type) {
  bool r_is_dict = (build_data_type->id() == Type::DICTIONARY);
  if (r_is_dict) {
    return HashJoinDictBuild::DataTypeAfterRemapping();
  } else {
    return build_data_type;
  }
}

Result<std::shared_ptr<ArrayData>> HashJoinDictProbe::RemapInput(
    const HashJoinDictBuild* opt_build_side, const Datum& data, int64_t batch_length,
    const std::shared_ptr<DataType>& probe_data_type,
    const std::shared_ptr<DataType>& build_data_type, ExecContext* ctx) {
  // Cases:
  // 1. Dictionary(probe)-Dictionary(build)
  // 2. Dictionary(probe)-Value(build)
  // 3. Value(probe)-Dictionary(build)
  //
  bool l_is_dict = (probe_data_type->id() == Type::DICTIONARY);
  bool r_is_dict = (build_data_type->id() == Type::DICTIONARY);
  if (l_is_dict) {
    auto dict = HashJoinDictUtil::ExtractDictionary(data);
    const auto& dict_type = checked_cast<const DictionaryType&>(*probe_data_type);

    // Verify that the dictionary is always the same.
    if (dictionary_) {
      if (!dictionary_->Equals(dict)) {
        return Status::NotImplemented(
            "Unifying differing dictionaries for probe key of hash join");
      }
    } else {
      dictionary_ = dict;

      // Precompute helper data for the given dictionary if this is the first call.
      if (r_is_dict) {
        ARROW_DCHECK(opt_build_side);
        ARROW_ASSIGN_OR_RAISE(
            remapped_ids_,
            opt_build_side->RemapInputValues(ctx, Datum(dict->data()), dict->length()));
      } else {
        std::vector<TypeHolder> encoder_types = {dict_type.value_type()};
        encoder_.Init(encoder_types, ctx);
        RETURN_NOT_OK(
            encoder_.EncodeAndAppend(ExecSpan({*dict->data()}, dict->length())));
      }
    }

    if (r_is_dict) {
      // CASE 1:
      // Remap dictionary ids
      return HashJoinDictUtil::IndexRemapUsingLUT(ctx, data, batch_length, remapped_ids_,
                                                  probe_data_type);
    } else {
      // CASE 2:
      // Decode selected rows from encoder.
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> row_ids_arr,
                            HashJoinDictUtil::ConvertToInt32(dict_type.index_type(), data,
                                                             batch_length, ctx));
      // Change nulls to internal::RowEncoder::kRowIdForNulls() in index.
      int32_t* row_ids =
          reinterpret_cast<int32_t*>(row_ids_arr->buffers[1]->mutable_data());
      const uint8_t* non_nulls = row_ids_arr->buffers[0]->data();
      for (int64_t i = 0; i < batch_length; ++i) {
        if (!bit_util::GetBit(non_nulls, i)) {
          row_ids[i] = RowEncoder::kRowIdForNulls();
        }
      }

      ARROW_ASSIGN_OR_RAISE(ExecBatch batch, encoder_.Decode(batch_length, row_ids));
      return batch.values[0].array();
    }
  } else {
    // CASE 3:
    // Map values to dictionary ids from build side.
    // Values missing in the dictionary will get assigned a special constant
    // HashJoinDictUtil::kMissingValueId (different than any valid id).
    //
    ARROW_DCHECK(r_is_dict);
    ARROW_DCHECK(opt_build_side);
    return opt_build_side->RemapInputValues(ctx, data, batch_length);
  }
}

void HashJoinDictProbe::CleanUp() {
  dictionary_.reset();
  remapped_ids_.reset();
  encoder_.Clear();
}

Status HashJoinDictBuildMulti::Init(
    const SchemaProjectionMaps<HashJoinProjection>& proj_map,
    const ExecBatch* opt_non_empty_batch, ExecContext* ctx) {
  int num_keys = proj_map.num_cols(HashJoinProjection::KEY);
  needs_remap_.resize(num_keys);
  remap_imp_.resize(num_keys);
  for (int i = 0; i < num_keys; ++i) {
    needs_remap_[i] = HashJoinDictBuild::KeyNeedsProcessing(
        proj_map.data_type(HashJoinProjection::KEY, i));
  }

  bool build_side_empty = (opt_non_empty_batch == nullptr);

  if (!build_side_empty) {
    auto key_to_input = proj_map.map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
    for (int i = 0; i < num_keys; ++i) {
      const std::shared_ptr<DataType>& data_type =
          proj_map.data_type(HashJoinProjection::KEY, i);
      if (data_type->id() == Type::DICTIONARY) {
        const auto& dict_type = checked_cast<const DictionaryType&>(*data_type);
        const auto& dict = HashJoinDictUtil::ExtractDictionary(
            opt_non_empty_batch->values[key_to_input.get(i)]);
        RETURN_NOT_OK(remap_imp_[i].Init(ctx, dict, dict_type.index_type(),
                                         dict_type.value_type()));
      }
    }
  } else {
    for (int i = 0; i < num_keys; ++i) {
      const std::shared_ptr<DataType>& data_type =
          proj_map.data_type(HashJoinProjection::KEY, i);
      if (data_type->id() == Type::DICTIONARY) {
        const auto& dict_type = checked_cast<const DictionaryType&>(*data_type);
        RETURN_NOT_OK(remap_imp_[i].Init(ctx, nullptr, dict_type.index_type(),
                                         dict_type.value_type()));
      }
    }
  }
  return Status::OK();
}

void HashJoinDictBuildMulti::InitEncoder(
    const SchemaProjectionMaps<HashJoinProjection>& proj_map, RowEncoder* encoder,
    ExecContext* ctx) {
  int num_cols = proj_map.num_cols(HashJoinProjection::KEY);
  std::vector<TypeHolder> data_types(num_cols);
  for (int icol = 0; icol < num_cols; ++icol) {
    std::shared_ptr<DataType> data_type =
        proj_map.data_type(HashJoinProjection::KEY, icol);
    if (HashJoinDictBuild::KeyNeedsProcessing(data_type)) {
      data_type = HashJoinDictBuild::DataTypeAfterRemapping();
    }
    data_types[icol] = data_type;
  }
  encoder->Init(data_types, ctx);
}

Status HashJoinDictBuildMulti::EncodeBatch(
    size_t thread_index, const SchemaProjectionMaps<HashJoinProjection>& proj_map,
    const ExecBatch& batch, RowEncoder* encoder, ExecContext* ctx) const {
  ExecBatch projected({}, batch.length);
  int num_cols = proj_map.num_cols(HashJoinProjection::KEY);
  projected.values.resize(num_cols);

  auto to_input = proj_map.map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_cols; ++icol) {
    projected.values[icol] = batch.values[to_input.get(icol)];

    if (needs_remap_[icol]) {
      ARROW_ASSIGN_OR_RAISE(
          projected.values[icol],
          remap_imp_[icol].RemapInput(ctx, projected.values[icol], batch.length,
                                      proj_map.data_type(HashJoinProjection::KEY, icol)));
    }
  }
  return encoder->EncodeAndAppend(ExecSpan(projected));
}

Status HashJoinDictBuildMulti::PostDecode(
    const SchemaProjectionMaps<HashJoinProjection>& proj_map,
    ExecBatch* decoded_key_batch, ExecContext* ctx) {
  // Post process build side keys that use dictionary
  int num_keys = proj_map.num_cols(HashJoinProjection::KEY);
  for (int i = 0; i < num_keys; ++i) {
    if (needs_remap_[i]) {
      ARROW_ASSIGN_OR_RAISE(
          decoded_key_batch->values[i],
          remap_imp_[i].RemapOutput(*decoded_key_batch->values[i].array(), ctx));
    }
  }
  return Status::OK();
}

void HashJoinDictProbeMulti::Init(size_t num_threads) {
  local_states_.resize(num_threads);
  for (size_t i = 0; i < local_states_.size(); ++i) {
    local_states_[i].is_initialized = false;
  }
}

bool HashJoinDictProbeMulti::BatchRemapNeeded(
    size_t thread_index, const SchemaProjectionMaps<HashJoinProjection>& proj_map_probe,
    const SchemaProjectionMaps<HashJoinProjection>& proj_map_build, ExecContext* ctx) {
  InitLocalStateIfNeeded(thread_index, proj_map_probe, proj_map_build, ctx);
  DCHECK_LT(thread_index, local_states_.size());
  return local_states_[thread_index].any_needs_remap;
}

void HashJoinDictProbeMulti::InitLocalStateIfNeeded(
    size_t thread_index, const SchemaProjectionMaps<HashJoinProjection>& proj_map_probe,
    const SchemaProjectionMaps<HashJoinProjection>& proj_map_build, ExecContext* ctx) {
  ThreadLocalState& local_state = local_states_[thread_index];

  // Check if we need to remap any of the input keys because of dictionary encoding
  // on either side of the join
  //
  int num_cols = proj_map_probe.num_cols(HashJoinProjection::KEY);
  local_state.any_needs_remap = false;
  local_state.needs_remap.resize(num_cols);
  local_state.remap_imp.resize(num_cols);
  for (int i = 0; i < num_cols; ++i) {
    local_state.needs_remap[i] = HashJoinDictProbe::KeyNeedsProcessing(
        proj_map_probe.data_type(HashJoinProjection::KEY, i),
        proj_map_build.data_type(HashJoinProjection::KEY, i));
    if (local_state.needs_remap[i]) {
      local_state.any_needs_remap = true;
    }
  }

  if (local_state.any_needs_remap) {
    InitEncoder(proj_map_probe, proj_map_build, &local_state.post_remap_encoder, ctx);
  }
}

void HashJoinDictProbeMulti::InitEncoder(
    const SchemaProjectionMaps<HashJoinProjection>& proj_map_probe,
    const SchemaProjectionMaps<HashJoinProjection>& proj_map_build, RowEncoder* encoder,
    ExecContext* ctx) {
  int num_cols = proj_map_probe.num_cols(HashJoinProjection::KEY);
  std::vector<TypeHolder> data_types(num_cols);
  for (int icol = 0; icol < num_cols; ++icol) {
    std::shared_ptr<DataType> data_type =
        proj_map_probe.data_type(HashJoinProjection::KEY, icol);
    std::shared_ptr<DataType> build_data_type =
        proj_map_build.data_type(HashJoinProjection::KEY, icol);
    if (HashJoinDictProbe::KeyNeedsProcessing(data_type, build_data_type)) {
      data_type = HashJoinDictProbe::DataTypeAfterRemapping(build_data_type);
    }
    data_types[icol] = data_type;
  }
  encoder->Init(data_types, ctx);
}

Status HashJoinDictProbeMulti::EncodeBatch(
    size_t thread_index, const SchemaProjectionMaps<HashJoinProjection>& proj_map_probe,
    const SchemaProjectionMaps<HashJoinProjection>& proj_map_build,
    const HashJoinDictBuildMulti& dict_build, const ExecBatch& batch,
    RowEncoder** out_encoder, ExecBatch* opt_out_key_batch, ExecContext* ctx) {
  ThreadLocalState& local_state = local_states_[thread_index];
  InitLocalStateIfNeeded(thread_index, proj_map_probe, proj_map_build, ctx);

  ExecBatch projected({}, batch.length);
  int num_cols = proj_map_probe.num_cols(HashJoinProjection::KEY);
  projected.values.resize(num_cols);

  auto to_input = proj_map_probe.map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_cols; ++icol) {
    projected.values[icol] = batch.values[to_input.get(icol)];

    if (local_state.needs_remap[icol]) {
      ARROW_ASSIGN_OR_RAISE(
          projected.values[icol],
          local_state.remap_imp[icol].RemapInput(
              &(dict_build.get_dict_build(icol)), projected.values[icol], batch.length,
              proj_map_probe.data_type(HashJoinProjection::KEY, icol),
              proj_map_build.data_type(HashJoinProjection::KEY, icol), ctx));
    }
  }

  if (opt_out_key_batch) {
    *opt_out_key_batch = projected;
  }

  local_state.post_remap_encoder.Clear();
  RETURN_NOT_OK(local_state.post_remap_encoder.EncodeAndAppend(ExecSpan(projected)));
  *out_encoder = &local_state.post_remap_encoder;

  return Status::OK();
}

}  // namespace acero
}  // namespace arrow

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

#include <gmock/gmock-matchers.h>

#include <memory>
#include <numeric>
#include <random>
#include <unordered_set>

#include "arrow/acero/options.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/compute/kernels/row_encoder_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/thread_pool.h"

using testing::UnorderedElementsAreArray;

namespace arrow {

using compute::call;
using compute::default_exec_context;
using compute::ExecSpan;
using compute::field_ref;
using compute::SortIndices;
using compute::SortKey;
using compute::Take;
using compute::internal::RowEncoder;

namespace acero {

BatchesWithSchema GenerateBatchesFromString(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::string_view>& json_strings, int multiplicity = 1) {
  BatchesWithSchema out_batches{{}, schema};

  std::vector<TypeHolder> types;
  for (auto&& field : schema->fields()) {
    types.emplace_back(field->type());
  }

  for (auto&& s : json_strings) {
    out_batches.batches.push_back(ExecBatchFromJSON(types, s));
  }

  size_t batch_count = out_batches.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out_batches.batches.push_back(out_batches.batches[i]);
    }
  }

  return out_batches;
}

void CheckRunOutput(JoinType type, const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r_batches,
                    const std::vector<FieldRef>& left_keys,
                    const std::vector<FieldRef>& right_keys,
                    const BatchesWithSchema& exp_batches, bool parallel = false) {
  Declaration left{"source",
                   SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                     /*slow=*/false)}};
  Declaration right{"source",
                    SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                      /*slow=*/false)}};
  HashJoinNodeOptions join_options{type, left_keys, right_keys};
  Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_options};

  ASSERT_OK_AND_ASSIGN(auto out_table, DeclarationToTable(std::move(join), parallel));

  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  if (exp_table->num_rows() == 0) {
    ASSERT_EQ(exp_table->num_rows(), out_table->num_rows());
  } else {
    std::vector<SortKey> sort_keys;
    for (auto&& f : exp_batches.schema->fields()) {
      sort_keys.emplace_back(f->name());
    }
    ASSERT_OK_AND_ASSIGN(auto exp_table_sort_ids,
                         SortIndices(exp_table, SortOptions(sort_keys)));
    ASSERT_OK_AND_ASSIGN(auto exp_table_sorted, Take(exp_table, exp_table_sort_ids));
    ASSERT_OK_AND_ASSIGN(auto out_table_sort_ids,
                         SortIndices(out_table, SortOptions(sort_keys)));
    ASSERT_OK_AND_ASSIGN(auto out_table_sorted, Take(out_table, out_table_sort_ids));

    AssertTablesEqual(*exp_table_sorted.table(), *out_table_sorted.table(),
                      /*same_chunk_layout=*/false, /*flatten=*/true);
  }
}

void RunNonEmptyTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("l_i32", int32()), field("l_str", utf8())});
  auto r_schema = schema({field("r_str", utf8()), field("r_i32", int32())});
  BatchesWithSchema l_batches, r_batches, exp_batches;

  int multiplicity = parallel ? 100 : 1;

  l_batches = GenerateBatchesFromString(
      l_schema,
      {R"([[0,"d"], [1,"b"]])", R"([[2,"d"], [3,"a"], [4,"a"]])",
       R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
      multiplicity);

  r_batches = GenerateBatchesFromString(
      r_schema,
      {R"([["f", 0], ["b", 1], ["b", 2]])", R"([["c", 3], ["g", 4]])", R"([["e", 5]])"},
      multiplicity);

  switch (type) {
    case JoinType::LEFT_SEMI:
      exp_batches = GenerateBatchesFromString(
          l_schema, {R"([[1,"b"]])", R"([])", R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
          multiplicity);
      break;
    case JoinType::RIGHT_SEMI:
      exp_batches = GenerateBatchesFromString(
          r_schema, {R"([["b", 1], ["b", 2]])", R"([["c", 3]])", R"([["e", 5]])"},
          multiplicity);
      break;
    case JoinType::LEFT_ANTI:
      exp_batches = GenerateBatchesFromString(
          l_schema, {R"([[0,"d"]])", R"([[2,"d"], [3,"a"], [4,"a"]])", R"([])"},
          multiplicity);
      break;
    case JoinType::RIGHT_ANTI:
      exp_batches = GenerateBatchesFromString(
          r_schema, {R"([["f", 0]])", R"([["g", 4]])", R"([])"}, multiplicity);
      break;
    case JoinType::INNER:
    case JoinType::LEFT_OUTER:
    case JoinType::RIGHT_OUTER:
    case JoinType::FULL_OUTER:
    default:
      FAIL() << "join type not implemented!";
  }

  CheckRunOutput(type, l_batches, r_batches,
                 /*left_keys=*/{{"l_str"}}, /*right_keys=*/{{"r_str"}}, exp_batches,
                 parallel);
}

void RunEmptyTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("l_i32", int32()), field("l_str", utf8())});
  auto r_schema = schema({field("r_str", utf8()), field("r_i32", int32())});

  int multiplicity = parallel ? 100 : 1;

  BatchesWithSchema l_empty, r_empty, l_n_empty, r_n_empty;

  l_empty = GenerateBatchesFromString(l_schema, {R"([])"}, multiplicity);
  r_empty = GenerateBatchesFromString(r_schema, {R"([])"}, multiplicity);

  l_n_empty =
      GenerateBatchesFromString(l_schema, {R"([[0,"d"], [1,"b"]])"}, multiplicity);
  r_n_empty = GenerateBatchesFromString(r_schema, {R"([["f", 0], ["b", 1], ["b", 2]])"},
                                        multiplicity);

  std::vector<FieldRef> l_keys{{"l_str"}};
  std::vector<FieldRef> r_keys{{"r_str"}};

  switch (type) {
    case JoinType::LEFT_SEMI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, l_empty, parallel);
      break;
    case JoinType::RIGHT_SEMI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, r_empty, parallel);
      break;
    case JoinType::LEFT_ANTI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, l_n_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, l_empty, parallel);
      break;
    case JoinType::RIGHT_ANTI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, r_n_empty, parallel);
      break;
    case JoinType::INNER:
    case JoinType::LEFT_OUTER:
    case JoinType::RIGHT_OUTER:
    case JoinType::FULL_OUTER:
    default:
      FAIL() << "join type not implemented!";
  }
}

class HashJoinTest : public testing::TestWithParam<std::tuple<JoinType, bool>> {};

INSTANTIATE_TEST_SUITE_P(
    HashJoinTest, HashJoinTest,
    ::testing::Combine(::testing::Values(JoinType::LEFT_SEMI, JoinType::RIGHT_SEMI,
                                         JoinType::LEFT_ANTI, JoinType::RIGHT_ANTI),
                       ::testing::Values(false, true)));

TEST_P(HashJoinTest, TestSemiJoins) {
  RunNonEmptyTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

TEST_P(HashJoinTest, TestSemiJoinsEmpty) {
  RunEmptyTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

struct RandomDataTypeConstraints {
  int64_t data_type_enabled_mask;
  // Null related
  double min_null_probability;
  double max_null_probability;
  // Binary related
  int min_binary_length;
  int max_binary_length;
  // String related
  int min_string_length;
  int max_string_length;

  void Default() {
    data_type_enabled_mask = kInt1 | kInt2 | kInt4 | kInt8 | kBool | kBinary | kString;
    min_null_probability = 0.0;
    max_null_probability = 0.2;
    min_binary_length = 1;
    max_binary_length = 40;
    min_string_length = 0;
    max_string_length = 40;
  }

  void OnlyInt(int int_size, bool allow_nulls) {
    Default();
    data_type_enabled_mask = int_size == 8   ? kInt8
                             : int_size == 4 ? kInt4
                             : int_size == 2 ? kInt2
                                             : kInt1;
    if (!allow_nulls) {
      max_null_probability = 0.0;
    }
  }

  void OnlyString(bool allow_nulls) {
    Default();
    data_type_enabled_mask = kString;
    if (!allow_nulls) {
      max_null_probability = 0.0;
    }
  }

  // Data type mask constants
  static constexpr int64_t kInt1 = 1;
  static constexpr int64_t kInt2 = 2;
  static constexpr int64_t kInt4 = 4;
  static constexpr int64_t kInt8 = 8;
  static constexpr int64_t kBool = 16;
  static constexpr int64_t kBinary = 32;
  static constexpr int64_t kString = 64;
};

struct RandomDataType {
  double null_probability;
  bool is_fixed_length;
  int fixed_length;
  int min_string_length;
  int max_string_length;

  static RandomDataType Random(Random64Bit& rng,
                               const RandomDataTypeConstraints& constraints) {
    RandomDataType result;
    if ((constraints.data_type_enabled_mask & constraints.kString) != 0) {
      if (constraints.data_type_enabled_mask != constraints.kString) {
        // Both string and fixed length types enabled
        // 50% chance of string
        result.is_fixed_length = ((rng.next() % 2) == 0);
      } else {
        result.is_fixed_length = false;
      }
    } else {
      result.is_fixed_length = true;
    }
    if (constraints.max_null_probability > 0.0) {
      // 25% chance of no nulls
      // Uniform distribution of null probability from min to max
      result.null_probability = ((rng.next() % 4) == 0)
                                    ? 0.0
                                    : static_cast<double>(rng.next() % 1025) / 1024.0 *
                                              (constraints.max_null_probability -
                                               constraints.min_null_probability) +
                                          constraints.min_null_probability;
    } else {
      result.null_probability = 0.0;
    }
    // Pick data type for fixed length
    if (result.is_fixed_length) {
      int log_type;
      for (;;) {
        log_type = rng.next() % 6;
        if (constraints.data_type_enabled_mask & (1ULL << log_type)) {
          break;
        }
      }
      if ((1ULL << log_type) == constraints.kBinary) {
        for (;;) {
          result.fixed_length = rng.from_range(constraints.min_binary_length,
                                               constraints.max_binary_length);
          if (result.fixed_length != 1 && result.fixed_length != 2 &&
              result.fixed_length != 4 && result.fixed_length != 8) {
            break;
          }
        }
      } else {
        result.fixed_length = ((1ULL << log_type) == constraints.kBool)
                                  ? 0
                                  : static_cast<int>(1ULL << log_type);
      }
    } else {
      // Pick parameters for string
      result.min_string_length =
          rng.from_range(constraints.min_string_length, constraints.max_string_length);
      result.max_string_length =
          rng.from_range(constraints.min_string_length, constraints.max_string_length);
      if (result.min_string_length > result.max_string_length) {
        std::swap(result.min_string_length, result.max_string_length);
      }
    }
    return result;
  }
};

struct RandomDataTypeVector {
  std::vector<RandomDataType> data_types;

  void AddRandom(Random64Bit& rng, const RandomDataTypeConstraints& constraints) {
    data_types.push_back(RandomDataType::Random(rng, constraints));
  }
};

std::vector<std::shared_ptr<Array>> GenRandomRecords(
    Random64Bit& rng, const std::vector<RandomDataType>& data_types, int num_rows) {
  std::vector<std::shared_ptr<Array>> result;
  random::RandomArrayGenerator rag(static_cast<random::SeedType>(rng.next()));
  for (size_t i = 0; i < data_types.size(); ++i) {
    if (data_types[i].is_fixed_length) {
      switch (data_types[i].fixed_length) {
        case 0:
          result.push_back(rag.Boolean(num_rows, 0.5, data_types[i].null_probability));
          break;
        case 1:
          result.push_back(rag.UInt8(num_rows, std::numeric_limits<uint8_t>::min(),
                                     std::numeric_limits<uint8_t>::max(),
                                     data_types[i].null_probability));
          break;
        case 2:
          result.push_back(rag.UInt16(num_rows, std::numeric_limits<uint16_t>::min(),
                                      std::numeric_limits<uint16_t>::max(),
                                      data_types[i].null_probability));
          break;
        case 4:
          result.push_back(rag.UInt32(num_rows, std::numeric_limits<uint32_t>::min(),
                                      std::numeric_limits<uint32_t>::max(),
                                      data_types[i].null_probability));
          break;
        case 8:
          result.push_back(rag.UInt64(num_rows, std::numeric_limits<uint64_t>::min(),
                                      std::numeric_limits<uint64_t>::max(),
                                      data_types[i].null_probability));
          break;
        default:
          result.push_back(rag.FixedSizeBinary(num_rows, data_types[i].fixed_length,
                                               data_types[i].null_probability));
          break;
      }
    } else {
      result.push_back(rag.String(num_rows, data_types[i].min_string_length,
                                  data_types[i].max_string_length,
                                  data_types[i].null_probability));
    }
  }
  return result;
}

// Index < 0 means appending null values to all columns.
//
void TakeUsingVector(ExecContext* ctx, const std::vector<std::shared_ptr<Array>>& input,
                     const std::vector<int32_t> indices,
                     std::vector<std::shared_ptr<Array>>* result) {
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Buffer> buf,
      AllocateBuffer(indices.size() * sizeof(int32_t), ctx->memory_pool()));
  int32_t* buf_indices = reinterpret_cast<int32_t*>(buf->mutable_data());
  bool has_null_rows = false;
  for (size_t i = 0; i < indices.size(); ++i) {
    if (indices[i] < 0) {
      buf_indices[i] = 0;
      has_null_rows = true;
    } else {
      buf_indices[i] = indices[i];
    }
  }
  std::shared_ptr<Array> indices_array = MakeArray(ArrayData::Make(
      int32(), indices.size(), {nullptr, std::move(buf)}, /*null_count=*/0));

  result->resize(input.size());
  for (size_t i = 0; i < result->size(); ++i) {
    ASSERT_OK_AND_ASSIGN(Datum new_array, Take(input[i], indices_array));
    (*result)[i] = new_array.make_array();
  }
  if (has_null_rows) {
    for (size_t i = 0; i < result->size(); ++i) {
      if ((*result)[i]->data()->buffers[0] == NULLPTR) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> null_buf,
                             AllocateBitmap(indices.size(), ctx->memory_pool()));
        uint8_t* non_nulls = null_buf->mutable_data();
        memset(non_nulls, 0xFF, bit_util::BytesForBits(indices.size()));
        if ((*result)[i]->data()->buffers.size() == 2) {
          (*result)[i] = MakeArray(
              ArrayData::Make((*result)[i]->type(), indices.size(),
                              {std::move(null_buf), (*result)[i]->data()->buffers[1]}));
        } else {
          (*result)[i] = MakeArray(
              ArrayData::Make((*result)[i]->type(), indices.size(),
                              {std::move(null_buf), (*result)[i]->data()->buffers[1],
                               (*result)[i]->data()->buffers[2]}));
        }
      }
      (*result)[i]->data()->SetNullCount(kUnknownNullCount);
    }
    for (size_t i = 0; i < indices.size(); ++i) {
      if (indices[i] < 0) {
        for (size_t col = 0; col < result->size(); ++col) {
          uint8_t* non_nulls = (*result)[col]->data()->buffers[0]->mutable_data();
          bit_util::ClearBit(non_nulls, i);
        }
      }
    }
  }
}

// Generate random arrays given list of data types and null probabilities.
// Make sure that all generated records are unique.
// The actual number of generated records may be lower than desired because duplicates
// will be removed without replacement.
//
std::vector<std::shared_ptr<Array>> GenRandomUniqueRecords(
    Random64Bit& rng, const RandomDataTypeVector& data_types, int num_desired,
    int* num_actual) {
  std::vector<std::shared_ptr<Array>> result =
      GenRandomRecords(rng, data_types.data_types, num_desired);

  ExecContext* ctx = default_exec_context();
  std::vector<TypeHolder> val_types;
  for (size_t i = 0; i < result.size(); ++i) {
    val_types.push_back(result[i]->type());
  }
  RowEncoder encoder;
  encoder.Init(val_types, ctx);
  ExecBatch batch({}, num_desired);
  batch.values.resize(result.size());
  for (size_t i = 0; i < result.size(); ++i) {
    batch.values[i] = result[i];
  }
  Status status = encoder.EncodeAndAppend(ExecSpan(batch));
  ARROW_DCHECK(status.ok());

  std::unordered_map<std::string, int> uniques;
  std::vector<int32_t> ids;
  for (int i = 0; i < num_desired; ++i) {
    if (uniques.find(encoder.encoded_row(i)) == uniques.end()) {
      uniques.insert(std::make_pair(encoder.encoded_row(i), i));
      ids.push_back(i);
    }
  }
  *num_actual = static_cast<int>(uniques.size());

  std::vector<std::shared_ptr<Array>> output;
  TakeUsingVector(ctx, result, ids, &output);
  return output;
}

std::vector<bool> NullInKey(const std::vector<JoinKeyCmp>& cmp,
                            const std::vector<std::shared_ptr<Array>>& key) {
  ARROW_DCHECK(cmp.size() <= key.size());
  ARROW_DCHECK(key.size() > 0);
  std::vector<bool> result;
  result.resize(key[0]->length());
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = false;
  }
  for (size_t i = 0; i < cmp.size(); ++i) {
    if (cmp[i] != JoinKeyCmp::EQ) {
      continue;
    }
    if (key[i]->data()->buffers[0] == NULLPTR) {
      continue;
    }
    const uint8_t* nulls = key[i]->data()->buffers[0]->data();
    if (!nulls) {
      continue;
    }
    for (size_t j = 0; j < result.size(); ++j) {
      if (!bit_util::GetBit(nulls, j)) {
        result[j] = true;
      }
    }
  }
  return result;
}

void GenRandomJoinTables(ExecContext* ctx, Random64Bit& rng, int num_rows_l,
                         int num_rows_r, int num_keys_common, int num_keys_left,
                         int num_keys_right, const RandomDataTypeVector& key_types,
                         const RandomDataTypeVector& payload_left_types,
                         const RandomDataTypeVector& payload_right_types,
                         std::vector<int32_t>* key_id_l, std::vector<int32_t>* key_id_r,
                         std::vector<std::shared_ptr<Array>>* left,
                         std::vector<std::shared_ptr<Array>>* right) {
  // Generate random keys dictionary
  //
  int num_keys_desired = num_keys_left + num_keys_right - num_keys_common;
  int num_keys_actual = 0;
  std::vector<std::shared_ptr<Array>> keys =
      GenRandomUniqueRecords(rng, key_types, num_keys_desired, &num_keys_actual);

  // There will be three dictionary id ranges:
  // - common keys [0..num_keys_common-1]
  // - keys on right that are not on left [num_keys_common..num_keys_right-1]
  // - keys on left that are not on right [num_keys_right..num_keys_actual-1]
  //
  num_keys_common = static_cast<int>(static_cast<int64_t>(num_keys_common) *
                                     num_keys_actual / num_keys_desired);
  num_keys_right = static_cast<int>(static_cast<int64_t>(num_keys_right) *
                                    num_keys_actual / num_keys_desired);
  ARROW_DCHECK(num_keys_right >= num_keys_common);
  num_keys_left = num_keys_actual - num_keys_right + num_keys_common;
  if (num_keys_left == 0) {
    ARROW_DCHECK(num_keys_common == 0 && num_keys_right > 0);
    ++num_keys_left;
    ++num_keys_common;
  }
  if (num_keys_right == 0) {
    ARROW_DCHECK(num_keys_common == 0 && num_keys_left > 0);
    ++num_keys_right;
    ++num_keys_common;
  }
  ARROW_DCHECK(num_keys_left >= num_keys_common);
  ARROW_DCHECK(num_keys_left + num_keys_right - num_keys_common == num_keys_actual);

  key_id_l->resize(num_rows_l);
  for (int i = 0; i < num_rows_l; ++i) {
    (*key_id_l)[i] = rng.from_range(0, num_keys_left - 1);
    if ((*key_id_l)[i] >= num_keys_common) {
      (*key_id_l)[i] += num_keys_right - num_keys_common;
    }
  }

  key_id_r->resize(num_rows_r);
  for (int i = 0; i < num_rows_r; ++i) {
    (*key_id_r)[i] = rng.from_range(0, num_keys_right - 1);
  }

  std::vector<std::shared_ptr<Array>> key_l;
  std::vector<std::shared_ptr<Array>> key_r;
  TakeUsingVector(ctx, keys, *key_id_l, &key_l);
  TakeUsingVector(ctx, keys, *key_id_r, &key_r);
  std::vector<std::shared_ptr<Array>> payload_l =
      GenRandomRecords(rng, payload_left_types.data_types, num_rows_l);
  std::vector<std::shared_ptr<Array>> payload_r =
      GenRandomRecords(rng, payload_right_types.data_types, num_rows_r);

  left->resize(key_l.size() + payload_l.size());
  for (size_t i = 0; i < key_l.size(); ++i) {
    (*left)[i] = key_l[i];
  }
  for (size_t i = 0; i < payload_l.size(); ++i) {
    (*left)[key_l.size() + i] = payload_l[i];
  }
  right->resize(key_r.size() + payload_r.size());
  for (size_t i = 0; i < key_r.size(); ++i) {
    (*right)[i] = key_r[i];
  }
  for (size_t i = 0; i < payload_r.size(); ++i) {
    (*right)[key_r.size() + i] = payload_r[i];
  }
}

std::vector<std::shared_ptr<Array>> ConstructJoinOutputFromRowIds(
    ExecContext* ctx, const std::vector<int32_t>& row_ids_l,
    const std::vector<int32_t>& row_ids_r, const std::vector<std::shared_ptr<Array>>& l,
    const std::vector<std::shared_ptr<Array>>& r,
    const std::vector<int>& shuffle_output_l, const std::vector<int>& shuffle_output_r) {
  std::vector<std::shared_ptr<Array>> full_output_l;
  std::vector<std::shared_ptr<Array>> full_output_r;
  TakeUsingVector(ctx, l, row_ids_l, &full_output_l);
  TakeUsingVector(ctx, r, row_ids_r, &full_output_r);
  std::vector<std::shared_ptr<Array>> result;
  result.resize(shuffle_output_l.size() + shuffle_output_r.size());
  for (size_t i = 0; i < shuffle_output_l.size(); ++i) {
    result[i] = full_output_l[shuffle_output_l[i]];
  }
  for (size_t i = 0; i < shuffle_output_r.size(); ++i) {
    result[shuffle_output_l.size() + i] = full_output_r[shuffle_output_r[i]];
  }
  return result;
}

BatchesWithSchema TableToBatches(Random64Bit& rng, int num_batches,
                                 const std::vector<std::shared_ptr<Array>>& table,
                                 const std::string& column_name_prefix) {
  BatchesWithSchema result;

  std::vector<std::shared_ptr<Field>> fields;
  fields.resize(table.size());
  for (size_t i = 0; i < table.size(); ++i) {
    fields[i] = std::make_shared<Field>(column_name_prefix + std::to_string(i),
                                        table[i]->type(), true);
  }
  result.schema = std::make_shared<Schema>(std::move(fields));

  int64_t length = table[0]->length();
  num_batches = std::min(num_batches, static_cast<int>(length));

  std::vector<int64_t> batch_offsets;
  batch_offsets.push_back(0);
  batch_offsets.push_back(length);
  std::unordered_set<int64_t> batch_offset_set;
  for (int i = 0; i < num_batches - 1; ++i) {
    for (;;) {
      int64_t offset = rng.from_range(static_cast<int64_t>(1), length - 1);
      if (batch_offset_set.find(offset) == batch_offset_set.end()) {
        batch_offset_set.insert(offset);
        batch_offsets.push_back(offset);
        break;
      }
    }
  }
  std::sort(batch_offsets.begin(), batch_offsets.end());

  for (int i = 0; i < num_batches; ++i) {
    int64_t batch_offset = batch_offsets[i];
    int64_t batch_length = batch_offsets[i + 1] - batch_offsets[i];
    ExecBatch batch({}, batch_length);
    batch.values.resize(table.size());
    for (size_t col = 0; col < table.size(); ++col) {
      batch.values[col] = table[col]->data()->Slice(batch_offset, batch_length);
    }
    result.batches.push_back(batch);
  }

  return result;
}

// -1 in result means outputting all corresponding fields as nulls
//
void HashJoinSimpleInt(JoinType join_type, const std::vector<int32_t>& l,
                       const std::vector<bool>& null_in_key_l,
                       const std::vector<int32_t>& r,
                       const std::vector<bool>& null_in_key_r,
                       std::vector<int32_t>* result_l, std::vector<int32_t>* result_r,
                       int64_t output_length_limit, bool* length_limit_reached) {
  *length_limit_reached = false;

  bool switch_sides = false;
  switch (join_type) {
    case JoinType::RIGHT_SEMI:
      join_type = JoinType::LEFT_SEMI;
      switch_sides = true;
      break;
    case JoinType::RIGHT_ANTI:
      join_type = JoinType::LEFT_ANTI;
      switch_sides = true;
      break;
    case JoinType::RIGHT_OUTER:
      join_type = JoinType::LEFT_OUTER;
      switch_sides = true;
      break;
    default:
      break;
  }
  const std::vector<int32_t>& build = switch_sides ? l : r;
  const std::vector<int32_t>& probe = switch_sides ? r : l;
  const std::vector<bool>& null_in_key_build =
      switch_sides ? null_in_key_l : null_in_key_r;
  const std::vector<bool>& null_in_key_probe =
      switch_sides ? null_in_key_r : null_in_key_l;
  std::vector<int32_t>* result_build = switch_sides ? result_l : result_r;
  std::vector<int32_t>* result_probe = switch_sides ? result_r : result_l;

  std::unordered_multimap<int64_t, int64_t> map_build;
  for (size_t i = 0; i < build.size(); ++i) {
    map_build.insert(std::make_pair(build[i], i));
  }
  std::vector<bool> match_build;
  match_build.resize(build.size());
  for (size_t i = 0; i < build.size(); ++i) {
    match_build[i] = false;
  }

  for (int32_t i = 0; i < static_cast<int32_t>(probe.size()); ++i) {
    std::vector<int32_t> match_probe;
    if (!null_in_key_probe[i]) {
      auto range = map_build.equal_range(probe[i]);
      for (auto it = range.first; it != range.second; ++it) {
        if (!null_in_key_build[it->second]) {
          match_probe.push_back(static_cast<int32_t>(it->second));
          match_build[it->second] = true;
        }
      }
    }
    switch (join_type) {
      case JoinType::LEFT_SEMI:
        if (!match_probe.empty()) {
          result_probe->push_back(i);
          result_build->push_back(-1);
        }
        break;
      case JoinType::LEFT_ANTI:
        if (match_probe.empty()) {
          result_probe->push_back(i);
          result_build->push_back(-1);
        }
        break;
      case JoinType::INNER:
        for (size_t j = 0; j < match_probe.size(); ++j) {
          result_probe->push_back(i);
          result_build->push_back(match_probe[j]);
        }
        break;
      case JoinType::LEFT_OUTER:
      case JoinType::FULL_OUTER:
        if (match_probe.empty()) {
          result_probe->push_back(i);
          result_build->push_back(-1);
        } else {
          for (size_t j = 0; j < match_probe.size(); ++j) {
            result_probe->push_back(i);
            result_build->push_back(match_probe[j]);
          }
        }
        break;
      default:
        ARROW_DCHECK(false);
        break;
    }

    if (static_cast<int64_t>(result_probe->size()) >= output_length_limit) {
      *length_limit_reached = true;
      return;
    }
  }

  if (join_type == JoinType::FULL_OUTER) {
    for (int32_t i = 0; i < static_cast<int32_t>(build.size()); ++i) {
      if (!match_build[i]) {
        result_probe->push_back(-1);
        result_build->push_back(i);
      }
    }
  }
}

std::vector<int> GenShuffle(Random64Bit& rng, int length) {
  std::vector<int> shuffle(length);
  std::iota(shuffle.begin(), shuffle.end(), 0);
  for (int i = 0; i < length * 2; ++i) {
    int from = rng.from_range(0, length - 1);
    int to = rng.from_range(0, length - 1);
    if (from != to) {
      std::swap(shuffle[from], shuffle[to]);
    }
  }
  return shuffle;
}

void GenJoinFieldRefs(Random64Bit& rng, int num_key_fields, bool no_output,
                      const std::vector<std::shared_ptr<Array>>& original_input,
                      const std::string& field_name_prefix,
                      std::vector<std::shared_ptr<Array>>* new_input,
                      std::vector<FieldRef>* keys, std::vector<FieldRef>* output,
                      std::vector<int>* output_field_ids) {
  // Permute input
  std::vector<int> shuffle = GenShuffle(rng, static_cast<int>(original_input.size()));
  new_input->resize(original_input.size());
  for (size_t i = 0; i < original_input.size(); ++i) {
    (*new_input)[i] = original_input[shuffle[i]];
  }

  // Compute key field refs
  keys->resize(num_key_fields);
  for (size_t i = 0; i < shuffle.size(); ++i) {
    if (shuffle[i] < num_key_fields) {
      bool use_by_name_ref = (rng.from_range(0, 1) == 0);
      if (use_by_name_ref) {
        (*keys)[shuffle[i]] = FieldRef(field_name_prefix + std::to_string(i));
      } else {
        (*keys)[shuffle[i]] = FieldRef(static_cast<int>(i));
      }
    }
  }

  // Compute output field refs
  if (!no_output) {
    int num_output = rng.from_range(1, static_cast<int>(original_input.size() + 1));
    output_field_ids->resize(num_output);
    output->resize(num_output);
    for (int i = 0; i < num_output; ++i) {
      int col_id = rng.from_range(0, static_cast<int>(original_input.size() - 1));
      (*output_field_ids)[i] = col_id;
      (*output)[i] = (rng.from_range(0, 1) == 0)
                         ? FieldRef(field_name_prefix + std::to_string(col_id))
                         : FieldRef(col_id);
    }
  }
}

std::shared_ptr<Table> HashJoinSimple(
    ExecContext* ctx, JoinType join_type, const std::vector<JoinKeyCmp>& cmp,
    int num_key_fields, const std::vector<int32_t>& key_id_l,
    const std::vector<int32_t>& key_id_r,
    const std::vector<std::shared_ptr<Array>>& original_l,
    const std::vector<std::shared_ptr<Array>>& original_r,
    const std::vector<std::shared_ptr<Array>>& l,
    const std::vector<std::shared_ptr<Array>>& r, const std::vector<int>& output_ids_l,
    const std::vector<int>& output_ids_r, int64_t output_length_limit,
    bool* length_limit_reached) {
  std::vector<std::shared_ptr<Array>> key_l(num_key_fields);
  std::vector<std::shared_ptr<Array>> key_r(num_key_fields);
  for (int i = 0; i < num_key_fields; ++i) {
    key_l[i] = original_l[i];
    key_r[i] = original_r[i];
  }
  std::vector<bool> null_key_l = NullInKey(cmp, key_l);
  std::vector<bool> null_key_r = NullInKey(cmp, key_r);

  std::vector<int32_t> row_ids_l;
  std::vector<int32_t> row_ids_r;
  HashJoinSimpleInt(join_type, key_id_l, null_key_l, key_id_r, null_key_r, &row_ids_l,
                    &row_ids_r, output_length_limit, length_limit_reached);

  std::vector<std::shared_ptr<Array>> result = ConstructJoinOutputFromRowIds(
      ctx, row_ids_l, row_ids_r, l, r, output_ids_l, output_ids_r);

  std::vector<std::shared_ptr<Field>> fields(result.size());
  for (size_t i = 0; i < result.size(); ++i) {
    fields[i] = std::make_shared<Field>("a" + std::to_string(i), result[i]->type(), true);
  }
  std::shared_ptr<Schema> schema = std::make_shared<Schema>(std::move(fields));
  return Table::Make(schema, result, result[0]->length());
}

Result<std::vector<ExecBatch>> HashJoinWithExecPlan(
    Random64Bit& rng, bool parallel, const HashJoinNodeOptions& join_options,
    const std::shared_ptr<Schema>& output_schema,
    const std::vector<std::shared_ptr<Array>>& l,
    const std::vector<std::shared_ptr<Array>>& r, int num_batches_l, int num_batches_r) {
  // add left source
  BatchesWithSchema l_batches = TableToBatches(rng, num_batches_l, l, "l_");
  Declaration left{"source",
                   SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                     /*slow=*/false)}};
  // add right source
  BatchesWithSchema r_batches = TableToBatches(rng, num_batches_r, r, "r_");
  Declaration right{"source",
                    SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                      /*slow=*/false)}};
  Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_options};

  ARROW_ASSIGN_OR_RAISE(BatchesWithCommonSchema batches_and_schema,
                        DeclarationToExecBatches(std::move(join), parallel));
  return batches_and_schema.batches;
}

TEST(HashJoin, Suffix) {
  BatchesWithSchema input_left;
  input_left.batches = {ExecBatchFromJSON({int32(), int32(), int32()}, R"([
                   [1, 4, 7],
                   [2, 5, 8],
                   [3, 6, 9]
                 ])")};
  input_left.schema = schema(
      {field("lkey", int32()), field("shared", int32()), field("ldistinct", int32())});

  BatchesWithSchema input_right;
  input_right.batches = {ExecBatchFromJSON({int32(), int32(), int32()}, R"([
                   [1, 10, 13],
                   [2, 11, 14],
                   [3, 12, 15]
                 ])")};
  input_right.schema = schema(
      {field("rkey", int32()), field("shared", int32()), field("rdistinct", int32())});

  BatchesWithSchema expected;
  expected.batches = {
      ExecBatchFromJSON({int32(), int32(), int32(), int32(), int32(), int32()}, R"([
    [1, 4, 7, 1, 10, 13],
    [2, 5, 8, 2, 11, 14],
    [3, 6, 9, 3, 12, 15]
  ])")};

  expected.schema = schema({field("lkey", int32()), field("shared_l", int32()),
                            field("ldistinct", int32()), field("rkey", int32()),
                            field("shared_r", int32()), field("rdistinct", int32())});

  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  Declaration left{"source",
                   SourceNodeOptions{input_left.schema, input_left.gen(/*parallel=*/false,
                                                                       /*slow=*/false)}};
  Declaration right{
      "source", SourceNodeOptions{input_right.schema, input_right.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}};
  HashJoinNodeOptions join_opts{JoinType::INNER,
                                /*left_keys=*/{"lkey"},
                                /*right_keys=*/{"rkey"}, literal(true), "_l", "_r"};

  Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_opts};

  ASSERT_OK_AND_ASSIGN(auto actual, DeclarationToExecBatches(std::move(join)));

  AssertExecBatchesEqualIgnoringOrder(expected.schema, expected.batches, actual.batches);
  AssertSchemaEqual(expected.schema, actual.schema);
}

TEST(HashJoin, Random) {
  Random64Bit rng(42);
#if defined(THREAD_SANITIZER) || defined(ARROW_VALGRIND)
  const int num_tests = 15;
#elif defined(ADDRESS_SANITIZER)
  const int num_tests = 25;
#else
  const int num_tests = 100;
#endif
  for (int test_id = 0; test_id < num_tests; ++test_id) {
    bool parallel = (rng.from_range(0, 1) == 1);
    bool disable_bloom_filter = (rng.from_range(0, 1) == 1);
    auto exec_ctx = std::make_unique<ExecContext>(
        default_memory_pool(), parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

    // Constraints
    RandomDataTypeConstraints type_constraints;
    type_constraints.Default();
    // type_constraints.OnlyInt(1, true);
    constexpr int max_num_key_fields = 3;
    constexpr int max_num_payload_fields = 3;
    const char* join_type_names[] = {"LEFT_SEMI",   "RIGHT_SEMI", "LEFT_ANTI",
                                     "RIGHT_ANTI",  "INNER",      "LEFT_OUTER",
                                     "RIGHT_OUTER", "FULL_OUTER"};
    std::vector<JoinType> join_type_options{JoinType::LEFT_SEMI,   JoinType::RIGHT_SEMI,
                                            JoinType::LEFT_ANTI,   JoinType::RIGHT_ANTI,
                                            JoinType::INNER,       JoinType::LEFT_OUTER,
                                            JoinType::RIGHT_OUTER, JoinType::FULL_OUTER};
    constexpr int join_type_mask = 0xFF;
    // for INNER join only:
    // constexpr int join_type_mask = 0x10;
    std::vector<JoinKeyCmp> key_cmp_options{JoinKeyCmp::EQ, JoinKeyCmp::IS};
    constexpr int key_cmp_mask = 0x03;
    // for EQ only:
    // constexpr int key_cmp_mask = 0x01;
    constexpr int min_num_rows = 1;
    const int max_num_rows = parallel ? 20000 : 2000;
    constexpr int min_batch_size = 10;
    constexpr int max_batch_size = 100;

    // Generate list of key field data types
    int num_key_fields = rng.from_range(1, max_num_key_fields);
    RandomDataTypeVector key_types;
    for (int i = 0; i < num_key_fields; ++i) {
      key_types.AddRandom(rng, type_constraints);
    }

    // Generate lists of payload data types
    int num_payload_fields[2];
    RandomDataTypeVector payload_types[2];
    for (int i = 0; i < 2; ++i) {
      num_payload_fields[i] = rng.from_range(0, max_num_payload_fields);
      for (int j = 0; j < num_payload_fields[i]; ++j) {
        payload_types[i].AddRandom(rng, type_constraints);
      }
    }

    // Generate join type and comparison functions
    std::vector<JoinKeyCmp> key_cmp(num_key_fields);
    std::string key_cmp_str;
    for (int i = 0; i < num_key_fields; ++i) {
      for (;;) {
        int pos = rng.from_range(0, 1);
        if ((key_cmp_mask & (1 << pos)) > 0) {
          key_cmp[i] = key_cmp_options[pos];
          if (i > 0) {
            key_cmp_str += "_";
          }
          key_cmp_str += key_cmp[i] == JoinKeyCmp::EQ ? "EQ" : "IS";
          break;
        }
      }
    }
    JoinType join_type;
    std::string join_type_name;
    for (;;) {
      int pos = rng.from_range(0, 7);
      if ((join_type_mask & (1 << pos)) > 0) {
        join_type = join_type_options[pos];
        join_type_name = join_type_names[pos];
        break;
      }
    }

    // Generate input records
    int num_rows_l = rng.from_range(min_num_rows, max_num_rows);
    int num_rows_r = rng.from_range(min_num_rows, max_num_rows);
    int num_rows = std::min(num_rows_l, num_rows_r);
    int batch_size = rng.from_range(min_batch_size, max_batch_size);
    int num_keys = rng.from_range(std::max(1, num_rows / 10), num_rows);
    int num_keys_r = rng.from_range(std::max(1, num_keys / 2), num_keys);
    int num_keys_common = rng.from_range(std::max(1, num_keys_r / 2), num_keys_r);
    int num_keys_l = num_keys_common + (num_keys - num_keys_r);
    std::vector<int> key_id_vectors[2];
    std::vector<std::shared_ptr<Array>> input_arrays[2];
    GenRandomJoinTables(exec_ctx.get(), rng, num_rows_l, num_rows_r, num_keys_common,
                        num_keys_l, num_keys_r, key_types, payload_types[0],
                        payload_types[1], &(key_id_vectors[0]), &(key_id_vectors[1]),
                        &(input_arrays[0]), &(input_arrays[1]));
    std::vector<std::shared_ptr<Array>> shuffled_input_arrays[2];
    std::vector<FieldRef> key_fields[2];
    std::vector<FieldRef> output_fields[2];
    std::vector<int> output_field_ids[2];
    for (int i = 0; i < 2; ++i) {
      bool no_output = false;
      if (i == 0) {
        no_output =
            join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI;
      } else {
        no_output = join_type == JoinType::LEFT_SEMI || join_type == JoinType::LEFT_ANTI;
      }
      GenJoinFieldRefs(rng, num_key_fields, no_output, input_arrays[i],
                       std::string((i == 0) ? "l_" : "r_"), &(shuffled_input_arrays[i]),
                       &(key_fields[i]), &(output_fields[i]), &(output_field_ids[i]));
    }

    ARROW_SCOPED_TRACE(join_type_name, " ", key_cmp_str,
                       " parallel = ", (parallel ? "true" : "false"),
                       " bloom_filter = ", (disable_bloom_filter ? "false" : "true"));

    // Run reference join implementation
    std::vector<bool> null_in_key_vectors[2];
    for (int i = 0; i < 2; ++i) {
      null_in_key_vectors[i] = NullInKey(key_cmp, input_arrays[i]);
    }
    int64_t output_length_limit = 100000;
    bool length_limit_reached = false;
    std::shared_ptr<Table> output_rows_ref = HashJoinSimple(
        exec_ctx.get(), join_type, key_cmp, num_key_fields, key_id_vectors[0],
        key_id_vectors[1], input_arrays[0], input_arrays[1], shuffled_input_arrays[0],
        shuffled_input_arrays[1], output_field_ids[0], output_field_ids[1],
        output_length_limit, &length_limit_reached);
    if (length_limit_reached) {
      continue;
    }

    // Turn the last key comparison into a residual filter expression
    Expression filter = literal(true);
    if (key_cmp.size() > 1 && rng.from_range(0, 4) == 0) {
      for (size_t i = 0; i < key_cmp.size(); i++) {
        FieldRef left = key_fields[0][i];
        FieldRef right = key_fields[1][i];

        if (key_cmp[i] == JoinKeyCmp::EQ) {
          key_fields[0].erase(key_fields[0].begin() + i);
          key_fields[1].erase(key_fields[1].begin() + i);
          key_cmp.erase(key_cmp.begin() + i);
          if (right.IsFieldPath()) {
            auto indices = right.field_path()->indices();
            indices[0] += static_cast<int>(shuffled_input_arrays[0].size());
            right = FieldRef{indices};
          }

          Expression left_expr(field_ref(left));
          Expression right_expr(field_ref(right));

          filter = equal(left_expr, right_expr);
          break;
        }
      }
    }

    // Run tested join implementation
    HashJoinNodeOptions join_options{
        join_type,        key_fields[0], key_fields[1], output_fields[0],
        output_fields[1], key_cmp,       filter};
    join_options.disable_bloom_filter = disable_bloom_filter;
    std::vector<std::shared_ptr<Field>> output_schema_fields;
    for (int i = 0; i < 2; ++i) {
      for (size_t col = 0; col < output_fields[i].size(); ++col) {
        output_schema_fields.push_back(std::make_shared<Field>(
            std::string((i == 0) ? "l_" : "r_") + std::to_string(col),
            shuffled_input_arrays[i][output_field_ids[i][col]]->type(), true));
      }
    }
    std::shared_ptr<Schema> output_schema =
        std::make_shared<Schema>(std::move(output_schema_fields));

    ASSERT_OK_AND_ASSIGN(
        auto batches, HashJoinWithExecPlan(
                          rng, parallel, join_options, output_schema,
                          shuffled_input_arrays[0], shuffled_input_arrays[1],
                          static_cast<int>(bit_util::CeilDiv(num_rows_l, batch_size)),
                          static_cast<int>(bit_util::CeilDiv(num_rows_r, batch_size))));

    ASSERT_OK_AND_ASSIGN(auto output_rows_test,
                         TableFromExecBatches(output_schema, batches));

    // Compare results
    AssertTablesEqualIgnoringOrder(output_rows_ref, output_rows_test);
  }
}

void DecodeScalarsAndDictionariesInBatch(ExecBatch* batch, MemoryPool* pool) {
  for (size_t i = 0; i < batch->values.size(); ++i) {
    if (batch->values[i].is_scalar()) {
      ASSERT_OK_AND_ASSIGN(
          std::shared_ptr<Array> col,
          MakeArrayFromScalar(*(batch->values[i].scalar()), batch->length, pool));
      batch->values[i] = Datum(col);
    }
    if (batch->values[i].type()->id() == Type::DICTIONARY) {
      const auto& dict_type =
          checked_cast<const DictionaryType&>(*batch->values[i].type());
      std::shared_ptr<ArrayData> indices =
          ArrayData::Make(dict_type.index_type(), batch->values[i].array()->length,
                          batch->values[i].array()->buffers);
      const std::shared_ptr<ArrayData>& dictionary = batch->values[i].array()->dictionary;
      ASSERT_OK_AND_ASSIGN(Datum col, Take(*dictionary, *indices));
      batch->values[i] = col;
    }
  }
}

std::shared_ptr<Schema> UpdateSchemaAfterDecodingDictionaries(
    const std::shared_ptr<Schema>& schema) {
  std::vector<std::shared_ptr<Field>> output_fields(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) {
    const std::shared_ptr<Field>& field = schema->field(i);
    if (field->type()->id() == Type::DICTIONARY) {
      const auto& dict_type = checked_cast<const DictionaryType&>(*field->type());
      output_fields[i] = std::make_shared<Field>(field->name(), dict_type.value_type(),
                                                 true /* nullable */);
    } else {
      output_fields[i] = field->Copy();
    }
  }
  return std::make_shared<Schema>(std::move(output_fields));
}

void TestHashJoinDictionaryHelper(
    JoinType join_type, JoinKeyCmp cmp,
    // Whether to run parallel hash join.
    // This requires generating multiple copies of each input batch on one side of the
    // join. Expected results will be automatically adjusted to reflect the multiplication
    // of input batches.
    bool parallel, Datum l_key, Datum l_payload, Datum r_key, Datum r_payload,
    Datum l_out_key, Datum l_out_payload, Datum r_out_key, Datum r_out_payload,
    // Number of rows at the end of expected output that represent rows from the right
    // side that do not have a match on the left side. This number is needed to
    // automatically adjust expected result when multiplying input batches on the left
    // side.
    int expected_num_r_no_match,
    // Whether to swap two inputs to the hash join
    bool swap_sides,
    // If true, send length=0 batches, if false, skip these batches
    bool send_empty_batches = true) {
  int64_t l_length = l_key.is_array()       ? l_key.array()->length
                     : l_payload.is_array() ? l_payload.array()->length
                                            : -1;
  int64_t r_length = r_key.is_array()       ? r_key.array()->length
                     : r_payload.is_array() ? r_payload.array()->length
                                            : -1;
  ARROW_DCHECK(l_length >= 0 && r_length >= 0);

  constexpr int batch_multiplicity_for_parallel = 2;

  // Split both sides into exactly two batches
  int64_t l_first_length = l_length / 2;
  int64_t r_first_length = r_length / 2;
  BatchesWithSchema l_batches, r_batches;
  l_batches.batches.resize(2);
  r_batches.batches.resize(2);
  ASSERT_OK_AND_ASSIGN(
      l_batches.batches[0],
      ExecBatch::Make({l_key.is_array() ? l_key.array()->Slice(0, l_first_length) : l_key,
                       l_payload.is_array() ? l_payload.array()->Slice(0, l_first_length)
                                            : l_payload}));
  ASSERT_OK_AND_ASSIGN(
      l_batches.batches[1],
      ExecBatch::Make(
          {l_key.is_array()
               ? l_key.array()->Slice(l_first_length, l_length - l_first_length)
               : l_key,
           l_payload.is_array()
               ? l_payload.array()->Slice(l_first_length, l_length - l_first_length)
               : l_payload}));
  ASSERT_OK_AND_ASSIGN(
      r_batches.batches[0],
      ExecBatch::Make({r_key.is_array() ? r_key.array()->Slice(0, r_first_length) : r_key,
                       r_payload.is_array() ? r_payload.array()->Slice(0, r_first_length)
                                            : r_payload}));
  ASSERT_OK_AND_ASSIGN(
      r_batches.batches[1],
      ExecBatch::Make(
          {r_key.is_array()
               ? r_key.array()->Slice(r_first_length, r_length - r_first_length)
               : r_key,
           r_payload.is_array()
               ? r_payload.array()->Slice(r_first_length, r_length - r_first_length)
               : r_payload}));
  l_batches.schema =
      schema({field("l_key", l_key.type()), field("l_payload", l_payload.type())});
  r_batches.schema =
      schema({field("r_key", r_key.type()), field("r_payload", r_payload.type())});

  // Add copies of input batches on originally left side of the hash join
  if (parallel) {
    for (int i = 0; i < batch_multiplicity_for_parallel - 1; ++i) {
      l_batches.batches.push_back(l_batches.batches[0]);
      l_batches.batches.push_back(l_batches.batches[1]);
    }
  }

  // When the input is empty we can either send length=0 batches
  // or bypass the batches entirely
  if (l_length == 0 && !send_empty_batches) {
    l_batches.batches.resize(0);
  }
  if (r_length == 0 && !send_empty_batches) {
    r_batches.batches.resize(0);
  }

  Declaration left{"source",
                   SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                     /*slow=*/false)}};

  Declaration right{"source",
                    SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                      /*slow=*/false)}};
  HashJoinNodeOptions join_options{join_type,
                                   {FieldRef(swap_sides ? "r_key" : "l_key")},
                                   {FieldRef(swap_sides ? "l_key" : "r_key")},
                                   {FieldRef(swap_sides ? "r_key" : "l_key"),
                                    FieldRef(swap_sides ? "r_payload" : "l_payload")},
                                   {FieldRef(swap_sides ? "l_key" : "r_key"),
                                    FieldRef(swap_sides ? "l_payload" : "r_payload")},
                                   {cmp}};
  Declaration join{
      "hashjoin", {swap_sides ? right : left, swap_sides ? left : right}, join_options};
  ASSERT_OK_AND_ASSIGN(auto res, DeclarationToExecBatches(std::move(join), parallel));

  for (auto& batch : res.batches) {
    DecodeScalarsAndDictionariesInBatch(&batch, default_memory_pool());
  }
  std::shared_ptr<Schema> output_schema =
      UpdateSchemaAfterDecodingDictionaries(res.schema);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> output,
                       TableFromExecBatches(output_schema, res.batches));

  ExecBatch expected_batch;
  if (swap_sides) {
    ASSERT_OK_AND_ASSIGN(expected_batch, ExecBatch::Make({r_out_key, r_out_payload,
                                                          l_out_key, l_out_payload}));
  } else {
    ASSERT_OK_AND_ASSIGN(expected_batch, ExecBatch::Make({l_out_key, l_out_payload,
                                                          r_out_key, r_out_payload}));
  }

  DecodeScalarsAndDictionariesInBatch(&expected_batch, default_memory_pool());

  // Slice expected batch into two to separate rows on right side with no matches from
  // everything else.
  //
  std::vector<ExecBatch> expected_batches;
  ASSERT_OK_AND_ASSIGN(
      auto prefix_batch,
      ExecBatch::Make({expected_batch.values[0].array()->Slice(
                           0, expected_batch.length - expected_num_r_no_match),
                       expected_batch.values[1].array()->Slice(
                           0, expected_batch.length - expected_num_r_no_match),
                       expected_batch.values[2].array()->Slice(
                           0, expected_batch.length - expected_num_r_no_match),
                       expected_batch.values[3].array()->Slice(
                           0, expected_batch.length - expected_num_r_no_match)}));
  for (int i = 0; i < (parallel ? batch_multiplicity_for_parallel : 1); ++i) {
    expected_batches.push_back(prefix_batch);
  }
  if (expected_num_r_no_match > 0) {
    ASSERT_OK_AND_ASSIGN(
        auto suffix_batch,
        ExecBatch::Make({expected_batch.values[0].array()->Slice(
                             expected_batch.length - expected_num_r_no_match,
                             expected_num_r_no_match),
                         expected_batch.values[1].array()->Slice(
                             expected_batch.length - expected_num_r_no_match,
                             expected_num_r_no_match),
                         expected_batch.values[2].array()->Slice(
                             expected_batch.length - expected_num_r_no_match,
                             expected_num_r_no_match),
                         expected_batch.values[3].array()->Slice(
                             expected_batch.length - expected_num_r_no_match,
                             expected_num_r_no_match)}));
    expected_batches.push_back(suffix_batch);
  }

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> expected,
                       TableFromExecBatches(output_schema, expected_batches));

  // Compare results
  AssertTablesEqualIgnoringOrder(expected, output);
}

TEST(HashJoin, Dictionary) {
  auto int8_utf8 = dictionary(int8(), utf8());
  auto uint8_utf8 = arrow::dictionary(uint8(), utf8());
  auto int16_utf8 = arrow::dictionary(int16(), utf8());
  auto uint16_utf8 = arrow::dictionary(uint16(), utf8());
  auto int32_utf8 = arrow::dictionary(int32(), utf8());
  auto uint32_utf8 = arrow::dictionary(uint32(), utf8());
  auto int64_utf8 = arrow::dictionary(int64(), utf8());
  auto uint64_utf8 = arrow::dictionary(uint64(), utf8());
  std::shared_ptr<DataType> dict_types[] = {int8_utf8,   uint8_utf8, int16_utf8,
                                            uint16_utf8, int32_utf8, uint32_utf8,
                                            int64_utf8,  uint64_utf8};

  Random64Bit rng(43);

  // Dictionaries in payload columns
  for (auto parallel : {false, true}) {
    for (auto swap_sides : {false, true}) {
      TestHashJoinDictionaryHelper(
          JoinType::FULL_OUTER, JoinKeyCmp::EQ, parallel,
          // Input
          ArrayFromJSON(utf8(), R"(["a", "c", "c", "d"])"),
          DictArrayFromJSON(int8_utf8, R"([4, 2, 3, 0])",
                            R"(["p", "q", "r", null, "r"])"),
          ArrayFromJSON(utf8(), R"(["a", "a", "b", "c"])"),
          DictArrayFromJSON(int16_utf8, R"([0, 1, 0, 2])", R"(["r", null, "r", "q"])"),
          // Expected output
          ArrayFromJSON(utf8(), R"(["a", "a", "c", "c", "d", null])"),
          DictArrayFromJSON(int8_utf8, R"([4, 4, 2, 3, 0, null])",
                            R"(["p", "q", "r", null, "r"])"),
          ArrayFromJSON(utf8(), R"(["a", "a", "c", "c", null, "b"])"),
          DictArrayFromJSON(int16_utf8, R"([0, 1, 2, 2, null, 0])",
                            R"(["r", null, "r", "q"])"),
          1, swap_sides);
    }
  }

  // Dictionaries in key columns
  for (auto parallel : {false, true}) {
    for (auto swap_sides : {false, true}) {
      for (auto l_key_dict : {true, false}) {
        for (auto r_key_dict : {true, false}) {
          auto l_key_dict_type = dict_types[rng.from_range(0, 7)];
          auto r_key_dict_type = dict_types[rng.from_range(0, 7)];

          auto l_key = l_key_dict ? DictArrayFromJSON(l_key_dict_type, R"([2, 2, 0, 1])",
                                                      R"(["b", null, "a"])")
                                  : ArrayFromJSON(utf8(), R"(["a", "a", "b", null])");
          auto l_payload = ArrayFromJSON(utf8(), R"(["x", "y", "z", "y"])");
          auto r_key = r_key_dict
                           ? DictArrayFromJSON(int16_utf8, R"([1, 0, null, 1, 2])",
                                               R"([null, "b", "c"])")
                           : ArrayFromJSON(utf8(), R"(["b", null, null, "b", "c"])");
          auto r_payload = ArrayFromJSON(utf8(), R"(["p", "r", "p", "q", "s"])");

          // IS comparison function (null is equal to null when matching keys)
          TestHashJoinDictionaryHelper(
              JoinType::FULL_OUTER, JoinKeyCmp::IS, parallel,
              // Input
              l_key, l_payload, r_key, r_payload,
              // Expected
              l_key_dict ? DictArrayFromJSON(l_key_dict_type, R"([2, 2, 0, 0, 1, 1,
            null])",
                                             R"(["b", null, "a"])")
                         : ArrayFromJSON(utf8(), R"(["a", "a", "b", "b", null, null,
                       null])"),
              ArrayFromJSON(utf8(), R"(["x", "y", "z", "z", "y", "y", null])"),
              r_key_dict
                  ? DictArrayFromJSON(r_key_dict_type, R"([null, null, 0, 0, null, null,
                1])",
                                      R"(["b", "c"])")
                  : ArrayFromJSON(utf8(), R"([null, null, "b", "b", null, null, "c"])"),
              ArrayFromJSON(utf8(), R"([null, null, "p", "q", "r", "p", "s"])"), 1,
              swap_sides);

          // EQ comparison function (null is not matching null)
          TestHashJoinDictionaryHelper(
              JoinType::FULL_OUTER, JoinKeyCmp::EQ, parallel,
              // Input
              l_key, l_payload, r_key, r_payload,
              // Expected
              l_key_dict ? DictArrayFromJSON(l_key_dict_type,
                                             R"([2, 2, 0, 0, 1, null, null, null])",
                                             R"(["b", null, "a"])")
                         : ArrayFromJSON(
                               utf8(), R"(["a", "a", "b", "b", null, null, null, null])"),
              ArrayFromJSON(utf8(), R"(["x", "y", "z", "z", "y", null, null, null])"),
              r_key_dict
                  ? DictArrayFromJSON(r_key_dict_type,
                                      R"([null, null, 0, 0, null, null, null, 1])",
                                      R"(["b", "c"])")
                  : ArrayFromJSON(utf8(),
                                  R"([null, null, "b", "b", null, null, null, "c"])"),
              ArrayFromJSON(utf8(), R"([null, null, "p", "q", null, "r", "p", "s"])"), 3,
              swap_sides);
        }
      }
    }
  }

  // Empty build side
  {
    auto l_key_dict_type = dict_types[rng.from_range(0, 7)];
    auto l_payload_dict_type = dict_types[rng.from_range(0, 7)];
    auto r_key_dict_type = dict_types[rng.from_range(0, 7)];
    auto r_payload_dict_type = dict_types[rng.from_range(0, 7)];

    for (auto parallel : {false, true}) {
      for (auto swap_sides : {false, true}) {
        for (auto cmp : {JoinKeyCmp::IS, JoinKeyCmp::EQ}) {
          for (auto send_empty_batches : {false, true}) {
            TestHashJoinDictionaryHelper(
                JoinType::FULL_OUTER, cmp, parallel,
                // Input
                DictArrayFromJSON(l_key_dict_type, R"([2, 0, 1])", R"(["b", null, "a"])"),
                DictArrayFromJSON(l_payload_dict_type, R"([2, 2, 0])",
                                  R"(["x", "y", "z"])"),
                DictArrayFromJSON(r_key_dict_type, R"([])", R"([null, "b", "c"])"),
                DictArrayFromJSON(r_payload_dict_type, R"([])", R"(["p", "r", "s"])"),
                // Expected
                DictArrayFromJSON(l_key_dict_type, R"([2, 0, 1])", R"(["b", null, "a"])"),
                DictArrayFromJSON(l_payload_dict_type, R"([2, 2, 0])",
                                  R"(["x", "y", "z"])"),
                DictArrayFromJSON(r_key_dict_type, R"([null, null, null])",
                                  R"(["b", "c"])"),
                DictArrayFromJSON(r_payload_dict_type, R"([null, null, null])",
                                  R"(["p", "r", "s"])"),
                0, swap_sides, send_empty_batches);
          }
        }
      }
    }
  }

  // Empty probe side
  {
    auto l_key_dict_type = dict_types[rng.from_range(0, 7)];
    auto l_payload_dict_type = dict_types[rng.from_range(0, 7)];
    auto r_key_dict_type = dict_types[rng.from_range(0, 7)];
    auto r_payload_dict_type = dict_types[rng.from_range(0, 7)];

    for (auto parallel : {false, true}) {
      for (auto swap_sides : {false, true}) {
        for (auto cmp : {JoinKeyCmp::IS, JoinKeyCmp::EQ}) {
          for (auto send_empty_batches : {false, true}) {
            TestHashJoinDictionaryHelper(
                JoinType::FULL_OUTER, cmp, parallel,
                // Input
                DictArrayFromJSON(l_key_dict_type, R"([])", R"(["b", null, "a"])"),
                DictArrayFromJSON(l_payload_dict_type, R"([])", R"(["x", "y", "z"])"),
                DictArrayFromJSON(r_key_dict_type, R"([2, 0, 1, null])",
                                  R"([null, "b", "c"])"),
                DictArrayFromJSON(r_payload_dict_type, R"([1, 1, null, 0])",
                                  R"(["p", "r", "s"])"),
                // Expected
                DictArrayFromJSON(l_key_dict_type, R"([null, null, null, null])",
                                  R"(["b", null, "a"])"),
                DictArrayFromJSON(l_payload_dict_type, R"([null, null, null, null])",
                                  R"(["x", "y", "z"])"),
                DictArrayFromJSON(r_key_dict_type, R"([1, null, 0, null])",
                                  R"(["b", "c"])"),
                DictArrayFromJSON(r_payload_dict_type, R"([1, 1, null, 0])",
                                  R"(["p", "r", "s"])"),
                4, swap_sides, send_empty_batches);
          }
        }
      }
    }
  }
}

TEST(HashJoin, Scalars) {
  auto int8_utf8 = std::make_shared<DictionaryType>(int8(), utf8());
  auto int16_utf8 = std::make_shared<DictionaryType>(int16(), utf8());
  auto int32_utf8 = std::make_shared<DictionaryType>(int32(), utf8());

  // Scalars in payload columns
  for (auto use_scalar_dict : {false, true}) {
    TestHashJoinDictionaryHelper(
        JoinType::FULL_OUTER, JoinKeyCmp::EQ, false /*parallel*/,
        // Input
        ArrayFromJSON(utf8(), R"(["a", "c", "c", "d"])"),
        use_scalar_dict ? DictScalarFromJSON(int16_utf8, "1", R"(["z", "x", "y"])")
                        : ScalarFromJSON(utf8(), "\"x\""),
        ArrayFromJSON(utf8(), R"(["a", "a", "b", "c"])"),
        use_scalar_dict ? DictScalarFromJSON(int32_utf8, "0", R"(["z", "x", "y"])")
                        : ScalarFromJSON(utf8(), "\"z\""),
        // Expected output
        ArrayFromJSON(utf8(), R"(["a", "a", "c", "c", "d", null])"),
        ArrayFromJSON(utf8(), R"(["x", "x", "x", "x", "x", null])"),
        ArrayFromJSON(utf8(), R"(["a", "a", "c", "c", null, "b"])"),
        ArrayFromJSON(utf8(), R"(["z", "z", "z", "z", null, "z"])"), 1,
        false /*swap sides*/);
  }

  // Scalars in key columns
  for (auto use_scalar_dict : {false, true}) {
    for (auto swap_sides : {false, true}) {
      TestHashJoinDictionaryHelper(
          JoinType::FULL_OUTER, JoinKeyCmp::EQ, false /*parallel*/,
          // Input
          use_scalar_dict ? DictScalarFromJSON(int8_utf8, "1", R"(["b", "a", "c"])")
                          : ScalarFromJSON(utf8(), "\"a\""),
          ArrayFromJSON(utf8(), R"(["x", "y"])"),
          ArrayFromJSON(utf8(), R"(["a", null, "b"])"),
          ArrayFromJSON(utf8(), R"(["p", "q", "r"])"),
          // Expected output
          ArrayFromJSON(utf8(), R"(["a", "a", null, null])"),
          ArrayFromJSON(utf8(), R"(["x", "y", null, null])"),
          ArrayFromJSON(utf8(), R"(["a", "a", null, "b"])"),
          ArrayFromJSON(utf8(), R"(["p", "p", "q", "r"])"), 2, swap_sides);
    }
  }

  // Null scalars in key columns
  for (auto use_scalar_dict : {false, true}) {
    for (auto swap_sides : {false, true}) {
      TestHashJoinDictionaryHelper(
          JoinType::FULL_OUTER, JoinKeyCmp::EQ, false /*parallel*/,
          // Input
          use_scalar_dict ? DictScalarFromJSON(int16_utf8, "2", R"(["a", "b", null])")
                          : ScalarFromJSON(utf8(), "null"),
          ArrayFromJSON(utf8(), R"(["x", "y"])"),
          ArrayFromJSON(utf8(), R"(["a", null, "b"])"),
          ArrayFromJSON(utf8(), R"(["p", "q", "r"])"),
          // Expected output
          ArrayFromJSON(utf8(), R"([null, null, null, null, null])"),
          ArrayFromJSON(utf8(), R"(["x", "y", null, null, null])"),
          ArrayFromJSON(utf8(), R"([null, null, "a", null, "b"])"),
          ArrayFromJSON(utf8(), R"([null, null, "p", "q", "r"])"), 3, swap_sides);
      TestHashJoinDictionaryHelper(
          JoinType::FULL_OUTER, JoinKeyCmp::IS, false /*parallel*/,
          // Input
          use_scalar_dict ? DictScalarFromJSON(int16_utf8, "null", R"(["a", "b", null])")
                          : ScalarFromJSON(utf8(), "null"),
          ArrayFromJSON(utf8(), R"(["x", "y"])"),
          ArrayFromJSON(utf8(), R"(["a", null, "b"])"),
          ArrayFromJSON(utf8(), R"(["p", "q", "r"])"),
          // Expected output
          ArrayFromJSON(utf8(), R"([null, null, null, null])"),
          ArrayFromJSON(utf8(), R"(["x", "y", null, null])"),
          ArrayFromJSON(utf8(), R"([null, null, "a", "b"])"),
          ArrayFromJSON(utf8(), R"(["q", "q", "p", "r"])"), 2, swap_sides);
    }
  }

  // Scalars with the empty build/probe side
  for (auto use_scalar_dict : {false, true}) {
    for (auto swap_sides : {false, true}) {
      TestHashJoinDictionaryHelper(
          JoinType::FULL_OUTER, JoinKeyCmp::EQ, false /*parallel*/,
          // Input
          use_scalar_dict ? DictScalarFromJSON(int8_utf8, "1", R"(["b", "a", "c"])")
                          : ScalarFromJSON(utf8(), "\"a\""),
          ArrayFromJSON(utf8(), R"(["x", "y"])"), ArrayFromJSON(utf8(), R"([])"),
          ArrayFromJSON(utf8(), R"([])"),
          // Expected output
          ArrayFromJSON(utf8(), R"(["a", "a"])"), ArrayFromJSON(utf8(), R"(["x", "y"])"),
          ArrayFromJSON(utf8(), R"([null, null])"),
          ArrayFromJSON(utf8(), R"([null, null])"), 0, swap_sides);
    }
  }

  // Scalars vs dictionaries in key columns
  for (auto use_scalar_dict : {false, true}) {
    for (auto swap_sides : {false, true}) {
      TestHashJoinDictionaryHelper(
          JoinType::FULL_OUTER, JoinKeyCmp::EQ, false /*parallel*/,
          // Input
          use_scalar_dict ? DictScalarFromJSON(int32_utf8, "1", R"(["b", "a", "c"])")
                          : ScalarFromJSON(utf8(), "\"a\""),
          ArrayFromJSON(utf8(), R"(["x", "y"])"),
          DictArrayFromJSON(int32_utf8, R"([2, 2, 1])", R"(["b", null, "a"])"),
          ArrayFromJSON(utf8(), R"(["p", "q", "r"])"),
          // Expected output
          ArrayFromJSON(utf8(), R"(["a", "a", "a", "a", null])"),
          ArrayFromJSON(utf8(), R"(["x", "x", "y", "y", null])"),
          ArrayFromJSON(utf8(), R"(["a", "a", "a", "a", null])"),
          ArrayFromJSON(utf8(), R"(["p", "q", "p", "q", "r"])"), 1, swap_sides);
    }
  }

  // Scalars in key columns, Inner join to exercise Bloom filter
  for (auto use_scalar_dict : {false, true}) {
    for (auto swap_sides : {false, true}) {
      TestHashJoinDictionaryHelper(
          JoinType::INNER, JoinKeyCmp::EQ, false /*parallel*/,
          // Input
          use_scalar_dict ? DictScalarFromJSON(int8_utf8, "1", R"(["b", "a", "c"])")
                          : ScalarFromJSON(utf8(), "\"a\""),
          ArrayFromJSON(utf8(), R"(["x", "y"])"),
          ArrayFromJSON(utf8(), R"(["a", null, "b"])"),
          ArrayFromJSON(utf8(), R"(["p", "q", "r"])"),
          // Expected output
          ArrayFromJSON(utf8(), R"(["a", "a"])"), ArrayFromJSON(utf8(), R"(["x", "y"])"),
          ArrayFromJSON(utf8(), R"(["a", "a"])"), ArrayFromJSON(utf8(), R"(["p", "p"])"),
          2, swap_sides);
    }
  }
}

TEST(HashJoin, DictNegative) {
  // For dictionary keys, all batches must share a single dictionary.
  // Eventually, differing dictionaries will be unified and indices transposed
  // during encoding to relieve this restriction.
  const auto dictA = ArrayFromJSON(utf8(), R"(["ex", "why", "zee", null])");
  const auto dictB = ArrayFromJSON(utf8(), R"(["different", "dictionary"])");

  Datum datumFirst = Datum(
      *DictionaryArray::FromArrays(ArrayFromJSON(int32(), R"([0, 1, 2, 3])"), dictA));
  Datum datumSecondA = Datum(
      *DictionaryArray::FromArrays(ArrayFromJSON(int32(), R"([3, 2, 2, 3])"), dictA));
  Datum datumSecondB = Datum(
      *DictionaryArray::FromArrays(ArrayFromJSON(int32(), R"([0, 1, 1, 0])"), dictB));

  for (int i = 0; i < 4; ++i) {
    BatchesWithSchema l, r;
    l.schema = schema({field("l_key", dictionary(int32(), utf8())),
                       field("l_payload", dictionary(int32(), utf8()))});
    r.schema = schema({field("r_key", dictionary(int32(), utf8())),
                       field("r_payload", dictionary(int32(), utf8()))});
    l.batches.resize(2);
    r.batches.resize(2);
    ASSERT_OK_AND_ASSIGN(l.batches[0], ExecBatch::Make({datumFirst, datumFirst}));
    ASSERT_OK_AND_ASSIGN(r.batches[0], ExecBatch::Make({datumFirst, datumFirst}));
    ASSERT_OK_AND_ASSIGN(l.batches[1],
                         ExecBatch::Make({i == 0 ? datumSecondB : datumSecondA,
                                          i == 1 ? datumSecondB : datumSecondA}));
    ASSERT_OK_AND_ASSIGN(r.batches[1],
                         ExecBatch::Make({i == 2 ? datumSecondB : datumSecondA,
                                          i == 3 ? datumSecondB : datumSecondA}));

    Declaration left{"source", SourceNodeOptions{l.schema, l.gen(/*parallel=*/false,
                                                                 /*slow=*/false)}};
    Declaration right{"source", SourceNodeOptions{r.schema, r.gen(/*parallel=*/false,
                                                                  /*slow=*/false)}};
    HashJoinNodeOptions join_options{JoinType::INNER,
                                     {FieldRef("l_key")},
                                     {FieldRef("r_key")},
                                     {FieldRef("l_key"), FieldRef("l_payload")},
                                     {FieldRef("r_key"), FieldRef("r_payload")},
                                     {JoinKeyCmp::EQ}};
    Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_options};

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented, ::testing::HasSubstr("Unifying differing dictionaries"),
        DeclarationToTable(std::move(join), /*use_threads=*/false));
  }
}

TEST(HashJoin, UnsupportedTypes) {
  // ARROW-14519
  const bool parallel = false;
  const bool slow = false;

  auto l_schema = schema({field("l_i32", int32()), field("l_list", list(int32()))});
  auto l_schema_nolist = schema({field("l_i32", int32())});
  auto r_schema = schema({field("r_i32", int32()), field("r_list", list(int32()))});
  auto r_schema_nolist = schema({field("r_i32", int32())});

  std::vector<std::pair<std::shared_ptr<Schema>, std::shared_ptr<Schema>>> cases{
      {l_schema, r_schema}, {l_schema_nolist, r_schema}, {l_schema, r_schema_nolist}};
  std::vector<FieldRef> l_keys{{"l_i32"}};
  std::vector<FieldRef> r_keys{{"r_i32"}};

  for (const auto& schemas : cases) {
    BatchesWithSchema l_batches = GenerateBatchesFromString(schemas.first, {R"([])"});
    BatchesWithSchema r_batches = GenerateBatchesFromString(schemas.second, {R"([])"});

    HashJoinNodeOptions join_options{JoinType::LEFT_SEMI, l_keys, r_keys};
    Declaration left{"source",
                     SourceNodeOptions{l_batches.schema, l_batches.gen(parallel, slow)}};
    Declaration right{"source",
                      SourceNodeOptions{r_batches.schema, r_batches.gen(parallel, slow)}};
    Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_options};

    ASSERT_RAISES(Invalid, DeclarationToStatus(std::move(join)));
  }
}

void TestSimpleJoinHelper(BatchesWithSchema input_left, BatchesWithSchema input_right,
                          BatchesWithSchema expected) {
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;

  Declaration left{"source",
                   SourceNodeOptions{input_left.schema, input_left.gen(/*parallel=*/false,
                                                                       /*slow=*/false)}};
  Declaration right{
      "source", SourceNodeOptions{input_right.schema, input_right.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}};

  HashJoinNodeOptions join_opts{JoinType::INNER,
                                /*left_keys=*/{"lkey"},
                                /*right_keys=*/{"rkey"}, literal(true), "_l", "_r"};

  Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_opts};

  ASSERT_OK_AND_ASSIGN(auto result, DeclarationToExecBatches(std::move(join)));

  ASSERT_OK_AND_ASSIGN(auto output_rows_test,
                       TableFromExecBatches(result.schema, result.batches));
  ASSERT_OK_AND_ASSIGN(auto expected_rows_test,
                       TableFromExecBatches(expected.schema, expected.batches));

  AssertTablesEqual(*output_rows_test, *expected_rows_test, /*same_chunk_layout=*/false,
                    /*flatten=*/true);
  AssertSchemaEqual(expected.schema, result.schema);
}

TEST(HashJoin, ExtensionTypesSwissJoin) {
  // For simpler types swiss join will be used.
  auto ext_arr = ExampleUuid();
  auto l_int_arr = ArrayFromJSON(int32(), "[1, 2, 3, 4]");
  auto l_int_arr2 = ArrayFromJSON(int32(), "[4, 5, 6, 7]");
  auto r_int_arr = ArrayFromJSON(int32(), "[4, 3, 2, null, 1]");

  BatchesWithSchema input_left;
  ASSERT_OK_AND_ASSIGN(ExecBatch left_batches,
                       ExecBatch::Make({l_int_arr, l_int_arr2, ext_arr}));
  input_left.batches = {left_batches};
  input_left.schema = schema(
      {field("lkey", int32()), field("shared", int32()), field("ldistinct", uuid())});

  BatchesWithSchema input_right;
  ASSERT_OK_AND_ASSIGN(ExecBatch right_batches, ExecBatch::Make({r_int_arr}));
  input_right.batches = {right_batches};
  input_right.schema = schema({field("rkey", int32())});

  BatchesWithSchema expected;
  ASSERT_OK_AND_ASSIGN(ExecBatch expected_batches,
                       ExecBatch::Make({l_int_arr, l_int_arr2, ext_arr, l_int_arr}));
  expected.batches = {expected_batches};
  expected.schema = schema({field("lkey", int32()), field("shared", int32()),
                            field("ldistinct", uuid()), field("rkey", int32())});

  TestSimpleJoinHelper(input_left, input_right, expected);
}

TEST(HashJoin, ExtensionTypesHashJoin) {
  // Swiss join doesn't support dictionaries so HashJoin will be used.
  auto dict_type = dictionary(int64(), int8());
  auto ext_arr = ExampleUuid();
  auto l_int_arr = ArrayFromJSON(int32(), "[1, 2, 3, 4]");
  auto l_int_arr2 = ArrayFromJSON(int32(), "[4, 5, 6, 7]");
  auto r_int_arr = ArrayFromJSON(int32(), "[4, 3, 2, null, 1]");
  auto l_dict_array =
      DictArrayFromJSON(dict_type, R"([2, 0, 1, null])", R"([null, 0, 1])");

  BatchesWithSchema input_left;
  ASSERT_OK_AND_ASSIGN(ExecBatch left_batches,
                       ExecBatch::Make({l_int_arr, l_int_arr2, ext_arr, l_dict_array}));
  input_left.batches = {left_batches};
  input_left.schema = schema({field("lkey", int32()), field("shared", int32()),
                              field("ldistinct", uuid()), field("dict_type", dict_type)});

  BatchesWithSchema input_right;
  ASSERT_OK_AND_ASSIGN(ExecBatch right_batches, ExecBatch::Make({r_int_arr}));
  input_right.batches = {right_batches};
  input_right.schema = schema({field("rkey", int32())});

  BatchesWithSchema expected;
  ASSERT_OK_AND_ASSIGN(
      ExecBatch expected_batches,
      ExecBatch::Make({l_int_arr, l_int_arr2, ext_arr, l_dict_array, l_int_arr}));
  expected.batches = {expected_batches};
  expected.schema = schema({field("lkey", int32()), field("shared", int32()),
                            field("ldistinct", uuid()), field("dict_type", dict_type),
                            field("rkey", int32())});

  TestSimpleJoinHelper(input_left, input_right, expected);
}

TEST(HashJoin, CheckHashJoinNodeOptionsValidation) {
  BatchesWithSchema input_left;
  input_left.batches = {ExecBatchFromJSON({int32(), int32(), int32()}, R"([
                   [1, 4, 7],
                   [2, 5, 8],
                   [3, 6, 9]
                 ])")};
  input_left.schema = schema(
      {field("lkey", int32()), field("shared", int32()), field("ldistinct", int32())});

  BatchesWithSchema input_right;
  input_right.batches = {ExecBatchFromJSON({int32(), int32(), int32()}, R"([
                   [1, 10, 13],
                   [2, 11, 14],
                   [3, 12, 15]
                 ])")};
  input_right.schema = schema(
      {field("rkey", int32()), field("shared", int32()), field("rdistinct", int32())});

  Declaration left{"source",
                   SourceNodeOptions{input_left.schema, input_left.gen(/*parallel=*/false,
                                                                       /*slow=*/false)}};
  Declaration right{
      "source", SourceNodeOptions{input_right.schema, input_right.gen(/*parallel=*/false,
                                                                      /*slow=*/false)}};

  std::vector<std::vector<FieldRef>> l_keys = {
      {},
      {FieldRef("lkey")},
      {FieldRef("lkey"), FieldRef("shared"), FieldRef("ldistinct")}};
  std::vector<std::vector<FieldRef>> r_keys = {
      {},
      {FieldRef("rkey")},
      {FieldRef("rkey"), FieldRef("shared"), FieldRef("rdistinct")}};
  std::vector<std::vector<JoinKeyCmp>> key_cmps = {
      {}, {JoinKeyCmp::EQ}, {JoinKeyCmp::EQ, JoinKeyCmp::EQ, JoinKeyCmp::EQ}};

  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 3; ++j) {
      for (int k = 0; k < 3; ++k) {
        if (i == j && j == k && i != 0) {
          continue;
        }

        HashJoinNodeOptions options{JoinType::INNER, l_keys[j], r_keys[k], {}, {},
                                    key_cmps[i]};
        Declaration join{"hashjoin", {left, right}, options};
        EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("key_cmp and keys"),
                                        DeclarationToStatus(std::move(join)));
      }
    }
  }
}

TEST(HashJoin, ResidualFilter) {
  for (bool parallel : {false, true}) {
    SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

    BatchesWithSchema input_left;
    input_left.batches = {ExecBatchFromJSON({int32(), int32(), utf8()}, R"([
                   [1, 6, "alpha"],
                   [2, 5, "beta"],
                   [3, 4, "alpha"]
                 ])")};
    input_left.schema =
        schema({field("l1", int32()), field("l2", int32()), field("l_str", utf8())});

    BatchesWithSchema input_right;
    input_right.batches = {ExecBatchFromJSON({int32(), int32(), utf8()}, R"([
                   [5, 11, "alpha"],
                   [2, 12, "beta"],
                   [4, 16, "alpha"]
                 ])")};
    input_right.schema =
        schema({field("r1", int32()), field("r2", int32()), field("r_str", utf8())});

    Declaration left{
        "source",
        SourceNodeOptions{input_left.schema, input_left.gen(parallel, /*slow=*/false)}};
    Declaration right{
        "source",
        SourceNodeOptions{input_right.schema, input_right.gen(parallel, /*slow=*/false)}};

    Expression mul = call("multiply", {field_ref("l1"), field_ref("l2")});
    Expression combination = call("add", {mul, field_ref("r1")});
    Expression residual_filter = less_equal(combination, field_ref("r2"));

    HashJoinNodeOptions join_opts{
        JoinType::FULL_OUTER,
        /*left_keys=*/{"l_str"},
        /*right_keys=*/{"r_str"}, std::move(residual_filter), "l_", "r_"};

    Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_opts};

    ASSERT_OK_AND_ASSIGN(auto result,
                         DeclarationToExecBatches(std::move(join), parallel));

    std::vector<ExecBatch> expected = {
        ExecBatchFromJSON({int32(), int32(), utf8(), int32(), int32(), utf8()}, R"([
            [1, 6, "alpha", 4, 16, "alpha"],
            [1, 6, "alpha", 5, 11, "alpha"],
            [2, 5, "beta", 2, 12, "beta"],
            [3, 4, "alpha", 4, 16, "alpha"]])")};

    AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, expected);
  }
}

TEST(HashJoin, TrivialResidualFilter) {
  Expression always_true =
      equal(call("add", {field_ref("l1"), field_ref("r1")}), literal(2));  // 1 + 1 == 2
  Expression always_false =
      equal(call("add", {field_ref("l1"), field_ref("r1")}), literal(3));  // 1 + 1 == 3

  std::string expected_true = R"([[1, "alpha", 1, "alpha"]])";
  std::string expected_false = R"([])";

  std::vector<std::string> expected_strings = {expected_true, expected_false};
  std::vector<Expression> filters = {always_true, always_false};

  for (size_t test_id = 0; test_id < 2; test_id++) {
    for (bool parallel : {false, true}) {
      SCOPED_TRACE(parallel ? "parallel/merged" : "serial");

      BatchesWithSchema input_left;
      input_left.batches = {ExecBatchFromJSON({int32(), utf8()}, R"([
                   [1, "alpha"]
                 ])")};
      input_left.schema = schema({field("l1", int32()), field("l_str", utf8())});

      BatchesWithSchema input_right;
      input_right.batches = {ExecBatchFromJSON({int32(), utf8()}, R"([
                   [1, "alpha"]
                 ])")};
      input_right.schema = schema({field("r1", int32()), field("r_str", utf8())});

      auto exec_ctx = std::make_unique<ExecContext>(
          default_memory_pool(),
          parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

      Declaration left{
          "source",
          SourceNodeOptions{input_left.schema, input_left.gen(parallel, /*slow=*/false)}};
      Declaration right{"source",
                        SourceNodeOptions{input_right.schema,
                                          input_right.gen(parallel, /*slow=*/false)}};

      HashJoinNodeOptions join_opts{
          JoinType::INNER,
          /*left_keys=*/{"l_str"},
          /*right_keys=*/{"r_str"}, filters[test_id], "l_", "r_"};

      Declaration join{"hashjoin", {std::move(left), std::move(right)}, join_opts};

      ASSERT_OK_AND_ASSIGN(auto result,
                           DeclarationToExecBatches(std::move(join), parallel));

      std::vector<ExecBatch> expected = {ExecBatchFromJSON(
          {int32(), utf8(), int32(), utf8()}, expected_strings[test_id])};

      AssertExecBatchesEqualIgnoringOrder(result.schema, result.batches, expected);
    }
  }
}

HashJoinNodeOptions GenerateHashJoinNodeOptions(Random64Bit& rng, int num_left_cols,
                                                int num_right_cols) {
  HashJoinNodeOptions opts;
  opts.join_type = static_cast<JoinType>(rng.from_range(0, 7));
  bool is_left_join = opts.join_type == JoinType::LEFT_SEMI ||
                      opts.join_type == JoinType::LEFT_ANTI ||
                      opts.join_type == JoinType::LEFT_OUTER;
  bool is_right_join = opts.join_type == JoinType::RIGHT_SEMI ||
                       opts.join_type == JoinType::RIGHT_ANTI ||
                       opts.join_type == JoinType::RIGHT_OUTER;

  int num_keys = rng.from_range(1, std::min(num_left_cols, num_right_cols));
  for (int i = 0; i < num_left_cols; i++) {
    bool is_out = rng.from_range(0, 2) != 2;
    if (is_out && !is_right_join) opts.left_output.push_back(FieldRef(i));
  }
  for (int i = 0; i < num_right_cols; i++) {
    bool is_out = rng.from_range(0, 2) == 2;
    if (is_out && !is_left_join) opts.right_output.push_back(FieldRef(i));
  }
  // We need at least one output
  if (opts.right_output.empty() && opts.left_output.empty()) {
    if (is_left_join) {
      int col = rng.from_range(0, num_left_cols - 1);
      opts.left_output.push_back(FieldRef(col));
    } else if (is_right_join) {
      int col = rng.from_range(0, num_right_cols - 1);
      opts.right_output.push_back(FieldRef(col));
    } else {
      if (rng.from_range(0, 1) == 0) {
        int col = rng.from_range(0, num_left_cols - 1);
        opts.left_output.push_back(FieldRef(col));
      } else {
        int col = rng.from_range(0, num_right_cols - 1);
        opts.right_output.push_back(FieldRef(col));
      }
    }
  }

  for (int i = 0; i < num_keys; i++) {
    int left = rng.from_range(0, num_left_cols - 1);
    int right = rng.from_range(0, num_right_cols - 1);
    bool is_or_eq = rng.from_range(0, 1) == 0;
    opts.left_keys.push_back(FieldRef(left));
    opts.right_keys.push_back(FieldRef(right));
    opts.key_cmp.push_back(is_or_eq ? JoinKeyCmp::IS : JoinKeyCmp::EQ);
  }
  return opts;
}

void TestSingleChainOfHashJoins(Random64Bit& rng) {
  int num_joins = rng.from_range(2, 5);
  std::vector<HashJoinNodeOptions> opts;
  int num_left_cols = rng.from_range(1, 8);
  int num_right_cols = rng.from_range(1, 8);
  HashJoinNodeOptions first_opt =
      GenerateHashJoinNodeOptions(rng, num_left_cols, num_right_cols);
  opts.push_back(std::move(first_opt));

  std::unordered_map<std::string, std::string> metadata_map;
  metadata_map["min"] = "0";
  metadata_map["max"] = "10";
  auto metadata = key_value_metadata(metadata_map);
  std::vector<std::shared_ptr<Field>> left_fields;
  for (int i = 0; i < num_left_cols; i++)
    left_fields.push_back(field(std::string("l") + std::to_string(i), int32(), metadata));
  std::vector<std::shared_ptr<Field>> first_right_fields;
  for (int i = 0; i < num_right_cols; i++)
    first_right_fields.push_back(
        field(std::string("r_0_") + std::to_string(i), int32(), metadata));

  BatchesWithSchema input_left = MakeRandomBatches(schema(std::move(left_fields)));
  std::vector<BatchesWithSchema> input_right;
  input_right.push_back(MakeRandomBatches(schema(std::move(first_right_fields))));

  for (int i = 1; i < num_joins; i++) {
    int num_right_cols = rng.from_range(1, 8);
    HashJoinNodeOptions opt =
        GenerateHashJoinNodeOptions(rng,
                                    static_cast<int>(opts[i - 1].left_output.size() +
                                                     opts[i - 1].right_output.size()),
                                    num_right_cols);
    opts.push_back(std::move(opt));

    std::vector<std::shared_ptr<Field>> right_fields;
    for (int j = 0; j < num_right_cols; j++)
      right_fields.push_back(
          field(std::string("r_") + std::to_string(i) + "_" + std::to_string(j), int32(),
                metadata));
    BatchesWithSchema input = MakeRandomBatches(schema(std::move(right_fields)));
    input_right.push_back(std::move(input));
  }

  std::vector<ExecBatch> reference;
  for (bool bloom_filters : {false, true}) {
    bool kParallel = true;
    ARROW_SCOPED_TRACE(bloom_filters ? "bloom filtered" : "unfiltered");

    Declaration left{
        "source",
        SourceNodeOptions{input_left.schema, input_left.gen(kParallel, /*slow=*/false)}};

    Declaration last_join;
    for (int i = 0; i < num_joins; i++) {
      opts[i].disable_bloom_filter = !bloom_filters;
      Declaration right{"source",
                        SourceNodeOptions{input_right[i].schema,
                                          input_right[i].gen(kParallel, /*slow=*/false)}};

      std::vector<Declaration::Input> inputs;
      if (i == 0)
        inputs = {std::move(left), std::move(right)};
      else
        inputs = {std::move(last_join), std::move(right)};
      last_join = Declaration{"hashjoin", std::move(inputs), opts[i]};
    }

    ASSERT_OK_AND_ASSIGN(auto result,
                         DeclarationToExecBatches(std::move(last_join), kParallel));
    if (!bloom_filters)
      reference = std::move(result.batches);
    else
      AssertExecBatchesEqualIgnoringOrder(result.schema, reference, result.batches);
  }
}

TEST(HashJoin, ChainedIntegerHashJoins) {
  Random64Bit rng(42);
#ifdef ARROW_VALGRIND
  constexpr int kNumTests = 3;
#else
  constexpr int kNumTests = 30;
#endif
  for (int i = 0; i < kNumTests; i++) {
    ARROW_SCOPED_TRACE("Test ", std::to_string(i));
    TestSingleChainOfHashJoins(rng);
  }
}

}  // namespace acero
}  // namespace arrow

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

#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <unordered_set>

#include "arrow/api.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/string_view.h"
#include "arrow/util/thread_pool.h"

#define TRACED_TEST(t_class, t_name)          \
  static void _##t_class##_##t_name();        \
  TEST(t_class, t_name) {                     \
    ARROW_SCOPED_TRACE(#t_class "_" #t_name); \
    _##t_class##_##t_name();                  \
  }                                           \
  static void _##t_class##_##t_name()

using testing::UnorderedElementsAreArray;

namespace arrow {
namespace compute {

bool is_temporal_primitive(Type::type type_id) {
  switch (type_id) {
    case Type::TIME32:
    case Type::TIME64:
    case Type::DATE32:
    case Type::DATE64:
    case Type::TIMESTAMP:
      return true;
    default:
      return false;
  }
}

BatchesWithSchema MakeBatchesFromNumString(
    const std::shared_ptr<Schema>& schema,
    const std::vector<util::string_view>& json_strings, int multiplicity = 1) {
  FieldVector num_fields;
  for (auto field : schema->fields()) {
    num_fields.push_back(
        is_base_binary_like(field->type()->id()) ? field->WithType(int64()) : field);
  }
  auto num_schema =
      std::make_shared<Schema>(num_fields, schema->endianness(), schema->metadata());
  BatchesWithSchema num_batches =
      MakeBatchesFromString(num_schema, json_strings, multiplicity);
  BatchesWithSchema batches;
  batches.schema = schema;
  int n_fields = schema->num_fields();
  for (auto num_batch : num_batches.batches) {
    std::vector<Datum> values;
    for (int i = 0; i < n_fields; i++) {
      auto type = schema->field(i)->type();
      if (is_base_binary_like(type->id())) {
        // casting to string first enables casting to binary
        Datum as_string = Cast(num_batch.values[i], utf8()).ValueOrDie();
        values.push_back(Cast(as_string, type).ValueOrDie());
      } else {
        values.push_back(num_batch.values[i]);
      }
    }
    ExecBatch batch(values, num_batch.length);
    batches.batches.push_back(batch);
  }
  return batches;
}

void BuildNullArray(std::shared_ptr<Array>& empty, const std::shared_ptr<DataType>& type,
                    int64_t length) {
  ASSERT_OK_AND_ASSIGN(auto builder, MakeBuilder(type, default_memory_pool()));
  ASSERT_OK(builder->Reserve(length));
  ASSERT_OK(builder->AppendNulls(length));
  ASSERT_OK(builder->Finish(&empty));
}

void BuildZeroPrimitiveArray(std::shared_ptr<Array>& empty,
                             const std::shared_ptr<DataType>& type, int64_t length) {
  ASSERT_OK_AND_ASSIGN(auto builder, MakeBuilder(type, default_memory_pool()));
  ASSERT_OK(builder->Reserve(length));
  ASSERT_OK_AND_ASSIGN(auto scalar, MakeScalar(type, 0));
  ASSERT_OK(builder->AppendScalar(*scalar, length));
  ASSERT_OK(builder->Finish(&empty));
}

template <typename Builder>
void BuildZeroBaseBinaryArray(std::shared_ptr<Array>& empty, int64_t length) {
  Builder builder(default_memory_pool());
  ASSERT_OK(builder.Reserve(length));
  for (int64_t i = 0; i < length; i++) {
    ASSERT_OK(builder.Append("0", /*length=*/1));
  }
  ASSERT_OK(builder.Finish(&empty));
}

// mutates by copying from_key into to_key and changing from_key to zero
BatchesWithSchema MutateByKey(BatchesWithSchema& batches, std::string from_key,
                              std::string to_key, bool replace_key = false,
                              bool null_key = false) {
  int from_index = batches.schema->GetFieldIndex(from_key);
  int n_fields = batches.schema->num_fields();
  auto fields = batches.schema->fields();
  BatchesWithSchema new_batches;
  auto new_field = batches.schema->field(from_index)->WithName(to_key);
  new_batches.schema = (replace_key ? batches.schema->SetField(from_index, new_field)
                                    : batches.schema->AddField(from_index, new_field))
                           .ValueOrDie();
  for (const ExecBatch& batch : batches.batches) {
    std::vector<Datum> new_values;
    for (int i = 0; i < n_fields; i++) {
      const Datum& value = batch.values[i];
      if (i == from_index) {
        auto type = fields[i]->type();
        if (null_key) {
          std::shared_ptr<Array> empty;
          BuildNullArray(empty, type, batch.length);
          new_values.push_back(empty);
        } else if (is_primitive(type->id())) {
          std::shared_ptr<Array> empty;
          BuildZeroPrimitiveArray(empty, type, batch.length);
          new_values.push_back(empty);
        } else if (is_base_binary_like(type->id())) {
          std::shared_ptr<Array> empty;
          switch (type->id()) {
            case Type::STRING:
              BuildZeroBaseBinaryArray<StringBuilder>(empty, batch.length);
              break;
            case Type::LARGE_STRING:
              BuildZeroBaseBinaryArray<LargeStringBuilder>(empty, batch.length);
              break;
            case Type::BINARY:
              BuildZeroBaseBinaryArray<BinaryBuilder>(empty, batch.length);
              break;
            case Type::LARGE_BINARY:
              BuildZeroBaseBinaryArray<LargeBinaryBuilder>(empty, batch.length);
              break;
            default:
              DCHECK(false);
              break;
          }
          new_values.push_back(empty);
        } else {
          new_values.push_back(Subtract(value, value).ValueOrDie());
        }
        if (replace_key) {
          continue;
        }
      }
      new_values.push_back(value);
    }
    new_batches.batches.emplace_back(new_values, batch.length);
  }
  return new_batches;
}

// code generation for the by_key types supported by AsofJoinNodeOptions constructors
// which cannot be directly done using templates because of failure to deduce the template
// argument for an invocation with a string- or initializer_list-typed keys-argument
#define EXPAND_BY_KEY_TYPE(macro) \
  macro(const FieldRef);          \
  macro(std::vector<FieldRef>);   \
  macro(std::initializer_list<FieldRef>);

void CheckRunOutput(const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r0_batches,
                    const BatchesWithSchema& r1_batches,
                    const BatchesWithSchema& exp_batches,
                    const AsofJoinNodeOptions join_options) {
  auto exec_ctx =
      arrow::internal::make_unique<ExecContext>(default_memory_pool(), nullptr);
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(exec_ctx.get()));

  Declaration join{"asofjoin", join_options};

  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r0_batches.schema, r0_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r1_batches.schema, r1_batches.gen(false, false)}});

  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  ASSERT_FINISHES_OK_AND_ASSIGN(auto res, StartAndCollect(plan.get(), sink_gen));

  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  ASSERT_OK_AND_ASSIGN(auto res_table, TableFromExecBatches(exp_batches.schema, res));

  AssertTablesEqual(*exp_table, *res_table,
                    /*same_chunk_layout=*/true, /*flatten=*/true);
}

#define CHECK_RUN_OUTPUT(by_key_type)                                            \
  void CheckRunOutput(                                                           \
      const BatchesWithSchema& l_batches, const BatchesWithSchema& r0_batches,   \
      const BatchesWithSchema& r1_batches, const BatchesWithSchema& exp_batches, \
      const FieldRef time, by_key_type keys, const int64_t tolerance) {          \
    CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches,               \
                   AsofJoinNodeOptions(time, keys, tolerance));                  \
  }

EXPAND_BY_KEY_TYPE(CHECK_RUN_OUTPUT)

void DoInvalidPlanTest(const BatchesWithSchema& l_batches,
                       const BatchesWithSchema& r_batches,
                       const AsofJoinNodeOptions& join_options,
                       const std::string& expected_error_str,
                       bool then_run_plan = false) {
  ExecContext exec_ctx;
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(&exec_ctx));

  Declaration join{"asofjoin", join_options};
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(false, false)}});

  if (then_run_plan) {
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                  .AddToPlan(plan.get()));
    EXPECT_FINISHES_AND_RAISES_WITH_MESSAGE_THAT(Invalid,
                                                 ::testing::HasSubstr(expected_error_str),
                                                 StartAndCollect(plan.get(), sink_gen));
  } else {
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(expected_error_str),
                                    join.AddToPlan(plan.get()));
  }
}

void DoRunInvalidPlanTest(const BatchesWithSchema& l_batches,
                          const BatchesWithSchema& r_batches,
                          const AsofJoinNodeOptions& join_options,
                          const std::string& expected_error_str) {
  DoInvalidPlanTest(l_batches, r_batches, join_options, expected_error_str);
}

void DoRunInvalidPlanTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema,
                          const AsofJoinNodeOptions& join_options,
                          const std::string& expected_error_str) {
  BatchesWithSchema l_batches = MakeBatchesFromNumString(l_schema, {R"([])"});
  BatchesWithSchema r_batches = MakeBatchesFromNumString(r_schema, {R"([])"});

  return DoRunInvalidPlanTest(l_batches, r_batches, join_options, expected_error_str);
}

void DoRunInvalidPlanTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema, int64_t tolerance,
                          const std::string& expected_error_str) {
  DoRunInvalidPlanTest(l_schema, r_schema, AsofJoinNodeOptions("time", "key", tolerance),
                       expected_error_str);
}

void DoRunInvalidTypeTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, 0, "Unsupported type for ");
}

void DoRunInvalidToleranceTest(const std::shared_ptr<Schema>& l_schema,
                               const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, -1,
                       "AsOfJoin tolerance must be non-negative but is ");
}

void DoRunMissingKeysTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, 0, "Bad join key on table : No match");
}

void DoRunEmptyByKeyTest(const std::shared_ptr<Schema>& l_schema,
                         const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, AsofJoinNodeOptions("time", {}, 0),
                       "AsOfJoin by_key must not be empty");
}

void DoRunMissingOnKeyTest(const std::shared_ptr<Schema>& l_schema,
                           const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, AsofJoinNodeOptions("invalid_time", "key", 0),
                       "Bad join key on table : No match");
}

void DoRunMissingByKeyTest(const std::shared_ptr<Schema>& l_schema,
                           const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, AsofJoinNodeOptions("time", "invalid_key", 0),
                       "Bad join key on table : No match");
}

void DoRunNestedOnKeyTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, AsofJoinNodeOptions({0, "time"}, "key", 0),
                       "Bad join key on table : No match");
}

void DoRunNestedByKeyTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, AsofJoinNodeOptions("time", FieldRef{0, 1}, 0),
                       "Bad join key on table : No match");
}

void DoRunAmbiguousOnKeyTest(const std::shared_ptr<Schema>& l_schema,
                             const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, 0, "Bad join key on table : Multiple matches");
}

void DoRunAmbiguousByKeyTest(const std::shared_ptr<Schema>& l_schema,
                             const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, 0, "Bad join key on table : Multiple matches");
}

std::string GetJsonString(int n_rows, int n_cols, bool unordered = false) {
  std::stringstream s;
  s << '[';
  for (int i = 0; i < n_rows; i++) {
    if (i > 0) {
      s << ", ";
    }
    s << '[';
    for (int j = 0; j < n_cols; j++) {
      if (j > 0) {
        s << ", " << j;
      } else if (j < 2) {
        s << (i ^ unordered);
      } else {
        s << i;
      }
    }
    s << ']';
  }
  s << ']';
  return s.str();
}

void DoRunUnorderedPlanTest(bool l_unordered, bool r_unordered,
                            const std::shared_ptr<Schema>& l_schema,
                            const std::shared_ptr<Schema>& r_schema,
                            const AsofJoinNodeOptions& join_options,
                            const std::string& expected_error_str) {
  ASSERT_TRUE(l_unordered || r_unordered);
  int n_rows = 5;
  std::string l_str = GetJsonString(n_rows, l_schema->num_fields(), l_unordered);
  std::string r_str = GetJsonString(n_rows, r_schema->num_fields(), r_unordered);
  BatchesWithSchema l_batches = MakeBatchesFromNumString(l_schema, {l_str});
  BatchesWithSchema r_batches = MakeBatchesFromNumString(r_schema, {r_str});

  return DoInvalidPlanTest(l_batches, r_batches, join_options, expected_error_str,
                           /*then_run_plan=*/true);
}

void DoRunUnorderedPlanTest(bool l_unordered, bool r_unordered,
                            const std::shared_ptr<Schema>& l_schema,
                            const std::shared_ptr<Schema>& r_schema) {
  DoRunUnorderedPlanTest(l_unordered, r_unordered, l_schema, r_schema,
                         AsofJoinNodeOptions("time", "key", 1000),
                         "out-of-order on-key values");
}

void DoRunNullByKeyPlanTest(const std::shared_ptr<Schema>& l_schema,
                            const std::shared_ptr<Schema>& r_schema) {
  AsofJoinNodeOptions join_options{"time", "key2", 1000};
  std::string expected_error_str = "unexpected null by-key values";
  int n_rows = 5;
  std::string l_str = GetJsonString(n_rows, l_schema->num_fields());
  std::string r_str = GetJsonString(n_rows, r_schema->num_fields());
  BatchesWithSchema l_batches = MakeBatchesFromNumString(l_schema, {l_str});
  BatchesWithSchema r_batches = MakeBatchesFromNumString(r_schema, {r_str});
  l_batches = MutateByKey(l_batches, "key", "key2", true, true);
  r_batches = MutateByKey(r_batches, "key", "key2", true, true);

  return DoInvalidPlanTest(l_batches, r_batches, join_options, expected_error_str,
                           /*then_run_plan=*/true);
}

struct BasicTestTypes {
  std::shared_ptr<DataType> time, key, l_val, r0_val, r1_val;
};

struct BasicTest {
  BasicTest(const std::vector<util::string_view>& l_data,
            const std::vector<util::string_view>& r0_data,
            const std::vector<util::string_view>& r1_data,
            const std::vector<util::string_view>& exp_nokey_data,
            const std::vector<util::string_view>& exp_data, int64_t tolerance)
      : l_data(std::move(l_data)),
        r0_data(std::move(r0_data)),
        r1_data(std::move(r1_data)),
        exp_nokey_data(std::move(exp_nokey_data)),
        exp_data(std::move(exp_data)),
        tolerance(tolerance) {}

  template <typename TypeCond>
  static inline void init_types(const std::vector<std::shared_ptr<DataType>>& all_types,
                                std::vector<std::shared_ptr<DataType>>& types,
                                TypeCond type_cond) {
    if (types.size() == 0) {
      for (auto type : all_types) {
        if (type_cond(type)) {
          types.push_back(type);
        }
      }
    }
  }

  void RunSingleByKey(std::vector<std::shared_ptr<DataType>> time_types = {},
                      std::vector<std::shared_ptr<DataType>> key_types = {},
                      std::vector<std::shared_ptr<DataType>> l_types = {},
                      std::vector<std::shared_ptr<DataType>> r0_types = {},
                      std::vector<std::shared_ptr<DataType>> r1_types = {}) {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_batches) {
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches, "time", "key",
                     tolerance);
    });
  }
  void RunDoubleByKey(std::vector<std::shared_ptr<DataType>> time_types = {},
                      std::vector<std::shared_ptr<DataType>> key_types = {},
                      std::vector<std::shared_ptr<DataType>> l_types = {},
                      std::vector<std::shared_ptr<DataType>> r0_types = {},
                      std::vector<std::shared_ptr<DataType>> r1_types = {}) {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_batches) {
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches, "time",
                     {"key", "key"}, tolerance);
    });
  }
  void RunMutateByKey(std::vector<std::shared_ptr<DataType>> time_types = {},
                      std::vector<std::shared_ptr<DataType>> key_types = {},
                      std::vector<std::shared_ptr<DataType>> l_types = {},
                      std::vector<std::shared_ptr<DataType>> r0_types = {},
                      std::vector<std::shared_ptr<DataType>> r1_types = {}) {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_batches) {
      l_batches = MutateByKey(l_batches, "key", "key2");
      r0_batches = MutateByKey(r0_batches, "key", "key2");
      r1_batches = MutateByKey(r1_batches, "key", "key2");
      exp_batches = MutateByKey(exp_batches, "key", "key2");
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches, "time",
                     {"key", "key2"}, tolerance);
    });
  }
  void RunMutateNoKey(std::vector<std::shared_ptr<DataType>> time_types = {},
                      std::vector<std::shared_ptr<DataType>> key_types = {},
                      std::vector<std::shared_ptr<DataType>> l_types = {},
                      std::vector<std::shared_ptr<DataType>> r0_types = {},
                      std::vector<std::shared_ptr<DataType>> r1_types = {}) {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_batches) {
      l_batches = MutateByKey(l_batches, "key", "key2", true);
      r0_batches = MutateByKey(r0_batches, "key", "key2", true);
      r1_batches = MutateByKey(r1_batches, "key", "key2", true);
      exp_nokey_batches = MutateByKey(exp_nokey_batches, "key", "key2", true);
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_nokey_batches, "time", "key2",
                     tolerance);
    });
  }
  void RunMutateNullKey(std::vector<std::shared_ptr<DataType>> time_types = {},
                        std::vector<std::shared_ptr<DataType>> key_types = {},
                        std::vector<std::shared_ptr<DataType>> l_types = {},
                        std::vector<std::shared_ptr<DataType>> r0_types = {},
                        std::vector<std::shared_ptr<DataType>> r1_types = {}) {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_batches) {
      l_batches = MutateByKey(l_batches, "key", "key2", true, true);
      r0_batches = MutateByKey(r0_batches, "key", "key2", true, true);
      r1_batches = MutateByKey(r1_batches, "key", "key2", true, true);
      exp_nokey_batches = MutateByKey(exp_nokey_batches, "key", "key2", true, true);
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_nokey_batches,
                     AsofJoinNodeOptions("time", "key2", tolerance,
                                         /*nullable_by_key=*/true));
    });
  }
  template <typename BatchesRunner>
  void RunBatches(BatchesRunner batches_runner,
                  std::vector<std::shared_ptr<DataType>> time_types = {},
                  std::vector<std::shared_ptr<DataType>> key_types = {},
                  std::vector<std::shared_ptr<DataType>> l_types = {},
                  std::vector<std::shared_ptr<DataType>> r0_types = {},
                  std::vector<std::shared_ptr<DataType>> r1_types = {}) {
    std::vector<std::shared_ptr<DataType>> all_types = {
        utf8(),
        large_utf8(),
        binary(),
        large_binary(),
        int8(),
        int16(),
        int32(),
        int64(),
        uint8(),
        uint16(),
        uint32(),
        uint64(),
        date32(),
        date64(),
        time32(TimeUnit::MILLI),
        time32(TimeUnit::SECOND),
        time64(TimeUnit::NANO),
        time64(TimeUnit::MICRO),
        timestamp(TimeUnit::NANO, "UTC"),
        timestamp(TimeUnit::MICRO, "UTC"),
        timestamp(TimeUnit::MILLI, "UTC"),
        timestamp(TimeUnit::SECOND, "UTC"),
        float32(),
        float64()};
    using T = const std::shared_ptr<DataType>;
    // byte_width > 1 below allows fitting the tested data
    init_types(all_types, time_types,
               [](T& t) { return t->byte_width() > 1 && !is_floating(t->id()); });
    ASSERT_NE(0, time_types.size());
    init_types(all_types, key_types, [](T& t) { return !is_floating(t->id()); });
    ASSERT_NE(0, key_types.size());
    init_types(all_types, l_types, [](T& t) { return true; });
    ASSERT_NE(0, l_types.size());
    init_types(all_types, r0_types, [](T& t) { return t->byte_width() > 1; });
    ASSERT_NE(0, r0_types.size());
    init_types(all_types, r1_types, [](T& t) { return t->byte_width() > 1; });
    ASSERT_NE(0, r1_types.size());

    // sample a limited number of type-combinations to keep the runnning time reasonable
    // the scoped-traces below help reproduce a test failure, should it happen
    auto start_time = std::chrono::system_clock::now();
    auto seed = start_time.time_since_epoch().count();
    ARROW_SCOPED_TRACE("Types seed: ", seed);
    std::default_random_engine engine(static_cast<unsigned int>(seed));
    std::uniform_int_distribution<size_t> time_distribution(0, time_types.size() - 1);
    std::uniform_int_distribution<size_t> key_distribution(0, key_types.size() - 1);
    std::uniform_int_distribution<size_t> l_distribution(0, l_types.size() - 1);
    std::uniform_int_distribution<size_t> r0_distribution(0, r0_types.size() - 1);
    std::uniform_int_distribution<size_t> r1_distribution(0, r1_types.size() - 1);

    for (int i = 0; i < 1000; i++) {
      auto time_type = time_types[time_distribution(engine)];
      ARROW_SCOPED_TRACE("Time type: ", *time_type);
      auto key_type = key_types[key_distribution(engine)];
      ARROW_SCOPED_TRACE("Key type: ", *key_type);
      auto l_type = l_types[l_distribution(engine)];
      ARROW_SCOPED_TRACE("Left type: ", *l_type);
      auto r0_type = r0_types[r0_distribution(engine)];
      ARROW_SCOPED_TRACE("Right-0 type: ", *r0_type);
      auto r1_type = r1_types[r1_distribution(engine)];
      ARROW_SCOPED_TRACE("Right-1 type: ", *r1_type);

      RunTypes({time_type, key_type, l_type, r0_type, r1_type}, batches_runner);

      auto end_time = std::chrono::system_clock::now();
      std::chrono::duration<double> diff = end_time - start_time;
      if (diff.count() > 2) {
        std::cerr << "AsofJoin test reached time limit at iteration " << i << std::endl;
        // this normally happens on slow CI systems, but is fine
        break;
      }
    }
  }
  template <typename BatchesRunner>
  void RunTypes(BasicTestTypes basic_test_types, BatchesRunner batches_runner) {
    const BasicTestTypes& b = basic_test_types;
    auto l_schema =
        schema({field("time", b.time), field("key", b.key), field("l_v0", b.l_val)});
    auto r0_schema =
        schema({field("time", b.time), field("key", b.key), field("r0_v0", b.r0_val)});
    auto r1_schema =
        schema({field("time", b.time), field("key", b.key), field("r1_v0", b.r1_val)});

    auto exp_schema = schema({
        field("time", b.time),
        field("key", b.key),
        field("l_v0", b.l_val),
        field("r0_v0", b.r0_val),
        field("r1_v0", b.r1_val),
    });

    // Test three table join
    BatchesWithSchema l_batches, r0_batches, r1_batches, exp_nokey_batches, exp_batches;
    l_batches = MakeBatchesFromNumString(l_schema, l_data);
    r0_batches = MakeBatchesFromNumString(r0_schema, r0_data);
    r1_batches = MakeBatchesFromNumString(r1_schema, r1_data);
    exp_nokey_batches = MakeBatchesFromNumString(exp_schema, exp_nokey_data);
    exp_batches = MakeBatchesFromNumString(exp_schema, exp_data);
    batches_runner(l_batches, r0_batches, r1_batches, exp_nokey_batches, exp_batches);
  }

  std::vector<util::string_view> l_data;
  std::vector<util::string_view> r0_data;
  std::vector<util::string_view> r1_data;
  std::vector<util::string_view> exp_nokey_data;
  std::vector<util::string_view> exp_data;
  int64_t tolerance;
};

class AsofJoinTest : public testing::Test {};

#define ASOFJOIN_TEST_SET(name, num)                           \
  TRACED_TEST(AsofJoinTest, Test##name##num##_SingleByKey) {   \
    Get##name##Test##num().RunSingleByKey();                   \
  }                                                            \
  TRACED_TEST(AsofJoinTest, Test##name##num##_DoubleByKey) {   \
    Get##name##Test##num().RunDoubleByKey();                   \
  }                                                            \
  TRACED_TEST(AsofJoinTest, Test##name##num##_MutateByKey) {   \
    Get##name##Test##num().RunMutateByKey();                   \
  }                                                            \
  TRACED_TEST(AsofJoinTest, Test##name##num##_MutateNoKey) {   \
    Get##name##Test##num().RunMutateNoKey();                   \
  }                                                            \
  TRACED_TEST(AsofJoinTest, Test##name##num##_MutateNullKey) { \
    Get##name##Test##num().RunMutateNullKey();                 \
  }

BasicTest GetBasicTest1() {
  // Single key, single batch
  return BasicTest(
      /*l*/ {R"([[0, 1, 1], [1000, 1, 2]])"},
      /*r0*/ {R"([[0, 1, 11]])"},
      /*r1*/ {R"([[1000, 1, 101]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, null], [1000, 0, 2, 11, 101]])"},
      /*exp*/ {R"([[0, 1, 1, 11, null], [1000, 1, 2, 11, 101]])"}, 1000);
}
ASOFJOIN_TEST_SET(Basic, 1)

BasicTest GetBasicTest2() {
  // Single key, multiple batches
  return BasicTest(
      /*l*/ {R"([[0, 1, 1]])", R"([[1000, 1, 2]])"},
      /*r0*/ {R"([[0, 1, 11]])", R"([[1000, 1, 12]])"},
      /*r1*/ {R"([[0, 1, 101]])", R"([[1000, 1, 102]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, 101], [1000, 0, 2, 12, 102]])"},
      /*exp*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"}, 1000);
}
ASOFJOIN_TEST_SET(Basic, 2)

BasicTest GetBasicTest3() {
  // Single key, multiple left batches, single right batches
  return BasicTest(
      /*l*/ {R"([[0, 1, 1]])", R"([[1000, 1, 2]])"},
      /*r0*/ {R"([[0, 1, 11], [1000, 1, 12]])"},
      /*r1*/ {R"([[0, 1, 101], [1000, 1, 102]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, 101], [1000, 0, 2, 12, 102]])"},
      /*exp*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"}, 1000);
}
ASOFJOIN_TEST_SET(Basic, 3)

BasicTest GetBasicTest4() {
  // Multi key, multiple batches, misaligned batches
  return BasicTest(
      /*l*/
      {R"([[0, 1, 1], [0, 2, 21], [500, 1, 2], [1000, 2, 22], [1500, 1, 3], [1500, 2, 23]])",
       R"([[2000, 1, 4], [2000, 2, 24]])"},
      /*r0*/
      {R"([[0, 1, 11], [500, 2, 31], [1000, 1, 12]])",
       R"([[1500, 2, 32], [2000, 1, 13], [2500, 2, 33]])"},
      /*r1*/
      {R"([[0, 2, 1001], [500, 1, 101]])",
       R"([[1000, 1, 102], [1500, 2, 1002], [2000, 1, 103]])"},
      /*exp_nokey*/
      {R"([[0, 0, 1, 11, 1001], [0, 0, 21, 11, 1001], [500, 0, 2, 31, 101], [1000, 0, 22, 12, 102], [1500, 0, 3, 32, 1002], [1500, 0, 23, 32, 1002]])",
       R"([[2000, 0, 4, 13, 103], [2000, 0, 24, 13, 103]])"},
      /*exp*/
      {R"([[0, 1, 1, 11, null], [0, 2, 21, null, 1001], [500, 1, 2, 11, 101], [1000, 2, 22, 31, 1001], [1500, 1, 3, 12, 102], [1500, 2, 23, 32, 1002]])",
       R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"},
      1000);
}
ASOFJOIN_TEST_SET(Basic, 4)

BasicTest GetBasicTest5() {
  // Multi key, multiple batches, misaligned batches, smaller tolerance
  return BasicTest(/*l*/
                   {R"([[0, 1, 1], [0, 2, 21], [500, 1, 2], [1000, 2, 22], [1500, 1, 3], [1500, 2, 23]])",
                    R"([[2000, 1, 4], [2000, 2, 24]])"},
                   /*r0*/
                   {R"([[0, 1, 11], [500, 2, 31], [1000, 1, 12]])",
                    R"([[1500, 2, 32], [2000, 1, 13], [2500, 2, 33]])"},
                   /*r1*/
                   {R"([[0, 2, 1001], [500, 1, 101]])",
                    R"([[1000, 1, 102], [1500, 2, 1002], [2000, 1, 103]])"},
                   /*exp_nokey*/
                   {R"([[0, 0, 1, 11, 1001], [0, 0, 21, 11, 1001], [500, 0, 2, 31, 101], [1000, 0, 22, 12, 102], [1500, 0, 3, 32, 1002], [1500, 0, 23, 32, 1002]])",
                    R"([[2000, 0, 4, 13, 103], [2000, 0, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, 11, null], [0, 2, 21, null, 1001], [500, 1, 2, 11, 101], [1000, 2, 22, 31, null], [1500, 1, 3, 12, 102], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"},
                   500);
}
ASOFJOIN_TEST_SET(Basic, 5)

BasicTest GetBasicTest6() {
  // Multi key, multiple batches, misaligned batches, zero tolerance
  return BasicTest(/*l*/
                   {R"([[0, 1, 1], [0, 2, 21], [500, 1, 2], [1000, 2, 22], [1500, 1, 3], [1500, 2, 23]])",
                    R"([[2000, 1, 4], [2000, 2, 24]])"},
                   /*r0*/
                   {R"([[0, 1, 11], [500, 2, 31], [1000, 1, 12]])",
                    R"([[1500, 2, 32], [2000, 1, 13], [2500, 2, 33]])"},
                   /*r1*/
                   {R"([[0, 2, 1001], [500, 1, 101]])",
                    R"([[1000, 1, 102], [1500, 2, 1002], [2000, 1, 103]])"},
                   /*exp_nokey*/
                   {R"([[0, 0, 1, 11, 1001], [0, 0, 21, 11, 1001], [500, 0, 2, 31, 101], [1000, 0, 22, 12, 102], [1500, 0, 3, 32, 1002], [1500, 0, 23, 32, 1002]])",
                    R"([[2000, 0, 4, 13, 103], [2000, 0, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, 11, null], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, null], [1500, 1, 3, null, null], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, null, null]])"},
                   0);
}
ASOFJOIN_TEST_SET(Basic, 6)

BasicTest GetEmptyTest1() {
  // Empty left batch
  return BasicTest(/*l*/
                   {R"([])", R"([[2000, 1, 4], [2000, 2, 24]])"},
                   /*r0*/
                   {R"([[0, 1, 11], [500, 2, 31], [1000, 1, 12]])",
                    R"([[1500, 2, 32], [2000, 1, 13], [2500, 2, 33]])"},
                   /*r1*/
                   {R"([[0, 2, 1001], [500, 1, 101]])",
                    R"([[1000, 1, 102], [1500, 2, 1002], [2000, 1, 103]])"},
                   /*exp_nokey*/
                   {R"([[2000, 0, 4, 13, 103], [2000, 0, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"}, 1000);
}
ASOFJOIN_TEST_SET(Empty, 1)

BasicTest GetEmptyTest2() {
  // Empty left input
  return BasicTest(/*l*/
                   {R"([])"},
                   /*r0*/
                   {R"([[0, 1, 11], [500, 2, 31], [1000, 1, 12]])",
                    R"([[1500, 2, 32], [2000, 1, 13], [2500, 2, 33]])"},
                   /*r1*/
                   {R"([[0, 2, 1001], [500, 1, 101]])",
                    R"([[1000, 1, 102], [1500, 2, 1002], [2000, 1, 103]])"},
                   /*exp_nokey*/
                   {R"([])"},
                   /*exp*/
                   {R"([])"}, 1000);
}
ASOFJOIN_TEST_SET(Empty, 2)

BasicTest GetEmptyTest3() {
  // Empty right batch
  return BasicTest(/*l*/
                   {R"([[0, 1, 1], [0, 2, 21], [500, 1, 2], [1000, 2, 22], [1500, 1, 3], [1500, 2, 23]])",
                    R"([[2000, 1, 4], [2000, 2, 24]])"},
                   /*r0*/
                   {R"([])", R"([[1500, 2, 32], [2000, 1, 13], [2500, 2, 33]])"},
                   /*r1*/
                   {R"([[0, 2, 1001], [500, 1, 101]])",
                    R"([[1000, 1, 102], [1500, 2, 1002], [2000, 1, 103]])"},
                   /*exp_nokey*/
                   {R"([[0, 0, 1, null, 1001], [0, 0, 21, null, 1001], [500, 0, 2, null, 101], [1000, 0, 22, null, 102], [1500, 0, 3, 32, 1002], [1500, 0, 23, 32, 1002]])",
                    R"([[2000, 0, 4, 13, 103], [2000, 0, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, null, null], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 1001], [1500, 1, 3, null, 102], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"},
                   1000);
}
ASOFJOIN_TEST_SET(Empty, 3)

BasicTest GetEmptyTest4() {
  // Empty right input
  return BasicTest(/*l*/
                   {R"([[0, 1, 1], [0, 2, 21], [500, 1, 2], [1000, 2, 22], [1500, 1, 3], [1500, 2, 23]])",
                    R"([[2000, 1, 4], [2000, 2, 24]])"},
                   /*r0*/
                   {R"([])"},
                   /*r1*/
                   {R"([[0, 2, 1001], [500, 1, 101]])",
                    R"([[1000, 1, 102], [1500, 2, 1002], [2000, 1, 103]])"},
                   /*exp_nokey*/
                   {R"([[0, 0, 1, null, 1001], [0, 0, 21, null, 1001], [500, 0, 2, null, 101], [1000, 0, 22, null, 102], [1500, 0, 3, null, 1002], [1500, 0, 23, null, 1002]])",
                    R"([[2000, 0, 4, null, 103], [2000, 0, 24, null, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, null, null], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 1001], [1500, 1, 3, null, 102], [1500, 2, 23, null, 1002]])",
                    R"([[2000, 1, 4, null, 103], [2000, 2, 24, null, 1002]])"},
                   1000);
}
ASOFJOIN_TEST_SET(Empty, 4)

BasicTest GetEmptyTest5() {
  // All empty
  return BasicTest(/*l*/
                   {R"([])"},
                   /*r0*/
                   {R"([])"},
                   /*r1*/
                   {R"([])"},
                   /*exp_nokey*/
                   {R"([])"},
                   /*exp*/
                   {R"([])"}, 1000);
}
ASOFJOIN_TEST_SET(Empty, 5)

TRACED_TEST(AsofJoinTest, TestUnsupportedOntype) {
  DoRunInvalidTypeTest(schema({field("time", list(int32())), field("key", int32()),
                               field("l_v0", float64())}),
                       schema({field("time", list(int32())), field("key", int32()),
                               field("r0_v0", float32())}));
}

TRACED_TEST(AsofJoinTest, TestUnsupportedBytype) {
  DoRunInvalidTypeTest(schema({field("time", int64()), field("key", list(int32())),
                               field("l_v0", float64())}),
                       schema({field("time", int64()), field("key", list(int32())),
                               field("r0_v0", float32())}));
}

TRACED_TEST(AsofJoinTest, TestUnsupportedDatatype) {
  // List is unsupported
  DoRunInvalidTypeTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()),
              field("r0_v0", list(int32()))}));
}

TRACED_TEST(AsofJoinTest, TestMissingKeys) {
  DoRunMissingKeysTest(
      schema({field("time1", int64()), field("key", int32()), field("l_v0", float64())}),
      schema(
          {field("time1", int64()), field("key", int32()), field("r0_v0", float64())}));

  DoRunMissingKeysTest(
      schema({field("time", int64()), field("key1", int32()), field("l_v0", float64())}),
      schema(
          {field("time", int64()), field("key1", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestUnsupportedTolerance) {
  // Utf8 is unsupported
  DoRunInvalidToleranceTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestEmptyByKey) {
  DoRunEmptyByKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestMissingOnKey) {
  DoRunMissingOnKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestMissingByKey) {
  DoRunMissingByKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestNestedOnKey) {
  DoRunNestedOnKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestNestedByKey) {
  DoRunNestedByKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestAmbiguousOnKey) {
  DoRunAmbiguousOnKeyTest(
      schema({field("time", int64()), field("time", int64()), field("key", int32()),
              field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestAmbiguousByKey) {
  DoRunAmbiguousByKeyTest(
      schema({field("time", int64()), field("key", int64()), field("key", int32()),
              field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestLeftUnorderedOnKey) {
  DoRunUnorderedPlanTest(
      /*l_unordered=*/true, /*r_unordered=*/false,
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestRightUnorderedOnKey) {
  DoRunUnorderedPlanTest(
      /*l_unordered=*/false, /*r_unordered=*/true,
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestUnorderedOnKey) {
  DoRunUnorderedPlanTest(
      /*l_unordered=*/true, /*r_unordered=*/true,
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

TRACED_TEST(AsofJoinTest, TestNullByKey) {
  DoRunNullByKeyPlanTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
}

}  // namespace compute
}  // namespace arrow

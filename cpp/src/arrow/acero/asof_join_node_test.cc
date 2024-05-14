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
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <string_view>
#include "arrow/acero/exec_plan.h"
#include "arrow/testing/future_util.h"
#ifndef NDEBUG
#include <sstream>
#endif
#include <unordered_set>

#include "arrow/acero/options.h"
#ifndef NDEBUG
#include "arrow/acero/options_internal.h"
#endif
#include "arrow/acero/map_node.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/row_encoder_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/thread_pool.h"

#define TRACED_TEST(t_class, t_name, t_body)  \
  TEST(t_class, t_name) {                     \
    ARROW_SCOPED_TRACE(#t_class "_" #t_name); \
    t_body;                                   \
  }

#define TRACED_TEST_P(t_class, t_name, t_body)                              \
  TEST_P(t_class, t_name) {                                                 \
    ARROW_SCOPED_TRACE(#t_class "_" #t_name "_" + std::get<1>(GetParam())); \
    t_body;                                                                 \
  }

using testing::UnorderedElementsAreArray;

namespace arrow {

using compute::Cast;
using compute::Divide;
using compute::Multiply;
using compute::Subtract;

namespace acero {

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

Result<BatchesWithSchema> MakeBatchesFromNumString(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::string_view>& json_strings, int multiplicity = 1) {
  FieldVector num_fields;
  for (auto field : schema->fields()) {
    auto id = field->type()->id();
    bool adjust = id == Type::BOOL || is_base_binary_like(id);
    num_fields.push_back(adjust ? field->WithType(int64()) : field);
  }
  auto num_schema =
      std::make_shared<Schema>(num_fields, schema->endianness(), schema->metadata());
  BatchesWithSchema num_batches =
      MakeBatchesFromString(num_schema, json_strings, multiplicity);
  BatchesWithSchema batches;
  batches.schema = schema;
  int n_fields = schema->num_fields();
  for (auto num_batch : num_batches.batches) {
    Datum two(Int32Scalar(2));
    std::vector<Datum> values;
    for (int i = 0; i < n_fields; i++) {
      auto type = schema->field(i)->type();
      if (is_base_binary_like(type->id())) {
        // casting to string first enables casting to binary
        ARROW_ASSIGN_OR_RAISE(Datum as_string, Cast(num_batch.values[i], utf8()));
        ARROW_ASSIGN_OR_RAISE(Datum as_type, Cast(as_string, type));
        values.push_back(as_type);
      } else if (Type::BOOL == type->id()) {
        // the next 4 lines compute `as_bool` as `(bool)(x - 2*(x/2))`, i.e., the low bit
        // of `x`. Here, `x` stands for `num_batch.values[i]`, which is an `int64` value.
        // Taking the low bit is a somewhat arbitrary way of obtaining both `true` and
        // `false` values from the `int64` values in the test data, in order to get good
        // testing coverage. A simple cast to a Boolean value would not get good coverage
        // because all positive values would be cast to `true`.
        ARROW_ASSIGN_OR_RAISE(Datum div_two, Divide(num_batch.values[i], two));
        ARROW_ASSIGN_OR_RAISE(Datum rounded, Multiply(div_two, two));
        ARROW_ASSIGN_OR_RAISE(Datum low_bit, Subtract(num_batch.values[i], rounded));
        ARROW_ASSIGN_OR_RAISE(Datum as_bool, Cast(low_bit, type));
        values.push_back(as_bool);
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

AsofJoinNodeOptions GetRepeatedOptions(size_t repeat, FieldRef on_key,
                                       std::vector<FieldRef> by_key, int64_t tolerance) {
  std::vector<AsofJoinNodeOptions::Keys> input_keys(repeat);
  for (size_t i = 0; i < repeat; i++) {
    input_keys[i] = {on_key, by_key};
  }
  return AsofJoinNodeOptions(input_keys, tolerance);
}

// mutates by copying from_key into to_key and changing from_key to zero
Result<BatchesWithSchema> MutateByKey(BatchesWithSchema& batches, std::string from_key,
                                      std::string to_key, bool replace_key = false,
                                      bool null_key = false, bool remove_key = false) {
  int from_index = batches.schema->GetFieldIndex(from_key);
  int n_fields = batches.schema->num_fields();
  auto fields = batches.schema->fields();
  BatchesWithSchema new_batches;
  if (remove_key) {
    ARROW_ASSIGN_OR_RAISE(new_batches.schema, batches.schema->RemoveField(from_index));
  } else {
    auto new_field = batches.schema->field(from_index)->WithName(to_key);
    ARROW_ASSIGN_OR_RAISE(new_batches.schema,
                          replace_key ? batches.schema->SetField(from_index, new_field)
                                      : batches.schema->AddField(from_index, new_field));
  }
  for (const ExecBatch& batch : batches.batches) {
    std::vector<Datum> new_values;
    for (int i = 0; i < n_fields; i++) {
      const Datum& value = batch.values[i];
      if (i == from_index) {
        if (remove_key) {
          continue;
        }
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
          ARROW_ASSIGN_OR_RAISE(auto sub, Subtract(value, value));
          new_values.push_back(sub);
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

void CheckRunOutput(const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r0_batches,
                    const BatchesWithSchema& r1_batches,
                    const BatchesWithSchema& exp_batches,
                    const AsofJoinNodeOptions join_options,
                    std::function<void(const Table&, const Table&)> check_tables) {
  Declaration join{"asofjoin", join_options};

  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r0_batches.schema, r0_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r1_batches.schema, r1_batches.gen(false, false)}});

  ASSERT_OK_AND_ASSIGN(auto res_table,
                       DeclarationToTable(std::move(join), /*use_threads=*/false));

  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  check_tables(*exp_table, *res_table);
}

void DoInvalidPlanTest(const BatchesWithSchema& l_batches,
                       const BatchesWithSchema& r_batches,
                       const AsofJoinNodeOptions& join_options,
                       const std::string& expected_error_str,
                       bool fail_on_plan_creation = false) {
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(*threaded_exec_context()));

  Declaration join{"asofjoin", join_options};
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(false, false)}});

  if (fail_on_plan_creation) {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr(expected_error_str),
        DeclarationToStatus(std::move(join), /*use_threads=*/false));
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
  ASSERT_OK_AND_ASSIGN(auto l_batches, MakeBatchesFromNumString(l_schema, {R"([])"}));
  ASSERT_OK_AND_ASSIGN(auto r_batches, MakeBatchesFromNumString(r_schema, {R"([])"}));

  return DoRunInvalidPlanTest(l_batches, r_batches, join_options, expected_error_str);
}

void DoRunInvalidPlanTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema, int64_t tolerance,
                          const std::string& expected_error_str) {
  DoRunInvalidPlanTest(l_schema, r_schema,
                       GetRepeatedOptions(2, "time", {"key"}, tolerance),
                       expected_error_str);
}

void DoRunInvalidTypeTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, 0, "Unsupported type for ");
}

void DoRunMissingKeysTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, 0, "Bad join key on table : No match");
}

void DoRunMissingOnKeyTest(const std::shared_ptr<Schema>& l_schema,
                           const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema,
                       GetRepeatedOptions(2, "invalid_time", {"key"}, 0),
                       "Bad join key on table : No match");
}

void DoRunMissingByKeyTest(const std::shared_ptr<Schema>& l_schema,
                           const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema,
                       GetRepeatedOptions(2, "time", {"invalid_key"}, 0),
                       "Bad join key on table : No match");
}

void DoRunNestedOnKeyTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema, GetRepeatedOptions(2, {0, "time"}, {"key"}, 0),
                       "Bad join key on table : No match");
}

void DoRunNestedByKeyTest(const std::shared_ptr<Schema>& l_schema,
                          const std::shared_ptr<Schema>& r_schema) {
  DoRunInvalidPlanTest(l_schema, r_schema,
                       GetRepeatedOptions(2, "time", {FieldRef{0, 1}}, 0),
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

// Gets a batch for testing as a Json string
// The batch will have n_rows rows n_cols columns, the first column being the on-field
// If unordered is true then the first column will be out-of-order
std::string GetTestBatchAsJsonString(int n_rows, int n_cols, bool unordered = false) {
  int order_mask = unordered ? 1 : 0;
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
        s << (i ^ order_mask);
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
  auto l_str = GetTestBatchAsJsonString(n_rows, l_schema->num_fields(), l_unordered);
  auto r_str = GetTestBatchAsJsonString(n_rows, r_schema->num_fields(), r_unordered);
  ASSERT_OK_AND_ASSIGN(auto l_batches, MakeBatchesFromNumString(l_schema, {l_str}));
  ASSERT_OK_AND_ASSIGN(auto r_batches, MakeBatchesFromNumString(r_schema, {r_str}));

  return DoInvalidPlanTest(l_batches, r_batches, join_options, expected_error_str,
                           /*then_run_plan=*/true);
}

void DoRunUnorderedPlanTest(bool l_unordered, bool r_unordered,
                            const std::shared_ptr<Schema>& l_schema,
                            const std::shared_ptr<Schema>& r_schema) {
  DoRunUnorderedPlanTest(l_unordered, r_unordered, l_schema, r_schema,
                         GetRepeatedOptions(2, "time", {"key"}, 1000),
                         "out-of-order on-key values");
}

struct BasicTestTypes {
  std::shared_ptr<DataType> time, key, l_val, r0_val, r1_val;
};

struct BasicTest {
  BasicTest(const std::vector<std::string_view>& l_data,
            const std::vector<std::string_view>& r0_data,
            const std::vector<std::string_view>& r1_data,
            const std::vector<std::string_view>& exp_nokey_data,
            const std::vector<std::string_view>& exp_emptykey_data,
            const std::vector<std::string_view>& exp_data, int64_t tolerance)
      : l_data(std::move(l_data)),
        r0_data(std::move(r0_data)),
        r1_data(std::move(r1_data)),
        exp_nokey_data(std::move(exp_nokey_data)),
        exp_emptykey_data(std::move(exp_emptykey_data)),
        exp_data(std::move(exp_data)),
        tolerance(tolerance) {}

  static inline void check_init(const std::vector<std::shared_ptr<DataType>>& types) {
    ASSERT_NE(0, types.size());
  }

  template <typename TypeCond>
  static inline std::vector<std::shared_ptr<DataType>> init_types(
      const std::vector<std::shared_ptr<DataType>>& all_types, TypeCond type_cond) {
    std::vector<std::shared_ptr<DataType>> types;
    for (auto type : all_types) {
      if (type_cond(type)) {
        types.push_back(type);
      }
    }
    check_init(types);
    return types;
  }

// code generation for the by_key types supported by AsofJoinNodeOptions constructors
// which cannot be directly done using templates because of failure to deduce the template
// argument for an invocation with a string- or initializer_list-typed keys-argument
#define EXPAND_BY_KEY_TYPE(macro) \
  macro(const FieldRef);          \
  macro(std::vector<FieldRef>);   \
  macro(std::initializer_list<FieldRef>);

#define CHECK_RUN_OUTPUT(by_key_type)                                            \
  void CheckRunOutput(                                                           \
      const BatchesWithSchema& l_batches, const BatchesWithSchema& r0_batches,   \
      const BatchesWithSchema& r1_batches, const BatchesWithSchema& exp_batches, \
      const FieldRef time, by_key_type key, const int64_t tolerance) {           \
    CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches,               \
                   GetRepeatedOptions(3, time, {key}, tolerance));               \
  }

  EXPAND_BY_KEY_TYPE(CHECK_RUN_OUTPUT)

#undef CHECK_RUN_OUTPUT
#undef EXPAND_BY_KEY_TYPE

  void CheckRunOutput(const BatchesWithSchema& l_batches,
                      const BatchesWithSchema& r0_batches,
                      const BatchesWithSchema& r1_batches,
                      const BatchesWithSchema& exp_batches,
                      const AsofJoinNodeOptions join_options) {
#ifndef NDEBUG
    auto debug_sstr = dynamic_cast<std::stringstream*>(join_options.debug_opts->os);
    if (debug_sstr) debug_sstr->str("");
#endif
    acero::CheckRunOutput(
        l_batches, r0_batches, r1_batches, exp_batches, join_options,
        [&](const Table& exp_table, const Table& res_table) {
#ifndef NDEBUG
          if (debug_sstr) {
            (*debug_sstr) << "Comparing flattened expected table:" << std::endl
                          << exp_table.ToString() << std::endl
                          << "with flattened result table:" << std::endl
                          << res_table.ToString() << std::endl;
          }
#endif
          AssertTablesEqual(exp_table, res_table, /*same_chunk_layout=*/true,
                            /*flatten=*/true);
        });
  }

  void RunSingleByKey() {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_emptykey_batches, B exp_batches) {
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches, "time", "key",
                     tolerance);
    });
  }
  static void DoSingleByKey(BasicTest& basic_tests) { basic_tests.RunSingleByKey(); }
  void RunDoubleByKey() {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_emptykey_batches, B exp_batches) {
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches, "time",
                     {"key", "key"}, tolerance);
    });
  }
  static void DoDoubleByKey(BasicTest& basic_tests) { basic_tests.RunDoubleByKey(); }
  void RunMutateByKey() {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_emptykey_batches, B exp_batches) {
      ASSERT_OK_AND_ASSIGN(l_batches, MutateByKey(l_batches, "key", "key2"));
      ASSERT_OK_AND_ASSIGN(r0_batches, MutateByKey(r0_batches, "key", "key2"));
      ASSERT_OK_AND_ASSIGN(r1_batches, MutateByKey(r1_batches, "key", "key2"));
      ASSERT_OK_AND_ASSIGN(exp_batches, MutateByKey(exp_batches, "key", "key2"));
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_batches, "time",
                     {"key", "key2"}, tolerance);
    });
  }
  static void DoMutateByKey(BasicTest& basic_tests) { basic_tests.RunMutateByKey(); }
  void RunMutateNoKey() {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_emptykey_batches, B exp_batches) {
      ASSERT_OK_AND_ASSIGN(l_batches, MutateByKey(l_batches, "key", "key2", true));
      ASSERT_OK_AND_ASSIGN(r0_batches, MutateByKey(r0_batches, "key", "key2", true));
      ASSERT_OK_AND_ASSIGN(r1_batches, MutateByKey(r1_batches, "key", "key2", true));
      ASSERT_OK_AND_ASSIGN(exp_nokey_batches,
                           MutateByKey(exp_nokey_batches, "key", "key2", true));
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_nokey_batches, "time", "key2",
                     tolerance);
    });
  }
  static void DoMutateNoKey(BasicTest& basic_tests) { basic_tests.RunMutateNoKey(); }
  void RunMutateNullKey() {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_emptykey_batches, B exp_batches) {
      ASSERT_OK_AND_ASSIGN(l_batches, MutateByKey(l_batches, "key", "key2", true, true));
      ASSERT_OK_AND_ASSIGN(r0_batches,
                           MutateByKey(r0_batches, "key", "key2", true, true));
      ASSERT_OK_AND_ASSIGN(r1_batches,
                           MutateByKey(r1_batches, "key", "key2", true, true));
      ASSERT_OK_AND_ASSIGN(exp_nokey_batches,
                           MutateByKey(exp_nokey_batches, "key", "key2", true, true));
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_nokey_batches,
                     GetRepeatedOptions(3, "time", {"key2"}, tolerance));
    });
  }
  static void DoMutateNullKey(BasicTest& basic_tests) { basic_tests.RunMutateNullKey(); }
  void RunMutateEmptyKey() {
    using B = BatchesWithSchema;
    RunBatches([this](B l_batches, B r0_batches, B r1_batches, B exp_nokey_batches,
                      B exp_emptykey_batches, B exp_batches) {
      ASSERT_OK_AND_ASSIGN(r0_batches,
                           MutateByKey(r0_batches, "key", "key", false, false, true));
      ASSERT_OK_AND_ASSIGN(r1_batches,
                           MutateByKey(r1_batches, "key", "key", false, false, true));
      CheckRunOutput(l_batches, r0_batches, r1_batches, exp_emptykey_batches,
                     GetRepeatedOptions(3, "time", {}, tolerance));
    });
  }
  static void DoMutateEmptyKey(BasicTest& basic_tests) {
    basic_tests.RunMutateEmptyKey();
  }
  template <typename BatchesRunner>
  void RunBatches(BatchesRunner batches_runner) {
    std::vector<std::shared_ptr<DataType>> all_types = {
        utf8(),
        large_utf8(),
        binary(),
        large_binary(),
        boolean(),
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
    auto time_types = init_types(
        all_types, [](T& t) { return t->byte_width() > 1 && !is_floating(t->id()); });
    auto key_types = init_types(
        all_types, [](T& t) { return !is_floating(t->id()) && t->id() != Type::BOOL; });
    auto l_types = init_types(all_types, [](T& t) { return true; });
    auto r0_types = init_types(all_types, [](T& t) { return t->byte_width() > 1; });
    auto r1_types = init_types(all_types, [](T& t) { return t->byte_width() > 1; });

    // sample a limited number of type-combinations to keep the running time reasonable
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

    for (int i = 0; i < 100; i++) {
      ARROW_SCOPED_TRACE("Iteration: ", i);
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
      if (testing::Test::HasFatalFailure()) break;

      auto end_time = std::chrono::system_clock::now();
      std::chrono::duration<double> diff = end_time - start_time;
      if (diff.count() > 0.2) {
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
    ASSERT_OK_AND_ASSIGN(auto l_batches, MakeBatchesFromNumString(l_schema, l_data));
    ASSERT_OK_AND_ASSIGN(auto r0_batches, MakeBatchesFromNumString(r0_schema, r0_data));
    ASSERT_OK_AND_ASSIGN(auto r1_batches, MakeBatchesFromNumString(r1_schema, r1_data));
    ASSERT_OK_AND_ASSIGN(auto exp_nokey_batches,
                         MakeBatchesFromNumString(exp_schema, exp_nokey_data));
    ASSERT_OK_AND_ASSIGN(auto exp_emptykey_batches,
                         MakeBatchesFromNumString(exp_schema, exp_emptykey_data));
    ASSERT_OK_AND_ASSIGN(auto exp_batches,
                         MakeBatchesFromNumString(exp_schema, exp_data));
    batches_runner(l_batches, r0_batches, r1_batches, exp_nokey_batches,
                   exp_emptykey_batches, exp_batches);
  }

  AsofJoinNodeOptions GetRepeatedOptions(size_t repeat, FieldRef on_key,
                                         std::vector<FieldRef> by_key,
                                         int64_t tolerance) {
    auto options = acero::GetRepeatedOptions(repeat, on_key, by_key, tolerance);
#ifndef NDEBUG
    options.debug_opts = debug_opts;
#endif
    return options;
  }

  std::vector<std::string_view> l_data;
  std::vector<std::string_view> r0_data;
  std::vector<std::string_view> r1_data;
  std::vector<std::string_view> exp_nokey_data;
  std::vector<std::string_view> exp_emptykey_data;
  std::vector<std::string_view> exp_data;
  int64_t tolerance;

#ifndef NDEBUG
  std::shared_ptr<DebugOptions> debug_opts;
#endif
};

using AsofJoinBasicParams = std::tuple<std::function<void(BasicTest&)>, std::string>;

void PrintTo(const AsofJoinBasicParams& x, ::std::ostream* os) {
  *os << "AsofJoinBasicParams: " << std::get<1>(x);
}

struct AsofJoinBasicTest : public testing::TestWithParam<AsofJoinBasicParams> {
 public:
  BasicTest PrepareTest(BasicTest basic_test) {
#ifndef NDEBUG
    basic_test.debug_opts = std::make_shared<DebugOptions>(&debug_sstr, &debug_mutex);
#endif
    return basic_test;
  }

#ifndef NDEBUG
  void SetUp() override { debug_sstr.str(std::string("")); }

  void TearDown() override {
    auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    if (test_info && test_info->result()->Failed()) {
      std::cerr << "AsofJoinTest debug:" << std::endl << debug_sstr.str() << std::endl;
    }
  }

  std::stringstream debug_sstr;
  std::mutex debug_mutex;
#endif
};

class AsofJoinTest : public testing::Test {};

BasicTest GetBasicTest1Backward() {
  // Single key, single batch
  return BasicTest(
      /*l*/ {R"([[0, 1, 1], [1000, 1, 2]])"},
      /*r0*/ {R"([[0, 1, 11]])"},
      /*r1*/ {R"([[1000, 1, 101]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, null], [1000, 0, 2, 11, 101]])"},
      /*exp_emptykey*/ {R"([[0, 1, 1, 11, null], [1000, 1, 2, 11, 101]])"},
      /*exp*/ {R"([[0, 1, 1, 11, null], [1000, 1, 2, 11, 101]])"}, -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic1Backward, {
  BasicTest basic_test = PrepareTest(GetBasicTest1Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest1Forward() {
  // Single key, single batch
  return BasicTest(
      /*l*/ {R"([[0, 1, 1], [1000, 1, 2]])"},
      /*r0*/ {R"([[1000, 1, 11]])"},
      /*r1*/ {R"([[2000, 1, 101]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, null], [1000, 0, 2, 11, 101]])"},
      /*exp_emptykey*/ {R"([[0, 1, 1, 11, null], [1000, 1, 2, 11, 101]])"},
      /*exp*/ {R"([[0, 1, 1, 11, null], [1000, 1, 2, 11, 101]])"}, 1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic1Forward, {
  BasicTest basic_test = PrepareTest(GetBasicTest1Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest2Backward() {
  // Single key, multiple batches
  return BasicTest(
      /*l*/ {R"([[0, 1, 1]])", R"([[1000, 1, 2]])"},
      /*r0*/ {R"([[0, 1, 11]])", R"([[1000, 1, 12]])"},
      /*r1*/ {R"([[0, 1, 101]])", R"([[1000, 1, 102]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, 101], [1000, 0, 2, 12, 102]])"},
      /*exp_emptykey*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"},
      /*exp*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"}, -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic2Backward, {
  BasicTest basic_test = PrepareTest(GetBasicTest2Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest2Forward() {
  // Single key, multiple batches
  return BasicTest(
      /*l*/ {R"([[0, 1, 1]])", R"([[1000, 1, 2]])"},
      /*r0*/ {R"([[500, 1, 11]])", R"([[1000, 1, 12]])"},
      /*r1*/ {R"([[500, 1, 101]])", R"([[1000, 1, 102]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, 101], [1000, 0, 2, 12, 102]])"},
      /*exp_emptykey*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"},
      /*exp*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"}, 1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic2Forward, {
  BasicTest basic_test = PrepareTest(GetBasicTest2Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest3Backward() {
  // Single key, multiple left batches, single right batches
  return BasicTest(
      /*l*/ {R"([[0, 1, 1]])", R"([[1000, 1, 2]])"},
      /*r0*/ {R"([[0, 1, 11], [1000, 1, 12]])"},
      /*r1*/ {R"([[0, 1, 101], [1000, 1, 102]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, 101], [1000, 0, 2, 12, 102]])"},
      /*exp_emptykey*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"},
      /*exp*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"}, -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic3Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestBasic3_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetBasicTest3Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest3Forward() {
  // Single key, multiple left batches, single right batches
  return BasicTest(
      /*l*/ {R"([[0, 1, 1]])", R"([[1000, 1, 2]])"},
      /*r0*/ {R"([[500, 1, 11], [1000, 1, 12]])"},
      /*r1*/ {R"([[500, 1, 101], [1000, 1, 102]])"},
      /*exp_nokey*/ {R"([[0, 0, 1, 11, 101], [1000, 0, 2, 12, 102]])"},
      /*exp_emptykey*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"},
      /*exp*/ {R"([[0, 1, 1, 11, 101], [1000, 1, 2, 12, 102]])"}, 1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic3Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestBasic3_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetBasicTest3Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest4Backward() {
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
      /*exp_emptykey*/
      {R"([[0, 1, 1, 11, 1001], [0, 2, 21, 11, 1001], [500, 1, 2, 31, 101], [1000, 2, 22, 12, 102], [1500, 1, 3, 32, 1002], [1500, 2, 23, 32, 1002]])",
       R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
      /*exp*/
      {R"([[0, 1, 1, 11, null], [0, 2, 21, null, 1001], [500, 1, 2, 11, 101], [1000, 2, 22, 31, 1001], [1500, 1, 3, 12, 102], [1500, 2, 23, 32, 1002]])",
       R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"},
      -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic4Backward, {
  BasicTest basic_test = PrepareTest(GetBasicTest4Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest4Forward() {
  // Multi key, multiple batches, misaligned batches
  return BasicTest(
      /*l*/
      {R"([[0, 1, 1], [0, 2, 21], [500, 1, 2], [1000, 2, 22], [1500, 1, 3], [1500, 2, 23]])",
       R"([[2000, 1, 4], [2000, 2, 24]])"},
      /*r0*/
      {R"([[0, 1, 11], [500, 2, 31], [1000, 1, 12]])",
       R"([[1600, 2, 32], [1900, 2, 33], [2100, 1, 13]])"},
      /*r1*/
      {R"([[0, 2, 1001], [500, 1, 101]])",
       R"([[1100, 1, 102], [1600, 2, 1002], [2100, 1, 103]])"},
      /*exp_nokey*/
      {R"([[0, 0, 1, 11, 1001], [0, 0, 21, 11, 1001], [500, 0, 2, 31, 101], [1000, 0, 22, 12, 102], [1500, 0, 3, 32, 1002], [1500, 0, 23, 32, 1002]])",
       R"([[2000, 0, 4, 13, 103], [2000, 0, 24, 13, 103]])"},
      /*exp_emptykey*/
      {R"([[0, 1, 1, 11, 1001], [0, 2, 21, 11, 1001], [500, 1, 2, 31, 101], [1000, 2, 22, 12, 102], [1500, 1, 3, 32, 1002], [1500, 2, 23, 32, 1002]])",
       R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
      /*exp*/
      {R"([[0, 1, 1, 11, 101], [0, 2, 21, 31, 1001], [500, 1, 2, 12, 101], [1000, 2, 22, 32, 1002], [1500, 1, 3, 13, 103], [1500, 2, 23, 32, 1002]])",
       R"([[2000, 1, 4, 13, 103], [2000, 2, 24, null, null]])"},
      1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic4Forward, {
  BasicTest basic_test = PrepareTest(GetBasicTest4Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest5Backward() {
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
                   /*exp_emptykey*/
                   {R"([[0, 1, 1, 11, 1001], [0, 2, 21, 11, 1001], [500, 1, 2, 31, 101], [1000, 2, 22, 12, 102], [1500, 1, 3, 32, 1002], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, 11, null], [0, 2, 21, null, 1001], [500, 1, 2, 11, 101], [1000, 2, 22, 31, null], [1500, 1, 3, 12, 102], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"},
                   -500);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic5Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestBasic5_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetBasicTest5Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest5Forward() {
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
                   /*exp_emptykey*/
                   {R"([[0, 1, 1, 11, 1001], [0, 2, 21, 11, 1001], [500, 1, 2, 31, 101], [1000, 2, 22, 12, 102], [1500, 1, 3, 32, 1002], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, 11, 101], [0, 2, 21, 31, 1001], [500, 1, 2, 12, 101], [1000, 2, 22, 32, 1002], [1500, 1, 3, 13, 103], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 33, null]])"},
                   500);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic5Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestBasic5_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetBasicTest5Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest6Backward() {
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
                   /*exp_emptykey*/
                   {R"([[0, 1, 1, 11, 1001], [0, 2, 21, 11, 1001], [500, 1, 2, 31, 101], [1000, 2, 22, 12, 102], [1500, 1, 3, 32, 1002], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, 11, null], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, null], [1500, 1, 3, null, null], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, null, null]])"},
                   0);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic6Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestBasic6_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetBasicTest6Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetBasicTest7Forward() {
  // Right times in distant future
  return BasicTest(
      /*l*/ {R"([[0, 1, 1]])", R"([[1000, 2, 2]])", R"([[2000, 1, 3]])"},
      /*r0*/ {R"([[0, 1, 10], [1500, 1, 11], [2500, 1, 12]])"},
      /*r1*/ {R"([[0, 1, 100], [1500, 1, 101], [2500, 1, 102]])"},
      /*exp_nokey*/
      {R"([[0, 0, 1, 10, 100], [1000, 0, 2, 11, 101], [2000, 0, 3, 12, 102]])"},
      /*exp_emptykey*/
      {R"([[0, 1, 1, 10, 100], [1000, 2, 2, 11, 101], [2000, 1, 3, 12, 102]])"},
      /*exp*/
      {R"([[0, 1, 1, 10, 100], [1000, 2, 2, null, null], [2000, 1, 3, 12, 102]])"}, 1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestBasic7Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestBasic7_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetBasicTest7Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest1Backward() {
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
                   /*exp_emptykey*/
                   {R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"}, -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty1Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty1Backward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest1Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest1Forward() {
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
                   /*exp_emptykey*/
                   {R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 33, null]])"}, 1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty1Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty1Forward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest1Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest2Backward() {
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
                   /*exp_emptykey*/
                   {R"([])"},
                   /*exp*/
                   {R"([])"}, -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty2Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty2Backward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest2Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest2Forward() {
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
                   /*exp_emptykey*/
                   {R"([])"},
                   /*exp*/
                   {R"([])"}, 1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty2Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty2Forward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest2Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest3Backward() {
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
                   /*exp_emptykey*/
                   {R"([[0, 1, 1, null, 1001], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 102], [1500, 1, 3, 32, 1002], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, null, null], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 1001], [1500, 1, 3, null, 102], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 32, 1002]])"},
                   -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty3Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty3Backward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest3Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest3Forward() {
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
                   {R"([[0, 0, 1, null, 1001], [0, 0, 21, null, 1001], [500, 0, 2, 32, 101], [1000, 0, 22, 32, 102], [1500, 0, 3, 32, 1002], [1500, 0, 23, 32, 1002]])",
                    R"([[2000, 0, 4, 13, 103], [2000, 0, 24, 13, 103]])"},
                   /*exp_emptykey*/
                   {R"([[0, 1, 1, null, 1001], [0, 2, 21, null, 1001], [500, 1, 2, 32, 101], [1000, 2, 22, 32, 102], [1500, 1, 3, 32, 1002], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 13, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, null, 101], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, 32, 1002], [1500, 1, 3, 13, 103], [1500, 2, 23, 32, 1002]])",
                    R"([[2000, 1, 4, 13, 103], [2000, 2, 24, 33, null]])"},
                   1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty3Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty3Forward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest3Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest4Backward() {
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
                   /*exp_emptykey*/
                   {R"([[0, 1, 1, null, 1001], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 102], [1500, 1, 3, null, 1002], [1500, 2, 23, null, 1002]])",
                    R"([[2000, 1, 4, null, 103], [2000, 2, 24, null, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, null, null], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 1001], [1500, 1, 3, null, 102], [1500, 2, 23, null, 1002]])",
                    R"([[2000, 1, 4, null, 103], [2000, 2, 24, null, 1002]])"},
                   -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty4Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty4Backward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest4Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest4Forward() {
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
                   /*exp_emptykey*/
                   {R"([[0, 1, 1, null, 1001], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 102], [1500, 1, 3, null, 1002], [1500, 2, 23, null, 1002]])",
                    R"([[2000, 1, 4, null, 103], [2000, 2, 24, null, 103]])"},
                   /*exp*/
                   {R"([[0, 1, 1, null, 101], [0, 2, 21, null, 1001], [500, 1, 2, null, 101], [1000, 2, 22, null, 1002], [1500, 1, 3, null, 103], [1500, 2, 23, null, 1002]])",
                    R"([[2000, 1, 4, null, 103], [2000, 2, 24, null, null]])"},
                   1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty4Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty4Forward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest4Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest5Backward() {
  // All empty
  return BasicTest(/*l*/
                   {R"([])"},
                   /*r0*/
                   {R"([])"},
                   /*r1*/
                   {R"([])"},
                   /*exp_nokey*/
                   {R"([])"},
                   /*exp_emptykey*/
                   {R"([])"},
                   /*exp*/
                   {R"([])"}, -1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty5Backward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty5Backward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest5Backward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

BasicTest GetEmptyTest5Forward() {
  // All empty
  return BasicTest(/*l*/
                   {R"([])"},
                   /*r0*/
                   {R"([])"},
                   /*r1*/
                   {R"([])"},
                   /*exp_nokey*/
                   {R"([])"},
                   /*exp_emptykey*/
                   {R"([])"},
                   /*exp*/
                   {R"([])"}, 1000);
}

TRACED_TEST_P(AsofJoinBasicTest, TestEmpty5Forward, {
  ARROW_SCOPED_TRACE("AsofJoinBasicTest_TestEmpty5Forward_" + std::get<1>(GetParam()));
  BasicTest basic_test = PrepareTest(GetEmptyTest5Forward());
  auto runner = std::get<0>(GetParam());
  runner(basic_test);
})

INSTANTIATE_TEST_SUITE_P(
    AsofJoinNodeTest, AsofJoinBasicTest,
    testing::Values(AsofJoinBasicParams(BasicTest::DoSingleByKey, "SingleByKey"),
                    AsofJoinBasicParams(BasicTest::DoDoubleByKey, "DoubleByKey"),
                    AsofJoinBasicParams(BasicTest::DoMutateByKey, "MutateByKey"),
                    AsofJoinBasicParams(BasicTest::DoMutateNoKey, "MutateNoKey"),
                    AsofJoinBasicParams(BasicTest::DoMutateNullKey, "MutateNullKey"),
                    AsofJoinBasicParams(BasicTest::DoMutateEmptyKey, "MutateEmptyKey")));

TRACED_TEST(AsofJoinTest, TestUnsupportedOntype, {
  DoRunInvalidTypeTest(schema({field("time", list(int32())), field("key", int32()),
                               field("l_v0", float64())}),
                       schema({field("time", list(int32())), field("key", int32()),
                               field("r0_v0", float32())}));
})

TRACED_TEST(AsofJoinTest, TestUnsupportedByType, {
  DoRunInvalidTypeTest(schema({field("time", int64()), field("key", list(int32())),
                               field("l_v0", float64())}),
                       schema({field("time", int64()), field("key", list(int32())),
                               field("r0_v0", float32())}));
})

TRACED_TEST(AsofJoinTest, TestUnsupportedDatatype, {
  // List is unsupported
  DoRunInvalidTypeTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()),
              field("r0_v0", list(int32()))}));
})

TRACED_TEST(AsofJoinTest, TestMissingKeys, {
  DoRunMissingKeysTest(
      schema({field("time1", int64()), field("key", int32()), field("l_v0", float64())}),
      schema(
          {field("time1", int64()), field("key", int32()), field("r0_v0", float64())}));

  DoRunMissingKeysTest(
      schema({field("time", int64()), field("key1", int32()), field("l_v0", float64())}),
      schema(
          {field("time", int64()), field("key1", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestMissingOnKey, {
  DoRunMissingOnKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestMissingByKey, {
  DoRunMissingByKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestNestedOnKey, {
  DoRunNestedOnKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestNestedByKey, {
  DoRunNestedByKeyTest(
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestAmbiguousOnKey, {
  DoRunAmbiguousOnKeyTest(
      schema({field("time", int64()), field("time", int64()), field("key", int32()),
              field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestAmbiguousByKey, {
  DoRunAmbiguousByKeyTest(
      schema({field("time", int64()), field("key", int64()), field("key", int32()),
              field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestLeftUnorderedOnKey, {
  DoRunUnorderedPlanTest(
      /*l_unordered=*/true, /*r_unordered=*/false,
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestRightUnorderedOnKey, {
  DoRunUnorderedPlanTest(
      /*l_unordered=*/false, /*r_unordered=*/true,
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

TRACED_TEST(AsofJoinTest, TestUnorderedOnKey, {
  DoRunUnorderedPlanTest(
      /*l_unordered=*/true, /*r_unordered=*/true,
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())}),
      schema({field("time", int64()), field("key", int32()), field("r0_v0", float64())}));
})

struct BackpressureCounters {
  std::atomic<int32_t> pause_count = 0;
  std::atomic<int32_t> resume_count = 0;
};

struct BackpressureCountingNodeOptions : public ExecNodeOptions {
  BackpressureCountingNodeOptions(BackpressureCounters* counters) : counters(counters) {}

  BackpressureCounters* counters;
};

struct BackpressureCountingNode : public MapNode {
  static constexpr const char* kKindName = "BackpressureCountingNode";
  static constexpr const char* kFactoryName = "backpressure_count";

  static void Register() {
    auto exec_reg = default_exec_factory_registry();
    if (!exec_reg->GetFactory(kFactoryName).ok()) {
      ASSERT_OK(exec_reg->AddFactory(kFactoryName, BackpressureCountingNode::Make));
    }
  }

  BackpressureCountingNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                           std::shared_ptr<Schema> output_schema,
                           const BackpressureCountingNodeOptions& options)
      : MapNode(plan, inputs, output_schema), counters(options.counters) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, kKindName));
    auto bp_options = static_cast<const BackpressureCountingNodeOptions&>(options);
    return plan->EmplaceNode<BackpressureCountingNode>(
        plan, inputs, inputs[0]->output_schema(), bp_options);
  }

  const char* kind_name() const override { return kKindName; }
  Result<ExecBatch> ProcessBatch(ExecBatch batch) override { return batch; }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    ++counters->pause_count;
    inputs()[0]->PauseProducing(this, counter);
  }
  void ResumeProducing(ExecNode* output, int32_t counter) override {
    ++counters->resume_count;
    inputs()[0]->ResumeProducing(this, counter);
  }

  BackpressureCounters* counters;
};

AsyncGenerator<std::optional<ExecBatch>> GetGen(
    AsyncGenerator<std::optional<ExecBatch>> gen) {
  return gen;
}
AsyncGenerator<std::optional<ExecBatch>> GetGen(BatchesWithSchema bws) {
  return bws.gen(false, false);
}

template <typename BatchesMaker>
void TestBackpressure(BatchesMaker maker, int batch_size, int num_l_batches,
                      int num_r0_batches, int num_r1_batches, bool slow_r0) {
  auto l_schema =
      schema({field("time", int32()), field("key", int32()), field("l_value", int32())});
  auto r0_schema =
      schema({field("time", int32()), field("key", int32()), field("r0_value", int32())});
  auto r1_schema =
      schema({field("time", int32()), field("key", int32()), field("r1_value", int32())});

  auto make_shift = [&maker, batch_size](int num_batches,
                                         const std::shared_ptr<Schema>& schema,
                                         int shift) {
    return maker({[](int row) -> int64_t { return row; },
                  [num_batches](int row) -> int64_t { return row / num_batches; },
                  [shift](int row) -> int64_t { return row * 10 + shift; }},
                 schema, num_batches, batch_size);
  };
  ASSERT_OK_AND_ASSIGN(auto l_batches, make_shift(num_l_batches, l_schema, 0));
  ASSERT_OK_AND_ASSIGN(auto r0_batches, make_shift(num_r0_batches, r0_schema, 1));
  ASSERT_OK_AND_ASSIGN(auto r1_batches, make_shift(num_r1_batches, r1_schema, 2));

  BackpressureCountingNode::Register();
  RegisterTestNodes();  // for GatedNode

  struct BackpressureSourceConfig {
    std::string name_prefix;
    bool is_gated;
    bool is_delayed;
    std::shared_ptr<Schema> schema;
    decltype(l_batches) batches;

    std::string name() const {
      return name_prefix + ";" + (is_gated ? "gated" : "ungated");
    }
  };

  auto gate_ptr = Gate::Make();
  auto& gate = *gate_ptr;
  GatedNodeOptions gate_options(gate_ptr.get());

  // Two ungated and one gated
  std::vector<BackpressureSourceConfig> source_configs = {
      {"0", false, false, l_schema, l_batches},
      {"1", true, slow_r0, r0_schema, r0_batches},
      {"2", false, false, r1_schema, r1_batches},
  };

  std::vector<BackpressureCounters> bp_counters(source_configs.size());
  std::vector<Declaration> src_decls;
  std::vector<std::shared_ptr<BackpressureCountingNodeOptions>> bp_options;
  std::vector<Declaration::Input> bp_decls;
  for (size_t i = 0; i < source_configs.size(); i++) {
    const auto& config = source_configs[i];
    if (config.is_delayed) {
      src_decls.emplace_back(
          "source",
          SourceNodeOptions(config.schema, MakeDelayedGen(config.batches, "slow_source",
                                                          /*delay_sec=*/0.5,
                                                          /*noisy=*/false)));
    } else {
      src_decls.emplace_back("source",
                             SourceNodeOptions(config.schema, GetGen(config.batches)));
    }
    bp_options.push_back(
        std::make_shared<BackpressureCountingNodeOptions>(&bp_counters[i]));
    std::shared_ptr<ExecNodeOptions> options = bp_options.back();
    std::vector<Declaration::Input> bp_in = {src_decls.back()};
    Declaration bp_decl = {BackpressureCountingNode::kFactoryName, bp_in,
                           std::move(options)};
    if (config.is_gated) {
      bp_decl = {std::string{GatedNodeOptions::kName}, {bp_decl}, gate_options};
    }
    bp_decls.emplace_back(bp_decl);
  }

  auto opts = GetRepeatedOptions(source_configs.size(), "time", {"key"}, 0);

  Declaration asofjoin = {"asofjoin", bp_decls, opts};

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<internal::ThreadPool> tpool,
                       internal::ThreadPool::Make(1));
  ExecContext exec_ctx(default_memory_pool(), tpool.get());
  Future<BatchesWithCommonSchema> batches_fut =
      DeclarationToExecBatchesAsync(asofjoin, exec_ctx);

  auto has_bp_been_applied = [&] {
    // One of the inputs is gated.  The other two will eventually be paused by the asof
    // join node
    for (size_t i = 0; i < source_configs.size(); i++) {
      const auto& counters = bp_counters[i];
      if (source_configs[i].is_gated) {
        if (counters.pause_count > 0) return false;
      } else {
        if (counters.pause_count != 1) return false;
      }
    }
    return true;
  };

  BusyWait(60.0, has_bp_been_applied);
  ASSERT_TRUE(has_bp_been_applied());

  gate.ReleaseAllBatches();
  ASSERT_FINISHES_OK_AND_ASSIGN(BatchesWithCommonSchema batches, batches_fut);

  // One of the inputs is gated and was released. The other two will eventually be resumed
  // by the asof join node
  for (size_t i = 0; i < source_configs.size(); i++) {
    const auto& counters = bp_counters[i];
    if (!source_configs[i].is_gated) {
      ASSERT_GE(counters.resume_count, 0);
    }
  }
}

TEST(AsofJoinTest, BackpressureWithBatches) {
  // Give the first right hand table a delay to stress test race conditions
  return TestBackpressure(MakeIntegerBatches, /*batch_size=*/1, /*num_l_batches=*/20,
                          /*num_r0_batches=*/50, /*num_r1_batches=*/20, /*slow_r0=*/true);
}

template <typename BatchesMaker>
void TestSequencing(BatchesMaker maker, int num_batches, int batch_size) {
  auto l_schema =
      schema({field("time", int32()), field("key", int32()), field("l_value", int32())});
  auto r_schema =
      schema({field("time", int32()), field("key", int32()), field("r0_value", int32())});

  auto make_shift = [&maker, num_batches, batch_size](
                        const std::shared_ptr<Schema>& schema, int shift) {
    return maker({[](int row) -> int64_t { return row; },
                  [num_batches](int row) -> int64_t { return row / num_batches; },
                  [shift](int row) -> int64_t { return row * 10 + shift; }},
                 schema, num_batches, batch_size);
  };
  ASSERT_OK_AND_ASSIGN(auto l_batches, make_shift(l_schema, 0));
  ASSERT_OK_AND_ASSIGN(auto r_batches, make_shift(r_schema, 1));

  Declaration l_src = {"source",
                       SourceNodeOptions(l_schema, l_batches.gen(false, false))};
  Declaration r_src = {"source",
                       SourceNodeOptions(r_schema, r_batches.gen(false, false))};

  Declaration asofjoin = {
      "asofjoin", {l_src, r_src}, GetRepeatedOptions(2, "time", {"key"}, 1000)};

  QueryOptions query_options;
  query_options.use_threads = false;
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema batches,
                       DeclarationToExecBatches(asofjoin, query_options));

  AssertExecBatchesSequenced(batches.batches);
}

TEST(AsofJoinTest, BatchSequencing) {
  return TestSequencing(MakeIntegerBatches, /*num_batches=*/32, /*batch_size=*/1);
}

template <typename BatchesMaker>
void TestSchemaResolution(BatchesMaker maker, int num_batches, int batch_size) {
  // GH-39803: The key hasher needs to resolve the types of key columns. All other
  // tests use int32 for all columns, but this test converts the key columns to
  // strings via a projection node to test that the column is correctly resolved
  // to string.
  auto l_schema =
      schema({field("time", int32()), field("key", int32()), field("l_value", int32())});
  auto r_schema =
      schema({field("time", int32()), field("key", int32()), field("r0_value", int32())});

  auto make_shift = [&maker, num_batches, batch_size](
                        const std::shared_ptr<Schema>& schema, int shift) {
    return maker({[](int row) -> int64_t { return row; },
                  [num_batches](int row) -> int64_t { return row / num_batches; },
                  [shift](int row) -> int64_t { return row * 10 + shift; }},
                 schema, num_batches, batch_size);
  };
  ASSERT_OK_AND_ASSIGN(auto l_batches, make_shift(l_schema, 0));
  ASSERT_OK_AND_ASSIGN(auto r_batches, make_shift(r_schema, 1));

  Declaration l_src = {"source",
                       SourceNodeOptions(l_schema, l_batches.gen(false, false))};
  Declaration r_src = {"source",
                       SourceNodeOptions(r_schema, r_batches.gen(false, false))};
  Declaration l_project = {
      "project",
      {std::move(l_src)},
      ProjectNodeOptions({compute::field_ref("time"),
                          compute::call("cast", {compute::field_ref("key")},
                                        compute::CastOptions::Safe(utf8())),
                          compute::field_ref("l_value")},
                         {"time", "key", "l_value"})};
  Declaration r_project = {
      "project",
      {std::move(r_src)},
      ProjectNodeOptions({compute::call("cast", {compute::field_ref("key")},
                                        compute::CastOptions::Safe(utf8())),
                          compute::field_ref("r0_value"), compute::field_ref("time")},
                         {"key", "r0_value", "time"})};

  Declaration asofjoin = {
      "asofjoin", {l_project, r_project}, GetRepeatedOptions(2, "time", {"key"}, 1000)};

  QueryOptions query_options;
  query_options.use_threads = false;
  ASSERT_OK_AND_ASSIGN(auto table, DeclarationToTable(asofjoin, query_options));

  Int32Builder expected_r0_b;
  for (int i = 1; i <= 91; i += 10) {
    ASSERT_OK(expected_r0_b.Append(i));
  }
  ASSERT_OK_AND_ASSIGN(auto expected_r0, expected_r0_b.Finish());

  auto actual_r0 = table->GetColumnByName("r0_value");
  std::vector<std::shared_ptr<arrow::Array>> chunks = {expected_r0};
  auto expected_r0_chunked = std::make_shared<arrow::ChunkedArray>(chunks);
  ASSERT_TRUE(actual_r0->Equals(expected_r0_chunked));
}

TEST(AsofJoinTest, OutputSchemaResolution) {
  return TestSchemaResolution(MakeIntegerBatches, /*num_batches=*/1, /*batch_size=*/10);
}

namespace {

Result<AsyncGenerator<std::optional<ExecBatch>>> MakeIntegerBatchGenForTest(
    const std::vector<std::function<int64_t(int)>>& gens,
    const std::shared_ptr<Schema>& schema, int num_batches, int batch_size) {
  return MakeIntegerBatchGen(gens, schema, num_batches, batch_size);
}

template <typename T>
T GetEnvValue(const std::string& var, T default_value) {
  const char* str = std::getenv(var.c_str());
  if (str == NULLPTR) {
    return default_value;
  }
  std::stringstream s(str);
  T value = default_value;
  s >> value;
  return value;
}

}  // namespace

TEST(AsofJoinTest, BackpressureWithBatchesGen) {
  GTEST_SKIP() << "Skipping - see GH-36331";
  int num_batches = GetEnvValue("ARROW_BACKPRESSURE_DEMO_NUM_BATCHES", 20);
  int batch_size = GetEnvValue("ARROW_BACKPRESSURE_DEMO_BATCH_SIZE", 1);
  return TestBackpressure(MakeIntegerBatchGenForTest, /*batch_size=*/batch_size,
                          /*num_l_batches=*/num_batches,
                          /*num_r0_batches=*/num_batches, /*num_r1_batches=*/num_batches,
                          /*slow_r0=*/false);
}

}  // namespace acero
}  // namespace arrow

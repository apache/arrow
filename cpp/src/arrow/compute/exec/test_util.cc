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

#include "arrow/compute/exec/test_util.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/ir_consumer.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/function_internal.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::Executor;

namespace compute {
namespace {

struct DummyNode : ExecNode {
  DummyNode(ExecPlan* plan, NodeVector inputs, int num_outputs,
            StartProducingFunc start_producing, StopProducingFunc stop_producing)
      : ExecNode(plan, std::move(inputs), {}, dummy_schema(), num_outputs),
        start_producing_(std::move(start_producing)),
        stop_producing_(std::move(stop_producing)) {
    input_labels_.resize(inputs_.size());
    for (size_t i = 0; i < input_labels_.size(); ++i) {
      input_labels_[i] = std::to_string(i);
    }
    finished_.MarkFinished();
  }

  const char* kind_name() const override { return "Dummy"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {}

  void ErrorReceived(ExecNode* input, Status error) override {}

  void InputFinished(ExecNode* input, int total_batches) override {}

  Status StartProducing() override {
    if (start_producing_) {
      RETURN_NOT_OK(start_producing_(this));
    }
    started_ = true;
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void StopProducing(ExecNode* output) override {
    EXPECT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void StopProducing() override {
    if (started_) {
      for (const auto& input : inputs_) {
        input->StopProducing(this);
      }
      if (stop_producing_) {
        stop_producing_(this);
      }
    }
  }

 private:
  void AssertIsOutput(ExecNode* output) {
    auto it = std::find(outputs_.begin(), outputs_.end(), output);
    ASSERT_NE(it, outputs_.end());
  }

  std::shared_ptr<Schema> dummy_schema() const {
    return schema({field("dummy", null())});
  }

  StartProducingFunc start_producing_;
  StopProducingFunc stop_producing_;
  std::unordered_set<ExecNode*> requested_stop_;
  bool started_ = false;
};

}  // namespace

ExecNode* MakeDummyNode(ExecPlan* plan, std::string label, std::vector<ExecNode*> inputs,
                        int num_outputs, StartProducingFunc start_producing,
                        StopProducingFunc stop_producing) {
  auto node =
      plan->EmplaceNode<DummyNode>(plan, std::move(inputs), num_outputs,
                                   std::move(start_producing), std::move(stop_producing));
  if (!label.empty()) {
    node->SetLabel(std::move(label));
  }
  return node;
}

ExecBatch ExecBatchFromJSON(const std::vector<TypeHolder>& types,
                            util::string_view json) {
  auto fields = ::arrow::internal::MapVector(
      [](const TypeHolder& th) { return field("", th.GetSharedPtr()); }, types);

  ExecBatch batch{*RecordBatchFromJSON(schema(std::move(fields)), json)};

  return batch;
}

ExecBatch ExecBatchFromJSON(const std::vector<TypeHolder>& types,
                            const std::vector<ArgShape>& shapes, util::string_view json) {
  DCHECK_EQ(types.size(), shapes.size());

  ExecBatch batch = ExecBatchFromJSON(types, json);

  auto value_it = batch.values.begin();
  for (ArgShape shape : shapes) {
    if (shape == ArgShape::SCALAR) {
      if (batch.length == 0) {
        *value_it = MakeNullScalar(value_it->type());
      } else {
        *value_it = value_it->make_array()->GetScalar(0).ValueOrDie();
      }
    }
    ++value_it;
  }

  return batch;
}

Future<> StartAndFinish(ExecPlan* plan) {
  RETURN_NOT_OK(plan->Validate());
  RETURN_NOT_OK(plan->StartProducing());
  return plan->finished();
}

Future<std::vector<ExecBatch>> StartAndCollect(
    ExecPlan* plan, AsyncGenerator<util::optional<ExecBatch>> gen) {
  RETURN_NOT_OK(plan->Validate());
  RETURN_NOT_OK(plan->StartProducing());

  auto collected_fut = CollectAsyncGenerator(gen);

  return AllComplete({plan->finished(), Future<>(collected_fut)})
      .Then([collected_fut]() -> Result<std::vector<ExecBatch>> {
        ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
        return ::arrow::internal::MapVector(
            [](util::optional<ExecBatch> batch) { return std::move(*batch); },
            std::move(collected));
      });
}

BatchesWithSchema MakeBasicBatches() {
  BatchesWithSchema out;
  out.batches = {
      ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
      ExecBatchFromJSON({int32(), boolean()}, "[[5, null], [6, false], [7, false]]")};
  out.schema = schema({field("i32", int32()), field("bool", boolean())});
  return out;
}

BatchesWithSchema MakeNestedBatches() {
  auto ty = struct_({field("i32", int32()), field("bool", boolean())});
  BatchesWithSchema out;
  out.batches = {
      ExecBatchFromJSON(
          {ty},
          R"([[{"i32": null, "bool": true}], [{"i32": 4, "bool": false}], [null]])"),
      ExecBatchFromJSON(
          {ty},
          R"([[{"i32": 5, "bool": null}], [{"i32": 6, "bool": false}], [{"i32": 7, "bool": false}]])")};
  out.schema = schema({field("struct", ty)});
  return out;
}

BatchesWithSchema MakeRandomBatches(const std::shared_ptr<Schema>& schema,
                                    int num_batches, int batch_size) {
  BatchesWithSchema out;

  random::RandomArrayGenerator rng(42);
  out.batches.resize(num_batches);

  for (int i = 0; i < num_batches; ++i) {
    out.batches[i] = ExecBatch(*rng.BatchOf(schema->fields(), batch_size));
    // add a tag scalar to ensure the batches are unique
    out.batches[i].values.emplace_back(i);
  }

  out.schema = schema;
  return out;
}

BatchesWithSchema MakeBatchesFromString(
    const std::shared_ptr<Schema>& schema,
    const std::vector<util::string_view>& json_strings, int multiplicity) {
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

Result<std::shared_ptr<Table>> SortTableOnAllFields(const std::shared_ptr<Table>& tab) {
  std::vector<SortKey> sort_keys;
  for (auto&& f : tab->schema()->fields()) {
    sort_keys.emplace_back(f->name());
  }
  ARROW_ASSIGN_OR_RAISE(auto sort_ids, SortIndices(tab, SortOptions(sort_keys)));
  ARROW_ASSIGN_OR_RAISE(auto tab_sorted, Take(tab, sort_ids));
  return tab_sorted.table();
}

void AssertTablesEqual(const std::shared_ptr<Table>& exp,
                       const std::shared_ptr<Table>& act) {
  ASSERT_EQ(exp->num_columns(), act->num_columns());
  if (exp->num_rows() == 0) {
    ASSERT_EQ(exp->num_rows(), act->num_rows());
  } else {
    ASSERT_OK_AND_ASSIGN(auto exp_sorted, SortTableOnAllFields(exp));
    ASSERT_OK_AND_ASSIGN(auto act_sorted, SortTableOnAllFields(act));

    AssertTablesEqual(*exp_sorted, *act_sorted,
                      /*same_chunk_layout=*/false, /*flatten=*/true);
  }
}

void AssertExecBatchesEqual(const std::shared_ptr<Schema>& schema,
                            const std::vector<ExecBatch>& exp,
                            const std::vector<ExecBatch>& act) {
  ASSERT_OK_AND_ASSIGN(auto exp_tab, TableFromExecBatches(schema, exp));
  ASSERT_OK_AND_ASSIGN(auto act_tab, TableFromExecBatches(schema, act));
  AssertTablesEqual(exp_tab, act_tab);
}

template <typename T>
static const T& OptionsAs(const ExecNodeOptions& opts) {
  const auto& ptr = checked_cast<const T&>(opts);
  return ptr;
}

template <typename T>
static const T& OptionsAs(const Declaration& decl) {
  return OptionsAs<T>(*decl.options);
}

bool operator==(const Declaration& l, const Declaration& r) {
  if (l.factory_name != r.factory_name) return false;
  if (l.inputs != r.inputs) return false;
  if (l.label != r.label) return false;

  if (l.factory_name == "catalog_source") {
    auto l_opts = &OptionsAs<CatalogSourceNodeOptions>(l);
    auto r_opts = &OptionsAs<CatalogSourceNodeOptions>(r);

    bool schemas_equal = l_opts->schema == nullptr
                             ? r_opts->schema == nullptr
                             : l_opts->schema->Equals(r_opts->schema);

    return l_opts->name == r_opts->name && schemas_equal &&
           l_opts->filter == r_opts->filter && l_opts->projection == r_opts->projection;
  }

  if (l.factory_name == "filter") {
    return OptionsAs<FilterNodeOptions>(l).filter_expression ==
           OptionsAs<FilterNodeOptions>(r).filter_expression;
  }

  if (l.factory_name == "project") {
    auto l_opts = &OptionsAs<ProjectNodeOptions>(l);
    auto r_opts = &OptionsAs<ProjectNodeOptions>(r);
    return l_opts->expressions == r_opts->expressions && l_opts->names == r_opts->names;
  }

  if (l.factory_name == "aggregate") {
    auto l_opts = &OptionsAs<AggregateNodeOptions>(l);
    auto r_opts = &OptionsAs<AggregateNodeOptions>(r);

    if (l_opts->aggregates.size() != r_opts->aggregates.size()) return false;
    for (size_t i = 0; i < l_opts->aggregates.size(); ++i) {
      auto l_agg = &l_opts->aggregates[i];
      auto r_agg = &r_opts->aggregates[i];

      if (l_agg->function != r_agg->function) return false;

      if (l_agg->options == r_agg->options) continue;
      if (l_agg->options == nullptr || r_agg->options == nullptr) return false;

      if (!l_agg->options->Equals(*r_agg->options)) return false;

      if (l_agg->target != r_agg->target) return false;
      if (l_agg->name != r_agg->name) return false;
    }

    return l_opts->keys == r_opts->keys;
  }

  if (l.factory_name == "order_by_sink") {
    auto l_opts = &OptionsAs<OrderBySinkNodeOptions>(l);
    auto r_opts = &OptionsAs<OrderBySinkNodeOptions>(r);

    return l_opts->generator == r_opts->generator &&
           l_opts->sort_options == r_opts->sort_options;
  }

  Unreachable("equality comparison is not supported for all ExecNodeOptions");
}

static inline void PrintToImpl(const std::string& factory_name,
                               const ExecNodeOptions& opts, std::ostream* os) {
  if (factory_name == "catalog_source") {
    auto o = &OptionsAs<CatalogSourceNodeOptions>(opts);
    *os << o->name << ", schema=" << o->schema->ToString();
    if (o->filter != literal(true)) {
      *os << ", filter=" << o->filter.ToString();
    }
    if (!o->projection.empty()) {
      *os << ", projection=[";
      for (const auto& ref : o->projection) {
        *os << ref.ToString() << ",";
      }
      *os << "]";
    }
    return;
  }

  if (factory_name == "filter") {
    return PrintTo(OptionsAs<FilterNodeOptions>(opts).filter_expression, os);
  }

  if (factory_name == "project") {
    auto o = &OptionsAs<ProjectNodeOptions>(opts);
    *os << "expressions={";
    for (const auto& expr : o->expressions) {
      PrintTo(expr, os);
      *os << ",";
    }
    *os << "},";

    if (!o->names.empty()) {
      *os << "names={";
      for (const auto& name : o->names) {
        *os << name << ",";
      }
      *os << "}";
    }
    return;
  }

  if (factory_name == "aggregate") {
    auto o = &OptionsAs<AggregateNodeOptions>(opts);

    *os << "aggregates={";
    for (const auto& agg : o->aggregates) {
      *os << "function=" << agg.function << "<";
      if (agg.options) PrintTo(*agg.options, os);
      *os << ">,";
      *os << "target=" << agg.target.ToString() << ",";
      *os << "name=" << agg.name;
    }
    *os << "},";

    if (!o->keys.empty()) {
      *os << ",keys={";
      for (const auto& key : o->keys) {
        *os << key.ToString() << ",";
      }
      *os << "}";
    }
    return;
  }

  if (factory_name == "order_by_sink") {
    auto o = &OptionsAs<OrderBySinkNodeOptions>(opts);
    if (o->generator) {
      *os << "NON_NULL_GENERATOR,";
    }
    return PrintTo(o->sort_options, os);
  }

  Unreachable("debug printing is not supported for all ExecNodeOptions");
}

void PrintTo(const Declaration& decl, std::ostream* os) {
  *os << decl.factory_name;

  if (decl.label != decl.factory_name) {
    *os << ":" << decl.label;
  }

  *os << "<";
  PrintToImpl(decl.factory_name, *decl.options, os);
  *os << ">";

  *os << "{";
  for (const auto& input : decl.inputs) {
    if (auto decl = util::get_if<Declaration>(&input)) {
      PrintTo(*decl, os);
    }
  }
  *os << "}";
}

Result<std::shared_ptr<Table>> MakeRandomTimeSeriesTable(
    const TableGenerationProperties& properties) {
  int total_columns = properties.num_columns + 2;
  std::vector<std::shared_ptr<Array>> columns;
  columns.reserve(total_columns);
  arrow::FieldVector field_vector;
  field_vector.reserve(total_columns);

  field_vector.push_back(field("time", int64()));
  field_vector.push_back(field("id", int32()));
  Int64Builder time_column_builder;
  Int32Builder id_column_builder;
  for (int64_t time = properties.start; time <= properties.end;
       time += properties.time_frequency) {
    for (int32_t id = 0; id < properties.num_ids; id++) {
      ARROW_RETURN_NOT_OK(time_column_builder.Append(time));
      ARROW_RETURN_NOT_OK(id_column_builder.Append(id));
    }
  }

  int64_t num_rows = time_column_builder.length();
  ARROW_ASSIGN_OR_RAISE(auto time_column, time_column_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(auto id_column, id_column_builder.Finish());
  columns.push_back(std::move(time_column));
  columns.push_back(std::move(id_column));

  for (int i = 0; i < properties.num_columns; i++) {
    field_vector.push_back(
        field(properties.column_prefix + std::to_string(i), float64()));
    random::RandomArrayGenerator rand(properties.seed + i);
    columns.push_back(
        rand.Float64(num_rows, properties.min_column_value, properties.max_column_value));
  }
  std::shared_ptr<arrow::Schema> schema = arrow::schema(std::move(field_vector));
  return Table::Make(schema, columns, num_rows);
}

}  // namespace compute
}  // namespace arrow

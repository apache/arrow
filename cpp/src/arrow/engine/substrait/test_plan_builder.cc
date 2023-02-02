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

#include "arrow/engine/substrait/test_plan_builder.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/plan_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"

#include "substrait/algebra.pb.h"

namespace arrow {
namespace engine {
namespace internal {

static const ConversionOptions kPlanBuilderConversionOptions;

Result<std::unique_ptr<substrait::ReadRel>> CreateRead(const Table& table,
                                                       ExtensionSet* ext_set) {
  auto read = std::make_unique<substrait::ReadRel>();

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::NamedStruct> schema,
                        ToProto(*table.schema(), ext_set, kPlanBuilderConversionOptions));
  read->set_allocated_base_schema(schema.release());

  auto named_table = std::make_unique<substrait::ReadRel::NamedTable>();
  named_table->add_names("test");
  read->set_allocated_named_table(named_table.release());

  return read;
}

void CreateDirectReference(int32_t index, substrait::Expression* expr) {
  auto reference = std::make_unique<substrait::Expression::FieldReference>();
  auto reference_segment = std::make_unique<substrait::Expression::ReferenceSegment>();
  auto struct_field =
      std::make_unique<substrait::Expression::ReferenceSegment::StructField>();
  struct_field->set_field(index);
  reference_segment->set_allocated_struct_field(struct_field.release());
  reference->set_allocated_direct_reference(reference_segment.release());

  auto root_reference =
      std::make_unique<substrait::Expression::FieldReference::RootReference>();
  reference->set_allocated_root_reference(root_reference.release());
  expr->set_allocated_selection(reference.release());
}

Result<std::unique_ptr<substrait::ProjectRel>> CreateProject(
    Id function_id, const std::vector<std::string>& arguments,
    const std::unordered_map<std::string, std::vector<std::string>> options,
    const std::vector<std::shared_ptr<DataType>>& arg_types, const DataType& output_type,
    ExtensionSet* ext_set) {
  auto project = std::make_unique<substrait::ProjectRel>();

  auto call = std::make_unique<substrait::Expression::ScalarFunction>();
  ARROW_ASSIGN_OR_RAISE(uint32_t function_anchor, ext_set->EncodeFunction(function_id));
  call->set_function_reference(function_anchor);

  std::size_t arg_index = 0;
  std::size_t table_arg_index = 0;
  for (const std::shared_ptr<DataType>& arg_type : arg_types) {
    substrait::FunctionArgument* argument = call->add_arguments();
    if (arg_type) {
      // If it has a type then it's a reference to the input table
      auto expression = std::make_unique<substrait::Expression>();
      CreateDirectReference(static_cast<int32_t>(table_arg_index++), expression.get());
      argument->set_allocated_value(expression.release());
    } else {
      // If it doesn't have a type then it's an enum
      const std::string& enum_value = arguments[arg_index];
      argument->set_enum_(enum_value);
    }
    arg_index++;
  }
  for (const auto& opt : options) {
    substrait::FunctionOption* option = call->add_options();
    option->set_name(opt.first);
    for (const std::string& pref : opt.second) {
      option->add_preference(pref);
    }
  }

  ARROW_ASSIGN_OR_RAISE(
      std::unique_ptr<substrait::Type> output_type_substrait,
      ToProto(output_type, /*nullable=*/true, ext_set, kPlanBuilderConversionOptions));
  call->set_allocated_output_type(output_type_substrait.release());

  substrait::Expression* call_expression = project->add_expressions();
  call_expression->set_allocated_scalar_function(call.release());

  return project;
}

Result<std::unique_ptr<substrait::AggregateRel>> CreateAgg(Id function_id,
                                                           const std::vector<int>& keys,
                                                           std::vector<int> arg_idxs,
                                                           const DataType& output_type,
                                                           ExtensionSet* ext_set) {
  auto agg = std::make_unique<substrait::AggregateRel>();

  if (!keys.empty()) {
    substrait::AggregateRel::Grouping* grouping = agg->add_groupings();
    for (int key : keys) {
      substrait::Expression* key_expr = grouping->add_grouping_expressions();
      CreateDirectReference(key, key_expr);
    }
  }

  substrait::AggregateRel::Measure* measure_wrapper = agg->add_measures();
  auto agg_func = std::make_unique<substrait::AggregateFunction>();
  ARROW_ASSIGN_OR_RAISE(uint32_t function_anchor, ext_set->EncodeFunction(function_id));

  agg_func->set_function_reference(function_anchor);

  for (int arg_idx : arg_idxs) {
    substrait::FunctionArgument* arg = agg_func->add_arguments();
    auto arg_expr = std::make_unique<substrait::Expression>();
    CreateDirectReference(arg_idx, arg_expr.get());
    arg->set_allocated_value(arg_expr.release());
  }

  agg_func->set_phase(substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  agg_func->set_invocation(
      substrait::AggregateFunction::AggregationInvocation::
          AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_ALL);

  ARROW_ASSIGN_OR_RAISE(
      std::unique_ptr<substrait::Type> output_type_substrait,
      ToProto(output_type, /*nullable=*/true, ext_set, kPlanBuilderConversionOptions));
  agg_func->set_allocated_output_type(output_type_substrait.release());
  measure_wrapper->set_allocated_measure(agg_func.release());

  return agg;
}

std::unique_ptr<substrait::Version> CreateTestVersion() {
  auto version = std::make_unique<substrait::Version>();
  version->set_major_number(std::numeric_limits<uint32_t>::max());
  version->set_minor_number(std::numeric_limits<uint32_t>::max());
  version->set_patch_number(std::numeric_limits<uint32_t>::max());
  version->set_producer("Arrow unit test");
  return version;
}

Result<std::unique_ptr<substrait::Plan>> CreatePlan(std::unique_ptr<substrait::Rel> root,
                                                    ExtensionSet* ext_set) {
  auto plan = std::make_unique<substrait::Plan>();
  plan->set_allocated_version(CreateTestVersion().release());

  substrait::PlanRel* plan_rel = plan->add_relations();
  auto rel_root = std::make_unique<substrait::RelRoot>();
  rel_root->set_allocated_input(root.release());
  plan_rel->set_allocated_root(rel_root.release());

  ARROW_RETURN_NOT_OK(AddExtensionSetToPlan(*ext_set, plan.get()));
  return plan;
}

Result<std::shared_ptr<Buffer>> CreateScanProjectSubstrait(
    Id function_id, const std::shared_ptr<Table>& input_table,
    const std::vector<std::string>& arguments,
    const std::unordered_map<std::string, std::vector<std::string>>& options,
    const std::vector<std::shared_ptr<DataType>>& data_types,
    const DataType& output_type) {
  ExtensionSet ext_set;
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::ReadRel> read,
                        CreateRead(*input_table, &ext_set));
  ARROW_ASSIGN_OR_RAISE(
      std::unique_ptr<substrait::ProjectRel> project,
      CreateProject(function_id, arguments, options, data_types, output_type, &ext_set));

  auto read_rel = std::make_unique<substrait::Rel>();
  read_rel->set_allocated_read(read.release());
  project->set_allocated_input(read_rel.release());

  auto project_rel = std::make_unique<substrait::Rel>();
  project_rel->set_allocated_project(project.release());

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::Plan> plan,
                        CreatePlan(std::move(project_rel), &ext_set));
  return Buffer::FromString(plan->SerializeAsString());
}

Result<std::shared_ptr<Buffer>> CreateScanAggSubstrait(
    Id function_id, const std::shared_ptr<Table>& input_table,
    const std::vector<int>& key_idxs, const std::vector<int>& arg_idxs,
    const DataType& output_type) {
  ExtensionSet ext_set;

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::ReadRel> read,
                        CreateRead(*input_table, &ext_set));
  ARROW_ASSIGN_OR_RAISE(
      std::unique_ptr<substrait::AggregateRel> agg,
      CreateAgg(function_id, key_idxs, arg_idxs, output_type, &ext_set));

  auto read_rel = std::make_unique<substrait::Rel>();
  read_rel->set_allocated_read(read.release());
  agg->set_allocated_input(read_rel.release());

  auto agg_rel = std::make_unique<substrait::Rel>();
  agg_rel->set_allocated_aggregate(agg.release());

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::Plan> plan,
                        CreatePlan(std::move(agg_rel), &ext_set));
  return Buffer::FromString(plan->SerializeAsString());
}

}  // namespace internal
}  // namespace engine
}  // namespace arrow

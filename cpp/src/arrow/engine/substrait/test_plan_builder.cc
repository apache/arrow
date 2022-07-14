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

#include <cstdint>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/engine/substrait/plan_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/util/macros.h"
#include "arrow/util/make_unique.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"

namespace arrow {

using internal::make_unique;

namespace engine {
namespace internal {

Result<std::unique_ptr<substrait::ReadRel>> CreateRead(const Table& table,
                                                       ExtensionSet* ext_set) {
  auto read = make_unique<substrait::ReadRel>();

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::NamedStruct> schema,
                        ToProto(*table.schema(), ext_set));
  read->set_allocated_base_schema(schema.release());

  auto named_table = make_unique<substrait::ReadRel::NamedTable>();
  named_table->add_names("test");
  read->set_allocated_named_table(named_table.release());

  return read;
}

Result<std::unique_ptr<substrait::ProjectRel>> CreateProject(
    Id function_id, const std::vector<std::string>& arguments,
    const std::vector<std::shared_ptr<DataType>>& arg_types, const DataType& output_type,
    ExtensionSet* ext_set) {
  auto project = make_unique<substrait::ProjectRel>();

  auto call = make_unique<substrait::Expression::ScalarFunction>();
  ARROW_ASSIGN_OR_RAISE(uint32_t function_anchor, ext_set->EncodeFunction(function_id));
  call->set_function_reference(function_anchor);

  std::size_t arg_index = 0;
  std::size_t table_arg_index = 0;
  for (const std::shared_ptr<DataType>& arg_type : arg_types) {
    substrait::FunctionArgument* argument = call->add_arguments();
    if (arg_type) {
      // If it has a type then it's a reference to the input table
      auto expression = make_unique<substrait::Expression>();
      auto reference = make_unique<substrait::Expression::FieldReference>();
      auto reference_segment = make_unique<substrait::Expression::ReferenceSegment>();
      auto struct_field =
          make_unique<substrait::Expression::ReferenceSegment::StructField>();
      struct_field->set_field(static_cast<int32_t>(table_arg_index++));
      reference_segment->set_allocated_struct_field(struct_field.release());
      reference->set_allocated_direct_reference(reference_segment.release());

      auto root_reference =
          make_unique<substrait::Expression::FieldReference::RootReference>();
      reference->set_allocated_root_reference(root_reference.release());
      expression->set_allocated_selection(reference.release());
      argument->set_allocated_value(expression.release());
    } else {
      // If it doesn't have a type then it's an enum
      const std::string& enum_value = arguments[arg_index];
      auto enum_ = make_unique<substrait::FunctionArgument::Enum>();
      if (enum_value.size() > 0) {
        enum_->set_specified(enum_value);
      } else {
        auto unspecified = make_unique<google::protobuf::Empty>();
        enum_->set_allocated_unspecified(unspecified.release());
      }
      argument->set_allocated_enum_(enum_.release());
    }
    arg_index++;
  }

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::Type> output_type_substrait,
                        ToProto(output_type, /*nullable=*/true, ext_set));
  call->set_allocated_output_type(output_type_substrait.release());

  substrait::Expression* call_expression = project->add_expressions();
  call_expression->set_allocated_scalar_function(call.release());

  return project;
}

Result<std::unique_ptr<substrait::Plan>> CreatePlan(
    std::unique_ptr<substrait::ReadRel> read,
    std::unique_ptr<substrait::ProjectRel> project, ExtensionSet* ext_set) {
  auto plan = make_unique<substrait::Plan>();

  auto read_rel = make_unique<substrait::Rel>();
  read_rel->set_allocated_read(read.release());
  project->set_allocated_input(read_rel.release());

  auto project_rel = make_unique<substrait::Rel>();
  project_rel->set_allocated_project(project.release());

  substrait::PlanRel* plan_rel = plan->add_relations();
  auto rel_root = make_unique<substrait::RelRoot>();
  rel_root->set_allocated_input(project_rel.release());
  plan_rel->set_allocated_root(rel_root.release());

  ARROW_RETURN_NOT_OK(AddExtensionSetToPlan(*ext_set, plan.get()));
  return plan;
}

Result<std::shared_ptr<Buffer>> CreateScanProjectSubstrait(
    Id function_id, const std::shared_ptr<Table>& input_table,
    const std::vector<std::string>& arguments,
    const std::vector<std::shared_ptr<DataType>>& data_types,
    const DataType& output_type) {
  ExtensionSet extension_set;
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::ReadRel> read,
                        CreateRead(*input_table, &extension_set));
  ARROW_ASSIGN_OR_RAISE(
      std::unique_ptr<substrait::ProjectRel> project,
      CreateProject(function_id, arguments, data_types, output_type, &extension_set));
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::Plan> plan,
                        CreatePlan(std::move(read), std::move(project), &extension_set));
  return Buffer::FromString(plan->SerializeAsString());
}

}  // namespace internal
}  // namespace engine
}  // namespace arrow

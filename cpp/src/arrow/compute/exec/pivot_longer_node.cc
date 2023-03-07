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

#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

// A row template that's been bound to a schema
struct BoundRowTemplate {
  std::vector<std::string> feature_values;
  std::vector<std::optional<FieldPath>> measurement_paths;

  static Result<BoundRowTemplate> Make(const PivotLongerRowTemplate& unbound,
                                       const Schema& schema) {
    std::vector<std::optional<FieldPath>> measurement_paths;
    for (const auto& meas_ref : unbound.measurement_values) {
      if (meas_ref) {
        ARROW_ASSIGN_OR_RAISE(FieldPath meas_path, meas_ref->FindOne(schema));
        if (meas_path.indices().size() > 1) {
          // This should actually be pretty easy to add.  Just need to figure out how to
          // convert from a nested measurement ref to a column in an exec batch
          return Status::NotImplemented("nested measurement refs in pivot longer node");
        }
        measurement_paths.push_back(std::move(meas_path));
      } else {
        measurement_paths.push_back(std::nullopt);
      }
    }
    return BoundRowTemplate(unbound.feature_values, std::move(measurement_paths));
  }

 private:
  BoundRowTemplate(std::vector<std::string> feature_values,
                   std::vector<std::optional<FieldPath>> measurement_paths)
      : feature_values(std::move(feature_values)),
        measurement_paths(std::move(measurement_paths)) {}
};

class PivotLongerNode : public ExecNode, public TracedNode {
 public:
  static Result<std::shared_ptr<Schema>> MakeOutputSchema(
      const PivotLongerNodeOptions& options,
      const std::shared_ptr<Schema>& input_schema) {
    // Some of this is pure validation and not strictly needed to create the output schema
    // but it's simpler to just do all validation here than to try and split between this
    // method and PivotLongerNode::Make
    if (options.row_templates.empty()) {
      return Status::Invalid("There must be at least one row template");
    }
    if (options.feature_field_names.empty() || options.measurement_field_names.empty()) {
      return Status::Invalid(
          "There must be at least one feature column and one measurement column and they "
          "must "
          "have names");
    }

    for (const auto& row_template : options.row_templates) {
      if (row_template.feature_values.size() != options.feature_field_names.size()) {
        return Status::Invalid("There were names given for ",
                               options.feature_field_names.size(),
                               " feature columns but one of the row templates only had ",
                               row_template.feature_values.size(), " feature values");
      }
      if (row_template.measurement_values.size() !=
          options.measurement_field_names.size()) {
        return Status::Invalid(
            "There were names given for ", options.measurement_field_names.size(),
            " measurement columns but one of the row templates only had ",
            row_template.measurement_values.size(), " field references");
      }
    }

    std::vector<std::shared_ptr<Field>> fields(input_schema->fields());
    for (const auto& name : options.feature_field_names) {
      fields.push_back(field(name, utf8()));
    }
    std::vector<std::shared_ptr<DataType>> measurement_types(
        options.measurement_field_names.size());
    for (const auto& row_template : options.row_templates) {
      for (std::size_t i = 0; i < row_template.measurement_values.size(); i++) {
        if (!row_template.measurement_values[i].has_value()) {
          continue;
        }
        ARROW_ASSIGN_OR_RAISE(FieldPath meas_path,
                              row_template.measurement_values[i]->FindOne(*input_schema));
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Field> meas_field,
                              meas_path.Get(*input_schema));
        if (measurement_types[i]) {
          if (!measurement_types[i]->Equals(meas_field->type())) {
            return Status::Invalid(
                "Mixed measurement types at measurement index ", i,
                ".  Some row templates had the type ", measurement_types[i]->ToString(),
                " but later row templates had the type ", meas_field->type()->ToString(),
                ".  All row templates must reference the same type for a measurement "
                "column.");
          }
        } else {
          measurement_types[i] = meas_field->type();
        }
      }
    }
    for (std::size_t i = 0; i < measurement_types.size(); i++) {
      if (!measurement_types[i]) {
        return Status::Invalid(
            "All row templates had nullopt for the meausrement column at index ", i, " (",
            options.measurement_field_names[i], ")");
      }
      fields.push_back(
          field(options.measurement_field_names[i], std::move(measurement_types[i])));
    }
    return schema(std::move(fields));
  }

  PivotLongerNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                  std::shared_ptr<Schema> output_schema, PivotLongerNodeOptions options,
                  std::vector<BoundRowTemplate> templates)
      : ExecNode(plan, std::move(inputs), {"input"}, std::move(output_schema)),
        TracedNode(this),
        options_(std::move(options)),
        templates_(std::move(templates)) {
    // Relies on the fact that the measurement types are the last fields
    // in the output schema.
    std::size_t num_out_fields = output_schema_->fields().size();
    std::size_t num_measurements = options_.measurement_field_names.size();
    std::size_t meas_offset = num_out_fields - num_measurements;
    for (std::size_t i = 0; i < num_measurements; i++) {
      meas_types_.push_back(output_schema_->fields()[meas_offset + i]->type());
    }
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "PivotLongerNode"));

    const auto& pivot_options = checked_cast<const PivotLongerNodeOptions&>(options);

    std::vector<BoundRowTemplate> templates;
    for (const auto& row_template : pivot_options.row_templates) {
      ARROW_ASSIGN_OR_RAISE(
          BoundRowTemplate bound_template,
          BoundRowTemplate::Make(row_template, *inputs[0]->output_schema()));
      templates.push_back(std::move(bound_template));
    }

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Schema> output_schema,
        PivotLongerNode::MakeOutputSchema(pivot_options, inputs[0]->output_schema()));
    return plan->EmplaceNode<PivotLongerNode>(plan, std::move(inputs),
                                              std::move(output_schema), pivot_options,
                                              std::move(templates));
  }

  const char* kind_name() const override { return "PivotLongerNode"; }

  Status InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    return output_->InputFinished(
        this, total_batches * static_cast<int>(options_.row_templates.size()));
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }

  Status StopProducingImpl() override { return Status::OK(); }

  ExecBatch ApplyTemplate(const BoundRowTemplate& templ, const ExecBatch& input) const {
    std::vector<Datum> values(input.values);
    for (const auto& feature_val : templ.feature_values) {
      // Feature names are added as string scalars
      values.push_back(Datum(feature_val));
    }
    for (std::size_t meas_idx = 0; meas_idx < templ.measurement_paths.size();
         meas_idx++) {
      const std::optional<FieldPath>& opt_meas_path = templ.measurement_paths[meas_idx];
      if (opt_meas_path) {
        values.push_back(input.values[opt_meas_path->indices()[0]]);
      } else {
        values.push_back(MakeNullScalar(meas_types_[meas_idx]));
      }
    }
    return ExecBatch(std::move(values), input.length);
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);
    DCHECK_EQ(input, inputs_[0]);
    for (const auto& row_template : templates_) {
      ExecBatch template_batch = ApplyTemplate(row_template, batch);
      ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(template_batch)));
    }
    return Status::OK();
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    ss << "features=[";
    bool first = true;
    for (const auto& name : options_.feature_field_names) {
      if (first) {
        first = false;
      } else {
        ss << ", ";
      }
      ss << name;
    }
    ss << "] measurements=[";
    first = true;
    for (const auto& name : options_.measurement_field_names) {
      if (first) {
        first = false;
      } else {
        ss << ", ";
      }
      ss << name;
    }
    ss << "]";
    return ss.str();
  }

 private:
  PivotLongerNodeOptions options_;
  std::vector<BoundRowTemplate> templates_;
  std::vector<std::shared_ptr<DataType>> meas_types_;
};

}  // namespace

namespace internal {

void RegisterPivotLongerNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory(std::string(PivotLongerNodeOptions::kName),
                                 PivotLongerNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/exec/exec_plan.h"

#include <sstream>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

class ProjectNode : public MapNode {
 public:
  ProjectNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
              std::shared_ptr<Schema> output_schema, std::vector<Expression> exprs,
              bool async_mode)
      : MapNode(plan, std::move(inputs), std::move(output_schema), async_mode),
        exprs_(std::move(exprs)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "ProjectNode"));

    const auto& project_options = checked_cast<const ProjectNodeOptions&>(options);
    auto exprs = project_options.expressions;
    auto names = project_options.names;

    if (names.size() == 0) {
      names.resize(exprs.size());
      for (size_t i = 0; i < exprs.size(); ++i) {
        names[i] = exprs[i].ToString();
      }
    }

    FieldVector fields(exprs.size());
    int i = 0;
    for (auto& expr : exprs) {
      if (!expr.IsBound()) {
        ARROW_ASSIGN_OR_RAISE(expr, expr.Bind(*inputs[0]->output_schema()));
      }
      fields[i] = field(std::move(names[i]), expr.type());
      ++i;
    }
    return plan->EmplaceNode<ProjectNode>(plan, std::move(inputs),
                                          schema(std::move(fields)), std::move(exprs),
                                          project_options.async_mode);
  }

  const char* kind_name() const override { return "ProjectNode"; }

  Result<ExecBatch> DoProject(const ExecBatch& target) {
    std::vector<Datum> values{exprs_.size()};
    for (size_t i = 0; i < exprs_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(Expression simplified_expr,
                            SimplifyWithGuarantee(exprs_[i], target.guarantee));

      ARROW_ASSIGN_OR_RAISE(values[i], ExecuteScalarExpression(simplified_expr, target,
                                                               plan()->exec_context()));
    }
    return ExecBatch{std::move(values), target.length};
  }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);
    auto func = [this](ExecBatch batch) { return DoProject(std::move(batch)); };
    this->SubmitTask(std::move(func), std::move(batch));
  }

 protected:
  std::string ToStringExtra() const override {
    std::stringstream ss;
    ss << "projection=[";
    for (int i = 0; static_cast<size_t>(i) < exprs_.size(); i++) {
      if (i > 0) ss << ", ";
      auto repr = exprs_[i].ToString();
      if (repr != output_schema_->field(i)->name()) {
        ss << '"' << output_schema_->field(i)->name() << "\": ";
      }
      ss << repr;
    }
    ss << ']';
    return ss.str();
  }

 private:
  std::vector<Expression> exprs_;
};

}  // namespace

namespace internal {

void RegisterProjectNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("project", ProjectNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

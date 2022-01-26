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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
namespace arrow {

using internal::checked_cast;

namespace compute {
namespace {

class FilterNode : public MapNode {
 public:
  FilterNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
             std::shared_ptr<Schema> output_schema, Expression filter, bool async_mode)
      : MapNode(plan, std::move(inputs), std::move(output_schema), async_mode),
        filter_(std::move(filter)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "FilterNode"));
    auto schema = inputs[0]->output_schema();

    const auto& filter_options = checked_cast<const FilterNodeOptions&>(options);

    auto filter_expression = filter_options.filter_expression;
    if (!filter_expression.IsBound()) {
      ARROW_ASSIGN_OR_RAISE(filter_expression, filter_expression.Bind(*schema));
    }

    if (filter_expression.type()->id() != Type::BOOL) {
      return Status::TypeError("Filter expression must evaluate to bool, but ",
                               filter_expression.ToString(), " evaluates to ",
                               filter_expression.type()->ToString());
    }
    return plan->EmplaceNode<FilterNode>(plan, std::move(inputs), std::move(schema),
                                         std::move(filter_expression),
                                         filter_options.async_mode);
  }

  const char* kind_name() const override { return "FilterNode"; }

  Result<ExecBatch> DoFilter(const ExecBatch& target) {
    ARROW_ASSIGN_OR_RAISE(Expression simplified_filter,
                          SimplifyWithGuarantee(filter_, target.guarantee));

    ARROW_ASSIGN_OR_RAISE(Datum mask, ExecuteScalarExpression(simplified_filter, target,
                                                              plan()->exec_context()));

    if (mask.is_scalar()) {
      const auto& mask_scalar = mask.scalar_as<BooleanScalar>();
      if (mask_scalar.is_valid && mask_scalar.value) {
        return target;
      }

      return target.Slice(0, 0);
    }

    // if the values are all scalar then the mask must also be
    DCHECK(!std::all_of(target.values.begin(), target.values.end(),
                        [](const Datum& value) { return value.is_scalar(); }));

    auto values = target.values;
    for (auto& value : values) {
      if (value.is_scalar()) continue;
      ARROW_ASSIGN_OR_RAISE(value, Filter(value, mask, FilterOptions::Defaults()));
    }
    return ExecBatch::Make(std::move(values));
  }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);
    auto func = [this](ExecBatch batch) { return DoFilter(std::move(batch)); };
    this->SubmitTask(std::move(func), std::move(batch));
  }

 protected:
  std::string ToStringExtra(int indent) const override {
    return "filter=" + filter_.ToString();
  }

 private:
  Expression filter_;
};
}  // namespace

namespace internal {
void RegisterFilterNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("filter", FilterNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

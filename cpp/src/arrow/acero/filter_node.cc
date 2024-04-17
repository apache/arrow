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

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

using compute::FilterOptions;

namespace acero {
namespace {

class FilterNode : public MapNode {
 public:
  FilterNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
             std::shared_ptr<Schema> output_schema, Expression filter)
      : MapNode(plan, std::move(inputs), std::move(output_schema)),
        filter_(std::move(filter)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "FilterNode"));
    auto schema = inputs[0]->output_schema();

    const auto& filter_options = checked_cast<const FilterNodeOptions&>(options);

    auto filter_expression = filter_options.filter_expression;
    if (!filter_expression.IsBound()) {
      ARROW_ASSIGN_OR_RAISE(
          filter_expression,
          filter_expression.Bind(*schema, plan->query_context()->exec_context()));
    }

    if (filter_expression.type()->id() != Type::BOOL) {
      return Status::TypeError("Filter expression must evaluate to bool, but ",
                               filter_expression.ToString(), " evaluates to ",
                               filter_expression.type()->ToString());
    }
    return plan->EmplaceNode<FilterNode>(plan, std::move(inputs), std::move(schema),
                                         std::move(filter_expression));
  }

  const char* kind_name() const override { return "FilterNode"; }

  Result<ExecBatch> ProcessBatch(ExecBatch batch) override {
    return ExecuteFilterBatch(filter_, std::move(batch),
                              plan()->query_context()->exec_context());
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
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
}  // namespace acero
}  // namespace arrow

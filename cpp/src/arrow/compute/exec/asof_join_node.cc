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

#include <unordered_set>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/asof_join.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace compute {

class AsofJoinNode : public ExecNode {
 public:
  AsofJoinNode(ExecPlan* plan,
               NodeVector inputs,
               const AsofJoinNodeOptions& join_options,
               std::shared_ptr<Schema> output_schema,
               std::unique_ptr<AsofJoinSchema> schema_mgr,
               std::unique_ptr<AsofJoinImpl> impl
               )
    : ExecNode(plan, inputs, {"left", "right"},
               /*output_schema=*/std::move(output_schema),
               /*num_outputs=*/1),
      impl_(std::move(impl)) {
    complete_.store(false);
  }

  static arrow::Result<ExecNode*> Make(ExecPlan *plan, std::vector<ExecNode*> inputs,
                                       const ExecNodeOptions &options) {
    std::unique_ptr<AsofJoinSchema> schema_mgr =
      ::arrow::internal::make_unique<AsofJoinSchema>();

    const auto& join_options = checked_cast<const AsofJoinNodeOptions&>(options);
    std::shared_ptr<Schema> output_schema = schema_mgr->MakeOutputSchema(inputs, join_options);
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<AsofJoinImpl> impl, AsofJoinImpl::MakeBasic());

    return plan->EmplaceNode<AsofJoinNode>(
                                           plan, inputs, join_options, std::move(output_schema), std::move(schema_mgr),
                                           std::move(impl)
                                           );
  }

  const char* kind_name() const override { return "AsofJoinNode"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {}
  void ErrorReceived(ExecNode* input, Status error) override {}
  void InputFinished(ExecNode* input, int total_batches) override {}
  Status StartProducing() override { return Status::OK();}
  void PauseProducing(ExecNode* output) override {}
  void ResumeProducing(ExecNode* output) override {}
  void StopProducing(ExecNode* output) override {}
  void StopProducing() override {}
  Future<> finished() override { return finished_; }

 private:
  std::atomic<bool> complete_;
  std::unique_ptr<AsofJoinSchema> schema_mgr_;
  std::unique_ptr<AsofJoinImpl> impl_;
};

std::shared_ptr<Schema> AsofJoinSchema::MakeOutputSchema(const std::vector<ExecNode*>& inputs,
                                                         const AsofJoinNodeOptions& options) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  assert(inputs.size() > 1);

  std::vector<std::string> keys(options.keys.size());

  // Directly map LHS fields
  for(int i = 0; i < inputs[0]->output_schema()->num_fields(); ++i)
    fields.push_back(inputs[0]->output_schema()->field(i));

  // Take all non-key, non-time RHS fields
  for(size_t j = 1; j < inputs.size(); ++j) {
    const auto &input_schema = inputs[j]->output_schema();
    for(int i = 0; i < input_schema->num_fields(); ++i) {
      const auto &name = input_schema->field(i)->name();
      if((std::find(keys.begin(), keys.end(), name) != keys.end()) && (name!= *options.time.name())) {
        fields.push_back(input_schema->field(i));
      }
    }
  }

  // Combine into a schema
  return std::make_shared<arrow::Schema>(fields);
}

namespace internal {
  void RegisterAsofJoinNode(ExecFactoryRegistry* registry) {
    DCHECK_OK(registry->AddFactory("asofjoin", AsofJoinNode::Make));
  }
}

}  // namespace compute
}  // namespace arrow

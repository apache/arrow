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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>

namespace compute = ::arrow::compute;

#if defined(ARROW_R_WITH_DATASET)

#include <arrow/dataset/scanner.h>

std::shared_ptr<compute::ExecNode> StartExecPlan(
    std::shared_ptr<arrow::dataset::Dataset> dataset) {
  auto plan = ValueOrStop(compute::ExecPlan::Make());
  // TODO: pass in ScanOptions by file type
  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  return std::shared_ptr<compute::ExecNode>(
      ValueOrStop(arrow::dataset::MakeScanNode(plan.get(), dataset, options)));
}

#endif

std::shared_ptr<compute::ExecNode> ExecNode_Filter(
    std::shared_ptr<compute::ExecNode> input,
    std::shared_ptr<compute::Expression> filter) {
  return std::shared_ptr<compute::ExecNode>(
      ValueOrStop(compute::MakeFilterNode(input.get(), /*label=*/"filter", *filter)),
      /* empty destructor: ExecNode lifetime is managed by an ExecPlan */
      [](...) {});
}

std::shared_ptr<compute::ExecNode> ExecNode_Project(
    std::shared_ptr<compute::ExecNode> input,
    std::vector<std::shared_ptr<compute::Expression>> exprs,
    std::vector<std::string> names = {}) {
  // We have shared_ptrs of expressions but need the Expressions
  std::vector<compute::Expression> expressions;
  for (auto expr : exprs) {
    expressions.push_back(*expr);
  }
  return std::shared_ptr<compute::ExecNode>(
      ValueOrStop(
          compute::MakeProjectNode(input.get(), /*label=*/"project", expressions, names)),
      [](...) {});
}

std::shared_ptr<compute::ExecNode> ExecNode_ScalarAggregate(
    std::shared_ptr<compute::ExecNode> input,
    std::vector<arrow::compute::internal::Aggregate> aggregates);

// ARROW_EXPORT
// Result<ExecNode*> MakeScalarAggregateNode(ExecNode* input, std::string label,
//                                           std::vector<internal::Aggregate> aggregates);

#endif

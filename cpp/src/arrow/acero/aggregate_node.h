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

#include <memory>
#include <vector>

#include "arrow/acero/visibility.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace acero {
namespace aggregate {

using compute::Aggregate;
using compute::default_exec_context;
using compute::ExecContext;
using compute::Kernel;
using compute::KernelState;
using compute::RowSegmenter;

struct ARROW_ACERO_EXPORT AggregateNodeArgs {
  std::shared_ptr<Schema> output_schema;
  std::vector<int> grouping_key_field_ids;
  std::vector<int> segment_key_field_ids;
  std::unique_ptr<RowSegmenter> segmenter;
  std::vector<std::vector<int>> target_fieldsets;
  std::vector<Aggregate> aggregates;
  std::vector<const Kernel*> kernels;
  std::vector<std::vector<TypeHolder>> kernel_intypes;
  std::vector<std::vector<std::unique_ptr<KernelState>>> states;
};

/// \brief Make the arguments of an aggregate node
///
/// \param[in] input_schema the schema of the input to the node
/// \param[in] keys the grouping keys for the aggregation
/// \param[in] segment_keys the segmenting keys for the aggregation
/// \param[in] num_states_per_kernel number of states per kernel for the aggregation
/// \param[in] exec_ctx the execution context for the aggregation
ARROW_ACERO_EXPORT Result<AggregateNodeArgs> MakeAggregateNodeArgs(
    const Schema& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggregates,
    size_t num_states_per_kernel = 1, ExecContext* exec_ctx = default_exec_context());

/// \brief Make the output schema of an aggregate node
///
/// \param[in] input_schema the schema of the input to the node
/// \param[in] keys the grouping keys for the aggregation
/// \param[in] segment_keys the segmenting keys for the aggregation
/// \param[in] num_states_per_kernel number of states per kernel for the aggregation
/// \param[in] exec_ctx the execution context for the aggregation
ARROW_ACERO_EXPORT Result<std::shared_ptr<Schema>> MakeOutputSchema(
    const Schema& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggregates,
    size_t num_states_per_kernel, ExecContext* exec_ctx = default_exec_context());

}  // namespace aggregate
}  // namespace acero
}  // namespace arrow

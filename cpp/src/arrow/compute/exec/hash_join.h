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

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

class ARROW_EXPORT HashJoinSchema {
 public:
  Status Init(JoinType join_type, const Schema& left_schema,
              const std::vector<FieldRef>& left_keys, const Schema& right_schema,
              const std::vector<FieldRef>& right_keys, const Expression& filter,
              const std::string& left_field_name_prefix,
              const std::string& right_field_name_prefix);

  Status Init(JoinType join_type, const Schema& left_schema,
              const std::vector<FieldRef>& left_keys,
              const std::vector<FieldRef>& left_output, const Schema& right_schema,
              const std::vector<FieldRef>& right_keys,
              const std::vector<FieldRef>& right_output, const Expression& filter,
              const std::string& left_field_name_prefix,
              const std::string& right_field_name_prefix);

  static Status ValidateSchemas(JoinType join_type, const Schema& left_schema,
                                const std::vector<FieldRef>& left_keys,
                                const std::vector<FieldRef>& left_output,
                                const Schema& right_schema,
                                const std::vector<FieldRef>& right_keys,
                                const std::vector<FieldRef>& right_output,
                                const std::string& left_field_name_prefix,
                                const std::string& right_field_name_prefix);

  Result<Expression> BindFilter(Expression filter, const Schema& left_schema,
                                const Schema& right_schema);
  std::shared_ptr<Schema> MakeOutputSchema(const std::string& left_field_name_prefix,
                                           const std::string& right_field_name_prefix);

  bool LeftPayloadIsEmpty() { return PayloadIsEmpty(0); }

  bool RightPayloadIsEmpty() { return PayloadIsEmpty(1); }

  static int kMissingField() {
    return SchemaProjectionMaps<HashJoinProjection>::kMissingField;
  }

  SchemaProjectionMaps<HashJoinProjection> proj_maps[2];

 private:
  static bool IsTypeSupported(const DataType& type);

  Status CollectFilterColumns(std::vector<FieldRef>& left_filter,
                              std::vector<FieldRef>& right_filter,
                              const Expression& filter, const Schema& left_schema,
                              const Schema& right_schema);

  Expression RewriteFilterToUseFilterSchema(int right_filter_offset,
                                            const SchemaProjectionMap& left_to_filter,
                                            const SchemaProjectionMap& right_to_filter,
                                            const Expression& filter);

  bool PayloadIsEmpty(int side) {
    ARROW_DCHECK(side == 0 || side == 1);
    return proj_maps[side].num_cols(HashJoinProjection::PAYLOAD) == 0;
  }

  static Result<std::vector<FieldRef>> ComputePayload(const Schema& schema,
                                                      const std::vector<FieldRef>& output,
                                                      const std::vector<FieldRef>& filter,
                                                      const std::vector<FieldRef>& key);
};

class HashJoinImpl {
 public:
  using OutputBatchCallback = std::function<void(ExecBatch)>;
  using FinishedCallback = std::function<void(int64_t)>;

  virtual ~HashJoinImpl() = default;
  virtual Status Init(ExecContext* ctx, JoinType join_type, bool use_sync_execution,
                      size_t num_threads, HashJoinSchema* schema_mgr,
                      std::vector<JoinKeyCmp> key_cmp, Expression filter,
                      OutputBatchCallback output_batch_callback,
                      FinishedCallback finished_callback,
                      TaskScheduler::ScheduleImpl schedule_task_callback) = 0;
  virtual Status InputReceived(size_t thread_index, int side, ExecBatch batch) = 0;
  virtual Status InputFinished(size_t thread_index, int side) = 0;
  virtual void Abort(TaskScheduler::AbortContinuationImpl pos_abort_callback) = 0;

  static Result<std::unique_ptr<HashJoinImpl>> MakeBasic();
};

}  // namespace compute
}  // namespace arrow

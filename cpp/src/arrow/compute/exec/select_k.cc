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

#include "arrow/compute/exec/select_k.h"

#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec/options.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

class SelectKBasicImpl : public SelectKImpl {
  Status Init(ExecContext* ctx, SelectKOptions select_k_options,
              const std::shared_ptr<Schema>& output_schema) override {
    ctx_ = ctx;
    select_k_options_ = select_k_options;
    output_schema_ = output_schema;
    return Status::OK();
  }
  Status InputReceived(std::shared_ptr<RecordBatch> batch) override {
    std::unique_lock<std::mutex> lock(mutex_);
    batches_.push_back(batch);
    return Status::OK();
  }

  Result<Datum> DoFinish() override {
    std::unique_lock<std::mutex> lock(mutex_);
    ARROW_ASSIGN_OR_RAISE(auto table,
                          Table::FromRecordBatches(output_schema_, std::move(batches_)));

    ARROW_ASSIGN_OR_RAISE(auto indices, SelectKUnstable(table, select_k_options_, ctx_));
    return Take(table, indices, TakeOptions::NoBoundsCheck(), ctx_);
  }

  void Abort(bool pos_abort_callback) override {}

 private:
  ExecContext* ctx_;
  std::shared_ptr<Schema> output_schema_;
  SelectKOptions select_k_options_;
  std::mutex mutex_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;
};

Result<std::unique_ptr<SelectKImpl>> SelectKImpl::MakeBasic() {
  std::unique_ptr<SelectKImpl> impl{new SelectKBasicImpl()};
  return std::move(impl);
}

}  // namespace compute
}  // namespace arrow

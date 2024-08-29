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

#include "arrow/acero/order_by_impl.h"

#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include "arrow/acero/options.h"
#include "arrow/compute/api_vector.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

using compute::TakeOptions;

namespace acero {

class SortBasicImpl : public OrderByImpl {
 public:
  SortBasicImpl(ExecContext* ctx, const std::shared_ptr<Schema>& output_schema,
                const SortOptions& options = SortOptions{})
      : ctx_(ctx), output_schema_(output_schema), options_(options) {}

  void InputReceived(const std::shared_ptr<RecordBatch>& batch) override {
    std::unique_lock<std::mutex> lock(mutex_);
    batches_.push_back(batch);
  }

  Result<Datum> DoFinish() override {
    std::unique_lock<std::mutex> lock(mutex_);
    ARROW_ASSIGN_OR_RAISE(auto table,
                          Table::FromRecordBatches(output_schema_, std::move(batches_)));
    ARROW_ASSIGN_OR_RAISE(auto indices, SortIndices(table, options_, ctx_));
    return Take(table, indices, TakeOptions::NoBoundsCheck(), ctx_);
  }

  std::string ToString() const override { return options_.ToString(); }

 protected:
  ExecContext* ctx_;
  std::shared_ptr<Schema> output_schema_;
  std::mutex mutex_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;

 private:
  const SortOptions options_;
};  // namespace compute

class SelectKBasicImpl : public SortBasicImpl {
 public:
  SelectKBasicImpl(ExecContext* ctx, const std::shared_ptr<Schema>& output_schema,
                   const SelectKOptions& options)
      : SortBasicImpl(ctx, output_schema), options_(options) {}

  Result<Datum> DoFinish() override {
    std::unique_lock<std::mutex> lock(mutex_);
    ARROW_ASSIGN_OR_RAISE(auto table,
                          Table::FromRecordBatches(output_schema_, std::move(batches_)));
    ARROW_ASSIGN_OR_RAISE(auto indices, SelectKUnstable(table, options_, ctx_));
    return Take(table, indices, TakeOptions::NoBoundsCheck(), ctx_);
  }

  std::string ToString() const override { return options_.ToString(); }

 private:
  const SelectKOptions options_;
};

Result<std::unique_ptr<OrderByImpl>> OrderByImpl::MakeSort(
    ExecContext* ctx, const std::shared_ptr<Schema>& output_schema,
    const SortOptions& options) {
  std::unique_ptr<OrderByImpl> impl{new SortBasicImpl(ctx, output_schema, options)};
  return std::move(impl);
}

Result<std::unique_ptr<OrderByImpl>> OrderByImpl::MakeSelectK(
    ExecContext* ctx, const std::shared_ptr<Schema>& output_schema,
    const SelectKOptions& options) {
  std::unique_ptr<OrderByImpl> impl{new SelectKBasicImpl(ctx, output_schema, options)};
  return std::move(impl);
}

}  // namespace acero
}  // namespace arrow

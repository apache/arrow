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

#include "arrow/engine/substrait/driver.h"

#include <unordered_map>

#include "arrow-adbc/adbc.h"
#include "arrow-adbc/driver_base.h"

#include "arrow/c/bridge.h"
#include "arrow/engine/substrait/api.h"
#include "arrow/table.h"

using adbc::common::Error;

namespace arrow::engine {

namespace internal {

AdbcStatusCode StatusToAdbc(Status status, AdbcError* error,
                            AdbcDriver* driver = nullptr) {
  if (status.ok()) {
    return ADBC_STATUS_OK;
  }

  Error err(status.ToString());
  err.ToAdbc(error, driver);
  return ADBC_STATUS_INTERNAL;
}

// Stolen from R bindings...maybe a better way to get RecordBatchReader output now
class AccumulatingConsumer : public acero::SinkNodeConsumer {
 public:
  const std::vector<std::shared_ptr<RecordBatch>>& batches() { return batches_; }

  Status Init(const std::shared_ptr<Schema>& schema,
              acero::BackpressureControl* backpressure_control,
              acero::ExecPlan* exec_plan) override {
    schema_ = schema;
    return Status::OK();
  }

  Status Consume(compute::ExecBatch batch) override {
    auto record_batch = batch.ToRecordBatch(schema_);
    ARROW_RETURN_NOT_OK(record_batch);
    batches_.push_back(record_batch.ValueUnsafe());

    return Status::OK();
  }

  Future<> Finish() override { return Future<>::MakeFinished(); }

 private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;
};

class AceroDatabase : public adbc::common::DatabaseObjectBase {};

class AceroConnection : public adbc::common::ConnectionObjectBase {};

class AceroStatement : public adbc::common::StatementObjectBase {
 public:
  AdbcStatusCode SetSqlQuery(const char* query, AdbcError* error) override {
    auto maybe_serialized_plan = SubstraitFromJSON("Plan", query);
    if (maybe_serialized_plan.ok()) {
      serialized_plan_ = *maybe_serialized_plan;
    }

    return StatusToAdbc(maybe_serialized_plan.status(), error);
  }

  AdbcStatusCode SetSubstraitPlan(const uint8_t* plan, size_t length,
                                  AdbcError* error) override {
    serialized_plan_ = std::make_shared<Buffer>(plan, length);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                              AdbcError* error) override {
    return StatusToAdbc(ExecuteQueryImpl(stream, rows_affected), error);
  }

 private:
  std::shared_ptr<Buffer> serialized_plan_;
  std::shared_ptr<acero::ExecPlan> exec_plan_;

  Status InitExecPlan() {
    ARROW_ASSIGN_OR_RAISE(exec_plan_, acero::ExecPlan::Make())
    return Status::OK();
  }

  Status ExecuteQueryImpl(ArrowArrayStream* stream, int64_t* rows_affected) {
    std::vector<std::shared_ptr<AccumulatingConsumer>> consumers;
    auto consumer_factory = [&consumers] {
      consumers.emplace_back(new AccumulatingConsumer());
      return consumers.back();
    };

    ARROW_RETURN_NOT_OK(InitExecPlan());
    ARROW_ASSIGN_OR_RAISE(auto decls,
                          engine::DeserializePlans(*serialized_plan_, consumer_factory));
    for (const acero::Declaration& decl : decls) {
      ARROW_RETURN_NOT_OK(decl.AddToPlan(exec_plan_.get()));
    }

    ARROW_RETURN_NOT_OK(exec_plan_->Validate());
    exec_plan_->StartProducing();
    ARROW_RETURN_NOT_OK(exec_plan_->finished().status());

    std::vector<std::shared_ptr<RecordBatch>> all_batches;
    for (const auto& consumer : consumers) {
      for (const auto& batch : consumer->batches()) {
        all_batches.push_back(batch);
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches(std::move(all_batches)));
    auto reader = std::make_shared<arrow::TableBatchReader>(table);
    ARROW_RETURN_NOT_OK(ExportRecordBatchReader(reader, stream));
    return Status::OK();
  }
};

using AceroDriver = adbc::common::Driver<AceroDatabase, AceroConnection, AceroStatement>;

}  // namespace internal

uint8_t AceroDriverInitFunc(int version, void* raw_driver, void* error) {
  return internal::AceroDriver::Init(version, raw_driver,
                                     reinterpret_cast<AdbcError*>(error));
}

}  // namespace arrow::engine

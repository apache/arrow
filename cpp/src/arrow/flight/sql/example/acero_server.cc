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

#include "arrow/flight/sql/example/acero_server.h"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <unordered_map>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/flight/sql/types.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace flight {
namespace sql {
namespace acero_example {

namespace {
/// \brief A SinkNodeConsumer that saves the schema as given to it by
///   the ExecPlan. Used to retrieve the schema of a Substrait plan to
///   fulfill the Flight SQL API contract.
class GetSchemaSinkNodeConsumer : public compute::SinkNodeConsumer {
 public:
  Status Init(const std::shared_ptr<Schema>& schema, compute::BackpressureControl*,
              compute::ExecPlan* plan) override {
    schema_ = schema;
    return Status::OK();
  }
  Status Consume(compute::ExecBatch exec_batch) override { return Status::OK(); }
  Future<> Finish() override { return Status::OK(); }

  const std::shared_ptr<Schema>& schema() const { return schema_; }

 private:
  std::shared_ptr<Schema> schema_;
};

/// \brief A SinkNodeConsumer that internally saves batches into a
///   queue, so that it can be read from a RecordBatchReader. In other
///   words, this bridges a push-based interface (ExecPlan) to a
///   pull-based interface (RecordBatchReader).
class QueuingSinkNodeConsumer : public compute::SinkNodeConsumer {
 public:
  QueuingSinkNodeConsumer() : schema_(nullptr), finished_(false) {}

  Status Init(const std::shared_ptr<Schema>& schema, compute::BackpressureControl*,
              compute::ExecPlan* plan) override {
    schema_ = schema;
    return Status::OK();
  }

  Status Consume(compute::ExecBatch exec_batch) override {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      batches_.push_back(std::move(exec_batch));
      batches_added_.notify_all();
    }

    return Status::OK();
  }

  Future<> Finish() override {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      finished_ = true;
      batches_added_.notify_all();
    }

    return Status::OK();
  }

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  arrow::Result<std::shared_ptr<RecordBatch>> Next() {
    compute::ExecBatch batch;
    {
      std::unique_lock<std::mutex> guard(mutex_);
      batches_added_.wait(guard, [this] { return !batches_.empty() || finished_; });

      if (finished_ && batches_.empty()) {
        return nullptr;
      }
      batch = std::move(batches_.front());
      batches_.pop_front();
    }

    return batch.ToRecordBatch(schema_);
  }

 private:
  std::mutex mutex_;
  std::condition_variable batches_added_;
  std::deque<compute::ExecBatch> batches_;
  std::shared_ptr<Schema> schema_;
  bool finished_;
};

/// \brief A RecordBatchReader that pulls from the
///   QueuingSinkNodeConsumer above, blocking until results are
///   available as necessary.
class ConsumerBasedRecordBatchReader : public RecordBatchReader {
 public:
  explicit ConsumerBasedRecordBatchReader(
      std::shared_ptr<compute::ExecPlan> plan,
      std::shared_ptr<QueuingSinkNodeConsumer> consumer)
      : plan_(std::move(plan)), consumer_(std::move(consumer)) {}

  std::shared_ptr<Schema> schema() const override { return consumer_->schema(); }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    return consumer_->Next().Value(batch);
  }

  // TODO(ARROW-17242): FlightDataStream needs to call Close()
  Status Close() override { return plan_->finished().status(); }

 private:
  std::shared_ptr<compute::ExecPlan> plan_;
  std::shared_ptr<QueuingSinkNodeConsumer> consumer_;
};

/// \brief An implementation of a Flight SQL service backed by Acero.
class AceroFlightSqlServer : public FlightSqlServerBase {
 public:
  AceroFlightSqlServer() {
    RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT,
                    SqlInfoResult(true));
    RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION,
                    SqlInfoResult(std::string("0.6.0")));
    RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION,
                    SqlInfoResult(std::string("0.6.0")));
    RegisterSqlInfo(
        SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION,
        SqlInfoResult(
            SqlInfoOptions::SqlSupportedTransaction::SQL_SUPPORTED_TRANSACTION_NONE));
    RegisterSqlInfo(SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_CANCEL,
                    SqlInfoResult(false));
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSubstraitPlan(
      const ServerCallContext& context, const StatementSubstraitPlan& command,
      const FlightDescriptor& descriptor) override {
    if (!command.transaction_id.empty()) {
      return Status::NotImplemented("Transactions are unsupported");
    }

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> output_schema,
                          GetPlanSchema(command.plan.plan));

    ARROW_LOG(INFO) << "GetFlightInfoSubstraitPlan: preparing plan with output schema "
                    << *output_schema;

    return MakeFlightInfo(command.plan.plan, descriptor, *output_schema);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command,
      const FlightDescriptor& descriptor) override {
    std::shared_ptr<arrow::Buffer> plan;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      auto it = prepared_.find(command.prepared_statement_handle);
      if (it == prepared_.end()) {
        return Status::KeyError("Prepared statement not found");
      }
      plan = it->second;
    }

    return MakeFlightInfo(plan->ToString(), descriptor, Schema({}));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) override {
    // GetFlightInfoSubstraitPlan encodes the plan into the ticket
    std::shared_ptr<Buffer> serialized_plan =
        Buffer::FromString(command.statement_handle);
    std::shared_ptr<QueuingSinkNodeConsumer> consumer =
        std::make_shared<QueuingSinkNodeConsumer>();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<compute::ExecPlan> plan,
                          engine::DeserializePlan(*serialized_plan, consumer));

    ARROW_LOG(INFO) << "DoGetStatement: executing plan " << plan->ToString();

    plan->StartProducing();

    auto reader = std::make_shared<ConsumerBasedRecordBatchReader>(std::move(plan),
                                                                   std::move(consumer));
    return std::make_unique<RecordBatchStream>(reader);
  }

  arrow::Result<int64_t> DoPutCommandSubstraitPlan(
      const ServerCallContext& context, const StatementSubstraitPlan& command) override {
    return Status::NotImplemented("Updates are unsupported");
  }

  Status DoPutPreparedStatementQuery(const ServerCallContext& context,
                                     const PreparedStatementQuery& command,
                                     FlightMessageReader* reader,
                                     FlightMetadataWriter* writer) override {
    return Status::NotImplemented("NYI");
  }

  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const ServerCallContext& context, const PreparedStatementUpdate& command,
      FlightMessageReader* reader) override {
    return Status::NotImplemented("Updates are unsupported");
  }

  arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedSubstraitPlan(
      const ServerCallContext& context,
      const ActionCreatePreparedSubstraitPlanRequest& request) override {
    if (!request.transaction_id.empty()) {
      return Status::NotImplemented("Transactions are unsupported");
    }
    // There's not any real point to precompiling the plan, since the
    // consumer has to be provided here. So this is effectively the
    // same as a non-prepared plan.
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema,
                          GetPlanSchema(request.plan.plan));

    std::string handle;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      handle = std::to_string(counter_++);
      prepared_[handle] = Buffer::FromString(request.plan.plan);
    }

    return ActionCreatePreparedStatementResult{
        /*dataset_schema=*/std::move(schema),
        /*parameter_schema=*/nullptr,
        handle,
    };
  }

  Status ClosePreparedStatement(
      const ServerCallContext& context,
      const ActionClosePreparedStatementRequest& request) override {
    std::lock_guard<std::mutex> guard(mutex_);
    prepared_.erase(request.prepared_statement_handle);
    return Status::OK();
  }

 private:
  arrow::Result<std::shared_ptr<arrow::Schema>> GetPlanSchema(
      const std::string& serialized_plan) {
    std::shared_ptr<Buffer> plan_buf = Buffer::FromString(serialized_plan);
    std::shared_ptr<GetSchemaSinkNodeConsumer> consumer =
        std::make_shared<GetSchemaSinkNodeConsumer>();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<compute::ExecPlan> plan,
                          engine::DeserializePlan(*plan_buf, consumer));
    std::shared_ptr<Schema> output_schema;
    for (compute::ExecNode* possible_sink : plan->nodes()) {
      if (possible_sink->is_sink()) {
        // Force SinkNodeConsumer::Init to be called
        ARROW_RETURN_NOT_OK(possible_sink->StartProducing());
        output_schema = consumer->schema();
        break;
      }
    }
    if (!output_schema) {
      return Status::Invalid("Could not infer output schema");
    }
    return output_schema;
  }

  arrow::Result<std::unique_ptr<FlightInfo>> MakeFlightInfo(
      const std::string& plan, const FlightDescriptor& descriptor, const Schema& schema) {
    ARROW_ASSIGN_OR_RAISE(auto ticket, CreateStatementQueryTicket(plan));
    std::vector<FlightEndpoint> endpoints{
        FlightEndpoint{Ticket{std::move(ticket)}, /*locations=*/{}}};
    ARROW_ASSIGN_OR_RAISE(auto info,
                          FlightInfo::Make(schema, descriptor, std::move(endpoints),
                                           /*total_records=*/-1, /*total_bytes=*/-1));
    return std::make_unique<FlightInfo>(std::move(info));
  }

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> prepared_;
  int64_t counter_;
};

}  // namespace

arrow::Result<std::unique_ptr<FlightSqlServerBase>> MakeAceroServer() {
  return std::make_unique<AceroFlightSqlServer>();
}

}  // namespace acero_example
}  // namespace sql
}  // namespace flight
}  // namespace arrow

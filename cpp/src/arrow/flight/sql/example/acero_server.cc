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

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/flight/sql/types.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace flight {
namespace sql {
namespace acero_example {

namespace {

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
    ARROW_ASSIGN_OR_RAISE(engine::PlanInfo plan,
                          engine::DeserializePlan(*serialized_plan));

    ARROW_LOG(INFO)
        << "DoGetStatement: executing plan "
        << acero::DeclarationToString(plan.root.declaration).ValueOr("Invalid plan");

    ARROW_ASSIGN_OR_RAISE(auto reader, acero::DeclarationToReader(plan.root.declaration));
    return std::make_unique<RecordBatchStream>(std::move(reader));
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
    ARROW_ASSIGN_OR_RAISE(engine::PlanInfo plan, engine::DeserializePlan(*plan_buf));
    return acero::DeclarationToSchema(plan.root.declaration);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> MakeFlightInfo(
      const std::string& plan, const FlightDescriptor& descriptor, const Schema& schema) {
    ARROW_ASSIGN_OR_RAISE(auto ticket, CreateStatementQueryTicket(plan));
    std::vector<FlightEndpoint> endpoints{
        FlightEndpoint{Ticket{std::move(ticket)}, /*locations=*/{}}};
    ARROW_ASSIGN_OR_RAISE(
        auto info,
        FlightInfo::Make(schema, descriptor, std::move(endpoints),
                         /*total_records=*/-1, /*total_bytes=*/-1, /*ordered=*/false));
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

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

#include "adbc/c/driver.h"
#include "adbc/c/types.h"
#include "adbc/util.h"
#include "arrow/c/bridge.h"
#include "arrow/flight/client.h"
#include "arrow/flight/sql/client.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/string_builder.h"

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

namespace {

using arrow::Status;

class FlightSqlStatementImpl : public arrow::RecordBatchReader {
 public:
  explicit FlightSqlStatementImpl(flightsql::FlightSqlClient* client,
                                  std::unique_ptr<flight::FlightInfo> info)
      : client_(client), info_(std::move(info)) {}

  //----------------------------------------------------------
  // arrow::RecordBatchReader Methods
  //----------------------------------------------------------

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    flight::FlightStreamChunk chunk;
    while (current_stream_ && !chunk.data) {
      RETURN_NOT_OK(current_stream_->Next(&chunk));
      if (chunk.data) {
        *batch = chunk.data;
        break;
      }
      if (!chunk.data && !chunk.app_metadata) {
        RETURN_NOT_OK(NextStream());
      }
    }
    return Status::OK();
  }

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  // TODO: a lot of these could be implemented in a common mixin
  enum AdbcStatusCode Close() { return ADBC_STATUS_OK; }

  static enum AdbcStatusCode CloseMethod(struct AdbcStatement* statement) {
    if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(
        statement->private_data);
    return (*ptr)->Close();
  }

  static void ReleaseMethod(struct AdbcStatement* statement) {
    if (!statement->private_data) return;
    delete reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(
        statement->private_data);
    statement->private_data = nullptr;
  }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  enum AdbcStatusCode GetResults(const std::shared_ptr<FlightSqlStatementImpl>& self,
                                 struct ArrowArrayStream* out) {
    auto status = NextStream();
    if (!status.ok()) {
      // TODO: error handling
      return ADBC_STATUS_IO;
    }
    if (!schema_) {
      return ADBC_STATUS_UNKNOWN;
    }

    status = arrow::ExportRecordBatchReader(self, out);
    if (!status.ok()) {
      // TODO: error handling
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode GetResultsMethod(struct AdbcStatement* statement,
                                              struct ArrowArrayStream* out) {
    if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(
        statement->private_data);
    return (*ptr)->GetResults(*ptr, out);
  }

  size_t num_partitions() const { return info_->endpoints().size(); }

  static enum AdbcStatusCode NumPartitionsMethod(struct AdbcStatement* statement,
                                                 size_t* partitions) {
    if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(
        statement->private_data);
    *partitions = (*ptr)->num_partitions();
    return ADBC_STATUS_OK;
  }

  enum AdbcStatusCode GetPartitionDescSize(size_t index, size_t* length) const {
    // TODO: we're only encoding the ticket, not the actual locations
    if (index >= info_->endpoints().size()) {
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    *length = info_->endpoints()[index].ticket.ticket.size();
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode GetPartitionDescSizeMethod(struct AdbcStatement* statement,
                                                        size_t index, size_t* length) {
    if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(
        statement->private_data);
    return (*ptr)->GetPartitionDescSize(index, length);
  }

  enum AdbcStatusCode GetPartitionDesc(size_t index, uint8_t* out) const {
    if (index >= info_->endpoints().size()) {
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    const std::string& ticket = info_->endpoints()[index].ticket.ticket;
    std::memcpy(out, ticket.data(), ticket.size());
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode GetPartitionDescMethod(struct AdbcStatement* statement,
                                                    size_t index, uint8_t* partition) {
    if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(
        statement->private_data);
    return (*ptr)->GetPartitionDesc(index, partition);
  }

 private:
  Status NextStream() {
    if (next_endpoint_ >= info_->endpoints().size()) {
      current_stream_ = nullptr;
      return Status::OK();
    }
    // TODO: this needs to account for location
    flight::FlightCallOptions call_options;
    ARROW_ASSIGN_OR_RAISE(
        current_stream_,
        client_->DoGet(call_options, info_->endpoints()[next_endpoint_].ticket));
    next_endpoint_++;
    if (!schema_) {
      ARROW_ASSIGN_OR_RAISE(schema_, current_stream_->GetSchema());
    }
    return Status::OK();
  }

  flightsql::FlightSqlClient* client_;
  std::unique_ptr<flight::FlightInfo> info_;
  std::shared_ptr<arrow::Schema> schema_;
  size_t next_endpoint_ = 0;
  std::unique_ptr<flight::FlightStreamReader> current_stream_;
};

class AdbcFlightSqlImpl {
 public:
  explicit AdbcFlightSqlImpl(std::unique_ptr<flightsql::FlightSqlClient> client)
      : client_(std::move(client)) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  enum AdbcStatusCode Close() {
    auto status = client_->Close();
    if (!status.ok()) {
      LogError("Could not close client: ", status);
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode CloseMethod(struct AdbcConnection* connection) {
    if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
    return (*ptr)->Close();
  }

  static void ReleaseMethod(struct AdbcConnection* connection) {
    if (!connection->private_data) return;
    delete reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(
        connection->private_data);
    connection->private_data = nullptr;
  }

  char* GetError() {
    if (messages_.empty()) return nullptr;
    char* result = new char[messages_.front().size()];
    messages_.front().copy(result, messages_.front().size());
    messages_.pop();
    return result;
  }

  static char* GetErrorMethod(struct AdbcConnection* connection) {
    if (!connection->private_data) return nullptr;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
    return (*ptr)->GetError();
  }

  //----------------------------------------------------------
  // SQL Semantics
  //----------------------------------------------------------

  enum AdbcStatusCode SqlExecute(const char* query, struct AdbcStatement* out) {
    flight::FlightCallOptions call_options;
    std::unique_ptr<flight::FlightInfo> flight_info;
    auto status = client_->Execute(call_options, std::string(query)).Value(&flight_info);
    if (!status.ok()) {
      LogError("GetFlightInfo: ", status);
      return ADBC_STATUS_IO;
    }
    std::memset(out, 0, sizeof(*out));
    auto impl =
        std::make_shared<FlightSqlStatementImpl>(client_.get(), std::move(flight_info));
    out->close = &FlightSqlStatementImpl::CloseMethod;
    out->release = &FlightSqlStatementImpl::ReleaseMethod;
    out->get_results = &FlightSqlStatementImpl::GetResultsMethod;
    out->num_partitions = &FlightSqlStatementImpl::NumPartitionsMethod;
    out->get_partition_desc_size = &FlightSqlStatementImpl::GetPartitionDescSizeMethod;
    out->get_partition_desc = &FlightSqlStatementImpl::GetPartitionDescMethod;
    out->private_data = new std::shared_ptr<FlightSqlStatementImpl>(impl);
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode SqlExecuteMethod(struct AdbcConnection* connection,
                                              const char* query,
                                              struct AdbcStatement* out) {
    if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
    return (*ptr)->SqlExecute(query, out);
  }

  //----------------------------------------------------------
  // Partitioned Results
  //----------------------------------------------------------

  enum AdbcStatusCode DeserializePartitionDesc(const uint8_t* serialized_partition,
                                               size_t serialized_length,
                                               struct AdbcStatement* out) {
    std::vector<flight::FlightEndpoint> endpoints(1);
    endpoints[0].ticket.ticket = std::string(
        reinterpret_cast<const char*>(serialized_partition), serialized_length);
    auto maybe_info = flight::FlightInfo::Make(
        *arrow::schema({}), flight::FlightDescriptor::Command(""), endpoints,
        /*total_records=*/-1, /*total_bytes=*/-1);
    if (!maybe_info.ok()) {
      // TODO:
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    std::unique_ptr<flight::FlightInfo> flight_info(
        new flight::FlightInfo(maybe_info.MoveValueUnsafe()));

    std::memset(out, 0, sizeof(*out));
    auto impl =
        std::make_shared<FlightSqlStatementImpl>(client_.get(), std::move(flight_info));
    out->close = &FlightSqlStatementImpl::CloseMethod;
    out->release = &FlightSqlStatementImpl::ReleaseMethod;
    out->get_results = &FlightSqlStatementImpl::GetResultsMethod;
    out->num_partitions = &FlightSqlStatementImpl::NumPartitionsMethod;
    out->get_partition_desc_size = &FlightSqlStatementImpl::GetPartitionDescSizeMethod;
    out->get_partition_desc = &FlightSqlStatementImpl::GetPartitionDescMethod;
    out->private_data = new std::shared_ptr<FlightSqlStatementImpl>(impl);
    return ADBC_STATUS_OK;
  }

  static enum AdbcStatusCode DeserializePartitionDescMethod(
      struct AdbcConnection* connection, const uint8_t* serialized_partition,
      size_t serialized_length, struct AdbcStatement* statement) {
    if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
    auto* ptr =
        reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
    return (*ptr)->DeserializePartitionDesc(serialized_partition, serialized_length,
                                            statement);
  }

 private:
  template <typename... Args>
  void LogError(Args&&... args) {
    auto message =
        arrow::util::StringBuilder("[Flight SQL] ", std::forward<Args>(args)...);
    messages_.push(std::move(message));
  }

  std::unique_ptr<flightsql::FlightSqlClient> client_;
  std::queue<std::string> messages_;
};

}  // namespace

enum AdbcStatusCode AdbcDriverConnectionInit(const struct AdbcConnectionOptions* options,
                                             struct AdbcConnection* out) {
  std::unordered_map<std::string, std::string> option_pairs;
  auto status = adbc::ParseConnectionString(options->target).Value(&option_pairs);
  if (!status.ok()) {
    // TODO: move ConnectionError into util.h
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  auto location_it = option_pairs.find("Location");
  if (location_it == option_pairs.end()) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  flight::Location location;
  status = flight::Location::Parse(location_it->second, &location);
  if (!status.ok()) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  std::unique_ptr<flight::FlightClient> flight_client;
  status = flight::FlightClient::Connect(location, &flight_client);
  if (!status.ok()) {
    return ADBC_STATUS_IO;
  }
  std::unique_ptr<flightsql::FlightSqlClient> client(
      new flightsql::FlightSqlClient(std::move(flight_client)));

  auto impl = std::make_shared<AdbcFlightSqlImpl>(std::move(client));

  out->close = &AdbcFlightSqlImpl::CloseMethod;
  out->release = &AdbcFlightSqlImpl::ReleaseMethod;
  out->get_error = &AdbcFlightSqlImpl::GetErrorMethod;
  out->sql_execute = &AdbcFlightSqlImpl::SqlExecuteMethod;
  out->deserialize_partition_desc = &AdbcFlightSqlImpl::DeserializePartitionDescMethod;
  out->private_data = new std::shared_ptr<AdbcFlightSqlImpl>(impl);
  return ADBC_STATUS_OK;
}

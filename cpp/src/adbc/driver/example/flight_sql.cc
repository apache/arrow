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

#include "adbc/adbc.h"
#include "adbc/driver/util.h"
#include "arrow/c/bridge.h"
#include "arrow/flight/client.h"
#include "arrow/flight/sql/client.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/string_builder.h"
#include "arrow/util/string_view.h"

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

namespace {

void SetError(const arrow::Status& status, struct AdbcError* error) {
  if (!error) return;
  std::string message = arrow::util::StringBuilder("[Flight SQL]: ", status.ToString());
  if (error->message) {
    message.reserve(message.size() + 1 + std::strlen(error->message));
    message.append(1, '\n');
    message.append(error->message);
    delete[] error->message;
  }
  error->message = new char[message.size() + 1];
  message.copy(error->message, message.size());
  error->message[message.size()] = '\0';
}

using arrow::Status;

class FlightSqlStatementImpl : public arrow::RecordBatchReader {
 public:
  explicit FlightSqlStatementImpl(flightsql::FlightSqlClient* client,
                                  std::unique_ptr<flight::FlightInfo> info)
      : client_(client), info_(std::move(info)) {}

  static void Make(flightsql::FlightSqlClient* client,
                   std::unique_ptr<flight::FlightInfo> info, struct AdbcStatement* out) {
    std::memset(out, 0, sizeof(*out));
    auto impl = std::make_shared<FlightSqlStatementImpl>(client, std::move(info));
    out->private_data = new std::shared_ptr<FlightSqlStatementImpl>(impl);
  }

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
  enum AdbcStatusCode Close(struct AdbcError* error) { return ADBC_STATUS_OK; }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  enum AdbcStatusCode GetStream(const std::shared_ptr<FlightSqlStatementImpl>& self,
                                struct ArrowArrayStream* out, struct AdbcError* error) {
    auto status = NextStream();
    if (!status.ok()) {
      SetError(status, error);
      return ADBC_STATUS_IO;
    }
    if (!schema_) {
      return ADBC_STATUS_UNKNOWN;
    }

    status = arrow::ExportRecordBatchReader(self, out);
    if (!status.ok()) {
      SetError(status, error);
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  enum AdbcStatusCode GetPartitionDescSize(size_t* length,
                                           struct AdbcError* error) const {
    // TODO: we're only encoding the ticket, not the actual locations
    if (next_endpoint_ >= info_->endpoints().size()) {
      *length = 0;
      return ADBC_STATUS_OK;
    }
    *length = info_->endpoints()[next_endpoint_].ticket.ticket.size();
    return ADBC_STATUS_OK;
  }

  enum AdbcStatusCode GetPartitionDesc(uint8_t* partition_desc, struct AdbcError* error) {
    if (next_endpoint_ >= info_->endpoints().size()) {
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    const std::string& ticket = info_->endpoints()[next_endpoint_].ticket.ticket;
    std::memcpy(partition_desc, ticket.data(), ticket.size());
    next_endpoint_++;
    return ADBC_STATUS_OK;
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

  enum AdbcStatusCode Close(struct AdbcError* error) {
    auto status = client_->Close();
    if (!status.ok()) {
      SetError(status, error);
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // Metadata
  //----------------------------------------------------------

  enum AdbcStatusCode GetTableTypes(struct AdbcStatement* out, struct AdbcError* error) {
    flight::FlightCallOptions call_options;
    std::unique_ptr<flight::FlightInfo> flight_info;
    auto status = client_->GetTableTypes(call_options).Value(&flight_info);
    if (!status.ok()) {
      SetError(status, error);
      return ADBC_STATUS_IO;
    }
    FlightSqlStatementImpl::Make(client_.get(), std::move(flight_info), out);
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // SQL Semantics
  //----------------------------------------------------------

  enum AdbcStatusCode SqlExecute(const char* query, size_t query_length,
                                 struct AdbcStatement* out, struct AdbcError* error) {
    flight::FlightCallOptions call_options;
    std::unique_ptr<flight::FlightInfo> flight_info;
    auto status = client_->Execute(call_options, std::string(query, query_length))
                      .Value(&flight_info);
    if (!status.ok()) {
      SetError(status, error);
      return ADBC_STATUS_IO;
    }
    FlightSqlStatementImpl::Make(client_.get(), std::move(flight_info), out);
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // Partitioned Results
  //----------------------------------------------------------

  enum AdbcStatusCode DeserializePartitionDesc(const uint8_t* serialized_partition,
                                               size_t serialized_length,
                                               struct AdbcStatement* out,
                                               struct AdbcError* error) {
    std::vector<flight::FlightEndpoint> endpoints(1);
    endpoints[0].ticket.ticket = std::string(
        reinterpret_cast<const char*>(serialized_partition), serialized_length);
    auto maybe_info = flight::FlightInfo::Make(
        *arrow::schema({}), flight::FlightDescriptor::Command(""), endpoints,
        /*total_records=*/-1, /*total_bytes=*/-1);
    if (!maybe_info.ok()) {
      SetError(maybe_info.status(), error);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    std::unique_ptr<flight::FlightInfo> flight_info(
        new flight::FlightInfo(maybe_info.MoveValueUnsafe()));

    std::memset(out, 0, sizeof(*out));
    auto impl =
        std::make_shared<FlightSqlStatementImpl>(client_.get(), std::move(flight_info));
    out->private_data = new std::shared_ptr<FlightSqlStatementImpl>(impl);
    return ADBC_STATUS_OK;
  }

 private:
  std::unique_ptr<flightsql::FlightSqlClient> client_;
};

}  // namespace

void AdbcErrorRelease(struct AdbcError* error) {
  delete[] error->message;
  error->message = nullptr;
}

enum AdbcStatusCode AdbcConnectionDeserializePartitionDesc(
    struct AdbcConnection* connection, const uint8_t* serialized_partition,
    size_t serialized_length, struct AdbcStatement* statement, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
  return (*ptr)->DeserializePartitionDesc(serialized_partition, serialized_length,
                                          statement, error);
}

enum AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                                struct AdbcStatement* statement,
                                                struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
  return (*ptr)->GetTableTypes(statement, error);
}

enum AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions* options,
                                       struct AdbcConnection* out,
                                       struct AdbcError* error) {
  std::unordered_map<std::string, std::string> option_pairs;
  auto status = adbc::ParseConnectionString(
                    arrow::util::string_view(options->target, options->target_length))
                    .Value(&option_pairs);
  if (!status.ok()) {
    SetError(status, error);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  auto location_it = option_pairs.find("Location");
  if (location_it == option_pairs.end()) {
    SetError(Status::Invalid("Must provide Location option"), error);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  flight::Location location;
  status = flight::Location::Parse(location_it->second, &location);
  if (!status.ok()) {
    SetError(status, error);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  std::unique_ptr<flight::FlightClient> flight_client;
  status = flight::FlightClient::Connect(location, &flight_client);
  if (!status.ok()) {
    SetError(status, error);
    return ADBC_STATUS_IO;
  }
  std::unique_ptr<flightsql::FlightSqlClient> client(
      new flightsql::FlightSqlClient(std::move(flight_client)));

  auto impl = std::make_shared<AdbcFlightSqlImpl>(std::move(client));
  out->private_data = new std::shared_ptr<AdbcFlightSqlImpl>(impl);
  return ADBC_STATUS_OK;
}

enum AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

enum AdbcStatusCode AdbcConnectionSqlExecute(struct AdbcConnection* connection,
                                             const char* query, size_t query_length,
                                             struct AdbcStatement* out,
                                             struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<AdbcFlightSqlImpl>*>(connection->private_data);
  return (*ptr)->SqlExecute(query, query_length, out, error);
}

enum AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                                  uint8_t* partition_desc,
                                                  struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetPartitionDesc(partition_desc, error);
}

enum AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                      size_t* length,
                                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetPartitionDescSize(length, error);
}

enum AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetStream(*ptr, out, error);
}

enum AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

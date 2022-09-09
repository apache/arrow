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

#include <array>
#include <cmath>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_union.h"
#include "arrow/c/bridge.h"
#include "arrow/config.h"
#include "arrow/flight/client.h"
#include "arrow/flight/sql/adbc_driver_internal.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/visibility.h"
#include "arrow/io/memory.h"
#include "arrow/io/type_fwd.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

#ifdef ARROW_COMPUTE
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec.h"
#endif

namespace arrow::flight::sql {

using arrow::internal::checked_cast;

namespace {
static const std::vector<std::pair<Type::type, std::string>> kDefaultTypeMapping = {
    {Type::BINARY, "BLOB"},
    {Type::BOOL, "BOOLEAN"},
    {Type::DATE32, "DATE"},
    {Type::DATE64, "DATE"},
    {Type::DECIMAL128, "NUMERIC"},
    {Type::DECIMAL256, "NUMERIC"},
    {Type::DOUBLE, "DOUBLE PRECISION"},
    {Type::FLOAT, "REAL"},
    {Type::INT16, "SMALLINT"},
    {Type::INT32, "INT"},
    {Type::INT64, "BIGINT"},
    {Type::LARGE_BINARY, "BLOB"},
    {Type::LARGE_STRING, "TEXT"},
    {Type::STRING, "TEXT"},
    {Type::TIME32, "TIME"},
    {Type::TIME64, "TIME"},
    {Type::TIMESTAMP, "TIMESTAMP"},
};

/// \brief Client-side configuration to help paper over SQL dialect differences
///
/// This is needed when we have to generate SQL to implement ADBC
/// features that have no direct Flight SQL equivalent.  In
/// particular, it's needed for bulk ingestion to a target table.
struct FlightSqlQuirks {
  /// A mapping from Arrow type to SQL type string
  std::unordered_map<Type::type, std::string> ingest_type_mapping;

  FlightSqlQuirks()
      : ingest_type_mapping(kDefaultTypeMapping.begin(), kDefaultTypeMapping.end()) {}

  bool UpdateTypeMapping(std::string_view type_name, const char* value) {
    if (type_name == "binary") {
      ingest_type_mapping[Type::BINARY] = value;
    } else if (type_name == "bool") {
      ingest_type_mapping[Type::BOOL] = value;
    } else if (type_name == "date32") {
      ingest_type_mapping[Type::DATE32] = value;
    } else if (type_name == "date64") {
      ingest_type_mapping[Type::DATE64] = value;
    } else if (type_name == "decimal128") {
      ingest_type_mapping[Type::DECIMAL128] = value;
    } else if (type_name == "decimal256") {
      ingest_type_mapping[Type::DECIMAL256] = value;
    } else if (type_name == "double") {
      ingest_type_mapping[Type::DOUBLE] = value;
    } else if (type_name == "float") {
      ingest_type_mapping[Type::FLOAT] = value;
    } else if (type_name == "int16") {
      ingest_type_mapping[Type::INT16] = value;
    } else if (type_name == "int32") {
      ingest_type_mapping[Type::INT32] = value;
    } else if (type_name == "int64") {
      ingest_type_mapping[Type::INT64] = value;
    } else if (type_name == "large_binary") {
      ingest_type_mapping[Type::LARGE_BINARY] = value;
    } else if (type_name == "large_string") {
      ingest_type_mapping[Type::LARGE_STRING] = value;
    } else if (type_name == "string") {
      ingest_type_mapping[Type::STRING] = value;
    } else if (type_name == "time32") {
      ingest_type_mapping[Type::TIME32] = value;
    } else if (type_name == "time64") {
      ingest_type_mapping[Type::TIME64] = value;
    } else if (type_name == "timestamp") {
      ingest_type_mapping[Type::TIMESTAMP] = value;
    } else {
      return false;
    }
    return true;
  }
};

/// Config options that map to FlightClientOptions
constexpr std::string_view kClientOptionTlsRootCerts =
    "arrow.flight.sql.client_option.tls_root_certs";
constexpr std::string_view kClientOptionOverrideHostname =
    "arrow.flight.sql.client_option.override_hostname";
constexpr std::string_view kClientOptionCertChain =
    "arrow.flight.sql.client_option.cert_chain";
constexpr std::string_view kClientOptionPrivateKey =
    "arrow.flight.sql.client_option.private_key";
constexpr std::string_view kClientOptionGenericIntOption =
    "arrow.flight.sql.client_option.generic_int_option.";
constexpr std::string_view kClientOptionGenericStringOption =
    "arrow.flight.sql.client_option.generic_string_option.";
constexpr std::string_view kClientOptionDisableServerVerification =
    "arrow.flight.sql.client_option.disable_server_verification";
/// Config option to enable JDBC driver-like authorization
constexpr std::string_view kAuthorizationHeaderKey =
    "arrow.flight.sql.authorization_header";
/// Config options used to override the type mapping in FlightSqlQuirks
constexpr std::string_view kIngestTypePrefix = "arrow.flight.sql.quirks.ingest_type.";
/// Explicitly specify the Substrait version for Flight SQL (although
/// Substrait will eventually embed this into the plan itself)
constexpr std::string_view kStatementSubstraitVersionKey =
    "arrow.flight.sql.substrait.version";
/// Attach arbitrary key-value headers via Flight
constexpr std::string_view kCallHeaderPrefix = "arrow.flight.sql.rpc.call_header.";
/// A timeout for any DoGet requests
constexpr std::string_view kConnectionTimeoutFetchKey =
    "arrow.flight.sql.rpc.timeout_seconds.fetch";
/// A timeout for any GetFlightInfo requests
constexpr std::string_view kConnectionTimeoutQueryKey =
    "arrow.flight.sql.rpc.timeout_seconds.query";
/// A timeout for any DoPut requests, or miscellaneous DoAction requests
constexpr std::string_view kConnectionTimeoutUpdateKey =
    "arrow.flight.sql.rpc.timeout_seconds.update";
constexpr std::string_view kConnectionOptionAutocommit =
    ADBC_CONNECTION_OPTION_AUTOCOMMIT;
constexpr std::string_view kIngestOptionMode = ADBC_INGEST_OPTION_MODE;
constexpr std::string_view kIngestOptionModeAppend = ADBC_INGEST_OPTION_MODE_APPEND;
constexpr std::string_view kIngestOptionModeCreate = ADBC_INGEST_OPTION_MODE_CREATE;
constexpr std::string_view kIngestOptionTargetTable = ADBC_INGEST_OPTION_TARGET_TABLE;
constexpr std::string_view kOptionValueEnabled = ADBC_OPTION_VALUE_ENABLED;
constexpr std::string_view kOptionValueDisabled = ADBC_OPTION_VALUE_DISABLED;

enum class CallContext {
  kFetch,
  kQuery,
  kUpdate,
};

static const char kAuthorizationHeader[] = "authorization";

/// Implement auth in the same way as the Flight SQL JDBC driver
/// 1. Client ---[authorization: Basic XXX ]--> Server
/// 2. Client <--[authorization: Bearer YYY]--- Server
/// 3. Client ---[authorization: Bearer YYY]--> Server
class ClientAuthorizationMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  explicit ClientAuthorizationMiddlewareFactory(std::string initial_header)
      : token_(std::move(initial_header)) {}

  void StartCall(const CallInfo& info,
                 std::unique_ptr<ClientMiddleware>* middleware) override {
    // TODO: maybe we want a shared_ptr<string> and an atomic reference?
    std::lock_guard<std::mutex> guard(token_mutex_);
    // Middleware instances outlive the factory
    *middleware = std::make_unique<Impl>(this, token_);
  }

  void UpdateToken(std::string_view token) {
    std::lock_guard<std::mutex> guard(token_mutex_);
    token_ = std::string(token);
  }

 private:
  std::mutex token_mutex_;
  std::string token_;

  class Impl : public ClientMiddleware {
   public:
    explicit Impl(ClientAuthorizationMiddlewareFactory* factory, std::string token)
        : factory_(factory), token_(std::move(token)) {}
    void SendingHeaders(AddCallHeaders* outgoing_headers) override {
      outgoing_headers->AddHeader(kAuthorizationHeader, token_);
    }
    void ReceivedHeaders(const CallHeaders& incoming_headers) override {
      // Assume server isn't going to send multiple authorization headers
      auto it = incoming_headers.find(std::string_view(kAuthorizationHeader));
      if (it == incoming_headers.end()) return;
      if (it->second == token_) return;
      factory_->UpdateToken(it->second);
    }
    void CallCompleted(const Status&) override {}

   private:
    ClientAuthorizationMiddlewareFactory* factory_;
    std::string token_;
  };
};

/// \brief AdbcDatabase implementation
class FlightSqlDatabaseImpl {
 public:
  AdbcStatusCode Connect(std::unique_ptr<FlightSqlClient>* client,
                         struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!location_.has_value()) {
      SetError(error, "Cannot create connection from uninitialized database");
      return ADBC_STATUS_INVALID_STATE;
    }

    std::unique_ptr<FlightClient> flight_client;
    ADBC_ARROW_RETURN_NOT_OK(
        error, FlightClient::Connect(*location_, client_options_).Value(&flight_client));
    *client = std::make_unique<FlightSqlClient>(std::move(flight_client));
    ++connection_count_;
    return ADBC_STATUS_OK;
  }

  const std::shared_ptr<FlightSqlQuirks>& quirks() const { return quirks_; }
  FlightCallOptions MakeCallOptions(CallContext context) const {
    FlightCallOptions options;
    for (const auto& header : call_headers_) {
      options.headers.emplace_back(header.first, header.second);
    }
    return options;
  }

  AdbcStatusCode Init(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (location_.has_value()) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_STATE;
    }

    auto it = options_.find("uri");
    if (it == options_.end()) {
      SetError(error, "Must provide 'uri' option");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    Location location;
    ADBC_ARROW_RETURN_NOT_OK(error, Location::Parse(it->second).Value(&location));
    location_ = location;

    options_.clear();
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error) {
    if (key == nullptr) {
      SetError(error, "Key must not be null");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    std::string_view key_view(key);
    std::string_view val_view = value ? value : "";
    if (key_view.rfind(kIngestTypePrefix, 0) == 0) {
      // Changing the mapping from Arrow type <-> SQL type name, for when we
      // do a bulk ingest and have to generate a CREATE TABLE statement
      const std::string_view type_name = key_view.substr(kIngestTypePrefix.size());
      if (!quirks_->UpdateTypeMapping(type_name, value)) {
        SetError(error, "Unknown option value ", key_view, "=", val_view, ": type name ",
                 type_name, " is not recognized");
        return ADBC_STATUS_INVALID_ARGUMENT;
      }
      return ADBC_STATUS_OK;
    } else if (key_view.rfind(kCallHeaderPrefix, 0) == 0) {
      // Add a custom header to all outgoing calls (can also be set at
      // connection, statement level)
      std::string header(key_view.substr(kCallHeaderPrefix.size()));
      if (value == nullptr) {
        call_headers_.erase(header);
      } else {
        call_headers_.insert({std::move(header), std::string(val_view)});
      }
      return ADBC_STATUS_OK;
    } else if (key_view == kClientOptionTlsRootCerts) {
      client_options_.tls_root_certs = val_view;
      return ADBC_STATUS_OK;
    } else if (key_view == kClientOptionOverrideHostname) {
      client_options_.override_hostname = val_view;
      return ADBC_STATUS_OK;
    } else if (key_view == kClientOptionCertChain) {
      client_options_.cert_chain = val_view;
      return ADBC_STATUS_OK;
    } else if (key_view == kClientOptionPrivateKey) {
      client_options_.private_key = val_view;
      return ADBC_STATUS_OK;
    } else if (key_view == kClientOptionDisableServerVerification) {
      if (val_view == kOptionValueEnabled) {
        client_options_.disable_server_verification = true;
        return ADBC_STATUS_OK;
      } else if (val_view == kOptionValueDisabled) {
        client_options_.disable_server_verification = false;
        return ADBC_STATUS_OK;
      }
      SetError(error,
               "Invalid boolean value for client option disable_server_verification: ",
               val_view);
      return ADBC_STATUS_INVALID_ARGUMENT;
    } else if (key_view.rfind(kClientOptionGenericIntOption, 0) == 0) {
      const std::string_view option_name =
          key_view.substr(kClientOptionGenericIntOption.size());
      int32_t option_value = 0;
      if (!arrow::internal::StringConverter<Int32Type>().Convert(
              Int32Type(), val_view.data(), val_view.size(), &option_value)) {
        SetError(error,
                 "Invalid integer value for client option generic_options: ", val_view);
        return ADBC_STATUS_INVALID_ARGUMENT;
      }
      client_options_.generic_options.emplace_back(option_name, option_value);
      return ADBC_STATUS_OK;
    } else if (key_view.rfind(kClientOptionGenericStringOption, 0) == 0) {
      const std::string_view option_name =
          key_view.substr(kClientOptionGenericStringOption.size());
      client_options_.generic_options.emplace_back(option_name, std::string(val_view));
      return ADBC_STATUS_OK;
    } else if (key_view == kAuthorizationHeaderKey) {
      client_options_.middleware.push_back(
          std::make_shared<ClientAuthorizationMiddlewareFactory>(std::string(val_view)));
      return ADBC_STATUS_OK;
    }

    if (location_.has_value()) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_STATE;
    }
    options_[std::string(key_view)] = std::string(val_view);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Disconnect(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (--connection_count_ < 0) {
      SetError(error, "Connection count underflow");
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Release(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (connection_count_ > 0) {
      SetError(error, "Cannot release database with ", connection_count_,
               " open connections");
      return ADBC_STATUS_INTERNAL;
    }

    return ADBC_STATUS_OK;
  }

 private:
  std::optional<Location> location_;
  FlightClientOptions client_options_ = DefaultClientOptions();
  std::shared_ptr<FlightSqlQuirks> quirks_ = std::make_shared<FlightSqlQuirks>();
  std::unordered_map<std::string, std::string> call_headers_;
  std::unordered_map<std::string, std::string> options_;
  std::mutex mutex_;
  int connection_count_ = 0;
};

/// \brief A RecordBatchReader that reads the endpoints of a FlightInfo
class FlightInfoReader : public RecordBatchReader {
 public:
  explicit FlightInfoReader(FlightSqlClient* client, FlightCallOptions call_options,
                            std::unique_ptr<FlightInfo> info)
      : client_(client),
        call_options_(std::move(call_options)),
        info_(std::move(info)),
        next_endpoint_(0) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    FlightStreamChunk chunk;
    while (current_stream_ && !chunk.data) {
      ARROW_ASSIGN_OR_RAISE(chunk, current_stream_->Next());
      if (chunk.data) {
        *batch = chunk.data;
        break;
      }
      if (!chunk.data && !chunk.app_metadata) {
        RETURN_NOT_OK(NextStream());
      }
    }
    if (!current_stream_) *batch = nullptr;
    return Status::OK();
  }

  Status Close() override {
    if (current_stream_) {
      current_stream_->Cancel();
      current_stream_.reset();
    }
    return Status::OK();
  }

  AdbcStatusCode Init(struct AdbcError* error) {
    ADBC_ARROW_RETURN_NOT_OK(error, NextStream());
    if (!schema_) {
      // Empty result set - fall back on schema in FlightInfo
      ipc::DictionaryMemo memo;
      ADBC_ARROW_RETURN_NOT_OK(error, info_->GetSchema(&memo).Value(&schema_));
    }
    return ADBC_STATUS_OK;
  }

  /// \brief Export to an ArrowArrayStream
  static AdbcStatusCode Export(FlightSqlClient* client, FlightCallOptions call_options,
                               std::unique_ptr<FlightInfo> info,
                               struct ArrowArrayStream* stream, struct AdbcError* error) {
    auto reader = std::make_shared<FlightInfoReader>(client, std::move(call_options),
                                                     std::move(info));
    ADBC_RETURN_NOT_OK(reader->Init(error));
    ADBC_ARROW_RETURN_NOT_OK(error, ExportRecordBatchReader(std::move(reader), stream));
    return ADBC_STATUS_OK;
  }

 private:
  Status NextStream() {
    if (next_endpoint_ >= info_->endpoints().size()) {
      current_stream_ = nullptr;
      return Status::OK();
    }
    const FlightEndpoint& endpoint = info_->endpoints()[next_endpoint_];

    if (endpoint.locations.empty()) {
      ARROW_ASSIGN_OR_RAISE(current_stream_,
                            client_->DoGet(call_options_, endpoint.ticket));
    } else {
      // TODO(lidavidm): this should come from a connection pool
      std::string failures;
      current_stream_ = nullptr;
      for (const Location& location : endpoint.locations) {
        auto status =
            FlightClient::Connect(location, DefaultClientOptions()).Value(&data_client_);
        if (status.ok()) {
          status =
              data_client_->DoGet(call_options_, endpoint.ticket).Value(&current_stream_);
        }
        if (!status.ok()) {
          if (!failures.empty()) {
            failures += "; ";
          }
          failures += location.ToString();
          failures += ": ";
          failures += status.ToString();
          data_client_.reset();
          continue;
        }
        break;
      }

      if (!current_stream_) {
        return Status::IOError("Failed to connect to all endpoints: ", failures);
      }
    }
    next_endpoint_++;
    if (!schema_) {
      ARROW_ASSIGN_OR_RAISE(schema_, current_stream_->GetSchema());
    }
    return Status::OK();
  }

  FlightSqlClient* client_;
  FlightCallOptions call_options_;
  std::unique_ptr<FlightInfo> info_;
  size_t next_endpoint_;
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<FlightStreamReader> current_stream_;
  // TODO(lidavidm): use a common pool of cached clients with expiration
  std::unique_ptr<FlightClient> data_client_;
};

class FlightSqlConnectionImpl {
 public:
  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  FlightSqlClient* client() const { return client_.get(); }
  const FlightSqlQuirks& quirks() const { return *quirks_; }
  const Transaction& transaction() const { return transaction_; }
  FlightCallOptions MakeCallOptions(CallContext context) const {
    FlightCallOptions options = database_->MakeCallOptions(context);
    auto it = timeout_seconds_.find(context);
    if (it != timeout_seconds_.end()) {
      options.timeout = it->second;
    }
    for (const auto& header : call_headers_) {
      options.headers.emplace_back(header.first, header.second);
    }
    return options;
  }

  AdbcStatusCode Init(struct AdbcDatabase* database, struct AdbcError* error) {
    if (!database->private_data) {
      SetError(error, "Cannot create connection from uninitialized database");
      return ADBC_STATUS_INVALID_STATE;
    } else if (client_) {
      SetError(error, "Already initialized connection");
      return ADBC_STATUS_INVALID_STATE;
    }

    database_ = *reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(
        database->private_data);
    ADBC_RETURN_NOT_OK(database_->Connect(&client_, error));
    quirks_ = database_->quirks();

    // Ignore this if it fails - we'll just proceed
    ARROW_UNUSED(QuerySqlInfo());
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Close(struct AdbcError* error) {
    AdbcStatusCode return_status = ADBC_STATUS_OK;
    if (client_) {
      auto status = client_->Close();
      client_.reset();
      if (!status.ok()) {
        SetError(error, status);
        return_status = ADBC_STATUS_IO;
      }

      if (database_) {
        ADBC_RETURN_NOT_OK(database_->Disconnect(error));
      }
    }

    return return_status;
  }

  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error) {
    if (key == nullptr) {
      SetError(error, "Key must not be null");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    auto set_timeout_option = [=](CallContext context) -> AdbcStatusCode {
      double timeout = 0.0;
      const size_t len = std::strlen(value);
      if (!arrow::internal::StringToFloat(value, len, /*decimal_point=*/'.', &timeout)) {
        SetError(error, "Invalid timeout option value ", key, '=', value,
                 ": invalid floating point value");
        return ADBC_STATUS_INVALID_ARGUMENT;
      }
      if (std::isnan(timeout) || std::isinf(timeout) || timeout < 0) {
        SetError(error, "Invalid timeout option value ", key, '=', value,
                 ": timeout must be positive and finite");
        return ADBC_STATUS_INVALID_ARGUMENT;
      }

      if (timeout == 0) {
        timeout_seconds_.erase(context);
      } else {
        timeout_seconds_[context] = std::chrono::duration<double>(timeout);
      }
      return ADBC_STATUS_OK;
    };

    std::string_view key_view(key);
    std::string_view val_view = value ? value : "";
    if (key == kConnectionOptionAutocommit) {
      if (val_view == kOptionValueEnabled && !transaction_.is_valid()) {
        // No-op - don't error even if the server didn't support transactions
        return ADBC_STATUS_OK;
      }
      ADBC_RETURN_NOT_OK(CheckTransactionSupport(error));
      FlightCallOptions call_options = MakeCallOptions(CallContext::kUpdate);
      if (val_view == kOptionValueEnabled) {
        if (transaction_.is_valid()) {
          ADBC_ARROW_RETURN_NOT_OK(error, client_->Commit(call_options, transaction_));
          transaction_ = no_transaction();
        }
        return ADBC_STATUS_OK;
      } else if (val_view == kOptionValueDisabled) {
        if (transaction_.is_valid()) {
          ADBC_ARROW_RETURN_NOT_OK(error, client_->Commit(call_options, transaction_));
        }
        ADBC_ARROW_RETURN_NOT_OK(
            error, client_->BeginTransaction(call_options).Value(&transaction_));
        return ADBC_STATUS_OK;
      }
      SetError(error, "Invalid connection option value ", key_view, '=', val_view);
      return ADBC_STATUS_INVALID_ARGUMENT;
    } else if (key == kConnectionTimeoutFetchKey) {
      return set_timeout_option(CallContext::kFetch);
    } else if (key == kConnectionTimeoutQueryKey) {
      return set_timeout_option(CallContext::kQuery);
    } else if (key == kConnectionTimeoutUpdateKey) {
      return set_timeout_option(CallContext::kUpdate);
    } else if (key_view.rfind(kCallHeaderPrefix, 0) == 0) {
      std::string header(key_view.substr(kCallHeaderPrefix.size()));
      if (value == nullptr) {
        call_headers_.erase(header);
      } else {
        call_headers_.insert({std::move(header), value});
      }
      return ADBC_STATUS_OK;
    }
    SetError(error, "Unknown connection option ", key_view, '=', val_view);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  //----------------------------------------------------------
  // Metadata
  //----------------------------------------------------------

  AdbcStatusCode GetInfo(uint32_t* info_codes, size_t info_codes_length,
                         struct ArrowArrayStream* stream, struct AdbcError* error) {
    static std::shared_ptr<arrow::Schema> kInfoSchema = arrow::schema({
        arrow::field("info_name", arrow::uint32(), /*nullable=*/false),
        arrow::field(
            "info_value",
            arrow::dense_union({
                arrow::field("string_value", arrow::utf8()),
                arrow::field("bool_value", arrow::boolean()),
                arrow::field("int64_value", arrow::int64()),
                arrow::field("int32_bitmask", arrow::int32()),
                arrow::field("string_list", arrow::list(arrow::utf8())),
                arrow::field("int32_to_int32_list_map",
                             arrow::map(arrow::int32(), arrow::list(arrow::int32()))),
            })),
    });

    // XXX(ARROW-17558): type should be uint32_t not int
    std::vector<int> flight_sql_codes;
    std::vector<uint32_t> codes;
    if (info_codes && info_codes_length > 0) {
      for (size_t i = 0; i < info_codes_length; i++) {
        const uint32_t info_code = info_codes[i];
        switch (info_code) {
          case ADBC_INFO_VENDOR_NAME:
          case ADBC_INFO_VENDOR_VERSION:
          case ADBC_INFO_VENDOR_ARROW_VERSION:
            // These codes are equivalent between the two
            flight_sql_codes.push_back(info_code);
            break;
          case ADBC_INFO_DRIVER_NAME:
          case ADBC_INFO_DRIVER_VERSION:
          case ADBC_INFO_DRIVER_ARROW_VERSION:
            codes.push_back(info_code);
            break;
          default:
            SetError(error, "Unknown info code: ", info_code);
            return ADBC_STATUS_INVALID_ARGUMENT;
        }
      }
    } else {
      flight_sql_codes = {
          SqlInfoOptions::FLIGHT_SQL_SERVER_NAME,
          SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION,
          SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION,
      };
      codes = {
          ADBC_INFO_DRIVER_NAME,
          ADBC_INFO_DRIVER_VERSION,
          ADBC_INFO_DRIVER_ARROW_VERSION,
      };
    }

    RecordBatchVector result;

    UInt32Builder names;
    std::unique_ptr<ArrayBuilder> values;
    ADBC_ARROW_RETURN_NOT_OK(error,
                             MakeBuilder(kInfoSchema->field(1)->type()).Value(&values));
    auto* info_value = static_cast<DenseUnionBuilder*>(values.get());
    auto* info_string = static_cast<StringBuilder*>(info_value->child_builder(0).get());
    int64_t num_values = 0;

    constexpr int8_t kStringCode = 0;

    if (!flight_sql_codes.empty()) {
      FlightCallOptions call_options = MakeCallOptions(CallContext::kQuery);
      std::unique_ptr<FlightInfo> info;
      ADBC_ARROW_RETURN_NOT_OK(
          error, client_->GetSqlInfo(call_options, flight_sql_codes).Value(&info));
      FlightInfoReader reader(client_.get(), MakeCallOptions(CallContext::kFetch),
                              std::move(info));
      ADBC_RETURN_NOT_OK(reader.Init(error));

      if (!reader.schema()->Equals(*SqlSchema::GetSqlInfoSchema())) {
        SetError(error, "Server returned wrong schema, got: ", *reader.schema());
        return ADBC_STATUS_INTERNAL;
      }

      while (true) {
        std::shared_ptr<RecordBatch> batch;
        ADBC_ARROW_RETURN_NOT_OK(error, reader.Next().Value(&batch));
        if (!batch) break;

        const auto& sql_codes = checked_cast<const UInt32Array&>(*batch->column(0));
        const auto& sql_value = checked_cast<const DenseUnionArray&>(*batch->column(1));
        const auto& sql_string =
            checked_cast<const StringArray&>(*sql_value.field(kStringCode));
        for (int64_t i = 0; i < batch->num_rows(); i++) {
          // Shouldn't happen but oh well
          if (!sql_codes.IsValid(i)) continue;

          switch (sql_codes.Value(i)) {
            case SqlInfoOptions::FLIGHT_SQL_SERVER_NAME:
            case SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION:
            case SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION: {
              // These should all be string values where the codes are
              // equivalent between ADBC/Flight SQL
              ADBC_ARROW_RETURN_NOT_OK(error, names.Append(sql_codes.Value(i)));
              if (sql_value.type_code(i) != kStringCode) {
                SetError(error, "Server returned wrong type for info value ",
                         sql_codes.Value(i));
                return ADBC_STATUS_INTERNAL;
              }
              ADBC_ARROW_RETURN_NOT_OK(
                  error,
                  info_string->Append(sql_string.GetString(sql_value.value_offset(i))));
              ADBC_ARROW_RETURN_NOT_OK(error, info_value->Append(kStringCode));
              num_values++;
              break;
            }
            default:
              // Ignore if the server returns something unknown
              continue;
          }
        }
      }
    }

    if (!codes.empty()) {
      for (const uint32_t code : codes) {
        switch (code) {
          case ADBC_INFO_DRIVER_NAME:
            ADBC_ARROW_RETURN_NOT_OK(
                error, info_string->Append("Apache Arrow Flight SQL ADBC Driver"));
            ADBC_ARROW_RETURN_NOT_OK(error, info_value->Append(kStringCode));
            break;
          case ADBC_INFO_DRIVER_VERSION:
          case ADBC_INFO_DRIVER_ARROW_VERSION:
            ADBC_ARROW_RETURN_NOT_OK(error,
                                     info_string->Append(GetBuildInfo().version_string));
            ADBC_ARROW_RETURN_NOT_OK(error, info_value->Append(kStringCode));
            break;
          default:
            SetError(error, "Unknown info code: ", code);
            return ADBC_STATUS_INVALID_ARGUMENT;
        }
        ADBC_ARROW_RETURN_NOT_OK(error, names.Append(code));
        num_values++;
      }
    }

    ArrayVector cols(2);
    ADBC_ARROW_RETURN_NOT_OK(error, names.Finish(&cols[0]));
    ADBC_ARROW_RETURN_NOT_OK(error, values->Finish(&cols[1]));
    result.push_back(RecordBatch::Make(kInfoSchema, num_values, std::move(cols)));

    ADBC_ARROW_RETURN_NOT_OK(error,
                             ExportRecordBatches(kInfoSchema, std::move(result), stream));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetObjects(int depth, const char* catalog, const char* db_schema,
                            const char* table_name, const char** table_type,
                            const char* column_name, struct ArrowArrayStream* stream,
                            struct AdbcError* error) {
    static std::shared_ptr<arrow::DataType> kColumnSchema = arrow::struct_({
        arrow::field("column_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("ordinal_position", arrow::int32()),
        arrow::field("remarks", arrow::utf8()),
        arrow::field("xdbc_data_type", arrow::int16()),
        arrow::field("xdbc_type_name", arrow::utf8()),
        arrow::field("xdbc_column_size", arrow::int32()),
        arrow::field("xdbc_decimal_digits", arrow::int16()),
        arrow::field("xdbc_num_prec_radix", arrow::int16()),
        arrow::field("xdbc_nullable", arrow::int16()),
        arrow::field("xdbc_column_def", arrow::utf8()),
        arrow::field("xdbc_sql_data_type", arrow::int16()),
        arrow::field("xdbc_datetime_sub", arrow::int16()),
        arrow::field("xdbc_char_octet_length", arrow::int32()),
        arrow::field("xdbc_is_nullable", arrow::utf8()),
        arrow::field("xdbc_scope_catalog", arrow::utf8()),
        arrow::field("xdbc_scope_schema", arrow::utf8()),
        arrow::field("xdbc_scope_table", arrow::utf8()),
        arrow::field("xdbc_is_autoincrement", arrow::boolean()),
        arrow::field("xdbc_is_generatedcolumn", arrow::boolean()),
    });
    static std::shared_ptr<arrow::DataType> kUsageSchema = arrow::struct_({
        arrow::field("fk_catalog", arrow::utf8()),
        arrow::field("fk_db_schema", arrow::utf8()),
        arrow::field("fk_table", arrow::utf8(), /*nullable=*/false),
        arrow::field("fk_column_name", arrow::utf8(), /*nullable=*/false),
    });
    static std::shared_ptr<arrow::DataType> kConstraintSchema = arrow::struct_({
        arrow::field("constraint_name", arrow::utf8()),
        arrow::field("constraint_type", arrow::utf8(), /*nullable=*/false),
        arrow::field("constraint_column_names", arrow::list(arrow::utf8()),
                     /*nullable=*/false),
        arrow::field("constraint_column_usage", arrow::list(kUsageSchema)),
    });
    static std::shared_ptr<arrow::DataType> kTableSchema = arrow::struct_({
        arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_columns", arrow::list(kColumnSchema)),
        arrow::field("table_constraints", arrow::list(kConstraintSchema)),
    });
    static std::shared_ptr<arrow::DataType> kDbSchemaSchema = arrow::struct_({
        arrow::field("db_schema_name", arrow::utf8()),
        arrow::field("db_schema_tables", arrow::list(kTableSchema)),
    });
    static std::shared_ptr<arrow::Schema> kCatalogSchema = arrow::schema({
        arrow::field("catalog_name", arrow::utf8()),
        arrow::field("catalog_db_schemas", arrow::list(kDbSchemaSchema)),
    });
    FlightCallOptions call_options = MakeCallOptions(CallContext::kQuery);

    // To avoid an N+1 query problem, we assume result sets here will
    // fit in memory and build up a single response.

    std::string db_schema_filter = db_schema ? db_schema : "";
    std::string table_name_filter = table_name ? table_name : "";
    std::vector<std::string> table_type_filter;
    if (table_type) {
      while (*table_type) {
        table_type_filter.emplace_back(*table_type);
        table_type++;
      }
    }

    TableIndex table_index;
    std::shared_ptr<RecordBatch> table_data;

    if (depth == ADBC_OBJECT_DEPTH_ALL || depth >= ADBC_OBJECT_DEPTH_DB_SCHEMAS) {
      std::unique_ptr<FlightInfo> info;
      ADBC_ARROW_RETURN_NOT_OK(error,
                               client_
                                   ->GetDbSchemas(call_options, /*catalog=*/nullptr,
                                                  db_schema ? &db_schema_filter : nullptr)
                                   .Value(&info));
      FlightInfoReader reader(client_.get(), MakeCallOptions(CallContext::kFetch),
                              std::move(info));
      ADBC_RETURN_NOT_OK(reader.Init(error));
      ADBC_ARROW_RETURN_NOT_OK(error, IndexDbSchemas(&reader, &table_index));
    }
    if (depth == ADBC_OBJECT_DEPTH_ALL || depth >= ADBC_OBJECT_DEPTH_TABLES) {
      std::unique_ptr<FlightInfo> info;
      ADBC_ARROW_RETURN_NOT_OK(error,
                               client_
                                   ->GetTables(call_options, /*catalog=*/nullptr,
                                               db_schema ? &db_schema_filter : nullptr,
                                               table_name ? &table_name_filter : nullptr,
                                               /*include_schemas=*/true,
                                               table_type ? &table_type_filter : nullptr)
                                   .Value(&info));
      FlightInfoReader reader(client_.get(), MakeCallOptions(CallContext::kFetch),
                              std::move(info));
      ADBC_RETURN_NOT_OK(reader.Init(error));
      ADBC_ARROW_RETURN_NOT_OK(error, IndexTables(&reader, &table_index, &table_data));
    }

    std::unique_ptr<ArrayBuilder> raw_builder;
    ADBC_ARROW_RETURN_NOT_OK(error, MakeBuilder(kDbSchemaSchema).Value(&raw_builder));
    StringBuilder catalog_name_builder;
    ListBuilder catalog_db_schemas_builder(default_memory_pool(), std::move(raw_builder));
    auto* catalog_db_schemas_items_builder =
        checked_cast<StructBuilder*>(catalog_db_schemas_builder.value_builder());
    auto* db_schema_name_builder =
        checked_cast<StringBuilder*>(catalog_db_schemas_items_builder->field_builder(0));
    auto* db_schema_tables_builder =
        checked_cast<ListBuilder*>(catalog_db_schemas_items_builder->field_builder(1));
    auto* db_schema_tables_items_builder =
        checked_cast<StructBuilder*>(db_schema_tables_builder->value_builder());
    auto* table_name_builder =
        checked_cast<StringBuilder*>(db_schema_tables_items_builder->field_builder(0));
    auto* table_type_builder =
        checked_cast<StringBuilder*>(db_schema_tables_items_builder->field_builder(1));
    auto* table_columns_builder =
        checked_cast<ListBuilder*>(db_schema_tables_items_builder->field_builder(2));
    auto* table_constraints_builder =
        checked_cast<ListBuilder*>(db_schema_tables_items_builder->field_builder(3));
    auto* table_columns_items_builder =
        checked_cast<StructBuilder*>(table_columns_builder->value_builder());
    auto* column_name_builder =
        checked_cast<StringBuilder*>(table_columns_items_builder->field_builder(0));
    auto* ordinal_position_builder =
        checked_cast<Int32Builder*>(table_columns_items_builder->field_builder(1));

    std::unique_ptr<FlightInfo> info;
    ADBC_ARROW_RETURN_NOT_OK(error, client_->GetCatalogs(call_options).Value(&info));
    FlightInfoReader catalog_reader(client_.get(), MakeCallOptions(CallContext::kFetch),
                                    std::move(info));
    ADBC_RETURN_NOT_OK(catalog_reader.Init(error));

    ReaderIterator catalogs(*SqlSchema::GetCatalogsSchema(), &catalog_reader);
    ADBC_ARROW_RETURN_NOT_OK(error, catalogs.Init());
    while (true) {
      bool have_data = false;
      ADBC_ARROW_RETURN_NOT_OK(error, catalogs.Next().Value(&have_data));
      if (!have_data) break;

      std::optional<std::string> cur_catalog_name(catalogs.GetNullable<StringType>(0));
      // TODO(lidavidm): catalog is a filter string (evaluate with compute fn if
      // available)
      if (catalog && cur_catalog_name != catalog) continue;

      if (cur_catalog_name) {
        ADBC_ARROW_RETURN_NOT_OK(error, catalog_name_builder.Append(*cur_catalog_name));
      } else {
        ADBC_ARROW_RETURN_NOT_OK(error, catalog_name_builder.AppendNull());
      }

      if (depth == ADBC_OBJECT_DEPTH_ALL || depth >= ADBC_OBJECT_DEPTH_DB_SCHEMAS) {
        ADBC_ARROW_RETURN_NOT_OK(error, catalog_db_schemas_builder.Append());
      } else {
        ADBC_ARROW_RETURN_NOT_OK(error, catalog_db_schemas_builder.AppendNull());
        continue;
      }

      auto it = table_index.find(cur_catalog_name);
      if (it == table_index.end()) continue;

      for (const auto& schema_item : it->second) {
        ADBC_ARROW_RETURN_NOT_OK(error, catalog_db_schemas_items_builder->Append());
        const std::optional<std::string>& cur_schema_name = schema_item.first;

        if (cur_schema_name) {
          ADBC_ARROW_RETURN_NOT_OK(error,
                                   db_schema_name_builder->Append(*cur_schema_name));
        } else {
          ADBC_ARROW_RETURN_NOT_OK(error, db_schema_name_builder->AppendNull());
        }

        if (depth == ADBC_OBJECT_DEPTH_ALL || depth >= ADBC_OBJECT_DEPTH_TABLES) {
          ADBC_ARROW_RETURN_NOT_OK(error, db_schema_tables_builder->Append());
        } else {
          ADBC_ARROW_RETURN_NOT_OK(error, db_schema_tables_builder->AppendNull());
          continue;
        }

        std::pair<int64_t, int64_t> schema_tables_range = schema_item.second;
        for (int64_t i = schema_tables_range.first; i < schema_tables_range.second; i++) {
          const std::string_view table_name = GetNotNull<StringType>(*table_data, 2, i);
          const std::string_view table_type = GetNotNull<StringType>(*table_data, 3, i);
          const std::string_view table_schema = GetNotNull<BinaryType>(*table_data, 4, i);

          ADBC_ARROW_RETURN_NOT_OK(error, db_schema_tables_items_builder->Append());
          ADBC_ARROW_RETURN_NOT_OK(error, table_name_builder->Append(table_name));
          ADBC_ARROW_RETURN_NOT_OK(error, table_type_builder->Append(table_type));

          if (depth == ADBC_OBJECT_DEPTH_COLUMNS) {
            ADBC_ARROW_RETURN_NOT_OK(error, table_columns_builder->Append());

            std::shared_ptr<Schema> schema;
            io::BufferReader reader((Buffer(table_schema)));
            ipc::DictionaryMemo memo;
            ADBC_ARROW_RETURN_NOT_OK(error,
                                     ipc::ReadSchema(&reader, &memo).Value(&schema));

            int32_t ordinal_position = 1;
            for (const std::shared_ptr<Field>& field : schema->fields()) {
#ifdef ARROW_COMPUTE
              if (column_name && std::strlen(column_name) > 0) {
                Datum result;
                arrow::compute::MatchSubstringOptions options(column_name);
                ADBC_ARROW_RETURN_NOT_OK(
                    error, arrow::compute::CallFunction(
                               "match_like", {Datum(MakeScalar(field->name()))}, &options)
                               .Value(&result));
                DCHECK_EQ(result.kind(), Datum::Kind::SCALAR);
                if (!result.scalar_as<BooleanScalar>().value) continue;
              }
#else
              if (column_name && std::strlen(column_name) > 0) {
                SetError(error, "Cannot filter on column name without ARROW_COMPUTE");
                return ADBC_STATUS_NOT_IMPLEMENTED;
              }
#endif
              ADBC_ARROW_RETURN_NOT_OK(error, table_columns_items_builder->Append());
              ADBC_ARROW_RETURN_NOT_OK(error, column_name_builder->Append(field->name()));
              ADBC_ARROW_RETURN_NOT_OK(
                  error, ordinal_position_builder->Append(ordinal_position++));
              for (int i = 2; i < table_columns_items_builder->num_fields(); i++) {
                ADBC_ARROW_RETURN_NOT_OK(
                    error, table_columns_items_builder->field_builder(i)->AppendNull());
              }
            }

            // TODO(lidavidm): unimplemented for now
            ADBC_ARROW_RETURN_NOT_OK(error, table_constraints_builder->Append());
          } else {
            ADBC_ARROW_RETURN_NOT_OK(error, table_columns_builder->AppendNull());
            ADBC_ARROW_RETURN_NOT_OK(error, table_constraints_builder->AppendNull());
          }
        }
      }
    }

    ArrayVector arrays(2);
    ADBC_ARROW_RETURN_NOT_OK(error, catalog_name_builder.Finish(&arrays[0]));
    ADBC_ARROW_RETURN_NOT_OK(error, catalog_db_schemas_builder.Finish(&arrays[1]));
    const int64_t num_rows = arrays[0]->length();
    auto batch = RecordBatch::Make(kCatalogSchema, num_rows, std::move(arrays));

    ADBC_ARROW_RETURN_NOT_OK(
        error, ExportRecordBatches(kCatalogSchema, {std::move(batch)}, stream));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, struct ArrowSchema* schema,
                                struct AdbcError* error) {
    FlightCallOptions call_options = MakeCallOptions(CallContext::kQuery);
    std::string catalog_str, db_schema_str, table_name_str;

    if (catalog) catalog_str = catalog;
    if (db_schema) db_schema_str = db_schema;
    if (table_name) table_name_str = table_name;

    std::unique_ptr<FlightInfo> info;
    ADBC_ARROW_RETURN_NOT_OK(
        error, client_
                   ->GetTables(call_options, catalog ? &catalog_str : nullptr,
                               db_schema ? &db_schema_str : nullptr,
                               table_name ? &table_name_str : nullptr,
                               /*include_schema=*/true, /*table_types=*/nullptr)
                   .Value(&info));

    FlightInfoReader reader(client_.get(), MakeCallOptions(CallContext::kFetch),
                            std::move(info));
    ADBC_RETURN_NOT_OK(reader.Init(error));

    if (!reader.schema()->Equals(*SqlSchema::GetTablesSchemaWithIncludedSchema())) {
      SetError(error, "Server returned wrong schema, got:\n", *reader.schema(),
               "\nExpected:\n", *SqlSchema::GetTablesSchemaWithIncludedSchema());
      return ADBC_STATUS_INTERNAL;
    }

    while (true) {
      std::shared_ptr<RecordBatch> batch;
      ADBC_ARROW_RETURN_NOT_OK(error, reader.Next().Value(&batch));
      if (!batch) break;
      if (batch->num_rows() == 0) continue;

      const auto& schema_col = batch->column(4);
      if (schema_col->type()->id() != Type::BINARY) {
        SetError(error, "Server returned invalid schema; expected binary, found ",
                 *schema_col->type());
        return ADBC_STATUS_INTERNAL;
      }
      const auto& binary = checked_cast<const BinaryArray&>(*schema_col);
      if (!binary.IsValid(0)) {
        SetError(error, "Schema was null though field is non-null");
        return ADBC_STATUS_INTERNAL;
      }

      ipc::DictionaryMemo memo;
      io::BufferReader stream(Buffer::FromString(binary.GetString(0)));
      std::shared_ptr<Schema> arrow_schema;
      ADBC_ARROW_RETURN_NOT_OK(error,
                               ipc::ReadSchema(&stream, &memo).Value(&arrow_schema));
      ADBC_ARROW_RETURN_NOT_OK(error, arrow::ExportSchema(*arrow_schema, schema));
      return ADBC_STATUS_OK;
    }

    SetError(error, "No table found meeting criteria");
    return ADBC_STATUS_NOT_FOUND;
  }

  AdbcStatusCode GetTableTypes(struct ArrowArrayStream* stream, struct AdbcError* error) {
    FlightCallOptions call_options = MakeCallOptions(CallContext::kQuery);
    std::unique_ptr<FlightInfo> flight_info;
    auto status = client_->GetTableTypes(call_options).Value(&flight_info);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    return FlightInfoReader::Export(client_.get(), MakeCallOptions(CallContext::kFetch),
                                    std::move(flight_info), stream, error);
  }

  //----------------------------------------------------------
  // Partitioned Results
  //----------------------------------------------------------

  AdbcStatusCode ReadPartition(const uint8_t* serialized_partition,
                               size_t serialized_length, struct ArrowArrayStream* out,
                               struct AdbcError* error) {
    std::unique_ptr<FlightInfo> info;
    ADBC_ARROW_RETURN_NOT_OK(
        error, FlightInfo::Deserialize(
                   std::string_view(reinterpret_cast<const char*>(serialized_partition),
                                    serialized_length))
                   .Value(&info));
    return FlightInfoReader::Export(client_.get(), MakeCallOptions(CallContext::kFetch),
                                    std::move(info), out, error);
  }

  //----------------------------------------------------------
  // Transactions
  //----------------------------------------------------------

  AdbcStatusCode Commit(struct AdbcError* error) {
    if (transaction_.is_valid()) {
      ADBC_RETURN_NOT_OK(CheckTransactionSupport(error));
      FlightCallOptions call_options = MakeCallOptions(CallContext::kUpdate);
      ADBC_ARROW_RETURN_NOT_OK(error, client_->Commit(call_options, transaction_));
      ADBC_ARROW_RETURN_NOT_OK(
          error, client_->BeginTransaction(call_options).Value(&transaction_));
      return ADBC_STATUS_OK;
    }
    SetError(error, "Cannot commit when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  AdbcStatusCode Rollback(struct AdbcError* error) {
    if (transaction_.is_valid()) {
      ADBC_RETURN_NOT_OK(CheckTransactionSupport(error));
      FlightCallOptions call_options = MakeCallOptions(CallContext::kUpdate);
      ADBC_ARROW_RETURN_NOT_OK(error, client_->Rollback(call_options, transaction_));
      ADBC_ARROW_RETURN_NOT_OK(
          error, client_->BeginTransaction(call_options).Value(&transaction_));
      return ADBC_STATUS_OK;
    }
    SetError(error, "Cannot rollback when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

 private:
  // Check SqlInfo to determine what the server does/doesn't support.
  Status QuerySqlInfo() {
    FlightCallOptions call_options = MakeCallOptions(CallContext::kQuery);
    ARROW_ASSIGN_OR_RAISE(
        auto info, client_->GetSqlInfo(call_options,
                                       {
                                           SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION,
                                       }));

    FlightInfoReader reader(client_.get(), MakeCallOptions(CallContext::kFetch),
                            std::move(info));
    struct AdbcError error = {};
    if (reader.Init(&error) != ADBC_STATUS_OK) {
      std::string message = "Could not initialize reader: ";
      if (error.message) {
        message += error.message;
      }
      if (error.release) {
        error.release(&error);
      }
      return Status::IOError(std::move(message));
    }

    constexpr int8_t kInt32Code = 3;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto batch, reader.Next());
      if (!batch) break;

      const auto& sql_codes = checked_cast<const UInt32Array&>(*batch->column(0));
      const auto& sql_value = checked_cast<const DenseUnionArray&>(*batch->column(1));
      const auto& sql_int32 =
          checked_cast<const Int32Array&>(*sql_value.field(kInt32Code));
      for (int64_t i = 0; i < batch->num_rows(); i++) {
        if (!sql_codes.IsValid(i)) continue;

        switch (sql_codes.Value(i)) {
          case SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION: {
            if (sql_value.type_code(i) != kInt32Code) {
              continue;
            }
            const int32_t idx = sql_value.value_offset(i);
            if (!sql_int32.IsValid(idx)) {
              continue;
            }
            const int32_t value = sql_int32.IsValid(idx) && sql_int32.Value(idx);
            support_.transactions =
                (value == SqlInfoOptions::SQL_SUPPORTED_TRANSACTION_TRANSACTION ||
                 value == SqlInfoOptions::SQL_SUPPORTED_TRANSACTION_SAVEPOINT);
            break;
          }
          default:
            continue;
        }
      }
    }

    return reader.Close();
  }

  AdbcStatusCode CheckTransactionSupport(struct AdbcError* error) {
    if (!support_.transactions) {
      SetError(error, "Server does not report transaction support");
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }
    return ADBC_STATUS_OK;
  }

  struct {
    bool transactions = false;
  } support_;
  std::shared_ptr<FlightSqlDatabaseImpl> database_;
  std::unique_ptr<FlightSqlClient> client_;
  Transaction transaction_ = no_transaction();
  std::shared_ptr<FlightSqlQuirks> quirks_;
  std::unordered_map<CallContext, std::chrono::duration<double>> timeout_seconds_;
  std::unordered_map<std::string, std::string> call_headers_;
};

class FlightSqlPartitionsImpl {
 public:
  explicit FlightSqlPartitionsImpl(std::vector<std::string> partitions)
      : partitions_(std::move(partitions)) {
    pointers_.resize(partitions_.size());
    lengths_.reserve(partitions_.size());

    for (size_t i = 0; i < partitions_.size(); i++) {
      pointers_[i] = reinterpret_cast<const uint8_t*>(partitions_[i].data());
      lengths_[i] = partitions_[i].size();
    }
  }

  static AdbcStatusCode Export(const FlightInfo& info, struct AdbcPartitions* out,
                               struct AdbcError* error) {
    std::vector<std::string> partitions;
    partitions.reserve(info.endpoints().size());
    for (const FlightEndpoint& endpoint : info.endpoints()) {
      FlightInfo partition_info(FlightInfo::Data{
          info.serialized_schema(),
          info.descriptor(),
          {endpoint},
          /*total_records=*/-1,
          /*total_bytes=*/-1,
      });
      std::string serialized;
      ADBC_ARROW_RETURN_NOT_OK(error,
                               partition_info.SerializeToString().Value(&serialized));
      partitions.push_back(std::move(serialized));
    }

    auto* impl = new FlightSqlPartitionsImpl(std::move(partitions));
    out->num_partitions = impl->partitions_.size();
    out->partitions = impl->pointers_.data();
    out->partition_lengths = impl->lengths_.data();
    out->private_data = impl;
    out->release = &Release;
    return ADBC_STATUS_OK;
  }

  static void Release(struct AdbcPartitions* partitions) {
    auto* impl = static_cast<FlightSqlPartitionsImpl*>(partitions->private_data);
    delete impl;
    partitions->num_partitions = 0;
    partitions->partitions = nullptr;
    partitions->partition_lengths = nullptr;
    partitions->private_data = nullptr;
    partitions->release = nullptr;
  }

 private:
  std::vector<std::string> partitions_;
  std::vector<const uint8_t*> pointers_;
  std::vector<size_t> lengths_;
};

class FlightSqlStatementImpl {
 public:
  explicit FlightSqlStatementImpl(std::shared_ptr<FlightSqlConnectionImpl> connection)
      : connection_(std::move(connection)) {}

  AdbcStatusCode Bind(struct ArrowArray* array, struct ArrowSchema* schema,
                      struct AdbcError* error) {
    std::shared_ptr<RecordBatch> batch;
    ADBC_ARROW_RETURN_NOT_OK(error, ImportRecordBatch(array, schema).Value(&batch));
    ADBC_ARROW_RETURN_NOT_OK(
        error, RecordBatchReader::Make({std::move(batch)}).Value(&bind_parameters_));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Bind(struct ArrowArrayStream* stream, struct AdbcError* error) {
    ADBC_ARROW_RETURN_NOT_OK(error,
                             ImportRecordBatchReader(stream).Value(&bind_parameters_));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Close(struct AdbcError* error) {
    ADBC_RETURN_NOT_OK(ClosePreparedStatement(error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecutePartitions(struct ArrowSchema* schema,
                                   struct AdbcPartitions* partitions,
                                   int64_t* rows_affected, struct AdbcError* error) {
    std::unique_ptr<FlightInfo> info;
    ADBC_ARROW_RETURN_NOT_OK(error, ExecuteFlightInfo().Value(&info));
    if (rows_affected) *rows_affected = info->total_records();

    ipc::DictionaryMemo memo;
    std::shared_ptr<Schema> arrow_schema;
    ADBC_ARROW_RETURN_NOT_OK(error, info->GetSchema(&memo).Value(&arrow_schema));
    ADBC_ARROW_RETURN_NOT_OK(error, ExportSchema(*arrow_schema, schema));
    ADBC_RETURN_NOT_OK(FlightSqlPartitionsImpl::Export(*info, partitions, error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecuteQuery(struct ArrowArrayStream* out, int64_t* rows_affected,
                              struct AdbcError* error) {
    if (!ingest_.target_table.empty()) {
      if (out) {
        SetError(error, "Must not provide out for bulk ingest");
        return ADBC_STATUS_INVALID_ARGUMENT;
      }
      return ExecuteIngest(rows_affected, error);
    } else if (plan_.plan.empty() && query_.empty() && !prepared_statement_) {
      SetError(error, "Must provide query");
      return ADBC_STATUS_INVALID_STATE;
    }

    if (!out) {
      int64_t out_rows = 0;
      ADBC_ARROW_RETURN_NOT_OK(error, ExecuteUpdate().Value(&out_rows));
      if (rows_affected) *rows_affected = out_rows;
      return ADBC_STATUS_OK;
    }

    std::unique_ptr<FlightInfo> info;
    ADBC_ARROW_RETURN_NOT_OK(error, ExecuteFlightInfo().Value(&info));
    if (rows_affected) *rows_affected = info->total_records();
    return FlightInfoReader::Export(connection_->client(),
                                    MakeCallOptions(CallContext::kFetch), std::move(info),
                                    out, error);
  }

  AdbcStatusCode GetParameterSchema(struct ArrowSchema* schema, struct AdbcError* error) {
    if (!prepared_statement_) {
      SetError(error, "Must Prepare() before GetParameterSchema()");
      return ADBC_STATUS_INVALID_STATE;
    }
    ADBC_ARROW_RETURN_NOT_OK(
        error, arrow::ExportSchema(*prepared_statement_->parameter_schema(), schema));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Prepare(struct AdbcError* error) {
    if (plan_.plan.empty() && query_.empty()) {
      SetError(error, "Must provide query");
      return ADBC_STATUS_INVALID_STATE;
    }
    FlightCallOptions call_options = MakeCallOptions(CallContext::kUpdate);
    ADBC_RETURN_NOT_OK(ClosePreparedStatement(error));
    if (!plan_.plan.empty()) {
      DCHECK(query_.empty());
      ADBC_ARROW_RETURN_NOT_OK(
          error, connection_->client()
                     ->PrepareSubstrait(call_options, plan_, connection_->transaction())
                     .Value(&prepared_statement_));
    } else {
      ADBC_ARROW_RETURN_NOT_OK(
          error, connection_->client()
                     ->Prepare(call_options, query_, connection_->transaction())
                     .Value(&prepared_statement_));
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error) {
    if (key == nullptr) {
      SetError(error, "Key must not be null");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    std::string_view key_view(key);
    std::string_view val_view = value ? value : "";
    if (key_view == kIngestOptionTargetTable) {
      ADBC_RETURN_NOT_OK(ClearQueryParams(error));
      ingest_.target_table = val_view;
    } else if (key_view == kIngestOptionMode) {
      if (value == kIngestOptionModeCreate) {
        ingest_.append = false;
      } else if (value == kIngestOptionModeAppend) {
        ingest_.append = true;
      } else {
        SetError(error, "Invalid statement option value ", key_view, "=", val_view);
        return ADBC_STATUS_INVALID_ARGUMENT;
      }
    } else if (key_view == kStatementSubstraitVersionKey) {
      plan_.version = value;
    } else if (key_view.rfind(kCallHeaderPrefix, 0) == 0) {
      std::string header(key_view.substr(kCallHeaderPrefix.size()));
      if (value == nullptr) {
        call_headers_.erase(header);
      } else {
        call_headers_.insert({std::move(header), std::string(val_view)});
      }
      return ADBC_STATUS_OK;
    } else {
      SetError(error, "Unknown statement option ", key_view, "=", val_view);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetSqlQuery(const char* query, struct AdbcError* error) {
    ADBC_RETURN_NOT_OK(ClearQueryParams(error));
    query_ = query;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetSubstraitPlan(const uint8_t* plan, size_t length,
                                  struct AdbcError* error) {
    ADBC_RETURN_NOT_OK(ClearQueryParams(error));
    plan_.plan = std::string(reinterpret_cast<const char*>(plan), length);
    return ADBC_STATUS_OK;
  }

 private:
  AdbcStatusCode ClosePreparedStatement(struct AdbcError* error) {
    if (prepared_statement_) {
      FlightCallOptions call_options = MakeCallOptions(CallContext::kUpdate);
      ADBC_ARROW_RETURN_NOT_OK(error, prepared_statement_->Close(call_options));
      prepared_statement_ = nullptr;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ClearQueryParams(struct AdbcError* error) {
    ADBC_RETURN_NOT_OK(ClosePreparedStatement(error));
    ingest_.target_table.clear();
    plan_.plan.clear();
    query_.clear();
    if (bind_parameters_) {
      ADBC_ARROW_RETURN_NOT_OK(error, bind_parameters_->Close());
      bind_parameters_.reset();
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecuteIngest(int64_t* rows_affected, struct AdbcError* error) {
    if (!bind_parameters_) {
      SetError(error, "Must Bind() before bulk ingestion");
      return ADBC_STATUS_INVALID_STATE;
    }
    if (!IsApproximatelyValidIdentifier(ingest_.target_table)) {
      SetError(error, "Invalid target table ", ingest_.target_table,
               ": must be alphanumeric with underscores");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    FlightCallOptions call_options = MakeCallOptions(CallContext::kUpdate);
    if (!ingest_.append) {
      std::string create = "CREATE TABLE ";

      create += ingest_.target_table;
      create += " (";

      bool first = true;
      for (const std::shared_ptr<Field>& field : bind_parameters_->schema()->fields()) {
        if (!first) {
          create += ", ";
        }
        first = false;

        if (!IsApproximatelyValidIdentifier(field->name())) {
          SetError(error, "Invalid column name ", field->name(),
                   ": must be alphanumeric with underscores");
          return ADBC_STATUS_INVALID_ARGUMENT;
        }

        create += field->name();
        const std::unordered_map<Type::type, std::string>& mapping =
            connection_->quirks().ingest_type_mapping;
        const auto it = mapping.find(field->type()->id());
        if (it == mapping.end()) {
          SetError(error, "Data type not supported for bulk ingest: ", field->ToString());
          return ADBC_STATUS_NOT_IMPLEMENTED;
        } else {
          create += " ";
          create += it->second;
        }
      }
      create += ")";
      SetError(error, "Creating table via: ", create);

      ADBC_ARROW_RETURN_NOT_OK(
          error, connection_->client()
                     ->ExecuteUpdate(call_options, create, connection_->transaction())
                     .status());
    }

    std::string append = "INSERT INTO ";
    {
      append += ingest_.target_table;
      append += " VALUES (";
      for (int i = 0; i < bind_parameters_->schema()->num_fields(); i++) {
        if (i > 0) append += ", ";
        // TODO(lidavidm): add config option for parameter symbol (or
        // query Flight SQL metadata?)
        append += "?";
      }
      append += ")";
      SetError(error, "Updating table via: ", append);
    }

    std::shared_ptr<PreparedStatement> stmt;
    ADBC_ARROW_RETURN_NOT_OK(
        error, connection_->client()
                   ->Prepare(call_options, append, connection_->transaction())
                   .Value(&stmt));

    int64_t total_rows = 0;
    ADBC_ARROW_RETURN_NOT_OK(error, stmt->SetParameters(std::move(bind_parameters_)));
    ADBC_ARROW_RETURN_NOT_OK(error, stmt->ExecuteUpdate().Value(&total_rows));
    ADBC_ARROW_RETURN_NOT_OK(error, stmt->Close());
    if (rows_affected) *rows_affected = total_rows;
    return ADBC_STATUS_OK;
  }

  arrow::Result<int64_t> ExecuteUpdate() {
    FlightCallOptions call_options = MakeCallOptions(CallContext::kUpdate);
    if (prepared_statement_) {
      if (bind_parameters_) {
        ARROW_RETURN_NOT_OK(
            prepared_statement_->SetParameters(std::move(bind_parameters_)));
      }
      return prepared_statement_->ExecuteUpdate(call_options);
    }

    if (!plan_.plan.empty()) {
      return connection_->client()->ExecuteSubstraitUpdate(call_options, plan_,
                                                           connection_->transaction());
    }
    return connection_->client()->ExecuteUpdate(call_options, query_,
                                                connection_->transaction());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> ExecuteFlightInfo() {
    FlightCallOptions call_options = MakeCallOptions(CallContext::kQuery);
    if (prepared_statement_) {
      ARROW_RETURN_NOT_OK(
          prepared_statement_->SetParameters(std::move(bind_parameters_)));
      return prepared_statement_->Execute(call_options);
    }
    if (!plan_.plan.empty()) {
      return connection_->client()->ExecuteSubstrait(call_options, plan_,
                                                     connection_->transaction());
    }
    return connection_->client()->Execute(call_options, query_,
                                          connection_->transaction());
  }

  FlightCallOptions MakeCallOptions(CallContext context) const {
    FlightCallOptions options = connection_->MakeCallOptions(context);
    for (const auto& header : call_headers_) {
      options.headers.emplace_back(header.first, header.second);
    }
    return options;
  }

  std::shared_ptr<FlightSqlConnectionImpl> connection_;
  std::shared_ptr<PreparedStatement> prepared_statement_;
  SubstraitPlan plan_;
  std::string query_;
  std::shared_ptr<RecordBatchReader> bind_parameters_;
  std::unordered_map<std::string, std::string> call_headers_;
  // Bulk ingest state
  struct {
    std::string target_table;
    bool append = false;
  } ingest_;
};

AdbcStatusCode FlightSqlDatabaseNew(struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  auto impl = std::make_shared<FlightSqlDatabaseImpl>();
  database->private_data = new std::shared_ptr<FlightSqlDatabaseImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode FlightSqlDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                          const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode FlightSqlDatabaseInit(struct AdbcDatabase* database,
                                     struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode FlightSqlDatabaseRelease(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

AdbcStatusCode FlightSqlConnectionCommit(struct AdbcConnection* connection,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->Commit(error);
}

AdbcStatusCode FlightSqlConnectionGetInfo(struct AdbcConnection* connection,
                                          uint32_t* info_codes, size_t info_codes_length,
                                          struct ArrowArrayStream* stream,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->GetInfo(info_codes, info_codes_length, stream, error);
}

AdbcStatusCode FlightSqlConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_type,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->GetObjects(depth, catalog, db_schema, table_name, table_type,
                            column_name, stream, error);
}

AdbcStatusCode FlightSqlConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->GetTableSchema(catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode FlightSqlConnectionGetTableTypes(struct AdbcConnection* connection,
                                                struct ArrowArrayStream* stream,
                                                struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->GetTableTypes(stream, error);
}

AdbcStatusCode FlightSqlConnectionInit(struct AdbcConnection* connection,
                                       struct AdbcDatabase* database,
                                       struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->Init(database, error);
}

AdbcStatusCode FlightSqlConnectionNew(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  auto impl = std::make_shared<FlightSqlConnectionImpl>();
  connection->private_data = new std::shared_ptr<FlightSqlConnectionImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode FlightSqlConnectionReadPartition(struct AdbcConnection* connection,
                                                const uint8_t* serialized_partition,
                                                size_t serialized_length,
                                                struct ArrowArrayStream* out,
                                                struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->ReadPartition(serialized_partition, serialized_length, out, error);
}

AdbcStatusCode FlightSqlConnectionRelease(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

AdbcStatusCode FlightSqlConnectionRollback(struct AdbcConnection* connection,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->Rollback(error);
}

AdbcStatusCode FlightSqlConnectionSetOption(struct AdbcConnection* connection,
                                            const char* key, const char* value,
                                            struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode FlightSqlStatementBind(struct AdbcStatement* statement,
                                      struct ArrowArray* values,
                                      struct ArrowSchema* schema,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->Bind(values, schema, error);
}

AdbcStatusCode FlightSqlStatementBindStream(struct AdbcStatement* statement,
                                            struct ArrowArrayStream* stream,
                                            struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->Bind(stream, error);
}

AdbcStatusCode FlightSqlStatementExecutePartitions(struct AdbcStatement* statement,
                                                   struct ArrowSchema* schema,
                                                   struct AdbcPartitions* partitions,
                                                   int64_t* rows_affected,
                                                   struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->ExecutePartitions(schema, partitions, rows_affected, error);
}

AdbcStatusCode FlightSqlStatementExecuteQuery(struct AdbcStatement* statement,
                                              struct ArrowArrayStream* out,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->ExecuteQuery(out, rows_affected, error);
}

AdbcStatusCode FlightSqlStatementGetParameterSchema(struct AdbcStatement* statement,
                                                    struct ArrowSchema* schema,
                                                    struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetParameterSchema(schema, error);
}

AdbcStatusCode FlightSqlStatementNew(struct AdbcConnection* connection,
                                     struct AdbcStatement* statement,
                                     struct AdbcError* error) {
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  auto impl = std::make_shared<FlightSqlStatementImpl>(*ptr);
  statement->private_data = new std::shared_ptr<FlightSqlStatementImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode FlightSqlStatementPrepare(struct AdbcStatement* statement,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->Prepare(error);
}

AdbcStatusCode FlightSqlStatementRelease(struct AdbcStatement* statement,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

AdbcStatusCode FlightSqlStatementSetOption(struct AdbcStatement* statement,
                                           const char* key, const char* value,
                                           struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode FlightSqlStatementSetSqlQuery(struct AdbcStatement* statement,
                                             const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(query, error);
}

AdbcStatusCode FlightSqlStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                                  const uint8_t* plan, size_t length,
                                                  struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->SetSubstraitPlan(plan, length, error);
}
}  // namespace
}  // namespace arrow::flight::sql

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlDatabaseRelease(database, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* stream,
                                     struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionGetInfo(connection, info_codes,
                                                        info_codes_length, stream, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_type,
                                        const char* column_name,
                                        struct ArrowArrayStream* stream,
                                        struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionGetObjects(
      connection, depth, catalog, db_schema, table_name, table_type, column_name, stream,
      error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionGetTableSchema(
      connection, catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionGetTableTypes(connection, stream, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionReadPartition(
      connection, serialized_partition, serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionRelease(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementBindStream(statement, stream, error);
}

// XXX: cpplint gets confused if declared as struct ArrowSchema*
AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementExecutePartitions(
      statement, schema, partitions, rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementExecuteQuery(statement, out, rows_affected,
                                                            error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementGetParameterSchema(statement, schema,
                                                                  error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                             const uint8_t* plan, size_t length,
                                             struct AdbcError* error) {
  return arrow::flight::sql::FlightSqlStatementSetSubstraitPlan(statement, plan, length,
                                                                error);
}

extern "C" {
ARROW_FLIGHT_SQL_EXPORT
AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) return ADBC_STATUS_NOT_IMPLEMENTED;

  auto* driver = reinterpret_cast<struct AdbcDriver*>(raw_driver);
  std::memset(driver, 0, sizeof(*driver));

  driver->DatabaseInit = arrow::flight::sql::FlightSqlDatabaseInit;
  driver->DatabaseNew = arrow::flight::sql::FlightSqlDatabaseNew;
  driver->DatabaseRelease = arrow::flight::sql::FlightSqlDatabaseRelease;
  driver->DatabaseSetOption = arrow::flight::sql::FlightSqlDatabaseSetOption;

  driver->ConnectionCommit = arrow::flight::sql::FlightSqlConnectionCommit;
  driver->ConnectionGetInfo = arrow::flight::sql::FlightSqlConnectionGetInfo;
  driver->ConnectionGetObjects = arrow::flight::sql::FlightSqlConnectionGetObjects;
  driver->ConnectionGetTableSchema =
      arrow::flight::sql::FlightSqlConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = arrow::flight::sql::FlightSqlConnectionGetTableTypes;
  driver->ConnectionInit = arrow::flight::sql::FlightSqlConnectionInit;
  driver->ConnectionNew = arrow::flight::sql::FlightSqlConnectionNew;
  driver->ConnectionReadPartition = arrow::flight::sql::FlightSqlConnectionReadPartition;
  driver->ConnectionRelease = arrow::flight::sql::FlightSqlConnectionRelease;
  driver->ConnectionRollback = arrow::flight::sql::FlightSqlConnectionRollback;
  driver->ConnectionSetOption = arrow::flight::sql::FlightSqlConnectionSetOption;

  driver->StatementBind = arrow::flight::sql::FlightSqlStatementBind;
  driver->StatementBindStream = arrow::flight::sql::FlightSqlStatementBindStream;
  driver->StatementExecutePartitions =
      arrow::flight::sql::FlightSqlStatementExecutePartitions;
  driver->StatementExecuteQuery = arrow::flight::sql::FlightSqlStatementExecuteQuery;
  driver->StatementGetParameterSchema =
      arrow::flight::sql::FlightSqlStatementGetParameterSchema;
  driver->StatementNew = arrow::flight::sql::FlightSqlStatementNew;
  driver->StatementPrepare = arrow::flight::sql::FlightSqlStatementPrepare;
  driver->StatementRelease = arrow::flight::sql::FlightSqlStatementRelease;
  driver->StatementSetOption = arrow::flight::sql::FlightSqlStatementSetOption;
  driver->StatementSetSqlQuery = arrow::flight::sql::FlightSqlStatementSetSqlQuery;
  driver->StatementSetSubstraitPlan =
      arrow::flight::sql::FlightSqlStatementSetSubstraitPlan;

  return ADBC_STATUS_OK;
}
}

namespace arrow::flight::sql {

AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  return ::AdbcDriverInit(version, raw_driver, error);
}

}  // namespace arrow::flight::sql

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

#include "arrow/flight/types.h"

#include <memory>
#include <sstream>
#include <string_view>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/reader.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/string_builder.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {

const char* kSchemeGrpc = "grpc";
const char* kSchemeGrpcTcp = "grpc+tcp";
const char* kSchemeGrpcUnix = "grpc+unix";
const char* kSchemeGrpcTls = "grpc+tls";

const char* kErrorDetailTypeId = "flight::FlightStatusDetail";

const char* FlightStatusDetail::type_id() const { return kErrorDetailTypeId; }

std::string FlightStatusDetail::ToString() const { return CodeAsString(); }

FlightStatusCode FlightStatusDetail::code() const { return code_; }

std::string FlightStatusDetail::extra_info() const { return extra_info_; }

void FlightStatusDetail::set_extra_info(std::string extra_info) {
  extra_info_ = std::move(extra_info);
}

std::string FlightStatusDetail::CodeAsString() const {
  switch (code()) {
    case FlightStatusCode::Internal:
      return "Internal";
    case FlightStatusCode::TimedOut:
      return "TimedOut";
    case FlightStatusCode::Cancelled:
      return "Cancelled";
    case FlightStatusCode::Unauthenticated:
      return "Unauthenticated";
    case FlightStatusCode::Unauthorized:
      return "Unauthorized";
    case FlightStatusCode::Unavailable:
      return "Unavailable";
    case FlightStatusCode::Failed:
      return "Failed";
    default:
      return "Unknown";
  }
}

std::shared_ptr<FlightStatusDetail> FlightStatusDetail::UnwrapStatus(
    const arrow::Status& status) {
  if (!status.detail() || status.detail()->type_id() != kErrorDetailTypeId) {
    return nullptr;
  }
  return std::dynamic_pointer_cast<FlightStatusDetail>(status.detail());
}

Status MakeFlightError(FlightStatusCode code, std::string message,
                       std::string extra_info) {
  StatusCode arrow_code = arrow::StatusCode::IOError;
  return arrow::Status(arrow_code, std::move(message),
                       std::make_shared<FlightStatusDetail>(code, std::move(extra_info)));
}

bool FlightDescriptor::Equals(const FlightDescriptor& other) const {
  if (type != other.type) {
    return false;
  }
  switch (type) {
    case PATH:
      return path == other.path;
    case CMD:
      return cmd == other.cmd;
    default:
      return false;
  }
}

std::string FlightDescriptor::ToString() const {
  std::stringstream ss;
  ss << "<FlightDescriptor ";
  switch (type) {
    case PATH: {
      ss << "path='";
      bool first = true;
      for (const auto& p : path) {
        if (!first) {
          ss << "/";
        }
        first = false;
        ss << p;
      }
      ss << "'";
      break;
    }
    case CMD:
      ss << "cmd='" << cmd << "'";
      break;
    default:
      break;
  }
  ss << ">";
  return ss.str();
}

Status FlightPayload::Validate() const {
  static constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();
  if (descriptor && descriptor->size() > kInt32Max) {
    return Status::CapacityError("Descriptor size overflow (>= 2**31)");
  }
  if (app_metadata && app_metadata->size() > kInt32Max) {
    return Status::CapacityError("app_metadata size overflow (>= 2**31)");
  }
  if (ipc_message.body_length > kInt32Max) {
    return Status::Invalid("Cannot send record batches exceeding 2GiB yet");
  }
  return Status::OK();
}

arrow::Result<std::shared_ptr<Schema>> SchemaResult::GetSchema(
    ipc::DictionaryMemo* dictionary_memo) const {
  io::BufferReader schema_reader(raw_schema_);
  return ipc::ReadSchema(&schema_reader, dictionary_memo);
}

arrow::Result<std::unique_ptr<SchemaResult>> SchemaResult::Make(const Schema& schema) {
  std::string schema_in;
  RETURN_NOT_OK(internal::SchemaToString(schema, &schema_in));
  return std::make_unique<SchemaResult>(std::move(schema_in));
}

Status SchemaResult::GetSchema(ipc::DictionaryMemo* dictionary_memo,
                               std::shared_ptr<Schema>* out) const {
  return GetSchema(dictionary_memo).Value(out);
}

std::string SchemaResult::ToString() const {
  return "<SchemaResult raw_schema=(serialized)>";
}

bool SchemaResult::Equals(const SchemaResult& other) const {
  return raw_schema_ == other.raw_schema_;
}

arrow::Result<std::string> SchemaResult::SerializeToString() const {
  pb::SchemaResult pb_schema_result;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_schema_result));

  std::string out;
  if (!pb_schema_result.SerializeToString(&out)) {
    return Status::IOError("Serialized SchemaResult exceeded 2 GiB limit");
  }
  return out;
}

arrow::Result<SchemaResult> SchemaResult::Deserialize(std::string_view serialized) {
  pb::SchemaResult pb_schema_result;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized SchemaResult size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_schema_result.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid SchemaResult");
  }
  return SchemaResult{pb_schema_result.schema()};
}

arrow::Result<std::string> FlightDescriptor::SerializeToString() const {
  pb::FlightDescriptor pb_descriptor;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_descriptor));

  std::string out;
  if (!pb_descriptor.SerializeToString(&out)) {
    return Status::IOError("Serialized FlightDescriptor exceeded 2 GiB limit");
  }
  return out;
}

Status FlightDescriptor::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<FlightDescriptor> FlightDescriptor::Deserialize(
    std::string_view serialized) {
  pb::FlightDescriptor pb_descriptor;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized FlightDescriptor size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_descriptor.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid FlightDescriptor");
  }
  FlightDescriptor out;
  RETURN_NOT_OK(internal::FromProto(pb_descriptor, &out));
  return out;
}

Status FlightDescriptor::Deserialize(const std::string& serialized,
                                     FlightDescriptor* out) {
  return Deserialize(serialized).Value(out);
}

std::string Ticket::ToString() const {
  std::stringstream ss;
  ss << "<Ticket ticket='" << ticket << "'>";
  return ss.str();
}

bool Ticket::Equals(const Ticket& other) const { return ticket == other.ticket; }

arrow::Result<std::string> Ticket::SerializeToString() const {
  pb::Ticket pb_ticket;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_ticket));

  std::string out;
  if (!pb_ticket.SerializeToString(&out)) {
    return Status::IOError("Serialized Ticket exceeded 2 GiB limit");
  }
  return out;
}

Status Ticket::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<Ticket> Ticket::Deserialize(std::string_view serialized) {
  pb::Ticket pb_ticket;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized Ticket size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_ticket.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid Ticket");
  }
  Ticket out;
  RETURN_NOT_OK(internal::FromProto(pb_ticket, &out));
  return out;
}

Status Ticket::Deserialize(const std::string& serialized, Ticket* out) {
  return Deserialize(serialized).Value(out);
}

arrow::Result<FlightInfo> FlightInfo::Make(const Schema& schema,
                                           const FlightDescriptor& descriptor,
                                           const std::vector<FlightEndpoint>& endpoints,
                                           int64_t total_records, int64_t total_bytes,
                                           bool ordered) {
  FlightInfo::Data data;
  data.descriptor = descriptor;
  data.endpoints = endpoints;
  data.total_records = total_records;
  data.total_bytes = total_bytes;
  data.ordered = ordered;
  RETURN_NOT_OK(internal::SchemaToString(schema, &data.schema));
  return FlightInfo(data);
}

arrow::Result<std::shared_ptr<Schema>> FlightInfo::GetSchema(
    ipc::DictionaryMemo* dictionary_memo) const {
  if (reconstructed_schema_) {
    return schema_;
  }
  io::BufferReader schema_reader(data_.schema);
  RETURN_NOT_OK(ipc::ReadSchema(&schema_reader, dictionary_memo).Value(&schema_));
  reconstructed_schema_ = true;
  return schema_;
}

Status FlightInfo::GetSchema(ipc::DictionaryMemo* dictionary_memo,
                             std::shared_ptr<Schema>* out) const {
  return GetSchema(dictionary_memo).Value(out);
}

arrow::Result<std::string> FlightInfo::SerializeToString() const {
  pb::FlightInfo pb_info;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_info));

  std::string out;
  if (!pb_info.SerializeToString(&out)) {
    return Status::IOError("Serialized FlightInfo exceeded 2 GiB limit");
  }
  return out;
}

Status FlightInfo::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightInfo::Deserialize(
    std::string_view serialized) {
  pb::FlightInfo pb_info;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized FlightInfo size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_info.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid FlightInfo");
  }
  FlightInfo::Data data;
  RETURN_NOT_OK(internal::FromProto(pb_info, &data));
  return std::make_unique<FlightInfo>(std::move(data));
}

Status FlightInfo::Deserialize(const std::string& serialized,
                               std::unique_ptr<FlightInfo>* out) {
  return Deserialize(serialized).Value(out);
}

std::string FlightInfo::ToString() const {
  std::stringstream ss;
  ss << "<FlightInfo schema=";
  if (schema_) {
    ss << schema_->ToString();
  } else {
    ss << "(serialized)";
  }
  ss << " descriptor=" << data_.descriptor.ToString();
  ss << " endpoints=[";
  bool first = true;
  for (const auto& endpoint : data_.endpoints) {
    if (!first) ss << ", ";
    ss << endpoint.ToString();
    first = false;
  }
  ss << "] total_records=" << data_.total_records;
  ss << " total_bytes=" << data_.total_bytes;
  ss << " ordered=" << (data_.ordered ? "true" : "false");
  ss << '>';
  return ss.str();
}

bool FlightInfo::Equals(const FlightInfo& other) const {
  return data_.schema == other.data_.schema &&
         data_.descriptor == other.data_.descriptor &&
         data_.endpoints == other.data_.endpoints &&
         data_.total_records == other.data_.total_records &&
         data_.total_bytes == other.data_.total_bytes &&
         data_.ordered == other.data_.ordered;
}

Location::Location() { uri_ = std::make_shared<arrow::internal::Uri>(); }

Status FlightListing::Next(std::unique_ptr<FlightInfo>* info) {
  return Next().Value(info);
}

arrow::Result<Location> Location::Parse(const std::string& uri_string) {
  Location location;
  RETURN_NOT_OK(location.uri_->Parse(uri_string));
  return location;
}

Status Location::Parse(const std::string& uri_string, Location* location) {
  return Parse(uri_string).Value(location);
}

arrow::Result<Location> Location::ForGrpcTcp(const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << "grpc+tcp://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

Status Location::ForGrpcTcp(const std::string& host, const int port, Location* location) {
  return ForGrpcTcp(host, port).Value(location);
}

arrow::Result<Location> Location::ForGrpcTls(const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << "grpc+tls://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

Status Location::ForGrpcTls(const std::string& host, const int port, Location* location) {
  return ForGrpcTls(host, port).Value(location);
}

arrow::Result<Location> Location::ForGrpcUnix(const std::string& path) {
  std::stringstream uri_string;
  uri_string << "grpc+unix://" << path;
  return Location::Parse(uri_string.str());
}

Status Location::ForGrpcUnix(const std::string& path, Location* location) {
  return ForGrpcUnix(path).Value(location);
}

arrow::Result<Location> Location::ForScheme(const std::string& scheme,
                                            const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << scheme << "://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

std::string Location::ToString() const { return uri_->ToString(); }
std::string Location::scheme() const {
  std::string scheme = uri_->scheme();
  if (scheme.empty()) {
    // Default to grpc+tcp
    return "grpc+tcp";
  }
  return scheme;
}

bool Location::Equals(const Location& other) const {
  return ToString() == other.ToString();
}

std::string FlightEndpoint::ToString() const {
  std::stringstream ss;
  ss << "<FlightEndpoint ticket=" << ticket.ToString();
  ss << " locations=[";
  bool first = true;
  for (const auto& location : locations) {
    if (!first) ss << ", ";
    ss << location.ToString();
    first = false;
  }
  ss << "]>";
  return ss.str();
}

bool FlightEndpoint::Equals(const FlightEndpoint& other) const {
  return ticket == other.ticket && locations == other.locations;
}

arrow::Result<std::string> FlightEndpoint::SerializeToString() const {
  pb::FlightEndpoint pb_flight_endpoint;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_flight_endpoint));

  std::string out;
  if (!pb_flight_endpoint.SerializeToString(&out)) {
    return Status::IOError("Serialized FlightEndpoint exceeded 2 GiB limit");
  }
  return out;
}

arrow::Result<FlightEndpoint> FlightEndpoint::Deserialize(std::string_view serialized) {
  pb::FlightEndpoint pb_flight_endpoint;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized FlightEndpoint size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_flight_endpoint.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid FlightEndpoint");
  }
  FlightEndpoint out;
  RETURN_NOT_OK(internal::FromProto(pb_flight_endpoint, &out));
  return out;
}

std::string ActionType::ToString() const {
  return arrow::util::StringBuilder("<ActionType type='", type, "' description='",
                                    description, "'>");
}

bool ActionType::Equals(const ActionType& other) const {
  return type == other.type && description == other.description;
}

arrow::Result<std::string> ActionType::SerializeToString() const {
  pb::ActionType pb_action_type;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_action_type));

  std::string out;
  if (!pb_action_type.SerializeToString(&out)) {
    return Status::IOError("Serialized ActionType exceeded 2 GiB limit");
  }
  return out;
}

arrow::Result<ActionType> ActionType::Deserialize(std::string_view serialized) {
  pb::ActionType pb_action_type;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized ActionType size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_action_type.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid ActionType");
  }
  ActionType out;
  RETURN_NOT_OK(internal::FromProto(pb_action_type, &out));
  return out;
}

std::string Criteria::ToString() const {
  return arrow::util::StringBuilder("<Criteria expression='", expression, "'>");
}

bool Criteria::Equals(const Criteria& other) const {
  return expression == other.expression;
}

arrow::Result<std::string> Criteria::SerializeToString() const {
  pb::Criteria pb_criteria;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_criteria));

  std::string out;
  if (!pb_criteria.SerializeToString(&out)) {
    return Status::IOError("Serialized Criteria exceeded 2 GiB limit");
  }
  return out;
}

arrow::Result<Criteria> Criteria::Deserialize(std::string_view serialized) {
  pb::Criteria pb_criteria;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized Criteria size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_criteria.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid Criteria");
  }
  Criteria out;
  RETURN_NOT_OK(internal::FromProto(pb_criteria, &out));
  return out;
}

std::string Action::ToString() const {
  std::stringstream ss;
  ss << "<Action type='" << type;
  ss << "' body=";
  if (body) {
    ss << "(" << body->size() << " bytes)";
  } else {
    ss << "(nullptr)";
  }
  ss << '>';
  return ss.str();
}

bool Action::Equals(const Action& other) const {
  return (type == other.type) &&
         ((body == other.body) || (body && other.body && body->Equals(*other.body)));
}

arrow::Result<std::string> Action::SerializeToString() const {
  pb::Action pb_action;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_action));

  std::string out;
  if (!pb_action.SerializeToString(&out)) {
    return Status::IOError("Serialized Action exceeded 2 GiB limit");
  }
  return out;
}

arrow::Result<Action> Action::Deserialize(std::string_view serialized) {
  pb::Action pb_action;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized Action size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_action.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid Action");
  }
  Action out;
  RETURN_NOT_OK(internal::FromProto(pb_action, &out));
  return out;
}

std::string Result::ToString() const {
  std::stringstream ss;
  ss << "<Result body=";
  if (body) {
    ss << "(" << body->size() << " bytes)>";
  } else {
    ss << "(nullptr)>";
  }
  return ss.str();
}

bool Result::Equals(const Result& other) const {
  return (body == other.body) || (body && other.body && body->Equals(*other.body));
}

arrow::Result<std::string> Result::SerializeToString() const {
  pb::Result pb_result;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_result));

  std::string out;
  if (!pb_result.SerializeToString(&out)) {
    return Status::IOError("Serialized Result exceeded 2 GiB limit");
  }
  return out;
}

arrow::Result<Result> Result::Deserialize(std::string_view serialized) {
  pb::Result pb_result;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized Result size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_result.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid Result");
  }
  Result out;
  RETURN_NOT_OK(internal::FromProto(pb_result, &out));
  return out;
}

Status ResultStream::Next(std::unique_ptr<Result>* info) { return Next().Value(info); }

Status MetadataRecordBatchReader::Next(FlightStreamChunk* next) {
  return Next().Value(next);
}

arrow::Result<std::vector<std::shared_ptr<RecordBatch>>>
MetadataRecordBatchReader::ToRecordBatches() {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, Next());
    if (!chunk.data) break;
    batches.emplace_back(std::move(chunk.data));
  }
  return batches;
}

Status MetadataRecordBatchReader::ReadAll(
    std::vector<std::shared_ptr<RecordBatch>>* batches) {
  return ToRecordBatches().Value(batches);
}

arrow::Result<std::shared_ptr<Table>> MetadataRecordBatchReader::ToTable() {
  ARROW_ASSIGN_OR_RAISE(auto batches, ToRecordBatches());
  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  return Table::FromRecordBatches(schema, std::move(batches));
}

Status MetadataRecordBatchReader::ReadAll(std::shared_ptr<Table>* table) {
  return ToTable().Value(table);
}

Status MetadataRecordBatchWriter::Begin(const std::shared_ptr<Schema>& schema) {
  return Begin(schema, ipc::IpcWriteOptions::Defaults());
}

namespace {
class MetadataRecordBatchReaderAdapter : public RecordBatchReader {
 public:
  explicit MetadataRecordBatchReaderAdapter(
      std::shared_ptr<Schema> schema, std::shared_ptr<MetadataRecordBatchReader> delegate)
      : schema_(std::move(schema)), delegate_(std::move(delegate)) {}
  std::shared_ptr<Schema> schema() const override { return schema_; }
  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    while (true) {
      ARROW_ASSIGN_OR_RAISE(FlightStreamChunk next, delegate_->Next());
      if (!next.data && !next.app_metadata) {
        // EOS
        *batch = nullptr;
        return Status::OK();
      } else if (next.data) {
        *batch = std::move(next.data);
        return Status::OK();
      }
      // Got metadata, but no data (which is valid) - read the next message
    }
  }

 private:
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<MetadataRecordBatchReader> delegate_;
};
};  // namespace

arrow::Result<std::shared_ptr<RecordBatchReader>> MakeRecordBatchReader(
    std::shared_ptr<MetadataRecordBatchReader> reader) {
  ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
  return std::make_shared<MetadataRecordBatchReaderAdapter>(std::move(schema),
                                                            std::move(reader));
}

SimpleFlightListing::SimpleFlightListing(const std::vector<FlightInfo>& flights)
    : position_(0), flights_(flights) {}

SimpleFlightListing::SimpleFlightListing(std::vector<FlightInfo>&& flights)
    : position_(0), flights_(std::move(flights)) {}

arrow::Result<std::unique_ptr<FlightInfo>> SimpleFlightListing::Next() {
  if (position_ >= static_cast<int>(flights_.size())) {
    return nullptr;
  }
  return std::make_unique<FlightInfo>(std::move(flights_[position_++]));
}

SimpleResultStream::SimpleResultStream(std::vector<Result>&& results)
    : results_(std::move(results)), position_(0) {}

arrow::Result<std::unique_ptr<Result>> SimpleResultStream::Next() {
  if (position_ >= results_.size()) {
    return nullptr;
  }
  return std::make_unique<Result>(std::move(results_[position_++]));
}

std::string BasicAuth::ToString() const {
  return arrow::util::StringBuilder("<BasicAuth username='", username,
                                    "' password=(redacted)>");
}

bool BasicAuth::Equals(const BasicAuth& other) const {
  return (username == other.username) && (password == other.password);
}

arrow::Result<BasicAuth> BasicAuth::Deserialize(std::string_view serialized) {
  pb::BasicAuth pb_result;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized BasicAuth size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_result.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid BasicAuth");
  }
  BasicAuth out;
  RETURN_NOT_OK(internal::FromProto(pb_result, &out));
  return out;
}

Status BasicAuth::Deserialize(const std::string& serialized, BasicAuth* out) {
  return Deserialize(serialized).Value(out);
}

arrow::Result<std::string> BasicAuth::SerializeToString() const {
  pb::BasicAuth pb_result;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_result));
  std::string out;
  if (!pb_result.SerializeToString(&out)) {
    return Status::IOError("Serialized BasicAuth exceeded 2 GiB limit");
  }
  return out;
}

Status BasicAuth::Serialize(const BasicAuth& basic_auth, std::string* out) {
  return basic_auth.SerializeToString().Value(out);
}
}  // namespace flight
}  // namespace arrow

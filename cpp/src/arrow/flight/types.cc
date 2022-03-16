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
#include <utility>

#include "arrow/buffer.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/reader.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/string_view.h"
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
  ss << "FlightDescriptor<";
  switch (type) {
    case PATH: {
      bool first = true;
      ss << "path = '";
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
      ss << "cmd = '" << cmd << "'";
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

Status SchemaResult::GetSchema(ipc::DictionaryMemo* dictionary_memo,
                               std::shared_ptr<Schema>* out) const {
  io::BufferReader schema_reader(raw_schema_);
  return ipc::ReadSchema(&schema_reader, dictionary_memo).Value(out);
}

arrow::Result<std::string> FlightDescriptor::SerializeToString() const {
  pb::FlightDescriptor pb_descriptor;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_descriptor));

  std::string out;
  if (!pb_descriptor.SerializeToString(&out)) {
    return Status::IOError("Serialized descriptor exceeded 2 GiB limit");
  }
  return out;
}

Status FlightDescriptor::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<FlightDescriptor> FlightDescriptor::Deserialize(
    arrow::util::string_view serialized) {
  pb::FlightDescriptor pb_descriptor;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized FlightDescriptor size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_descriptor.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid descriptor");
  }
  FlightDescriptor out;
  RETURN_NOT_OK(internal::FromProto(pb_descriptor, &out));
  return out;
}

Status FlightDescriptor::Deserialize(const std::string& serialized,
                                     FlightDescriptor* out) {
  return Deserialize(serialized).Value(out);
}

bool Ticket::Equals(const Ticket& other) const { return ticket == other.ticket; }

arrow::Result<std::string> Ticket::SerializeToString() const {
  pb::Ticket pb_ticket;
  RETURN_NOT_OK(internal::ToProto(*this, &pb_ticket));

  std::string out;
  if (!pb_ticket.SerializeToString(&out)) {
    return Status::IOError("Serialized ticket exceeded 2 GiB limit");
  }
  return out;
}

Status Ticket::SerializeToString(std::string* out) const {
  return SerializeToString().Value(out);
}

arrow::Result<Ticket> Ticket::Deserialize(arrow::util::string_view serialized) {
  pb::Ticket pb_ticket;
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Invalid("Serialized Ticket size should not exceed 2 GiB");
  }
  google::protobuf::io::ArrayInputStream input(serialized.data(),
                                               static_cast<int>(serialized.size()));
  if (!pb_ticket.ParseFromZeroCopyStream(&input)) {
    return Status::Invalid("Not a valid ticket");
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
                                           int64_t total_records, int64_t total_bytes) {
  FlightInfo::Data data;
  data.descriptor = descriptor;
  data.endpoints = endpoints;
  data.total_records = total_records;
  data.total_bytes = total_bytes;
  RETURN_NOT_OK(internal::SchemaToString(schema, &data.schema));
  return FlightInfo(data);
}

Status FlightInfo::GetSchema(ipc::DictionaryMemo* dictionary_memo,
                             std::shared_ptr<Schema>* out) const {
  if (reconstructed_schema_) {
    *out = schema_;
    return Status::OK();
  }
  io::BufferReader schema_reader(data_.schema);
  RETURN_NOT_OK(ipc::ReadSchema(&schema_reader, dictionary_memo).Value(&schema_));
  reconstructed_schema_ = true;
  *out = schema_;
  return Status::OK();
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
    arrow::util::string_view serialized) {
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
  return std::unique_ptr<FlightInfo>(new FlightInfo(std::move(data)));
}

Status FlightInfo::Deserialize(const std::string& serialized,
                               std::unique_ptr<FlightInfo>* out) {
  return Deserialize(serialized).Value(out);
}

Location::Location() { uri_ = std::make_shared<arrow::internal::Uri>(); }

Status Location::Parse(const std::string& uri_string, Location* location) {
  return location->uri_->Parse(uri_string);
}

Status Location::ForGrpcTcp(const std::string& host, const int port, Location* location) {
  std::stringstream uri_string;
  uri_string << "grpc+tcp://" << host << ':' << port;
  return Location::Parse(uri_string.str(), location);
}

Status Location::ForGrpcTls(const std::string& host, const int port, Location* location) {
  std::stringstream uri_string;
  uri_string << "grpc+tls://" << host << ':' << port;
  return Location::Parse(uri_string.str(), location);
}

Status Location::ForGrpcUnix(const std::string& path, Location* location) {
  std::stringstream uri_string;
  uri_string << "grpc+unix://" << path;
  return Location::Parse(uri_string.str(), location);
}

arrow::Result<Location> Location::ForScheme(const std::string& scheme,
                                            const std::string& host, const int port) {
  Location location;
  std::stringstream uri_string;
  uri_string << scheme << "://" << host << ':' << port;
  RETURN_NOT_OK(Location::Parse(uri_string.str(), &location));
  return location;
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

bool FlightEndpoint::Equals(const FlightEndpoint& other) const {
  return ticket == other.ticket && locations == other.locations;
}

bool ActionType::Equals(const ActionType& other) const {
  return type == other.type && description == other.description;
}

Status MetadataRecordBatchReader::ReadAll(
    std::vector<std::shared_ptr<RecordBatch>>* batches) {
  FlightStreamChunk chunk;

  while (true) {
    RETURN_NOT_OK(Next(&chunk));
    if (!chunk.data) break;
    batches->emplace_back(std::move(chunk.data));
  }
  return Status::OK();
}

Status MetadataRecordBatchReader::ReadAll(std::shared_ptr<Table>* table) {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  RETURN_NOT_OK(ReadAll(&batches));
  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  return Table::FromRecordBatches(schema, std::move(batches)).Value(table);
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
    FlightStreamChunk next;
    while (true) {
      RETURN_NOT_OK(delegate_->Next(&next));
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

Status SimpleFlightListing::Next(std::unique_ptr<FlightInfo>* info) {
  if (position_ >= static_cast<int>(flights_.size())) {
    *info = nullptr;
    return Status::OK();
  }
  *info = std::unique_ptr<FlightInfo>(new FlightInfo(std::move(flights_[position_++])));
  return Status::OK();
}

SimpleResultStream::SimpleResultStream(std::vector<Result>&& results)
    : results_(std::move(results)), position_(0) {}

Status SimpleResultStream::Next(std::unique_ptr<Result>* result) {
  if (position_ >= results_.size()) {
    *result = nullptr;
    return Status::OK();
  }
  *result = std::unique_ptr<Result>(new Result(std::move(results_[position_++])));
  return Status::OK();
}

arrow::Result<BasicAuth> BasicAuth::Deserialize(arrow::util::string_view serialized) {
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

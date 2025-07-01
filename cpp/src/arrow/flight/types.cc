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

#include <iomanip>
#include <memory>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/flight/protocol_internal.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/types_async.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/reader.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/base64.h"
#include "arrow/util/formatting.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/string_util.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace {

ARROW_NOINLINE
Status ProtoStringInputTooBig(const char* name) {
  return Status::Invalid("Serialized ", name, " size should not exceed 2 GiB");
}

ARROW_NOINLINE
Status ProtoStringOutputTooBig(const char* name) {
  return Status::Invalid("Serialized ", name, " exceeded 2 GiB limit");
}

ARROW_NOINLINE
Status InvalidProtoString(const char* name) {
  return Status::Invalid("Not a valid ", name);
}

// Status-returning ser/de functions that allow reuse of the same output objects

template <class PBType>
Status ParseFromString(const char* name, std::string_view serialized, PBType* out) {
  if (serialized.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return ProtoStringInputTooBig(name);
  }
  if (!out->ParseFromArray(serialized.data(), static_cast<int>(serialized.size()))) {
    return InvalidProtoString(name);
  }
  return Status::OK();
}

template <class PBType, class T>
Status SerializeToString(const char* name, const T& in, PBType* out_pb,
                         std::string* out) {
  RETURN_NOT_OK(internal::ToProto(in, out_pb));
  return out_pb->SerializeToString(out) ? Status::OK() : ProtoStringOutputTooBig(name);
}

// Result-returning ser/de functions (more convenient)

template <class PBType, class T>
arrow::Status DeserializeProtoString(const char* name, std::string_view serialized,
                                     T* out) {
  PBType pb;
  RETURN_NOT_OK(ParseFromString(name, serialized, &pb));
  return internal::FromProto(pb, out);
}

template <class PBType, class T>
Status SerializeToProtoString(const char* name, const T& in, std::string* out) {
  PBType pb;
  return SerializeToString<PBType>(name, in, &pb, out);
}

}  // namespace

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

static std::ostream& operator<<(std::ostream& os, std::vector<std::string> values) {
  os << '[';
  std::string sep = "";
  for (const auto& v : values) {
    os << sep << std::quoted(v);
    sep = ", ";
  }
  os << ']';

  return os;
}

template <typename T>
static std::ostream& operator<<(std::ostream& os, std::map<std::string, T> m) {
  os << '{';
  std::string sep = "";
  if constexpr (std::is_convertible_v<T, std::string_view>) {
    // std::string, char*, std::string_view
    for (const auto& [k, v] : m) {
      os << sep << '[' << k << "]: " << std::quoted(v) << '"';
      sep = ", ";
    }
  } else {
    for (const auto& [k, v] : m) {
      os << sep << '[' << k << "]: " << v;
      sep = ", ";
    }
  }
  os << '}';

  return os;
}

//------------------------------------------------------------
// Wrapper types for Flight RPC protobuf messages

std::string BasicAuth::ToString() const {
  return arrow::internal::JoinToString("<BasicAuth username='", username,
                                       "' password=(redacted)>");
}

bool BasicAuth::Equals(const BasicAuth& other) const {
  return (username == other.username) && (password == other.password);
}

arrow::Status BasicAuth::Deserialize(std::string_view serialized, BasicAuth* out) {
  return DeserializeProtoString<pb::BasicAuth, BasicAuth>("BasicAuth", serialized, out);
}

arrow::Status BasicAuth::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::BasicAuth>("BasicAuth", *this, out);
}

FlightDescriptor::FlightDescriptor() = default;

FlightDescriptor::FlightDescriptor(DescriptorType type, std::string cmd,
                                   std::vector<std::string> path) noexcept
    : type(type), cmd(std::move(cmd)), path(std::move(path)) {}

FlightDescriptor::~FlightDescriptor() = default;

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

arrow::Status FlightDescriptor::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::FlightDescriptor>("FlightDescriptor", *this, out);
}

arrow::Status FlightDescriptor::Deserialize(std::string_view serialized,
                                            FlightDescriptor* out) {
  return DeserializeProtoString<pb::FlightDescriptor, FlightDescriptor>(
      "FlightDescriptor", serialized, out);
}

arrow::Result<FlightInfo> FlightInfo::Make(const Schema& schema,
                                           const FlightDescriptor& descriptor,
                                           const std::vector<FlightEndpoint>& endpoints,
                                           int64_t total_records, int64_t total_bytes,
                                           bool ordered, std::string app_metadata) {
  FlightInfo::Data data;
  data.descriptor = descriptor;
  data.endpoints = endpoints;
  data.total_records = total_records;
  data.total_bytes = total_bytes;
  data.ordered = ordered;
  data.app_metadata = std::move(app_metadata);
  RETURN_NOT_OK(internal::SchemaToString(schema, &data.schema));
  return FlightInfo(std::move(data));
}

arrow::Result<FlightInfo> FlightInfo::Make(const std::shared_ptr<Schema>& schema,
                                           const FlightDescriptor& descriptor,
                                           const std::vector<FlightEndpoint>& endpoints,
                                           int64_t total_records, int64_t total_bytes,
                                           bool ordered, std::string app_metadata) {
  FlightInfo::Data data;
  data.descriptor = descriptor;
  data.endpoints = endpoints;
  data.total_records = total_records;
  data.total_bytes = total_bytes;
  data.ordered = ordered;
  data.app_metadata = std::move(app_metadata);
  if (schema) {
    RETURN_NOT_OK(internal::SchemaToString(*schema, &data.schema));
  }
  return FlightInfo(std::move(data));
}

arrow::Result<std::shared_ptr<Schema>> FlightInfo::GetSchema(
    ipc::DictionaryMemo* dictionary_memo) const {
  if (reconstructed_schema_) {
    return schema_;
  } else if (data_.schema.empty()) {
    reconstructed_schema_ = true;
    return schema_;
  }
  // Create a non-owned Buffer to avoid copying
  io::BufferReader schema_reader(std::make_shared<Buffer>(data_.schema));
  RETURN_NOT_OK(ipc::ReadSchema(&schema_reader, dictionary_memo).Value(&schema_));
  reconstructed_schema_ = true;
  return schema_;
}

arrow::Status FlightInfo::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::FlightInfo>("FlightInfo", *this, out);
}

arrow::Status FlightInfo::Deserialize(std::string_view serialized,
                                      std::unique_ptr<FlightInfo>* out) {
  return DeserializeProtoString<pb::FlightInfo, std::unique_ptr<FlightInfo>>(
      "FlightInfo", serialized, out);
}

std::string FlightInfo::ToString() const {
  std::stringstream ss;
  ss << "<FlightInfo schema=";
  if (data_.schema.empty()) {
    ss << "(empty)";
  } else if (schema_) {
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
  ss << " app_metadata='" << HexEncode(data_.app_metadata) << "'";
  ss << '>';
  return ss.str();
}

bool FlightInfo::Equals(const FlightInfo& other) const {
  return data_.schema == other.data_.schema &&
         data_.descriptor == other.data_.descriptor &&
         data_.endpoints == other.data_.endpoints &&
         data_.total_records == other.data_.total_records &&
         data_.total_bytes == other.data_.total_bytes &&
         data_.ordered == other.data_.ordered &&
         data_.app_metadata == other.data_.app_metadata;
}

arrow::Status PollInfo::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::PollInfo>("PollInfo", *this, out);
}

arrow::Status PollInfo::Deserialize(std::string_view serialized,
                                    std::unique_ptr<PollInfo>* out) {
  return DeserializeProtoString<pb::PollInfo, std::unique_ptr<PollInfo>>("PollInfo",
                                                                         serialized, out);
}

std::string PollInfo::ToString() const {
  std::stringstream ss;
  ss << "<PollInfo info=";
  if (info) {
    ss << info->ToString();
  } else {
    ss << "null";
  }
  ss << " descriptor=";
  if (descriptor) {
    ss << descriptor->ToString();
  } else {
    ss << "null";
  }
  ss << " progress=";
  if (progress) {
    ss << progress.value();
  } else {
    ss << "null";
  }
  ss << " expiration_time=";
  if (expiration_time) {
    auto type = timestamp(TimeUnit::NANO);
    arrow::internal::StringFormatter<TimestampType> formatter(type.get());
    auto expiration_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    expiration_time->time_since_epoch())
                                    .count();
    formatter(expiration_timestamp,
              [&ss](std::string_view formatted) { ss << formatted; });
  } else {
    ss << "null";
  }
  ss << '>';
  return ss.str();
}

bool PollInfo::Equals(const PollInfo& other) const {
  if ((info.get() != nullptr) != (other.info.get() != nullptr)) {
    return false;
  }
  if (info && *info != *other.info) {
    return false;
  }
  if (descriptor.has_value() != other.descriptor.has_value()) {
    return false;
  }
  if (descriptor && *descriptor != *other.descriptor) {
    return false;
  }
  if (progress.has_value() != other.progress.has_value()) {
    return false;
  }
  if (progress && fabs(*progress - *other.progress) > arrow::kDefaultAbsoluteTolerance) {
    return false;
  }
  if (expiration_time.has_value() != other.expiration_time.has_value()) {
    return false;
  }
  if (expiration_time && *expiration_time != *other.expiration_time) {
    return false;
  }
  return true;
}

std::string CancelFlightInfoRequest::ToString() const {
  std::stringstream ss;
  ss << "<CancelFlightInfoRequest info=" << info->ToString() << ">";
  return ss.str();
}

bool CancelFlightInfoRequest::Equals(const CancelFlightInfoRequest& other) const {
  return info == other.info;
}

arrow::Status CancelFlightInfoRequest::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::CancelFlightInfoRequest>("CancelFlightInfoRequest",
                                                             *this, out);
}

arrow::Status CancelFlightInfoRequest::Deserialize(std::string_view serialized,
                                                   CancelFlightInfoRequest* out) {
  return DeserializeProtoString<pb::CancelFlightInfoRequest, CancelFlightInfoRequest>(
      "CancelFlightInfoRequest", serialized, out);
}

std::string CancelFlightInfoResult::ToString() const {
  std::stringstream ss;
  ss << "<CancelFlightInfoResult status=" << status << ">";
  return ss.str();
}

bool CancelFlightInfoResult::Equals(const CancelFlightInfoResult& other) const {
  return status == other.status;
}

arrow::Status CancelFlightInfoResult::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::CancelFlightInfoResult>("CancelFlightInfoResult",
                                                            *this, out);
}

arrow::Status CancelFlightInfoResult::Deserialize(std::string_view serialized,
                                                  CancelFlightInfoResult* out) {
  return DeserializeProtoString<pb::CancelFlightInfoResult, CancelFlightInfoResult>(
      "CancelFlightInfoResult", serialized, out);
}

std::ostream& operator<<(std::ostream& os, CancelStatus status) {
  switch (status) {
    case CancelStatus::kUnspecified:
      os << "Unspecified";
      break;
    case CancelStatus::kCancelled:
      os << "Cancelled";
      break;
    case CancelStatus::kCancelling:
      os << "Cancelling";
      break;
    case CancelStatus::kNotCancellable:
      os << "NotCancellable";
      break;
  }
  return os;
}

// Session management messages

// SessionOptionValue

std::ostream& operator<<(std::ostream& os, const SessionOptionValue& v) {
  if (std::holds_alternative<std::monostate>(v)) {
    os << "<EMPTY>";
  } else {
    std::visit(
        [&](const auto& x) {
          if constexpr (std::is_convertible_v<std::decay_t<decltype(x)>,
                                              std::string_view>) {
            os << std::quoted(x);
          } else {
            os << x;
          }
        },
        v);
  }
  return os;
}

static bool CompareSessionOptionMaps(const std::map<std::string, SessionOptionValue>& a,
                                     const std::map<std::string, SessionOptionValue>& b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (const auto& [k, v] : a) {
    if (const auto it = b.find(k); it == b.end()) {
      return false;
    } else {
      const auto& b_v = it->second;
      if (v.index() != b_v.index()) {
        return false;
      }
      if (v != b_v) {
        return false;
      }
    }
  }
  return true;
}

// SetSessionOptionErrorValue

std::string ToString(const SetSessionOptionErrorValue& error_value) {
  static constexpr const char* SetSessionOptionStatusNames[] = {
      "Unspecified",
      "InvalidName",
      "InvalidValue",
      "Error",
  };
  return SetSessionOptionStatusNames[static_cast<int>(error_value)];
}

std::ostream& operator<<(std::ostream& os,
                         const SetSessionOptionErrorValue& error_value) {
  os << ToString(error_value);
  return os;
}

// SetSessionOptionsRequest

std::string SetSessionOptionsRequest::ToString() const {
  std::stringstream ss;
  ss << "<SetSessionOptionsRequest session_options=" << session_options << '>';
  return ss.str();
}

bool SetSessionOptionsRequest::Equals(const SetSessionOptionsRequest& other) const {
  return CompareSessionOptionMaps(session_options, other.session_options);
}

arrow::Status SetSessionOptionsRequest::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::SetSessionOptionsRequest>("SetSessionOptionsRequest",
                                                              *this, out);
}

arrow::Status SetSessionOptionsRequest::Deserialize(std::string_view serialized,
                                                    SetSessionOptionsRequest* out) {
  return DeserializeProtoString<pb::SetSessionOptionsRequest, SetSessionOptionsRequest>(
      "SetSessionOptionsRequest", serialized, out);
}

// SetSessionOptionsResult

std::ostream& operator<<(std::ostream& os, const SetSessionOptionsResult::Error& e) {
  os << '{' << e.value << '}';
  return os;
}

std::string SetSessionOptionsResult::ToString() const {
  std::stringstream ss;
  ss << "<SetSessionOptionsResult errors=" << errors << '>';
  return ss.str();
}

bool SetSessionOptionsResult::Equals(const SetSessionOptionsResult& other) const {
  if (errors != other.errors) {
    return false;
  }
  return true;
}

arrow::Status SetSessionOptionsResult::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::SetSessionOptionsResult>("SetSessionOptionsResult",
                                                             *this, out);
}

arrow::Status SetSessionOptionsResult::Deserialize(std::string_view serialized,
                                                   SetSessionOptionsResult* out) {
  return DeserializeProtoString<pb::SetSessionOptionsResult, SetSessionOptionsResult>(
      "SetSessionOptionsResult", serialized, out);
}

// GetSessionOptionsRequest

std::string GetSessionOptionsRequest::ToString() const {
  return "<GetSessionOptionsRequest>";
}

bool GetSessionOptionsRequest::Equals(const GetSessionOptionsRequest& other) const {
  return true;
}

arrow::Status GetSessionOptionsRequest::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::GetSessionOptionsRequest>("GetSessionOptionsRequest",
                                                              *this, out);
}

arrow::Status GetSessionOptionsRequest::Deserialize(std::string_view serialized,
                                                    GetSessionOptionsRequest* out) {
  return DeserializeProtoString<pb::GetSessionOptionsRequest, GetSessionOptionsRequest>(
      "GetSessionOptionsRequest", serialized, out);
}

// GetSessionOptionsResult

std::string GetSessionOptionsResult::ToString() const {
  std::stringstream ss;
  ss << "<GetSessionOptionsResult session_options=" << session_options << '>';
  return ss.str();
}

bool GetSessionOptionsResult::Equals(const GetSessionOptionsResult& other) const {
  return CompareSessionOptionMaps(session_options, other.session_options);
}

arrow::Status GetSessionOptionsResult::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::GetSessionOptionsResult>("GetSessionOptionsResult",
                                                             *this, out);
}

arrow::Status GetSessionOptionsResult::Deserialize(std::string_view serialized,
                                                   GetSessionOptionsResult* out) {
  return DeserializeProtoString<pb::GetSessionOptionsResult, GetSessionOptionsResult>(
      "GetSessionOptionsResult", serialized, out);
}

// CloseSessionRequest

std::string CloseSessionRequest::ToString() const { return "<CloseSessionRequest>"; }

bool CloseSessionRequest::Equals(const CloseSessionRequest& other) const { return true; }

arrow::Status CloseSessionRequest::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::CloseSessionRequest>("CloseSessionRequest", *this,
                                                         out);
}

arrow::Status CloseSessionRequest::Deserialize(std::string_view serialized,
                                               CloseSessionRequest* out) {
  return DeserializeProtoString<pb::CloseSessionRequest, CloseSessionRequest>(
      "CloseSessionRequest", serialized, out);
}

// CloseSessionStatus

std::string ToString(const CloseSessionStatus& status) {
  static constexpr const char* CloseSessionStatusNames[] = {
      "Unspecified",
      "Closed",
      "Closing",
      "NotClosable",
  };
  return CloseSessionStatusNames[static_cast<int>(status)];
}

std::ostream& operator<<(std::ostream& os, const CloseSessionStatus& status) {
  os << ToString(status);
  return os;
}

// CloseSessionResult

std::string CloseSessionResult::ToString() const {
  std::stringstream ss;
  ss << "<CloseSessionResult status=" << status << '>';
  return ss.str();
}

bool CloseSessionResult::Equals(const CloseSessionResult& other) const {
  return status == other.status;
}

arrow::Status CloseSessionResult::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::CloseSessionResult>("CloseSessionResult", *this, out);
}

arrow::Status CloseSessionResult::Deserialize(std::string_view serialized,
                                              CloseSessionResult* out) {
  return DeserializeProtoString<pb::CloseSessionResult, CloseSessionResult>(
      "CloseSessionResult", serialized, out);
}

// Ticket

std::string Ticket::ToString() const {
  std::stringstream ss;
  ss << "<Ticket ticket='" << ticket << "'>";
  return ss.str();
}

bool Ticket::Equals(const Ticket& other) const { return ticket == other.ticket; }

arrow::Status Ticket::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::Ticket>("Ticket", *this, out);
}

arrow::Status Ticket::Deserialize(std::string_view serialized, Ticket* out) {
  return DeserializeProtoString<pb::Ticket, Ticket>("Ticket", serialized, out);
}

Location::Location() { uri_ = std::make_shared<arrow::util::Uri>(); }

Location::~Location() = default;

arrow::Result<Location> Location::Parse(const std::string& uri_string) {
  Location location;
  RETURN_NOT_OK(location.uri_->Parse(uri_string));
  return location;
}

const Location& Location::ReuseConnection() {
  static Location kFallback =
      Location::Parse("arrow-flight-reuse-connection://?").ValueOrDie();
  return kFallback;
}

arrow::Result<Location> Location::ForGrpcTcp(const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << "grpc+tcp://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

arrow::Result<Location> Location::ForGrpcTls(const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << "grpc+tls://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

arrow::Result<Location> Location::ForGrpcUnix(const std::string& path) {
  std::stringstream uri_string;
  uri_string << "grpc+unix://" << path;
  return Location::Parse(uri_string.str());
}

arrow::Result<Location> Location::ForScheme(const std::string& scheme,
                                            const std::string& host, const int port) {
  std::stringstream uri_string;
  uri_string << scheme << "://" << host << ':' << port;
  return Location::Parse(uri_string.str());
}

std::string Location::scheme() const {
  std::string scheme = uri_->scheme();
  if (scheme.empty()) {
    // Default to grpc+tcp
    return "grpc+tcp";
  }
  return scheme;
}

std::string Location::ToString() const { return uri_->ToString(); }

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
  ss << "]";
  auto type = timestamp(TimeUnit::NANO);
  arrow::internal::StringFormatter<TimestampType> formatter(type.get());
  ss << " expiration_time=";
  if (expiration_time) {
    auto expiration_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    expiration_time.value().time_since_epoch())
                                    .count();
    formatter(expiration_timestamp,
              [&ss](std::string_view formatted) { ss << formatted; });
  } else {
    ss << "null";
  }
  ss << " app_metadata='" << HexEncode(app_metadata) << "'";
  ss << ">";
  return ss.str();
}

bool FlightEndpoint::Equals(const FlightEndpoint& other) const {
  if (ticket != other.ticket) {
    return false;
  }
  if (locations != other.locations) {
    return false;
  }
  if (expiration_time.has_value() != other.expiration_time.has_value()) {
    return false;
  }
  if (expiration_time) {
    if (expiration_time.value() != other.expiration_time.value()) {
      return false;
    }
  }
  if (app_metadata != other.app_metadata) {
    return false;
  }
  return true;
}

arrow::Status Location::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::Location>("Location", *this, out);
}

arrow::Status Location::Deserialize(std::string_view serialized, Location* out) {
  return DeserializeProtoString<pb::Location, Location>("Location", serialized, out);
}

arrow::Status FlightEndpoint::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::FlightEndpoint>("FlightEndpoint", *this, out);
}

arrow::Status FlightEndpoint::Deserialize(std::string_view serialized,
                                          FlightEndpoint* out) {
  return DeserializeProtoString<pb::FlightEndpoint, FlightEndpoint>("FlightEndpoint",
                                                                    serialized, out);
}

std::string RenewFlightEndpointRequest::ToString() const {
  std::stringstream ss;
  ss << "<RenewFlightEndpointRequest endpoint=" << endpoint.ToString() << ">";
  return ss.str();
}

bool RenewFlightEndpointRequest::Equals(const RenewFlightEndpointRequest& other) const {
  return endpoint == other.endpoint;
}

arrow::Status RenewFlightEndpointRequest::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::RenewFlightEndpointRequest>(
      "RenewFlightEndpointRequest", *this, out);
}

arrow::Status RenewFlightEndpointRequest::Deserialize(std::string_view serialized,
                                                      RenewFlightEndpointRequest* out) {
  return DeserializeProtoString<pb::RenewFlightEndpointRequest,
                                RenewFlightEndpointRequest>("RenewFlightEndpointRequest",
                                                            serialized, out);
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

std::string ActionType::ToString() const {
  return arrow::internal::JoinToString("<ActionType type='", type, "' description='",
                                       description, "'>");
}

const ActionType ActionType::kCancelFlightInfo =
    ActionType{"CancelFlightInfo",
               "Explicitly cancel a running FlightInfo.\n"
               "Request Message: CancelFlightInfoRequest\n"
               "Response Message: CancelFlightInfoResult"};
const ActionType ActionType::kRenewFlightEndpoint =
    ActionType{"RenewFlightEndpoint",
               "Extend expiration time of the given FlightEndpoint.\n"
               "Request Message: RenewFlightEndpointRequest\n"
               "Response Message: Renewed FlightEndpoint"};
const ActionType ActionType::kSetSessionOptions =
    ActionType{"SetSessionOptions",
               "Set client session options by name/value pairs.\n"
               "Request Message: SetSessionOptionsRequest\n"
               "Response Message: SetSessionOptionsResult"};
const ActionType ActionType::kGetSessionOptions =
    ActionType{"GetSessionOptions",
               "Get current client session options\n"
               "Request Message: GetSessionOptionsRequest\n"
               "Response Message: GetSessionOptionsResult"};
const ActionType ActionType::kCloseSession =
    ActionType{"CloseSession",
               "Explicitly close/invalidate the cookie-specified client session.\n"
               "Request Message: CloseSessionRequest\n"
               "Response Message: CloseSessionResult"};

bool ActionType::Equals(const ActionType& other) const {
  return type == other.type && description == other.description;
}

arrow::Status ActionType::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::ActionType>("ActionType", *this, out);
}

arrow::Status ActionType::Deserialize(std::string_view serialized, ActionType* out) {
  return DeserializeProtoString<pb::ActionType, ActionType>("ActionType", serialized,
                                                            out);
}

std::string Criteria::ToString() const {
  return arrow::internal::JoinToString("<Criteria expression='", expression, "'>");
}

bool Criteria::Equals(const Criteria& other) const {
  return expression == other.expression;
}

arrow::Status Criteria::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::Criteria>("Criteria", *this, out);
}

arrow::Status Criteria::Deserialize(std::string_view serialized, Criteria* out) {
  return DeserializeProtoString<pb::Criteria, Criteria>("Criteria", serialized, out);
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

arrow::Status Action::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::Action>("Action", *this, out);
}

arrow::Status Action::Deserialize(std::string_view serialized, Action* out) {
  return DeserializeProtoString<pb::Action, Action>("Action", serialized, out);
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

arrow::Status Result::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::Result>("Result", *this, out);
}

arrow::Status Result::Deserialize(std::string_view serialized, Result* out) {
  return DeserializeProtoString<pb::Result, Result>("Result", serialized, out);
}

arrow::Result<std::shared_ptr<Schema>> SchemaResult::GetSchema(
    ipc::DictionaryMemo* dictionary_memo) const {
  // Create a non-owned Buffer to avoid copying
  io::BufferReader schema_reader(std::make_shared<Buffer>(raw_schema_));
  return ipc::ReadSchema(&schema_reader, dictionary_memo);
}

arrow::Result<std::unique_ptr<SchemaResult>> SchemaResult::Make(const Schema& schema) {
  std::string schema_in;
  RETURN_NOT_OK(internal::SchemaToString(schema, &schema_in));
  return std::make_unique<SchemaResult>(std::move(schema_in));
}

std::string SchemaResult::ToString() const {
  return "<SchemaResult raw_schema=(serialized)>";
}

bool SchemaResult::Equals(const SchemaResult& other) const {
  return raw_schema_ == other.raw_schema_;
}

arrow::Status SchemaResult::SerializeToString(std::string* out) const {
  return SerializeToProtoString<pb::SchemaResult>("SchemaResult", *this, out);
}

arrow::Status SchemaResult::Deserialize(std::string_view serialized, SchemaResult* out) {
  return DeserializeProtoString<pb::SchemaResult, SchemaResult>("SchemaResult",
                                                                serialized, out);
}

//------------------------------------------------------------

Status ResultStream::Drain() {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto result, Next());
    if (!result) break;
  }
  return Status::OK();
}

FlightStreamChunk::FlightStreamChunk() noexcept = default;
FlightStreamChunk::~FlightStreamChunk() = default;

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

arrow::Result<std::shared_ptr<Table>> MetadataRecordBatchReader::ToTable() {
  ARROW_ASSIGN_OR_RAISE(auto batches, ToRecordBatches());
  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  return Table::FromRecordBatches(schema, std::move(batches));
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

//------------------------------------------------------------
// Error propagation helpers

std::string ToString(TransportStatusCode code) {
  switch (code) {
    case TransportStatusCode::kOk:
      return "kOk";
    case TransportStatusCode::kUnknown:
      return "kUnknown";
    case TransportStatusCode::kInternal:
      return "kInternal";
    case TransportStatusCode::kInvalidArgument:
      return "kInvalidArgument";
    case TransportStatusCode::kTimedOut:
      return "kTimedOut";
    case TransportStatusCode::kNotFound:
      return "kNotFound";
    case TransportStatusCode::kAlreadyExists:
      return "kAlreadyExists";
    case TransportStatusCode::kCancelled:
      return "kCancelled";
    case TransportStatusCode::kUnauthenticated:
      return "kUnauthenticated";
    case TransportStatusCode::kUnauthorized:
      return "kUnauthorized";
    case TransportStatusCode::kUnimplemented:
      return "kUnimplemented";
    case TransportStatusCode::kUnavailable:
      return "kUnavailable";
  }
  return "(unknown code)";
}

std::string TransportStatusDetail::ToString() const {
  std::string repr = "TransportStatusDetail{";
  repr += arrow::flight::ToString(code());
  repr += ", message=\"";
  repr += message();
  repr += "\", details={";

  bool first = true;
  for (const auto& [key, value] : details()) {
    if (!first) {
      repr += ", ";
    }
    first = false;

    repr += "{\"";
    repr += key;
    repr += "\", ";
    if (arrow::internal::EndsWith(key, "-bin")) {
      repr += arrow::util::base64_encode(value);
    } else {
      repr += "\"";
      repr += value;
      repr += "\"";
    }
    repr += "}";
  }

  repr += "}}";
  return repr;
}

std::optional<std::reference_wrapper<const TransportStatusDetail>>
TransportStatusDetail::Unwrap(const Status& status) {
  std::shared_ptr<StatusDetail> detail = status.detail();
  if (!detail) return std::nullopt;
  if (detail->type_id() != kTypeId) return std::nullopt;
  return std::cref(arrow::internal::checked_cast<const TransportStatusDetail&>(*detail));
}

//------------------------------------------------------------
// Async types

AsyncListenerBase::AsyncListenerBase() = default;
AsyncListenerBase::~AsyncListenerBase() = default;
void AsyncListenerBase::TryCancel() {
  if (rpc_state_) {
    rpc_state_->TryCancel();
  }
}

}  // namespace flight
}  // namespace arrow

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

// Platform-specific defines
#include "arrow/flight/platform.h"

#include "arrow/flight/server.h"

#include <signal.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/buffer.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/io-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/util/uri.h"

#include "arrow/flight/internal.h"
#include "arrow/flight/serialization-internal.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/types.h"

using FlightService = arrow::flight::protocol::FlightService;
using ServerContext = grpc::ServerContext;

template <typename T>
using ServerWriter = grpc::ServerWriter<T>;

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {

#define CHECK_ARG_NOT_NULL(VAL, MESSAGE)                              \
  if (VAL == nullptr) {                                               \
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, MESSAGE); \
  }

namespace {

// A MessageReader implementation that reads from a gRPC ServerReader
class FlightIpcMessageReader : public ipc::MessageReader {
 public:
  explicit FlightIpcMessageReader(
      grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
      std::shared_ptr<Buffer>* last_metadata)
      : reader_(reader), app_metadata_(last_metadata) {}

  Status ReadNextMessage(std::unique_ptr<ipc::Message>* out) override {
    if (stream_finished_) {
      *out = nullptr;
      *app_metadata_ = nullptr;
      return Status::OK();
    }
    internal::FlightData data;
    if (!internal::ReadPayload(reader_, &data)) {
      // Stream is finished
      stream_finished_ = true;
      if (first_message_) {
        return Status::Invalid(
            "Client provided malformed message or did not provide message");
      }
      *out = nullptr;
      *app_metadata_ = nullptr;
      return Status::OK();
    }

    if (first_message_) {
      if (!data.descriptor) {
        return Status::Invalid("DoPut must start with non-null descriptor");
      }
      descriptor_ = *data.descriptor;
      first_message_ = false;
    }

    RETURN_NOT_OK(data.OpenMessage(out));
    *app_metadata_ = std::move(data.app_metadata);
    return Status::OK();
  }

  const FlightDescriptor& descriptor() const { return descriptor_; }

 protected:
  grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader_;
  bool stream_finished_ = false;
  bool first_message_ = true;
  FlightDescriptor descriptor_;
  std::shared_ptr<Buffer>* app_metadata_;
};

class FlightMessageReaderImpl : public FlightMessageReader {
 public:
  explicit FlightMessageReaderImpl(
      grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader)
      : reader_(reader) {}

  Status Init() {
    message_reader_ = new FlightIpcMessageReader(reader_, &last_metadata_);
    return ipc::RecordBatchStreamReader::Open(
        std::unique_ptr<ipc::MessageReader>(message_reader_), &batch_reader_);
  }

  const FlightDescriptor& descriptor() const override {
    return message_reader_->descriptor();
  }

  std::shared_ptr<Schema> schema() const override { return batch_reader_->schema(); }

  Status Next(FlightStreamChunk* out) override {
    out->app_metadata = nullptr;
    RETURN_NOT_OK(batch_reader_->ReadNext(&out->data));
    out->app_metadata = std::move(last_metadata_);
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<ipc::DictionaryMemo> dictionary_memo_;
  grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader_;
  FlightIpcMessageReader* message_reader_;
  std::shared_ptr<Buffer> last_metadata_;
  std::shared_ptr<RecordBatchReader> batch_reader_;
};

class GrpcMetadataWriter : public FlightMetadataWriter {
 public:
  explicit GrpcMetadataWriter(
      grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* writer)
      : writer_(writer) {}

  Status WriteMetadata(const Buffer& buffer) override {
    pb::PutResult message{};
    message.set_app_metadata(buffer.data(), buffer.size());
    if (writer_->Write(message)) {
      return Status::OK();
    }
    return Status::IOError("Unknown error writing metadata.");
  }

 private:
  grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* writer_;
};

class GrpcServerAuthReader : public ServerAuthReader {
 public:
  explicit GrpcServerAuthReader(
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream)
      : stream_(stream) {}

  Status Read(std::string* token) override {
    pb::HandshakeRequest request;
    if (stream_->Read(&request)) {
      *token = std::move(*request.mutable_payload());
      return Status::OK();
    }
    return Status::IOError("Stream is closed.");
  }

 private:
  grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream_;
};

class GrpcServerAuthSender : public ServerAuthSender {
 public:
  explicit GrpcServerAuthSender(
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream)
      : stream_(stream) {}

  Status Write(const std::string& token) override {
    pb::HandshakeResponse response;
    response.set_payload(token);
    if (stream_->Write(response)) {
      return Status::OK();
    }
    return Status::IOError("Stream was closed.");
  }

 private:
  grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream_;
};

class FlightServiceImpl;
class GrpcServerCallContext : public ServerCallContext {
  const std::string& peer_identity() const override { return peer_identity_; }

 private:
  friend class FlightServiceImpl;
  ServerContext* context_;
  std::string peer_identity_;
};

// This class glues an implementation of FlightServerBase together with the
// gRPC service definition, so the latter is not exposed in the public API
class FlightServiceImpl : public FlightService::Service {
 public:
  explicit FlightServiceImpl(std::shared_ptr<ServerAuthHandler> auth_handler,
                             FlightServerBase* server)
      : auth_handler_(auth_handler), server_(server) {}

  template <typename UserType, typename Iterator, typename ProtoType>
  grpc::Status WriteStream(Iterator* iterator, ServerWriter<ProtoType>* writer) {
    if (!iterator) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "No items to iterate");
    }
    // Write flight info to stream until listing is exhausted
    while (true) {
      ProtoType pb_value;
      std::unique_ptr<UserType> value;
      GRPC_RETURN_NOT_OK(iterator->Next(&value));
      if (!value) {
        break;
      }
      GRPC_RETURN_NOT_OK(internal::ToProto(*value, &pb_value));

      // Blocking write
      if (!writer->Write(pb_value)) {
        // Write returns false if the stream is closed
        break;
      }
    }
    return grpc::Status::OK;
  }

  template <typename UserType, typename ProtoType>
  grpc::Status WriteStream(const std::vector<UserType>& values,
                           ServerWriter<ProtoType>* writer) {
    // Write flight info to stream until listing is exhausted
    for (const UserType& value : values) {
      ProtoType pb_value;
      GRPC_RETURN_NOT_OK(internal::ToProto(value, &pb_value));
      // Blocking write
      if (!writer->Write(pb_value)) {
        // Write returns false if the stream is closed
        break;
      }
    }
    return grpc::Status::OK;
  }

  // Authenticate the client (if applicable) and construct the call context
  grpc::Status CheckAuth(ServerContext* context, GrpcServerCallContext& flight_context) {
    flight_context.context_ = context;
    if (!auth_handler_) {
      flight_context.peer_identity_ = "";
      return grpc::Status::OK;
    }

    const auto client_metadata = context->client_metadata();
    const auto auth_header = client_metadata.find(internal::kGrpcAuthHeader);
    std::string token;
    if (auth_header == client_metadata.end()) {
      token = "";
    } else {
      token = std::string(auth_header->second.data(), auth_header->second.length());
    }
    GRPC_RETURN_NOT_OK(auth_handler_->IsValid(token, &flight_context.peer_identity_));
    return grpc::Status::OK;
  }

  grpc::Status Handshake(
      ServerContext* context,
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream) {
    if (!auth_handler_) {
      return grpc::Status(
          grpc::StatusCode::UNIMPLEMENTED,
          "This service does not have an authentication mechanism enabled.");
    }
    GrpcServerAuthSender outgoing{stream};
    GrpcServerAuthReader incoming{stream};
    GRPC_RETURN_NOT_OK(auth_handler_->Authenticate(&outgoing, &incoming));
    return grpc::Status::OK;
  }

  grpc::Status ListFlights(ServerContext* context, const pb::Criteria* request,
                           ServerWriter<pb::FlightInfo>* writer) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));

    // Retrieve the listing from the implementation
    std::unique_ptr<FlightListing> listing;

    Criteria criteria;
    if (request) {
      GRPC_RETURN_NOT_OK(internal::FromProto(*request, &criteria));
    }
    GRPC_RETURN_NOT_OK(server_->ListFlights(flight_context, &criteria, &listing));
    if (!listing) {
      // Treat null listing as no flights available
      return grpc::Status::OK;
    }
    return WriteStream<FlightInfo>(listing.get(), writer);
  }

  grpc::Status GetFlightInfo(ServerContext* context, const pb::FlightDescriptor* request,
                             pb::FlightInfo* response) {
    CHECK_ARG_NOT_NULL(request, "FlightDescriptor cannot be null");
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));

    FlightDescriptor descr;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &descr));

    std::unique_ptr<FlightInfo> info;
    GRPC_RETURN_NOT_OK(server_->GetFlightInfo(flight_context, descr, &info));

    if (!info) {
      // Treat null listing as no flights available
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Flight not found");
    }

    GRPC_RETURN_NOT_OK(internal::ToProto(*info, response));
    return grpc::Status::OK;
  }

  grpc::Status DoGet(ServerContext* context, const pb::Ticket* request,
                     ServerWriter<pb::FlightData>* writer) {
    CHECK_ARG_NOT_NULL(request, "ticket cannot be null");
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));

    Ticket ticket;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &ticket));

    std::unique_ptr<FlightDataStream> data_stream;
    GRPC_RETURN_NOT_OK(server_->DoGet(flight_context, ticket, &data_stream));

    if (!data_stream) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "No data in this flight");
    }

    // Write the schema as the first message in the stream
    FlightPayload schema_payload;
    GRPC_RETURN_NOT_OK(data_stream->GetSchemaPayload(&schema_payload));
    if (!internal::WritePayload(schema_payload, writer)) {
      // Connection terminated?  XXX return error code?
      return grpc::Status::OK;
    }

    // Consume data stream and write out payloads
    while (true) {
      FlightPayload payload;
      GRPC_RETURN_NOT_OK(data_stream->Next(&payload));
      if (payload.ipc_message.metadata == nullptr ||
          !internal::WritePayload(payload, writer))
        // No more messages to write, or connection terminated for some other
        // reason
        break;
    }
    return grpc::Status::OK;
  }

  grpc::Status DoPut(ServerContext* context,
                     grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));

    auto message_reader =
        std::unique_ptr<FlightMessageReaderImpl>(new FlightMessageReaderImpl(reader));
    GRPC_RETURN_NOT_OK(message_reader->Init());
    auto metadata_writer =
        std::unique_ptr<FlightMetadataWriter>(new GrpcMetadataWriter(reader));
    return internal::ToGrpcStatus(server_->DoPut(
        flight_context, std::move(message_reader), std::move(metadata_writer)));
  }

  grpc::Status ListActions(ServerContext* context, const pb::Empty* request,
                           ServerWriter<pb::ActionType>* writer) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));
    // Retrieve the listing from the implementation
    std::vector<ActionType> types;
    GRPC_RETURN_NOT_OK(server_->ListActions(flight_context, &types));
    return WriteStream<ActionType>(types, writer);
  }

  grpc::Status DoAction(ServerContext* context, const pb::Action* request,
                        ServerWriter<pb::Result>* writer) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));
    CHECK_ARG_NOT_NULL(request, "Action cannot be null");
    Action action;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &action));

    std::unique_ptr<ResultStream> results;
    GRPC_RETURN_NOT_OK(server_->DoAction(flight_context, action, &results));

    if (!results) {
      return grpc::Status::CANCELLED;
    }

    while (true) {
      std::unique_ptr<Result> result;
      GRPC_RETURN_NOT_OK(results->Next(&result));
      if (!result) {
        // No more results
        break;
      }
      pb::Result pb_result;
      GRPC_RETURN_NOT_OK(internal::ToProto(*result, &pb_result));
      if (!writer->Write(pb_result)) {
        // Stream may be closed
        break;
      }
    }
    return grpc::Status::OK;
  }

 private:
  std::shared_ptr<ServerAuthHandler> auth_handler_;
  FlightServerBase* server_;
};

}  // namespace

FlightMetadataWriter::~FlightMetadataWriter() = default;

//
// gRPC server lifecycle
//

#if (ATOMIC_INT_LOCK_FREE != 2 || ATOMIC_POINTER_LOCK_FREE != 2)
#error "atomic ints and atomic pointers not always lock-free!"
#endif

using ::arrow::internal::GetSignalHandler;
using ::arrow::internal::SetSignalHandler;
using ::arrow::internal::SignalHandler;

struct FlightServerBase::Impl {
  std::unique_ptr<FlightServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
#ifdef _WIN32
  // Signal handlers are executed in a separate thread on Windows, so getting
  // the current thread instance wouldn't make sense.  This means only a single
  // instance can receive signals on Windows.
  static std::atomic<Impl*> running_instance_;
#else
  static thread_local std::atomic<Impl*> running_instance_;
#endif

  // Signal handling
  std::vector<int> signals_;
  std::vector<SignalHandler> old_signal_handlers_;
  std::atomic<int> got_signal_;

  static void HandleSignal(int signum) {
    auto instance = running_instance_.load();
    if (instance != nullptr) {
      instance->DoHandleSignal(signum);
    }
  }

  void DoHandleSignal(int signum) {
    got_signal_ = signum;
    server_->Shutdown();
  }
};

#ifdef _WIN32
std::atomic<FlightServerBase::Impl*> FlightServerBase::Impl::running_instance_;
#else
thread_local std::atomic<FlightServerBase::Impl*>
    FlightServerBase::Impl::running_instance_;
#endif

FlightServerOptions::FlightServerOptions(const Location& location_)
    : location(location_), auth_handler(nullptr), tls_certificates() {}

FlightServerBase::FlightServerBase() { impl_.reset(new Impl); }

FlightServerBase::~FlightServerBase() {}

Status FlightServerBase::Init(FlightServerOptions& options) {
  std::shared_ptr<ServerAuthHandler> handler = std::move(options.auth_handler);
  impl_->service_.reset(new FlightServiceImpl(handler, this));

  grpc::ServerBuilder builder;
  // Allow uploading messages of any length
  builder.SetMaxReceiveMessageSize(-1);

  const Location& location = options.location;
  const std::string scheme = location.scheme();
  if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp || scheme == kSchemeGrpcTls) {
    std::stringstream address;
    address << location.uri_->host() << ':' << location.uri_->port_text();

    std::shared_ptr<grpc::ServerCredentials> creds;
    if (scheme == kSchemeGrpcTls) {
      grpc::SslServerCredentialsOptions ssl_options;
      for (const auto& pair : options.tls_certificates) {
        ssl_options.pem_key_cert_pairs.push_back({pair.pem_key, pair.pem_cert});
      }
      creds = grpc::SslServerCredentials(ssl_options);
    } else {
      creds = grpc::InsecureServerCredentials();
    }

    builder.AddListeningPort(address.str(), creds);
  } else if (scheme == kSchemeGrpcUnix) {
    std::stringstream address;
    address << "unix:" << location.uri_->path();
    builder.AddListeningPort(address.str(), grpc::InsecureServerCredentials());
  } else {
    return Status::NotImplemented("Scheme is not supported: " + scheme);
  }

  builder.RegisterService(impl_->service_.get());

  // Disable SO_REUSEPORT - it makes debugging/testing a pain as
  // leftover processes can handle requests on accident
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);

  impl_->server_ = builder.BuildAndStart();
  if (!impl_->server_) {
    return Status::UnknownError("Server did not start properly");
  }
  return Status::OK();
}

Status FlightServerBase::SetShutdownOnSignals(const std::vector<int> sigs) {
  impl_->signals_ = sigs;
  impl_->old_signal_handlers_.clear();
  return Status::OK();
}

Status FlightServerBase::Serve() {
  if (!impl_->server_) {
    return Status::UnknownError("Server did not start properly");
  }
  impl_->got_signal_ = 0;
  impl_->old_signal_handlers_.clear();
  impl_->running_instance_ = impl_.get();

  // Override existing signal handlers with our own handler so as to stop the server.
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    int signum = impl_->signals_[i];
    SignalHandler new_handler(&Impl::HandleSignal), old_handler;
    RETURN_NOT_OK(SetSignalHandler(signum, new_handler, &old_handler));
    impl_->old_signal_handlers_.push_back(old_handler);
  }

  impl_->server_->Wait();
  impl_->running_instance_ = nullptr;

  // Restore signal handlers
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    RETURN_NOT_OK(
        SetSignalHandler(impl_->signals_[i], impl_->old_signal_handlers_[i], nullptr));
  }

  return Status::OK();
}

int FlightServerBase::GotSignal() const { return impl_->got_signal_; }

Status FlightServerBase::Shutdown() {
  auto server = impl_->server_.get();
  if (!server) {
    return Status::Invalid("Shutdown() on uninitialized FlightServerBase");
  }
  impl_->server_->Shutdown();
  return Status::OK();
}

Status FlightServerBase::ListFlights(const ServerCallContext& context,
                                     const Criteria* criteria,
                                     std::unique_ptr<FlightListing>* listings) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::GetFlightInfo(const ServerCallContext& context,
                                       const FlightDescriptor& request,
                                       std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoGet(const ServerCallContext& context, const Ticket& request,
                               std::unique_ptr<FlightDataStream>* data_stream) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoPut(const ServerCallContext& context,
                               std::unique_ptr<FlightMessageReader> reader,
                               std::unique_ptr<FlightMetadataWriter> writer) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoAction(const ServerCallContext& context, const Action& action,
                                  std::unique_ptr<ResultStream>* result) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::ListActions(const ServerCallContext& context,
                                     std::vector<ActionType>* actions) {
  return Status::NotImplemented("NYI");
}

// ----------------------------------------------------------------------
// Implement RecordBatchStream

class RecordBatchStream::RecordBatchStreamImpl {
 public:
  // Stages of the stream when producing paylaods
  enum class Stage {
    NEW,          // The stream has been created, but Next has not been called yet
    DICTIONARY,   // Dictionaries have been collected, and are being sent
    RECORD_BATCH  // Initial have been sent
  };

  RecordBatchStreamImpl(const std::shared_ptr<RecordBatchReader>& reader,
                        MemoryPool* pool)
      : pool_(pool), reader_(reader), ipc_options_(ipc::IpcOptions::Defaults()) {}

  std::shared_ptr<Schema> schema() { return reader_->schema(); }

  Status GetSchemaPayload(FlightPayload* payload) {
    return ipc::internal::GetSchemaPayload(*reader_->schema(), ipc_options_,
                                           &dictionary_memo_, &payload->ipc_message);
  }

  Status Next(FlightPayload* payload) {
    if (stage_ == Stage::NEW) {
      RETURN_NOT_OK(reader_->ReadNext(&current_batch_));
      if (!current_batch_) {
        // Signal that iteration is over
        payload->ipc_message.metadata = nullptr;
        return Status::OK();
      }
      RETURN_NOT_OK(CollectDictionaries(*current_batch_));
      stage_ = Stage::DICTIONARY;
    }

    if (stage_ == Stage::DICTIONARY) {
      if (dictionary_index_ == static_cast<int>(dictionaries_.size())) {
        stage_ = Stage::RECORD_BATCH;
        return ipc::internal::GetRecordBatchPayload(*current_batch_, ipc_options_, pool_,
                                                    &payload->ipc_message);
      } else {
        return GetNextDictionary(payload);
      }
    }

    RETURN_NOT_OK(reader_->ReadNext(&current_batch_));

    // TODO(wesm): Delta dictionaries
    if (!current_batch_) {
      // Signal that iteration is over
      payload->ipc_message.metadata = nullptr;
      return Status::OK();
    } else {
      return ipc::internal::GetRecordBatchPayload(*current_batch_, ipc_options_, pool_,
                                                  &payload->ipc_message);
    }
  }

 private:
  Status GetNextDictionary(FlightPayload* payload) {
    const auto& it = dictionaries_[dictionary_index_++];
    return ipc::internal::GetDictionaryPayload(it.first, it.second, ipc_options_, pool_,
                                               &payload->ipc_message);
  }

  Status CollectDictionaries(const RecordBatch& batch) {
    RETURN_NOT_OK(ipc::CollectDictionaries(batch, &dictionary_memo_));
    for (auto& pair : dictionary_memo_.id_to_dictionary()) {
      dictionaries_.push_back({pair.first, pair.second});
    }
    return Status::OK();
  }

  Stage stage_ = Stage::NEW;
  MemoryPool* pool_;
  std::shared_ptr<RecordBatchReader> reader_;
  ipc::DictionaryMemo dictionary_memo_;
  ipc::IpcOptions ipc_options_;
  std::shared_ptr<RecordBatch> current_batch_;
  std::vector<std::pair<int64_t, std::shared_ptr<Array>>> dictionaries_;

  // Index of next dictionary to send
  int dictionary_index_ = 0;
};

FlightDataStream::~FlightDataStream() {}

RecordBatchStream::RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader,
                                     MemoryPool* pool) {
  impl_.reset(new RecordBatchStreamImpl(reader, pool));
}

RecordBatchStream::~RecordBatchStream() {}

std::shared_ptr<Schema> RecordBatchStream::schema() { return impl_->schema(); }

Status RecordBatchStream::GetSchemaPayload(FlightPayload* payload) {
  return impl_->GetSchemaPayload(payload);
}

Status RecordBatchStream::Next(FlightPayload* payload) { return impl_->Next(payload); }

}  // namespace flight
}  // namespace arrow

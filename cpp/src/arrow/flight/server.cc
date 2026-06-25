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

// Flight server lifecycle implementation on top of the transport
// interface

// Platform-specific defines
#include "arrow/flight/platform.h"

#include "arrow/flight/server.h"

#include <chrono>
#include <deque>
#include <memory>
#include <utility>

#include "arrow/device.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/grpc_server.h"
#include "arrow/flight/transport_server_internal.h"
#include "arrow/flight/transport_server.h"
#include "arrow/flight/types.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace flight {

/// Server implementation. Manages the lifecycle of the "real" server
/// (ServerTransport) and contains
struct FlightServerBase::Impl {
  std::unique_ptr<internal::ServerTransport> transport_;
  internal::ServerSignalState signal_state_;
};

FlightServerOptions::FlightServerOptions(const Location& location_)
    : location(location_),
      auth_handler(nullptr),
      tls_certificates(),
      verify_client(false),
      root_certificates(),
      middleware(),
      memory_manager(CPUDevice::Instance()->default_memory_manager()),
      builder_hook(nullptr) {}

FlightServerOptions::~FlightServerOptions() = default;

FlightServerBase::FlightServerBase() { impl_.reset(new Impl); }

FlightServerBase::~FlightServerBase() {}

Status FlightServerBase::Init(const FlightServerOptions& options) {
  flight::transport::grpc::InitializeFlightGrpcServer();

  const auto scheme = options.location.scheme();
  ARROW_ASSIGN_OR_RAISE(impl_->transport_,
                        internal::GetDefaultTransportRegistry()->MakeServer(
                            scheme, this, options.memory_manager));
  ARROW_ASSIGN_OR_RAISE(auto uri, internal::ParseLocationUri(options.location));
  return impl_->transport_->Init(options, uri);
}

int FlightServerBase::port() const { return internal::PortFromLocation(location()); }

Location FlightServerBase::location() const { return impl_->transport_->location(); }

Status FlightServerBase::SetShutdownOnSignals(const std::vector<int> sigs) {
  return impl_->signal_state_.SetShutdownOnSignals(sigs);
}

Status FlightServerBase::Serve() {
  return impl_->signal_state_.Serve(
      [this]() -> Status {
        if (!impl_->transport_) {
          return Status::UnknownError("Server did not start properly");
        }
        return impl_->transport_->Wait();
      },
      [this](const std::chrono::system_clock::time_point* deadline) -> Status {
        if (!impl_->transport_) {
          return Status::Invalid("Shutdown() on uninitialized FlightServerBase");
        }
        if (deadline) {
          return impl_->transport_->Shutdown(*deadline);
        }
        return impl_->transport_->Shutdown();
      },
      "Server did not start properly", "Error shutting down server");
}

int FlightServerBase::GotSignal() const { return impl_->signal_state_.GotSignal(); }

Status FlightServerBase::Shutdown(const std::chrono::system_clock::time_point* deadline) {
  return impl_->signal_state_.Shutdown(
      [this](const std::chrono::system_clock::time_point* maybe_deadline) -> Status {
        if (!impl_->transport_) {
          return Status::Invalid("Shutdown() on uninitialized FlightServerBase");
        }
        if (maybe_deadline) {
          return impl_->transport_->Shutdown(*maybe_deadline);
        }
        return impl_->transport_->Shutdown();
      },
      deadline);
}

Status FlightServerBase::Wait() {
  return impl_->signal_state_.Wait([this] {
    if (!impl_->transport_) {
      return Status::Invalid("Wait() on uninitialized FlightServerBase");
    }
    return impl_->transport_->Wait();
  });
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

Status FlightServerBase::PollFlightInfo(const ServerCallContext& context,
                                        const FlightDescriptor& request,
                                        std::unique_ptr<PollInfo>* info) {
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

Status FlightServerBase::DoExchange(const ServerCallContext& context,
                                    std::unique_ptr<FlightMessageReader> reader,
                                    std::unique_ptr<FlightMessageWriter> writer) {
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

Status FlightServerBase::GetSchema(const ServerCallContext& context,
                                   const FlightDescriptor& request,
                                   std::unique_ptr<SchemaResult>* schema) {
  return Status::NotImplemented("NYI");
}

// ----------------------------------------------------------------------
// Implement RecordBatchStream

class RecordBatchStream::RecordBatchStreamImpl {
 public:
  RecordBatchStreamImpl(const std::shared_ptr<RecordBatchReader>& reader,
                        const ipc::IpcWriteOptions& options)
      : reader_(reader), options_(options) {}

  std::shared_ptr<Schema> schema() { return reader_->schema(); }

  Status GetSchemaPayload(FlightPayload* payload) {
    if (!writer_) {
      RETURN_NOT_OK(InitializeWriter());
    }

    // Return the expected schema payload.
    if (payload_deque_.empty()) {
      return Status::UnknownError("No schema payload generated");
    }
    *payload = std::move(payload_deque_.front());
    payload_deque_.pop_front();
    return Status::OK();
  }

  Status Next(FlightPayload* payload) {
    // If we have previous payloads (dictionary messages or previous record batches)
    // we will return them before reading the next record batch.
    if (payload_deque_.empty()) {
      std::shared_ptr<RecordBatch> batch;
      RETURN_NOT_OK(reader_->ReadNext(&batch));
      if (!batch) {
        // End of stream
        if (writer_) {
          RETURN_NOT_OK(writer_->Close());
        }
        payload->ipc_message.metadata = nullptr;
        return Status::OK();
      }
      if (!writer_) {
        RETURN_NOT_OK(InitializeWriter());
        // If the writer has not been initialized yet, the first batch in the payload
        // queue is going to be a SCHEMA one. In this context, that is
        // unexpected, so drop it from the queue so that there is a RECORD_BATCH
        // message on the top (same as would be if the writer had been initialized
        // in GetSchemaPayload).
        if (payload_deque_.front().ipc_message.type == ipc::MessageType::SCHEMA) {
          payload_deque_.pop_front();
        }
      }
      // One WriteRecordBatch call might generate multiple payloads, so we
      // need to collect them in a deque.
      RETURN_NOT_OK(writer_->WriteRecordBatch(*batch));
    }

    // There must be at least one payload generated after WriteRecordBatch or
    // from previous calls to WriteRecordBatch.
    if (payload_deque_.empty()) {
      return Status::UnknownError("IPC writer didn't produce any payloads");
    }

    *payload = std::move(payload_deque_.front());
    payload_deque_.pop_front();
    return Status::OK();
  }

  Status Close() {
    if (writer_) {
      RETURN_NOT_OK(writer_->Close());
    }
    return reader_->Close();
  }

 private:
  // Simple payload writer that uses a deque to store generated payloads.
  class ServerRecordBatchPayloadWriter : public ipc::internal::IpcPayloadWriter {
   public:
    explicit ServerRecordBatchPayloadWriter(std::deque<FlightPayload>* payload_deque)
        : payload_deque_(payload_deque) {}

    Status Start() override { return Status::OK(); }

    Status WritePayload(const ipc::IpcPayload& ipc_payload) override {
      FlightPayload payload;
      payload.ipc_message = ipc_payload;

      payload_deque_->push_back(std::move(payload));
      return Status::OK();
    }

    Status Close() override { return Status::OK(); }

   private:
    std::deque<FlightPayload>* payload_deque_;
  };

  std::shared_ptr<RecordBatchReader> reader_;
  ipc::IpcWriteOptions options_;
  std::unique_ptr<ipc::RecordBatchWriter> writer_;
  std::deque<FlightPayload> payload_deque_;

  Status InitializeWriter() {
    auto payload_writer =
        std::make_unique<ServerRecordBatchPayloadWriter>(&payload_deque_);
    ARROW_ASSIGN_OR_RAISE(
        writer_, ipc::internal::OpenRecordBatchWriter(std::move(payload_writer),
                                                      reader_->schema(), options_));
    return Status::OK();
  }
};

FlightMetadataWriter::~FlightMetadataWriter() = default;

FlightDataStream::~FlightDataStream() {}
Status FlightDataStream::Close() { return Status::OK(); }

RecordBatchStream::RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader,
                                     const ipc::IpcWriteOptions& options) {
  impl_.reset(new RecordBatchStreamImpl(reader, options));
}

RecordBatchStream::~RecordBatchStream() {
  ARROW_WARN_NOT_OK(impl_->Close(), "Failed to close FlightDataStream");
}

Status RecordBatchStream::Close() { return impl_->Close(); }

std::shared_ptr<Schema> RecordBatchStream::schema() { return impl_->schema(); }

arrow::Result<FlightPayload> RecordBatchStream::GetSchemaPayload() {
  FlightPayload payload;
  RETURN_NOT_OK(impl_->GetSchemaPayload(&payload));
  return payload;
}

arrow::Result<FlightPayload> RecordBatchStream::Next() {
  FlightPayload payload;
  RETURN_NOT_OK(impl_->Next(&payload));
  return payload;
}

}  // namespace flight
}  // namespace arrow

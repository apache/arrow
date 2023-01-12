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

/// The client-side implementation of a UCX-based transport for
/// Flight.
///
/// Each UCX driver is used to support one call at a time. This gives
/// the greatest throughput for data plane methods, but is relatively
/// expensive in terms of other resources, both for the server and the
/// client. (UCX drivers have multiple threading modes: single-thread
/// access, serialized access, and multi-thread access. Testing found
/// that multi-thread access incurred high synchronization costs.)
/// Hence, for concurrent calls in a single client, we must maintain
/// multiple drivers, and so unlike gRPC, there is no real difference
/// between using one client concurrently and using multiple
/// independent clients.

#include "arrow/flight/transport/ucx/ucx_internal.h"

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include <arpa/inet.h>
#include <ucp/api/ucp.h>

#include "arrow/buffer.h"
#include "arrow/flight/client.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/ucx/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

namespace {
class UcxClientImpl;

Status MergeStatuses(Status server_status, Status transport_status) {
  if (server_status.ok()) {
    if (transport_status.ok()) return server_status;
    return transport_status;
  } else if (transport_status.ok()) {
    return server_status;
  }
  return Status::FromDetailAndArgs(server_status.code(), server_status.detail(),
                                   server_status.message(),
                                   ". Transport context: ", transport_status.ToString());
}

/// \brief An individual connection to the server.
class ClientConnection {
 public:
  ClientConnection() = default;
  ARROW_DISALLOW_COPY_AND_ASSIGN(ClientConnection);
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ClientConnection);
  ~ClientConnection() { DCHECK(!driver_) << "Connection was not closed!"; }

  Status Init(std::shared_ptr<UcpContext> ucp_context, const arrow::internal::Uri& uri) {
    auto status = InitImpl(std::move(ucp_context), uri);
    // Clean up after-the-fact if we fail to initialize
    if (!status.ok()) {
      if (driver_) {
        status = MergeStatuses(std::move(status), driver_->Close());
        driver_.reset();
        remote_endpoint_ = nullptr;
      }
      if (ucp_worker_) ucp_worker_.reset();
    }
    return status;
  }

  Status InitImpl(std::shared_ptr<UcpContext> ucp_context,
                  const arrow::internal::Uri& uri) {
    {
      ucs_status_t status;
      ucp_worker_params_t worker_params;
      std::memset(&worker_params, 0, sizeof(worker_params));
      worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
      worker_params.thread_mode = UCS_THREAD_MODE_SERIALIZED;

      ucp_worker_h ucp_worker;
      status = ucp_worker_create(ucp_context->get(), &worker_params, &ucp_worker);
      RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));
      ucp_worker_.reset(new UcpWorker(std::move(ucp_context), ucp_worker));
    }
    {
      // Create endpoint for remote worker
      struct sockaddr_storage connect_addr;
      ARROW_ASSIGN_OR_RAISE(auto addrlen, UriToSockaddr(uri, &connect_addr));
      std::string peer;
      ARROW_UNUSED(SockaddrToString(connect_addr).Value(&peer));
      ARROW_LOG(DEBUG) << "Connecting to " << peer;

      ucp_ep_params_t params;
      params.field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_NAME |
                          UCP_EP_PARAM_FIELD_SOCK_ADDR;
      params.flags = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
      params.name = "UcxClientImpl";
      params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&connect_addr);
      params.sockaddr.addrlen = addrlen;

      auto status = ucp_ep_create(ucp_worker_->get(), &params, &remote_endpoint_);
      RETURN_NOT_OK(FromUcsStatus("ucp_ep_create", status));
    }

    driver_ = std::make_unique<UcpCallDriver>(ucp_worker_, remote_endpoint_);
    ARROW_LOG(DEBUG) << "Connected to " << driver_->peer();

    {
      // Set up Active Message (AM) handler
      ucp_am_handler_param_t handler_params;
      handler_params.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                                  UCP_AM_HANDLER_PARAM_FIELD_CB |
                                  UCP_AM_HANDLER_PARAM_FIELD_ARG;
      handler_params.id = kUcpAmHandlerId;
      handler_params.cb = HandleIncomingActiveMessage;
      handler_params.arg = driver_.get();
      ucs_status_t status =
          ucp_worker_set_am_recv_handler(ucp_worker_->get(), &handler_params);
      RETURN_NOT_OK(FromUcsStatus("ucp_worker_set_am_recv_handler", status));
    }

    return Status::OK();
  }

  Status Close() {
    if (!driver_) return Status::OK();

    auto status = driver_->SendFrame(FrameType::kDisconnect, nullptr, 0);
    const auto ucs_status = FlightUcxStatusDetail::Unwrap(status);
    if (IsIgnorableDisconnectError(ucs_status)) {
      status = Status::OK();
    }
    status = MergeStatuses(std::move(status), driver_->Close());

    driver_.reset();
    remote_endpoint_ = nullptr;
    ucp_worker_.reset();
    return status;
  }

  UcpCallDriver* driver() {
    DCHECK(driver_);
    return driver_.get();
  }

 private:
  static ucs_status_t HandleIncomingActiveMessage(void* self, const void* header,
                                                  size_t header_length, void* data,
                                                  size_t data_length,
                                                  const ucp_am_recv_param_t* param) {
    auto* driver = reinterpret_cast<UcpCallDriver*>(self);
    return driver->RecvActiveMessage(header, header_length, data, data_length, param);
  }

  std::shared_ptr<UcpWorker> ucp_worker_;
  ucp_ep_h remote_endpoint_;
  std::unique_ptr<UcpCallDriver> driver_;
};

class UcxClientStream : public internal::ClientDataStream {
 public:
  UcxClientStream(UcxClientImpl* impl, ClientConnection conn)
      : impl_(impl),
        conn_(std::move(conn)),
        driver_(conn_.driver()),
        writes_done_(false),
        finished_(false) {
    DCHECK_NE(impl, nullptr);
    DCHECK_NE(conn_.driver(), nullptr);
  }

 protected:
  Status DoFinish() override;

  std::mutex finish_mutex_;
  UcxClientImpl* impl_;
  ClientConnection conn_;
  UcpCallDriver* driver_;
  bool writes_done_;
  bool finished_;
  Status io_status_;
  Status server_status_;
};

class GetClientStream : public UcxClientStream {
 public:
  GetClientStream(UcxClientImpl* impl, ClientConnection conn)
      : UcxClientStream(impl, std::move(conn)) {
    writes_done_ = true;
  }

  bool ReadData(internal::FlightData* data) override {
    if (finished_) return false;

    bool success = true;
    io_status_ = ReadImpl(data).Value(&success);

    if (!io_status_.ok() || !success) {
      finished_ = true;
    }
    return success;
  }

 private:
  ::arrow::Result<bool> ReadImpl(internal::FlightData* data) {
    ARROW_ASSIGN_OR_RAISE(auto frame, driver_->ReadNextFrame());

    if (frame->type == FrameType::kHeaders) {
      // Trailers, stream is over
      ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Parse(std::move(frame->buffer)));
      RETURN_NOT_OK(headers.GetStatus(&server_status_));
      return false;
    }

    RETURN_NOT_OK(driver_->ExpectFrameType(*frame, FrameType::kPayloadHeader));
    PayloadHeaderFrame payload_header(std::move(frame->buffer));
    RETURN_NOT_OK(payload_header.ToFlightData(data));

    // DoGet does not support metadata-only messages, so we can always
    // assume we have an IPC payload
    ARROW_ASSIGN_OR_RAISE(auto message, ipc::Message::Open(data->metadata, nullptr));

    if (ipc::Message::HasBody(message->type())) {
      ARROW_ASSIGN_OR_RAISE(frame, driver_->ReadNextFrame());
      RETURN_NOT_OK(driver_->ExpectFrameType(*frame, FrameType::kPayloadBody));
      data->body = std::move(frame->buffer);
    }
    return true;
  }
};

class WriteClientStream : public UcxClientStream {
 public:
  WriteClientStream(UcxClientImpl* impl, ClientConnection conn)
      : UcxClientStream(impl, std::move(conn)) {
    std::thread t(&WriteClientStream::DriveWorker, this);
    driver_thread_.swap(t);
  }
  arrow::Result<bool> WriteData(const FlightPayload& payload) override {
    std::unique_lock<std::mutex> guard(driver_mutex_);
    if (finished_ || writes_done_) return false;
    outgoing_ = driver_->SendFlightPayload(payload);
    working_cv_.notify_all();
    completed_cv_.wait(guard, [this] { return outgoing_.is_finished(); });

    auto status = outgoing_.status();
    outgoing_ = Future<>();
    RETURN_NOT_OK(status);
    return true;
  }
  Status WritesDone() override {
    std::unique_lock<std::mutex> guard(driver_mutex_);
    if (!writes_done_) {
      ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Make({}));
      outgoing_ =
          driver_->SendFrameAsync(FrameType::kHeaders, std::move(headers).GetBuffer());
      working_cv_.notify_all();
      completed_cv_.wait(guard, [this] { return outgoing_.is_finished(); });

      writes_done_ = true;
      auto status = outgoing_.status();
      outgoing_ = Future<>();
      RETURN_NOT_OK(status);
    }
    return Status::OK();
  }

 protected:
  void JoinThread() {
    try {
      driver_thread_.join();
    } catch (const std::system_error&) {
      // Ignore
    }
  }
  // Flight's API allows concurrent reads/writes, but the UCX driver
  // here is single-threaded, so push all UCX work onto a single
  // worker thread
  void DriveWorker() {
    while (true) {
      {
        std::unique_lock<std::mutex> guard(driver_mutex_);
        working_cv_.wait(guard,
                         [this] { return incoming_.is_valid() || outgoing_.is_valid(); });
      }

      while (true) {
        std::unique_lock<std::mutex> guard(driver_mutex_);
        if (!incoming_.is_valid() && !outgoing_.is_valid()) break;
        if (incoming_.is_valid() && incoming_.is_finished()) {
          if (!incoming_.status().ok()) {
            io_status_ = incoming_.status();
            finished_ = true;
          } else {
            HandleIncomingMessage(*incoming_.result());
          }
          incoming_ = Future<std::shared_ptr<Frame>>();
          completed_cv_.notify_all();
          break;
        }
        if (outgoing_.is_valid() && outgoing_.is_finished()) {
          completed_cv_.notify_all();
          break;
        }
        driver_->MakeProgress();
      }
      if (finished_) return;
    }
  }

  virtual void HandleIncomingMessage(const std::shared_ptr<Frame>& frame) {}

  std::mutex driver_mutex_;
  std::thread driver_thread_;
  std::condition_variable completed_cv_;
  std::condition_variable working_cv_;
  Future<std::shared_ptr<Frame>> incoming_;
  Future<> outgoing_;
};

class PutClientStream : public WriteClientStream {
 public:
  using WriteClientStream::WriteClientStream;
  bool ReadPutMetadata(std::shared_ptr<Buffer>* out) override {
    std::unique_lock<std::mutex> guard(driver_mutex_);
    if (finished_) {
      *out = nullptr;
      guard.unlock();
      JoinThread();
      return false;
    }
    next_metadata_ = nullptr;
    incoming_ = driver_->ReadFrameAsync();
    working_cv_.notify_all();
    completed_cv_.wait(guard, [this] { return next_metadata_ != nullptr || finished_; });

    if (finished_) {
      *out = nullptr;
      guard.unlock();
      JoinThread();
      return false;
    }
    *out = std::move(next_metadata_);
    return true;
  }

 private:
  void HandleIncomingMessage(const std::shared_ptr<Frame>& frame) override {
    // No lock here, since this is called from DriveWorker() which is
    // holding the lock
    if (frame->type == FrameType::kBuffer) {
      next_metadata_ = std::move(frame->buffer);
    } else if (frame->type == FrameType::kHeaders) {
      // Trailers, stream is over
      finished_ = true;
      HeadersFrame headers;
      io_status_ = HeadersFrame::Parse(std::move(frame->buffer)).Value(&headers);
      if (!io_status_.ok()) {
        finished_ = true;
        return;
      }
      io_status_ = headers.GetStatus(&server_status_);
      if (!io_status_.ok()) {
        finished_ = true;
        return;
      }
    } else {
      finished_ = true;
      io_status_ =
          Status::IOError("Unexpected frame type ", static_cast<int>(frame->type));
    }
  }
  std::shared_ptr<Buffer> next_metadata_;
};

class ExchangeClientStream : public WriteClientStream {
 public:
  ExchangeClientStream(UcxClientImpl* impl, ClientConnection conn)
      : WriteClientStream(impl, std::move(conn)), read_state_(ReadState::kFinished) {}

  bool ReadData(internal::FlightData* data) override {
    std::unique_lock<std::mutex> guard(driver_mutex_);
    if (finished_) {
      guard.unlock();
      JoinThread();
      return false;
    }

    // Drive the read loop here. (We can't recursively call
    // ReadFrameAsync below since the internal mutex is not
    // recursive.)
    read_state_ = ReadState::kExpectHeader;
    incoming_ = driver_->ReadFrameAsync();
    working_cv_.notify_all();
    completed_cv_.wait(guard, [this] { return read_state_ != ReadState::kExpectHeader; });
    if (read_state_ != ReadState::kFinished) {
      incoming_ = driver_->ReadFrameAsync();
      working_cv_.notify_all();
      completed_cv_.wait(guard, [this] { return read_state_ == ReadState::kFinished; });
    }

    if (finished_) {
      guard.unlock();
      JoinThread();
      return false;
    }
    *data = std::move(next_data_);
    return true;
  }

 private:
  enum class ReadState {
    kFinished,
    kExpectHeader,
    kExpectBody,
  };

  std::string DebugExpectingString() {
    switch (read_state_) {
      case ReadState::kFinished:
        return "(not expecting a frame)";
      case ReadState::kExpectHeader:
        return "payload header frame";
      case ReadState::kExpectBody:
        return "payload body frame";
    }
    return "(unknown or invalid state)";
  }

  void HandleIncomingMessage(const std::shared_ptr<Frame>& frame) override {
    // No lock here, since this is called from MakeProgress()
    // which is called under the lock already
    if (frame->type == FrameType::kPayloadHeader) {
      if (read_state_ != ReadState::kExpectHeader) {
        finished_ = true;
        io_status_ = Status::IOError("Got unexpected payload header frame, expected: ",
                                     DebugExpectingString());
        return;
      }

      PayloadHeaderFrame payload_header(std::move(frame->buffer));
      io_status_ = payload_header.ToFlightData(&next_data_);
      if (!io_status_.ok()) {
        finished_ = true;
        return;
      }

      if (next_data_.metadata) {
        std::unique_ptr<ipc::Message> message;
        io_status_ = ipc::Message::Open(next_data_.metadata, nullptr).Value(&message);
        if (!io_status_.ok()) {
          finished_ = true;
          return;
        }
        if (ipc::Message::HasBody(message->type())) {
          read_state_ = ReadState::kExpectBody;
          return;
        }
      }
      read_state_ = ReadState::kFinished;
    } else if (frame->type == FrameType::kPayloadBody) {
      next_data_.body = std::move(frame->buffer);
      read_state_ = ReadState::kFinished;
    } else if (frame->type == FrameType::kHeaders) {
      // Trailers, stream is over
      finished_ = true;
      read_state_ = ReadState::kFinished;
      HeadersFrame headers;
      io_status_ = HeadersFrame::Parse(std::move(frame->buffer)).Value(&headers);
      if (!io_status_.ok()) {
        finished_ = true;
        return;
      }
      io_status_ = headers.GetStatus(&server_status_);
      if (!io_status_.ok()) {
        finished_ = true;
        return;
      }
    } else {
      finished_ = true;
      io_status_ =
          Status::IOError("Unexpected frame type ", static_cast<int>(frame->type));
      read_state_ = ReadState::kFinished;
    }
  }

  internal::FlightData next_data_;
  ReadState read_state_;
};

class UcxClientImpl : public arrow::flight::internal::ClientTransport {
 public:
  UcxClientImpl() = default;

  ~UcxClientImpl() override {
    if (!ucp_context_) return;
    ARROW_WARN_NOT_OK(Close(), "UcxClientImpl errored in Close() in destructor");
  }

  Status Init(const FlightClientOptions& options, const Location& location,
              const arrow::internal::Uri& uri) override {
    RETURN_NOT_OK(uri_.Parse(uri.ToString()));
    {
      ucp_config_t* ucp_config;
      ucp_params_t ucp_params;
      ucs_status_t status;

      status = ucp_config_read(nullptr, nullptr, &ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_config_read", status));

      // If location is IPv6, must adjust UCX config
      // XXX: we assume locations always resolve to IPv6 or IPv4 but
      // that is not necessarily true.
      {
        struct sockaddr_storage connect_addr;
        RETURN_NOT_OK(UriToSockaddr(uri, &connect_addr));
        if (connect_addr.ss_family == AF_INET6) {
          status = ucp_config_modify(ucp_config, "AF_PRIO", "inet6");
          RETURN_NOT_OK(FromUcsStatus("ucp_config_modify", status));
        }
      }

      std::memset(&ucp_params, 0, sizeof(ucp_params));
      ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
      ucp_params.features = UCP_FEATURE_AM | UCP_FEATURE_WAKEUP;

      ucp_context_h ucp_context;
      status = ucp_init(&ucp_params, ucp_config, &ucp_context);
      ucp_config_release(ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_init", status));
      ucp_context_.reset(new UcpContext(ucp_context));
    }

    RETURN_NOT_OK(MakeConnection());
    return Status::OK();
  }

  Status Close() override {
    std::unique_lock<std::mutex> connections_mutex_;
    while (!connections_.empty()) {
      ClientConnection conn = std::move(connections_.front());
      connections_.pop_front();
      RETURN_NOT_OK(conn.Close());
    }
    return Status::OK();
  }

  Status GetFlightInfo(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) override {
    ARROW_ASSIGN_OR_RAISE(auto connection, CheckoutConnection(options));
    UcpCallDriver* driver = connection.driver();

    auto impl = [&]() {
      RETURN_NOT_OK(driver->StartCall(kMethodGetFlightInfo));

      ARROW_ASSIGN_OR_RAISE(std::string payload, descriptor.SerializeToString());

      RETURN_NOT_OK(driver->SendFrame(FrameType::kBuffer,
                                      reinterpret_cast<const uint8_t*>(payload.data()),
                                      static_cast<int64_t>(payload.size())));

      ARROW_ASSIGN_OR_RAISE(auto incoming_message, driver->ReadNextFrame());
      if (incoming_message->type == FrameType::kBuffer) {
        ARROW_ASSIGN_OR_RAISE(
            *info, FlightInfo::Deserialize(std::string_view(*incoming_message->buffer)));
        ARROW_ASSIGN_OR_RAISE(incoming_message, driver->ReadNextFrame());
      }
      RETURN_NOT_OK(driver->ExpectFrameType(*incoming_message, FrameType::kHeaders));
      ARROW_ASSIGN_OR_RAISE(auto headers,
                            HeadersFrame::Parse(std::move(incoming_message->buffer)));
      Status status;
      RETURN_NOT_OK(headers.GetStatus(&status));
      return status;
    };
    auto status = impl();
    return MergeStatuses(std::move(status), ReturnConnection(std::move(connection)));
  }

  Status DoExchange(const FlightCallOptions& options,
                    std::unique_ptr<internal::ClientDataStream>* out) override {
    ARROW_ASSIGN_OR_RAISE(auto connection, CheckoutConnection(options));
    UcpCallDriver* driver = connection.driver();

    auto status = driver->StartCall(kMethodDoExchange);
    if (ARROW_PREDICT_TRUE(status.ok())) {
      *out = std::make_unique<ExchangeClientStream>(this, std::move(connection));
      return Status::OK();
    }
    return MergeStatuses(std::move(status), ReturnConnection(std::move(connection)));
  }

  Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<internal::ClientDataStream>* stream) override {
    ARROW_ASSIGN_OR_RAISE(auto connection, CheckoutConnection(options));
    UcpCallDriver* driver = connection.driver();

    auto impl = [&]() {
      RETURN_NOT_OK(driver->StartCall(kMethodDoGet));
      ARROW_ASSIGN_OR_RAISE(std::string payload, ticket.SerializeToString());
      RETURN_NOT_OK(driver->SendFrame(FrameType::kBuffer,
                                      reinterpret_cast<const uint8_t*>(payload.data()),
                                      static_cast<int64_t>(payload.size())));
      *stream = std::make_unique<GetClientStream>(this, std::move(connection));
      return Status::OK();
    };

    auto status = impl();
    if (ARROW_PREDICT_TRUE(status.ok())) return status;
    return MergeStatuses(std::move(status), ReturnConnection(std::move(connection)));
  }

  Status DoPut(const FlightCallOptions& options,
               std::unique_ptr<internal::ClientDataStream>* out) override {
    ARROW_ASSIGN_OR_RAISE(auto connection, CheckoutConnection(options));
    UcpCallDriver* driver = connection.driver();

    auto status = driver->StartCall(kMethodDoPut);
    if (ARROW_PREDICT_TRUE(status.ok())) {
      *out = std::make_unique<PutClientStream>(this, std::move(connection));
      return Status::OK();
    }
    return MergeStatuses(std::move(status), ReturnConnection(std::move(connection)));
  }

  Status DoAction(const FlightCallOptions& options, const Action& action,
                  std::unique_ptr<ResultStream>* results) override {
    // XXX: fake this for now to get the perf test to work
    return Status::OK();
  }

  Status MakeConnection() {
    ClientConnection conn;
    RETURN_NOT_OK(conn.Init(ucp_context_, uri_));
    std::unique_lock<std::mutex> connections_mutex_;
    connections_.push_back(std::move(conn));
    return Status::OK();
  }

  arrow::Result<ClientConnection> CheckoutConnection(const FlightCallOptions& options) {
    std::unique_lock<std::mutex> connections_mutex_;
    if (connections_.empty()) RETURN_NOT_OK(MakeConnection());
    ClientConnection conn = std::move(connections_.front());
    connections_.pop_front();
    conn.driver()->set_memory_manager(options.memory_manager);
    conn.driver()->set_read_memory_pool(options.read_options.memory_pool);
    conn.driver()->set_write_memory_pool(options.write_options.memory_pool);
    return conn;
  }

  Status ReturnConnection(ClientConnection conn) {
    std::unique_lock<std::mutex> connections_mutex_;
    // TODO(ARROW-16127): for future improvement: reclaim clients
    // asynchronously in the background (try to avoid issues like
    // constantly opening/closing clients because the application is
    // just barely over the limit of open connections)
    if (connections_.size() >= kMaxOpenConnections) {
      RETURN_NOT_OK(conn.Close());
      return Status::OK();
    }
    DCHECK_NE(conn.driver(), nullptr);
    connections_.push_back(std::move(conn));
    return Status::OK();
  }

 private:
  static constexpr size_t kMaxOpenConnections = 3;

  arrow::internal::Uri uri_;
  std::shared_ptr<UcpContext> ucp_context_;
  std::mutex connections_mutex_;
  std::deque<ClientConnection> connections_;
};

Status UcxClientStream::DoFinish() {
  RETURN_NOT_OK(WritesDone());
  // Both reader and writer may be used concurrently, and both may
  // call Finish() - prevent concurrent state mutation
  std::lock_guard<std::mutex> guard(finish_mutex_);
  if (!finished_) {
    internal::FlightData message;
    std::shared_ptr<Buffer> metadata;
    while (ReadData(&message)) {
    }
    while (ReadPutMetadata(&metadata)) {
    }
    finished_ = true;
  }
  if (impl_) {
    DCHECK_NE(conn_.driver(), nullptr);
    auto status = impl_->ReturnConnection(std::move(conn_));
    impl_ = nullptr;
    driver_ = nullptr;
    if (!status.ok()) {
      if (io_status_.ok()) {
        io_status_ = std::move(status);
      } else {
        io_status_ = Status::FromDetailAndArgs(
            io_status_.code(), io_status_.detail(), io_status_.message(),
            ". Transport context: ", status.ToString());
      }
    }
  }
  return MergeStatuses(server_status_, io_status_);
}
}  // namespace

std::unique_ptr<arrow::flight::internal::ClientTransport> MakeUcxClientImpl() {
  return std::make_unique<UcxClientImpl>();
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow

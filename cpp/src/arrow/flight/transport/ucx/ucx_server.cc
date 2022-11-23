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

#include "arrow/flight/transport/ucx/ucx_internal.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

#include <arpa/inet.h>
#include <ucp/api/ucp.h>

#include "arrow/buffer.h"
#include "arrow/flight/server.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/ucx/util_internal.h"
#include "arrow/flight/transport_server.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::ToChars;

namespace flight {
namespace transport {
namespace ucx {

// Send an error to the client and return OK.
// Statuses returned up to the main server loop trigger a kReset instead.
#define SERVER_RETURN_NOT_OK(driver, status)                                         \
  do {                                                                               \
    ::arrow::Status s = (status);                                                    \
    if (!s.ok()) {                                                                   \
      ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Make(s, {}));                \
      auto payload = std::move(headers).GetBuffer();                                 \
      RETURN_NOT_OK(                                                                 \
          driver->SendFrame(FrameType::kHeaders, payload->data(), payload->size())); \
      return ::arrow::Status::OK();                                                  \
    }                                                                                \
  } while (false)

#define FLIGHT_LOG(LEVEL) (ARROW_LOG(LEVEL) << "[server] ")
#define FLIGHT_LOG_PEER(LEVEL, PEER) \
  (ARROW_LOG(LEVEL) << "[server]"    \
                    << "[peer=" << (PEER) << "] ")

namespace {
class UcxServerCallContext : public flight::ServerCallContext {
 public:
  const std::string& peer_identity() const override { return peer_; }
  const std::string& peer() const override { return peer_; }
  ServerMiddleware* GetMiddleware(const std::string& key) const override {
    return nullptr;
  }
  bool is_cancelled() const override { return false; }

 private:
  std::string peer_;
};

class UcxServerStream : public internal::ServerDataStream {
 public:
  explicit UcxServerStream(UcpCallDriver* driver)
      : peer_(driver->peer()), driver_(driver), writes_done_(false) {}

  Status WritesDone() override {
    writes_done_ = true;
    return Status::OK();
  }

 protected:
  std::string peer_;
  UcpCallDriver* driver_;
  bool writes_done_;
};

class GetServerStream : public UcxServerStream {
 public:
  using UcxServerStream::UcxServerStream;

  arrow::Result<bool> WriteData(const FlightPayload& payload) override {
    if (writes_done_) return false;
    Future<> pending_send = driver_->SendFlightPayload(payload);
    while (!pending_send.is_finished()) {
      driver_->MakeProgress();
    }
    RETURN_NOT_OK(pending_send.status());
    return true;
  }
};

class PutServerStream : public UcxServerStream {
 public:
  explicit PutServerStream(UcpCallDriver* driver)
      : UcxServerStream(driver), finished_(false) {}

  bool ReadData(internal::FlightData* data) override {
    if (finished_) return false;

    bool success = true;
    auto status = ReadImpl(data).Value(&success);

    if (!status.ok() || !success) {
      finished_ = true;
      if (!status.ok()) {
        FLIGHT_LOG_PEER(WARNING, peer_) << "I/O error in DoPut: " << status.ToString();
        return false;
      }
    }
    return success;
  }

  Status WritePutMetadata(const Buffer& payload) override {
    if (finished_) return Status::OK();
    // Send synchronously (we don't control payload lifetime)
    return driver_->SendFrame(FrameType::kBuffer, payload.data(), payload.size());
  }

 private:
  ::arrow::Result<bool> ReadImpl(internal::FlightData* data) {
    ARROW_ASSIGN_OR_RAISE(auto frame, driver_->ReadNextFrame());
    if (frame->type == FrameType::kHeaders) {
      // Trailers, client is done writing
      return false;
    }
    RETURN_NOT_OK(driver_->ExpectFrameType(*frame, FrameType::kPayloadHeader));
    PayloadHeaderFrame payload_header(std::move(frame->buffer));
    RETURN_NOT_OK(payload_header.ToFlightData(data));

    if (data->metadata) {
      ARROW_ASSIGN_OR_RAISE(auto message, ipc::Message::Open(data->metadata, nullptr));

      if (ipc::Message::HasBody(message->type())) {
        ARROW_ASSIGN_OR_RAISE(frame, driver_->ReadNextFrame());
        RETURN_NOT_OK(driver_->ExpectFrameType(*frame, FrameType::kPayloadBody));
        data->body = std::move(frame->buffer);
      }
    }
    return true;
  }

  bool finished_;
};

class ExchangeServerStream : public PutServerStream {
 public:
  using PutServerStream::PutServerStream;

  arrow::Result<bool> WriteData(const FlightPayload& payload) override {
    if (writes_done_) return false;
    Future<> pending_send = driver_->SendFlightPayload(payload);
    while (!pending_send.is_finished()) {
      driver_->MakeProgress();
    }
    RETURN_NOT_OK(pending_send.status());
    return true;
  }
  Status WritePutMetadata(const Buffer& payload) override {
    return Status::NotImplemented("Not supported on this stream");
  }
};

class UcxServerImpl : public arrow::flight::internal::ServerTransport {
 public:
  using arrow::flight::internal::ServerTransport::ServerTransport;

  virtual ~UcxServerImpl() {
    if (listening_.load()) {
      ARROW_WARN_NOT_OK(Shutdown(), "Server did not shut down properly");
    }
  }

  Status Init(const FlightServerOptions& options,
              const arrow::internal::Uri& uri) override {
    const auto max_threads = std::max<uint32_t>(8, std::thread::hardware_concurrency());
    ARROW_ASSIGN_OR_RAISE(rpc_pool_, arrow::internal::ThreadPool::Make(max_threads));

    struct sockaddr_storage listen_addr;
    ARROW_ASSIGN_OR_RAISE(auto addrlen, UriToSockaddr(uri, &listen_addr));

    // Init UCX
    {
      ucp_config_t* ucp_config;
      ucp_params_t ucp_params;
      ucs_status_t status;

      status = ucp_config_read(nullptr, nullptr, &ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_config_read", status));

      // If location is IPv6, must adjust UCX config
      if (listen_addr.ss_family == AF_INET6) {
        status = ucp_config_modify(ucp_config, "AF_PRIO", "inet6");
        RETURN_NOT_OK(FromUcsStatus("ucp_config_modify", status));
      }

      // Allow application to override UCP config
      if (options.builder_hook) options.builder_hook(ucp_config);

      std::memset(&ucp_params, 0, sizeof(ucp_params));
      ucp_params.field_mask =
          UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
      ucp_params.features = UCP_FEATURE_AM | UCP_FEATURE_WAKEUP;
      ucp_params.mt_workers_shared = UCS_THREAD_MODE_MULTI;

      ucp_context_h ucp_context;
      status = ucp_init(&ucp_params, ucp_config, &ucp_context);
      ucp_config_release(ucp_config);
      RETURN_NOT_OK(FromUcsStatus("ucp_init", status));
      ucp_context_.reset(new UcpContext(ucp_context));
    }

    {
      // Create one worker to listen for incoming connections.
      ucp_worker_params_t worker_params;
      ucs_status_t status;

      std::memset(&worker_params, 0, sizeof(worker_params));
      worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
      worker_params.thread_mode = UCS_THREAD_MODE_MULTI;
      ucp_worker_h worker;
      status = ucp_worker_create(ucp_context_->get(), &worker_params, &worker);
      RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));
      worker_conn_.reset(new UcpWorker(ucp_context_, worker));
    }

    // Start listening for connections.
    {
      ucp_listener_params_t params;
      ucs_status_t status;

      params.field_mask =
          UCP_LISTENER_PARAM_FIELD_SOCK_ADDR | UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
      params.sockaddr.addr = reinterpret_cast<const sockaddr*>(&listen_addr);
      params.sockaddr.addrlen = addrlen;
      params.conn_handler.cb = HandleIncomingConnection;
      params.conn_handler.arg = this;

      status = ucp_listener_create(worker_conn_->get(), &params, &listener_);
      RETURN_NOT_OK(FromUcsStatus("ucp_listener_create", status));

      // Get the real address/port
      ucp_listener_attr_t attr;
      attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
      status = ucp_listener_query(listener_, &attr);
      RETURN_NOT_OK(FromUcsStatus("ucp_listener_query", status));

      std::string raw_uri = "ucx://";
      if (uri.host().find(':') != std::string::npos) {
        // IPv6 host
        raw_uri += '[';
        raw_uri += uri.host();
        raw_uri += ']';
      } else {
        raw_uri += uri.host();
      }
      raw_uri += ":";
      raw_uri +=
          ToChars(ntohs(reinterpret_cast<const sockaddr_in*>(&attr.sockaddr)->sin_port));
      std::string listen_str;
      ARROW_UNUSED(SockaddrToString(attr.sockaddr).Value(&listen_str));
      FLIGHT_LOG(DEBUG) << "Listening on " << listen_str;
      ARROW_ASSIGN_OR_RAISE(location_, Location::Parse(raw_uri));
    }

    {
      listening_.store(true);
      std::thread listener_thread(&UcxServerImpl::DriveConnections, this);
      listener_thread_.swap(listener_thread);
    }

    return Status::OK();
  }

  Status Shutdown() override {
    if (!listening_.load()) return Status::OK();
    Status status;

    // Wait for current RPCs to finish
    listening_.store(false);
    // Unstick the listener thread from ucp_worker_wait
    RETURN_NOT_OK(
        FromUcsStatus("ucp_worker_signal", ucp_worker_signal(worker_conn_->get())));
    status &= Wait();

    {
      // Reject all pending connections
      std::unique_lock<std::mutex> guard(pending_connections_mutex_);
      while (!pending_connections_.empty()) {
        status &=
            FromUcsStatus("ucp_listener_reject",
                          ucp_listener_reject(listener_, pending_connections_.front()));
        pending_connections_.pop();
      }
      ucp_listener_destroy(listener_);
      worker_conn_.reset();
    }

    status &= rpc_pool_->Shutdown();
    rpc_pool_.reset();

    ucp_context_.reset();
    return status;
  }

  Status Shutdown(const std::chrono::system_clock::time_point& deadline) override {
    // TODO(ARROW-16125): implement shutdown with deadline
    return Shutdown();
  }

  Status Wait() override {
    std::lock_guard<std::mutex> guard(join_mutex_);
    try {
      listener_thread_.join();
    } catch (const std::system_error& e) {
      if (e.code() != std::errc::invalid_argument) {
        return Status::UnknownError("Could not Wait(): ", e.what());
      }
      // Else, server wasn't running anyways
    }
    return Status::OK();
  }

  Location location() const override { return location_; }

 private:
  struct ClientWorker {
    std::shared_ptr<UcpWorker> worker;
    std::unique_ptr<UcpCallDriver> driver;
  };

  Status SendStatus(UcpCallDriver* driver, const Status& status) {
    ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Make(status, {}));
    auto payload = std::move(headers).GetBuffer();
    RETURN_NOT_OK(
        driver->SendFrame(FrameType::kHeaders, payload->data(), payload->size()));
    return Status::OK();
  }

  Status HandleGetFlightInfo(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ARROW_ASSIGN_OR_RAISE(auto frame, driver->ReadNextFrame());
    SERVER_RETURN_NOT_OK(driver, driver->ExpectFrameType(*frame, FrameType::kBuffer));
    FlightDescriptor descriptor;
    SERVER_RETURN_NOT_OK(driver,
                         FlightDescriptor::Deserialize(std::string_view(*frame->buffer))
                             .Value(&descriptor));

    std::unique_ptr<FlightInfo> info;
    std::string response;
    SERVER_RETURN_NOT_OK(driver, base_->GetFlightInfo(context, descriptor, &info));
    SERVER_RETURN_NOT_OK(driver, info->SerializeToString().Value(&response));
    RETURN_NOT_OK(driver->SendFrame(FrameType::kBuffer,
                                    reinterpret_cast<const uint8_t*>(response.data()),
                                    static_cast<int64_t>(response.size())));
    RETURN_NOT_OK(SendStatus(driver, Status::OK()));
    return Status::OK();
  }

  Status HandleDoGet(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ARROW_ASSIGN_OR_RAISE(auto frame, driver->ReadNextFrame());
    SERVER_RETURN_NOT_OK(driver, driver->ExpectFrameType(*frame, FrameType::kBuffer));
    Ticket ticket;
    SERVER_RETURN_NOT_OK(driver, Ticket::Deserialize(frame->view()).Value(&ticket));

    GetServerStream stream(driver);
    auto status = DoGet(context, std::move(ticket), &stream);
    RETURN_NOT_OK(SendStatus(driver, status));
    return Status::OK();
  }

  Status HandleDoPut(UcpCallDriver* driver) {
    UcxServerCallContext context;

    PutServerStream stream(driver);
    auto status = DoPut(context, &stream);
    RETURN_NOT_OK(SendStatus(driver, status));
    // Must drain any unread messages, or the next call will get confused
    internal::FlightData ignored;
    while (stream.ReadData(&ignored)) {
    }
    return Status::OK();
  }

  Status HandleDoExchange(UcpCallDriver* driver) {
    UcxServerCallContext context;

    ExchangeServerStream stream(driver);
    auto status = DoExchange(context, &stream);
    RETURN_NOT_OK(SendStatus(driver, status));
    // Must drain any unread messages, or the next call will get confused
    internal::FlightData ignored;
    while (stream.ReadData(&ignored)) {
    }
    return Status::OK();
  }

  Status HandleOneCall(UcpCallDriver* driver, Frame* frame) {
    SERVER_RETURN_NOT_OK(driver, driver->ExpectFrameType(*frame, FrameType::kHeaders));
    ARROW_ASSIGN_OR_RAISE(auto headers, HeadersFrame::Parse(std::move(frame->buffer)));
    ARROW_ASSIGN_OR_RAISE(auto method, headers.Get(":method:"));
    if (method == kMethodGetFlightInfo) {
      return HandleGetFlightInfo(driver);
    } else if (method == kMethodDoExchange) {
      return HandleDoExchange(driver);
    } else if (method == kMethodDoGet) {
      return HandleDoGet(driver);
    } else if (method == kMethodDoPut) {
      return HandleDoPut(driver);
    }
    RETURN_NOT_OK(SendStatus(driver, Status::NotImplemented(method)));
    return Status::OK();
  }

  void WorkerLoop(ucp_conn_request_h request) {
    std::string peer = "unknown:" + ToChars(counter_++);
    {
      ucp_conn_request_attr_t request_attr;
      std::memset(&request_attr, 0, sizeof(request_attr));
      request_attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
      if (ucp_conn_request_query(request, &request_attr) == UCS_OK) {
        ARROW_UNUSED(SockaddrToString(request_attr.client_address).Value(&peer));
      }
    }
    FLIGHT_LOG_PEER(DEBUG, peer) << "Received connection request";

    auto maybe_worker = CreateWorker();
    if (!maybe_worker.ok()) {
      FLIGHT_LOG_PEER(WARNING, peer)
          << "Failed to create worker" << maybe_worker.status().ToString();
      auto status = ucp_listener_reject(listener_, request);
      if (status != UCS_OK) {
        FLIGHT_LOG_PEER(WARNING, peer)
            << FromUcsStatus("ucp_listener_reject", status).ToString();
      }
      return;
    }
    auto worker = maybe_worker.MoveValueUnsafe();

    // Create an endpoint to the client, using the data worker
    {
      ucs_status_t status;
      ucp_ep_params_t params;
      std::memset(&params, 0, sizeof(params));
      params.field_mask = UCP_EP_PARAM_FIELD_CONN_REQUEST;
      params.conn_request = request;

      ucp_ep_h client_endpoint;

      status = ucp_ep_create(worker->worker->get(), &params, &client_endpoint);
      if (status != UCS_OK) {
        FLIGHT_LOG_PEER(WARNING, peer)
            << "Failed to create endpoint: "
            << FromUcsStatus("ucp_ep_create", status).ToString();
        return;
      }
      worker->driver.reset(new UcpCallDriver(worker->worker, client_endpoint));
      worker->driver->set_memory_manager(memory_manager_);
      peer = worker->driver->peer();
    }

    while (listening_.load()) {
      auto maybe_frame = worker->driver->ReadNextFrame();
      if (!maybe_frame.ok()) {
        if (!maybe_frame.status().IsCancelled()) {
          FLIGHT_LOG_PEER(WARNING, peer)
              << "Failed to read next message: " << maybe_frame.status().ToString();
        }
        break;
      }

      auto status = HandleOneCall(worker->driver.get(), maybe_frame->get());
      if (!status.ok()) {
        FLIGHT_LOG_PEER(WARNING, peer) << "Call failed: " << status.ToString();
        break;
      }
    }

    // Clean up
    auto status = worker->driver->Close();
    if (!status.ok()) {
      FLIGHT_LOG_PEER(WARNING, peer) << "Failed to close worker: " << status.ToString();
    }
    worker->worker.reset();
    FLIGHT_LOG_PEER(DEBUG, peer) << "Disconnected";
  }

  void DriveConnections() {
    while (listening_.load()) {
      while (ucp_worker_progress(worker_conn_->get())) {
      }
      {
        // Check for connect requests in queue
        std::unique_lock<std::mutex> guard(pending_connections_mutex_);
        while (!pending_connections_.empty()) {
          ucp_conn_request_h request = pending_connections_.front();
          pending_connections_.pop();

          auto submitted = rpc_pool_->Submit([this, request]() { WorkerLoop(request); });
          ARROW_WARN_NOT_OK(submitted.status(), "Failed to submit task to handle client");
        }
      }

      // Check listening_ in case we're shutting down. It is possible
      // that Shutdown() was called while we were in
      // ucp_worker_progress above, in which case if we don't check
      // listening_ here, we'll enter ucp_worker_wait and get stuck.
      if (!listening_.load()) break;
      auto status = ucp_worker_wait(worker_conn_->get());
      if (status != UCS_OK) {
        FLIGHT_LOG(WARNING) << FromUcsStatus("ucp_worker_wait", status).ToString();
      }
    }
  }

  void EnqueueClient(ucp_conn_request_h connection_request) {
    std::unique_lock<std::mutex> guard(pending_connections_mutex_);
    pending_connections_.push(connection_request);
    guard.unlock();
  }

  arrow::Result<std::shared_ptr<ClientWorker>> CreateWorker() {
    auto worker = std::make_shared<ClientWorker>();

    ucp_worker_params_t worker_params;
    std::memset(&worker_params, 0, sizeof(worker_params));
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

    ucp_worker_h ucp_worker;
    auto status = ucp_worker_create(ucp_context_->get(), &worker_params, &ucp_worker);
    RETURN_NOT_OK(FromUcsStatus("ucp_worker_create", status));
    worker->worker.reset(new UcpWorker(ucp_context_, ucp_worker));

    // Set up Active Message (AM) handler
    ucp_am_handler_param_t handler_params;
    std::memset(&handler_params, 0, sizeof(handler_params));
    handler_params.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                                UCP_AM_HANDLER_PARAM_FIELD_CB |
                                UCP_AM_HANDLER_PARAM_FIELD_ARG;
    handler_params.id = kUcpAmHandlerId;
    handler_params.cb = HandleIncomingActiveMessage;
    handler_params.arg = worker.get();

    status = ucp_worker_set_am_recv_handler(worker->worker->get(), &handler_params);
    RETURN_NOT_OK(FromUcsStatus("ucp_worker_set_am_recv_handler", status));
    return worker;
  }

  // Callback handler. A new client has connected to the server.
  static void HandleIncomingConnection(ucp_conn_request_h connection_request,
                                       void* data) {
    UcxServerImpl* server = reinterpret_cast<UcxServerImpl*>(data);
    // TODO(ARROW-16124): enable shedding load above some threshold
    // (which is a pitfall with gRPC/Java)
    server->EnqueueClient(connection_request);
  }

  static ucs_status_t HandleIncomingActiveMessage(void* self, const void* header,
                                                  size_t header_length, void* data,
                                                  size_t data_length,
                                                  const ucp_am_recv_param_t* param) {
    ClientWorker* worker = reinterpret_cast<ClientWorker*>(self);
    DCHECK(worker->driver);
    return worker->driver->RecvActiveMessage(header, header_length, data, data_length,
                                             param);
  }

  std::shared_ptr<UcpContext> ucp_context_;
  // Listen for and handle incoming connections
  std::shared_ptr<UcpWorker> worker_conn_;
  ucp_listener_h listener_;
  Location location_;

  // Counter for identifying peers when UCX doesn't give us a way
  std::atomic<size_t> counter_;

  std::shared_ptr<arrow::internal::ThreadPool> rpc_pool_;
  std::atomic<bool> listening_;
  std::thread listener_thread_;
  // std::thread::join cannot be called concurrently
  std::mutex join_mutex_;

  std::mutex pending_connections_mutex_;
  std::queue<ucp_conn_request_h> pending_connections_;
};
}  // namespace

std::unique_ptr<arrow::flight::internal::ServerTransport> MakeUcxServerImpl(
    FlightServerBase* base, std::shared_ptr<MemoryManager> memory_manager) {
  return std::make_unique<UcxServerImpl>(base, memory_manager);
}

#undef SERVER_RETURN_NOT_OK
#undef FLIGHT_LOG
#undef FLIGHT_LOG_PEER

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow

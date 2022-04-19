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

// Common implementation of UCX communication primitives.

#pragma once

#include <array>
#include <string>
#include <utility>
#include <vector>

#include <ucp/api/ucp.h>

#include "arrow/buffer.h"
#include "arrow/flight/server.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/ucx/util_internal.h"
#include "arrow/flight/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

//------------------------------------------------------------
// Protocol Constants

static constexpr char kMethodDoExchange[] = "DoExchange";
static constexpr char kMethodDoGet[] = "DoGet";
static constexpr char kMethodDoPut[] = "DoPut";
static constexpr char kMethodGetFlightInfo[] = "GetFlightInfo";

/// The header encoding the transport status.
static constexpr char kHeaderStatus[] = "flight-status";
/// The header encoding the transport status.
static constexpr char kHeaderMessage[] = "flight-message";
/// The header encoding the C++ status.
static constexpr char kHeaderStatusCode[] = "flight-status-code";
/// The header encoding the C++ status message.
static constexpr char kHeaderStatusMessage[] = "flight-status-message";
/// The header encoding the C++ status detail message.
static constexpr char kHeaderStatusDetail[] = "flight-status-detail";
/// The header encoding the C++ status detail binary data.
static constexpr char kHeaderStatusDetailBin[] = "flight-status-detail-bin";

//------------------------------------------------------------
// UCX Helpers

/// \brief A wrapper around a ucp_context_h.
///
/// Used so that multiple resources can share ownership of the
/// context. UCX has zero-copy optimizations where an application can
/// directly use a UCX buffer, but the lifetime of such buffers is
/// tied to the UCX context and worker, so ownership needs to be
/// preserved.
class UcpContext final {
 public:
  UcpContext() : ucp_context_(nullptr) {}
  explicit UcpContext(ucp_context_h context) : ucp_context_(context) {}
  ~UcpContext() {
    if (ucp_context_) ucp_cleanup(ucp_context_);
    ucp_context_ = nullptr;
  }
  ucp_context_h get() const {
    DCHECK(ucp_context_);
    return ucp_context_;
  }

 private:
  ucp_context_h ucp_context_;
};

/// \brief A wrapper around a ucp_worker_h.
class UcpWorker final {
 public:
  UcpWorker() : ucp_worker_(nullptr) {}
  UcpWorker(std::shared_ptr<UcpContext> context, ucp_worker_h worker)
      : ucp_context_(std::move(context)), ucp_worker_(worker) {}
  ~UcpWorker() {
    if (ucp_worker_) ucp_worker_destroy(ucp_worker_);
    ucp_worker_ = nullptr;
  }
  ucp_worker_h get() const {
    DCHECK(ucp_worker_);
    return ucp_worker_;
  }
  const UcpContext& context() const { return *ucp_context_; }

 private:
  std::shared_ptr<UcpContext> ucp_context_;
  ucp_worker_h ucp_worker_;
};

//------------------------------------------------------------
// Message Framing

/// \brief The message type.
enum class FrameType : uint8_t {
  /// Key-value headers. Sent at the beginning (client->server) and
  /// end (server->client) of a call. Also, for client-streaming calls
  /// (e.g. DoPut), the client should send a headers frame to signal
  /// end-of-stream.
  kHeaders = 0,
  /// Binary blob, does not contain Arrow data.
  kBuffer,
  /// Binary blob. Contains IPC metadata, app metadata.
  kPayloadHeader,
  /// Binary blob. Contains IPC body. Body is sent separately since it
  /// may use a different memory type.
  kPayloadBody,
  /// Ask server to disconnect (to avoid client/server waiting on each
  /// other and getting stuck).
  kDisconnect,
  /// Keep at end.
  kMaxFrameType = kDisconnect,
};

/// \brief The header of a message frame. Used when sending only.
///
/// A frame is expected to be sent over UCP Active Messages and
/// consists of a header (of kFrameHeaderBytes bytes) and a body.
///
/// The header is as follows:
/// +-------+---------------------------------+
/// | Bytes | Function                        |
/// +=======+=================================+
/// | 0     | Version tag (see kFrameVersion) |
/// | 1     | Frame type (see FrameType)      |
/// | 2-3   | Unused, reserved                |
/// | 4-7   | Frame counter (big-endian)      |
/// | 8-11  | Body size (big-endian)          |
/// +-------+---------------------------------+
///
/// The frame counter lets the receiver ensure messages are processed
/// in-order. (The message receive callback may use
/// ucp_am_recv_data_nbx which is asynchronous.)
///
/// The body size reports the expected message size (UCX chokes on
/// zero-size payloads which we occasionally want to send, so the size
/// field in the header lets us know when a payload was meant to be
/// empty).
struct FrameHeader {
  /// \brief The size of a frame header.
  static constexpr size_t kFrameHeaderBytes = 12;
  /// \brief The expected version tag in the header.
  static constexpr uint8_t kFrameVersion = 0x01;

  FrameHeader() = default;
  /// \brief Initialize the frame header.
  Status Set(FrameType frame_type, uint32_t counter, int64_t body_size);
  void* data() const { return header.data(); }
  size_t size() const { return kFrameHeaderBytes; }

  // mutable since UCX expects void* not const void*
  mutable std::array<uint8_t, kFrameHeaderBytes> header = {0};
};

/// \brief A single message received via UCX. Used when receiving only.
struct Frame {
  /// \brief The message type.
  FrameType type;
  /// \brief The message length.
  uint32_t size;
  /// \brief An incrementing message counter (may wrap over).
  uint32_t counter;
  /// \brief The message contents.
  std::unique_ptr<Buffer> buffer;

  Frame() = default;
  Frame(FrameType type_, uint32_t size_, uint32_t counter_,
        std::unique_ptr<Buffer> buffer_)
      : type(type_), size(size_), counter(counter_), buffer(std::move(buffer_)) {}

  util::string_view view() const {
    return util::string_view(reinterpret_cast<const char*>(buffer->data()), size);
  }

  /// \brief Parse a UCX active message header. This will not
  ///   initialize the buffer field.
  static arrow::Result<std::shared_ptr<Frame>> ParseHeader(const void* header,
                                                           size_t header_length);
};

/// \brief The active message handler callback ID.
static constexpr uint32_t kUcpAmHandlerId = 0x1024;

/// \brief A collection of key-value headers.
///
/// This should be stored in a frame of type kHeaders.
///
/// Format:
/// +-------+----------------------------------+
/// | Bytes | Contents                         |
/// +=======+==================================+
/// | 0-4   | # of headers (big-endian)        |
/// | 4-8   | Header key length (big-endian)   |
/// | 2-3   | Header value length (big-endian) |
/// | (...) | Header key                       |
/// | (...) | Header value                     |
/// | (...) | (repeat from row 2)              |
/// +-------+----------------------------------+
class HeadersFrame {
 public:
  /// \brief Get a header value (or an error if it was not found)
  arrow::Result<util::string_view> Get(const std::string& key);
  /// \brief Extract the server-sent status.
  Status GetStatus(Status* out);
  /// \brief Parse the headers from the buffer.
  static arrow::Result<HeadersFrame> Parse(std::unique_ptr<Buffer> buffer);
  /// \brief Create a new frame with the given headers.
  static arrow::Result<HeadersFrame> Make(
      const std::vector<std::pair<std::string, std::string>>& headers);
  /// \brief Create a new frame with the given headers and the given status.
  static arrow::Result<HeadersFrame> Make(
      const Status& status,
      const std::vector<std::pair<std::string, std::string>>& headers);

  /// \brief Take ownership of the underlying buffer.
  std::unique_ptr<Buffer> GetBuffer() && { return std::move(buffer_); }

 private:
  std::unique_ptr<Buffer> buffer_;
  std::vector<std::pair<util::string_view, util::string_view>> headers_;
};

/// \brief A representation of a kPayloadHeader frame (i.e. all of the
///   metadata in a FlightPayload/FlightData).
///
/// Data messages are sent in two parts: one containing all metadata
/// (the Flatbuffers header, FlightDescriptor, and app_metadata
/// fields) and one containing the actual data. This was done to avoid
/// having to concatenate these fields with the data itself (in the
/// cases where we are not using IOV).
///
/// Format:
/// +--------+----------------------------------+
/// | Bytes  | Contents                         |
/// +========+==================================+
/// | 0-4    | Descriptor length (big-endian)   |
/// | 4..a   | Descriptor bytes                 |
/// | a-a+4  | app_metadata length (big-endian) |
/// | a+4..b | app_metadata bytes               |
/// | b-b+4  | ipc_metadata length (big-endian) |
/// | b+4..c | ipc_metadata bytes               |
/// +--------+----------------------------------+
///
/// If a field is not present, its length is still there, but is set
/// to UINT32_MAX.
class PayloadHeaderFrame {
 public:
  explicit PayloadHeaderFrame(std::unique_ptr<Buffer> buffer)
      : buffer_(std::move(buffer)) {}
  /// \brief Unpack the internal buffer into a FlightData.
  Status ToFlightData(internal::FlightData* data);
  /// \brief Pack a payload into the internal buffer.
  static arrow::Result<PayloadHeaderFrame> Make(const FlightPayload& payload,
                                                MemoryPool* memory_pool);
  const uint8_t* data() const { return buffer_->data(); }
  int64_t size() const { return buffer_->size(); }

 private:
  std::unique_ptr<Buffer> buffer_;
};

/// \brief Manage the state of a UCX connection.
class UcpCallDriver {
 public:
  UcpCallDriver(std::shared_ptr<UcpWorker> worker, ucp_ep_h endpoint);

  UcpCallDriver(const UcpCallDriver&) = delete;
  UcpCallDriver(UcpCallDriver&&);
  void operator=(const UcpCallDriver&) = delete;
  UcpCallDriver& operator=(UcpCallDriver&&);

  ~UcpCallDriver();

  /// \brief Start a call by sending a headers frame. Client side only.
  ///
  /// \param[in] method The RPC method.
  Status StartCall(const std::string& method);

  /// \brief Synchronously send a generic message with binary payload.
  Status SendFrame(FrameType frame_type, const uint8_t* data, const int64_t size);
  /// \brief Asynchronously send a generic message with binary payload.
  ///
  /// The UCP driver must be manually polled (call MakeProgress()).
  Future<> SendFrameAsync(FrameType frame_type, std::unique_ptr<Buffer> buffer);
  /// \brief Asynchronously send a data message.
  ///
  /// The UCP driver must be manually polled (call MakeProgress()).
  Future<> SendFlightPayload(const FlightPayload& payload);

  /// \brief Synchronously read the next frame.
  arrow::Result<std::shared_ptr<Frame>> ReadNextFrame();
  /// \brief Asynchronously read the next frame.
  ///
  /// The UCP driver must be manually polled (call MakeProgress()).
  Future<std::shared_ptr<Frame>> ReadFrameAsync();

  /// \brief Validate that the frame is of the given type.
  Status ExpectFrameType(const Frame& frame, FrameType type);

  /// \brief Disconnect the other side of the connection. Note, this
  ///   can cause deadlock.
  Status Close();

  /// \brief Synchronously make progress (to adapt async to sync APIs)
  void MakeProgress();

  /// \brief Get the associated memory manager.
  const std::shared_ptr<MemoryManager>& memory_manager() const;
  /// \brief Set the associated memory manager.
  void set_memory_manager(std::shared_ptr<MemoryManager> memory_manager);
  /// \brief Set memory pool for scratch space used during reading.
  void set_read_memory_pool(MemoryPool* memory_pool);
  /// \brief Set memory pool for scratch space used during writing.
  void set_write_memory_pool(MemoryPool* memory_pool);
  /// \brief Get a debug string naming the peer.
  const std::string& peer() const;

  /// \brief Process an incoming active message. This will unblock the
  ///   corresponding call to ReadFrameAsync/ReadNextFrame.
  ucs_status_t RecvActiveMessage(const void* header, size_t header_length, void* data,
                                 const size_t data_length,
                                 const ucp_am_recv_param_t* param);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

ARROW_FLIGHT_EXPORT
std::unique_ptr<arrow::flight::internal::ClientTransport> MakeUcxClientImpl();

ARROW_FLIGHT_EXPORT
std::unique_ptr<arrow::flight::internal::ServerTransport> MakeUcxServerImpl(
    FlightServerBase* base, std::shared_ptr<MemoryManager> memory_manager);

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow

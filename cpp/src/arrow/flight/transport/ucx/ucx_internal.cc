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

#include <array>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "arrow/buffer.h"
#include "arrow/flight/transport/ucx/util_internal.h"
#include "arrow/flight/types.h"
#include "arrow/util/base64.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::ToChars;

namespace flight {
namespace transport {
namespace ucx {

using internal::TransportStatus;
using internal::TransportStatusCode;

// Defines to test different implementation strategies
// Enable the CONTIG path for CPU-only data
// #define ARROW_FLIGHT_UCX_SEND_CONTIG
// Enable ucp_mem_map in IOV path
// #define ARROW_FLIGHT_UCX_SEND_IOV_MAP

constexpr char kHeaderMethod[] = ":method:";

namespace {
Status SizeToUInt32BytesBe(const int64_t in, uint8_t* out) {
  if (ARROW_PREDICT_FALSE(in < 0)) {
    return Status::Invalid("Length cannot be negative");
  } else if (ARROW_PREDICT_FALSE(
                 in > static_cast<int64_t>(std::numeric_limits<uint32_t>::max()))) {
    return Status::Invalid("Length cannot exceed uint32_t");
  }
  UInt32ToBytesBe(static_cast<uint32_t>(in), out);
  return Status::OK();
}
ucs_memory_type InferMemoryType(const Buffer& buffer) {
  if (!buffer.is_cpu()) {
    return UCS_MEMORY_TYPE_CUDA;
  }
  return UCS_MEMORY_TYPE_UNKNOWN;
}
void TryMapBuffer(ucp_context_h context, const void* buffer, const size_t size,
                  ucs_memory_type memory_type, ucp_mem_h* memh_p) {
  ucp_mem_map_params_t map_param;
  std::memset(&map_param, 0, sizeof(map_param));
  map_param.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                         UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                         UCP_MEM_MAP_PARAM_FIELD_MEMORY_TYPE;
  map_param.address = const_cast<void*>(buffer);
  map_param.length = size;
  map_param.memory_type = memory_type;
  auto ucs_status = ucp_mem_map(context, &map_param, memh_p);
  if (ucs_status != UCS_OK) {
    *memh_p = nullptr;
    ARROW_LOG(WARNING) << "Could not map memory: "
                       << FromUcsStatus("ucp_mem_map", ucs_status);
  }
}
void TryMapBuffer(ucp_context_h context, const Buffer& buffer, ucp_mem_h* memh_p) {
  TryMapBuffer(context, reinterpret_cast<void*>(buffer.address()),
               static_cast<size_t>(buffer.size()), InferMemoryType(buffer), memh_p);
}
void TryUnmapBuffer(ucp_context_h context, ucp_mem_h memh_p) {
  if (memh_p) {
    auto ucs_status = ucp_mem_unmap(context, memh_p);
    if (ucs_status != UCS_OK) {
      ARROW_LOG(WARNING) << "Could not unmap memory: "
                         << FromUcsStatus("ucp_mem_unmap", ucs_status);
    }
  }
}

/// \brief Wrapper around a UCX zero copy buffer (a host memory DATA
///   buffer).
///
/// Owns a reference to the associated worker to avoid undefined
/// behavior.
class UcxDataBuffer : public Buffer {
 public:
  explicit UcxDataBuffer(std::shared_ptr<UcpWorker> worker, void* data, size_t size)
      : Buffer(reinterpret_cast<uint8_t*>(data), static_cast<int64_t>(size)),
        worker_(std::move(worker)) {}

  ~UcxDataBuffer() {
    ucp_am_data_release(worker_->get(),
                        const_cast<void*>(reinterpret_cast<const void*>(data())));
  }

 private:
  std::shared_ptr<UcpWorker> worker_;
};
};  // namespace

constexpr size_t FrameHeader::kFrameHeaderBytes;
constexpr uint8_t FrameHeader::kFrameVersion;

Status FrameHeader::Set(FrameType frame_type, uint32_t counter, int64_t body_size) {
  header[0] = kFrameVersion;
  header[1] = static_cast<uint8_t>(frame_type);
  UInt32ToBytesBe(counter, header.data() + 4);
  RETURN_NOT_OK(SizeToUInt32BytesBe(body_size, header.data() + 8));
  return Status::OK();
}

arrow::Result<std::shared_ptr<Frame>> Frame::ParseHeader(const void* header,
                                                         size_t header_length) {
  if (header_length < FrameHeader::kFrameHeaderBytes) {
    return Status::IOError("Header is too short, must be at least ",
                           FrameHeader::kFrameHeaderBytes, " bytes, got ", header_length);
  }

  const uint8_t* frame_header = reinterpret_cast<const uint8_t*>(header);
  if (frame_header[0] != FrameHeader::kFrameVersion) {
    return Status::IOError("Expected frame version ",
                           static_cast<int>(FrameHeader::kFrameVersion), " but got ",
                           static_cast<int>(frame_header[0]));
  } else if (frame_header[1] > static_cast<uint8_t>(FrameType::kMaxFrameType)) {
    return Status::IOError("Unknown frame type ", static_cast<int>(frame_header[1]));
  }

  const FrameType frame_type = static_cast<FrameType>(frame_header[1]);
  const uint32_t frame_counter = BytesToUInt32Be(frame_header + 4);
  const uint32_t frame_size = BytesToUInt32Be(frame_header + 8);

  if (frame_type == FrameType::kDisconnect) {
    return Status::Cancelled("Client initiated disconnect");
  }

  return std::make_shared<Frame>(frame_type, frame_size, frame_counter, nullptr);
}

arrow::Result<HeadersFrame> HeadersFrame::Parse(std::unique_ptr<Buffer> buffer) {
  HeadersFrame result;
  const uint8_t* payload = buffer->data();
  const uint8_t* end = payload + buffer->size();
  if (ARROW_PREDICT_FALSE((end - payload) < 4)) {
    return Status::Invalid("Buffer underflow, expected number of headers");
  }
  const uint32_t num_headers = BytesToUInt32Be(payload);
  payload += 4;
  for (uint32_t i = 0; i < num_headers; i++) {
    if (ARROW_PREDICT_FALSE((end - payload) < 4)) {
      return Status::Invalid("Buffer underflow, expected length of key ", i + 1);
    }
    const uint32_t key_length = BytesToUInt32Be(payload);
    payload += 4;

    if (ARROW_PREDICT_FALSE((end - payload) < 4)) {
      return Status::Invalid("Buffer underflow, expected length of value ", i + 1);
    }
    const uint32_t value_length = BytesToUInt32Be(payload);
    payload += 4;

    if (ARROW_PREDICT_FALSE((end - payload) < key_length)) {
      return Status::Invalid("Buffer underflow, expected key ", i + 1, " to have length ",
                             key_length, ", but only ", (end - payload), " bytes remain");
    }
    const std::string_view key(reinterpret_cast<const char*>(payload), key_length);
    payload += key_length;

    if (ARROW_PREDICT_FALSE((end - payload) < value_length)) {
      return Status::Invalid("Buffer underflow, expected value ", i + 1,
                             " to have length ", value_length, ", but only ",
                             (end - payload), " bytes remain");
    }
    const std::string_view value(reinterpret_cast<const char*>(payload), value_length);
    payload += value_length;
    result.headers_.emplace_back(key, value);
  }

  result.buffer_ = std::move(buffer);
  return result;
}
arrow::Result<HeadersFrame> HeadersFrame::Make(
    const std::vector<std::pair<std::string, std::string>>& headers) {
  int32_t total_length = 4 /* # of headers */;
  for (const auto& header : headers) {
    total_length += 4 /* key length */ + 4 /* value length */ +
                    header.first.size() /* key */ + header.second.size();
  }

  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(total_length));
  uint8_t* payload = buffer->mutable_data();

  RETURN_NOT_OK(SizeToUInt32BytesBe(headers.size(), payload));
  payload += 4;
  for (const auto& header : headers) {
    RETURN_NOT_OK(SizeToUInt32BytesBe(header.first.size(), payload));
    payload += 4;
    RETURN_NOT_OK(SizeToUInt32BytesBe(header.second.size(), payload));
    payload += 4;
    std::memcpy(payload, header.first.data(), header.first.size());
    payload += header.first.size();
    std::memcpy(payload, header.second.data(), header.second.size());
    payload += header.second.size();
  }
  return Parse(std::move(buffer));
}
arrow::Result<HeadersFrame> HeadersFrame::Make(
    const Status& status,
    const std::vector<std::pair<std::string, std::string>>& headers) {
  auto all_headers = headers;

  TransportStatus transport_status = TransportStatus::FromStatus(status);
  all_headers.emplace_back(kHeaderStatus,
                           ToChars(static_cast<int32_t>(transport_status.code)));
  all_headers.emplace_back(kHeaderMessage, std::move(transport_status.message));
  all_headers.emplace_back(kHeaderStatusCode,
                           ToChars(static_cast<int32_t>(status.code())));
  all_headers.emplace_back(kHeaderStatusMessage, status.message());
  if (status.detail()) {
    all_headers.emplace_back(kHeaderStatusDetail, status.detail()->ToString());
    auto fsd = FlightStatusDetail::UnwrapStatus(status);
    if (fsd && !fsd->extra_info().empty()) {
      all_headers.emplace_back(kHeaderStatusDetailBin, fsd->extra_info());
    }
  }
  return Make(all_headers);
}

arrow::Result<std::string_view> HeadersFrame::Get(const std::string& key) {
  for (const auto& pair : headers_) {
    if (pair.first == key) return pair.second;
  }
  return Status::KeyError(key);
}

Status HeadersFrame::GetStatus(Status* out) {
  static const std::string kUnknownMessage = "Server did not send status message header";
  std::string_view code_str, message_str;
  auto status = Get(kHeaderStatus).Value(&code_str);
  if (!status.ok()) {
    return Status::KeyError("Server did not send status code header ", kHeaderStatusCode);
  }
  if (code_str == "0") {  // == ToChars(TransportStatusCode::kOk)
    *out = Status::OK();
    return Status::OK();
  }

  status = Get(kHeaderMessage).Value(&message_str);
  if (!status.ok()) message_str = kUnknownMessage;

  TransportStatus transport_status = TransportStatus::FromCodeStringAndMessage(
      std::string(code_str), std::string(message_str));
  if (transport_status.code == TransportStatusCode::kOk) {
    *out = Status::OK();
    return Status::OK();
  }
  *out = transport_status.ToStatus();

  std::string_view detail_str, bin_str;
  std::optional<std::string> message, detail_message, detail_bin;
  if (!Get(kHeaderStatusCode).Value(&code_str).ok()) {
    // No Arrow status sent, go with the transport status
    return Status::OK();
  }
  if (Get(kHeaderStatusMessage).Value(&message_str).ok()) {
    message = std::string(message_str);
  }
  if (Get(kHeaderStatusDetail).Value(&detail_str).ok()) {
    detail_message = std::string(detail_str);
  }
  if (Get(kHeaderStatusDetailBin).Value(&bin_str).ok()) {
    detail_bin = std::string(bin_str);
  }
  *out = internal::ReconstructStatus(std::string(code_str), *out, std::move(message),
                                     std::move(detail_message), std::move(detail_bin),
                                     FlightStatusDetail::UnwrapStatus(*out));
  return Status::OK();
}

namespace {
static constexpr uint32_t kMissingFieldSentinel = std::numeric_limits<uint32_t>::max();
static constexpr uint32_t kInt32Max =
    static_cast<uint32_t>(std::numeric_limits<int32_t>::max());
arrow::Result<uint32_t> PayloadHeaderFieldSize(const std::string& field,
                                               const std::shared_ptr<Buffer>& data,
                                               uint32_t* total_size) {
  if (!data) return kMissingFieldSentinel;
  if (data->size() > kInt32Max) {
    return Status::Invalid(field, " must be less than 2 GiB, was: ", data->size());
  }
  *total_size += static_cast<uint32_t>(data->size());
  // Check for underflow
  if (*total_size < 0) return Status::Invalid("Payload header must fit in a uint32_t");
  return static_cast<uint32_t>(data->size());
}
uint8_t* PackField(uint32_t size, const std::shared_ptr<Buffer>& data, uint8_t* out) {
  UInt32ToBytesBe(size, out);
  if (size != kMissingFieldSentinel) {
    std::memcpy(out + 4, data->data(), size);
    return out + 4 + size;
  } else {
    return out + 4;
  }
}
}  // namespace

arrow::Result<PayloadHeaderFrame> PayloadHeaderFrame::Make(const FlightPayload& payload,
                                                           MemoryPool* memory_pool) {
  // Assemble all non-data fields here. Presumably this is much less
  // than data size so we will pay the copy.

  // Structure per field: [4 byte length][data]. If a field is not
  // present, UINT32_MAX is used as the sentinel (since 0-sized fields
  // are acceptable)
  uint32_t header_size = 12;
  ARROW_ASSIGN_OR_RAISE(
      const uint32_t descriptor_size,
      PayloadHeaderFieldSize("descriptor", payload.descriptor, &header_size));
  ARROW_ASSIGN_OR_RAISE(
      const uint32_t app_metadata_size,
      PayloadHeaderFieldSize("app_metadata", payload.app_metadata, &header_size));
  ARROW_ASSIGN_OR_RAISE(
      const uint32_t ipc_metadata_size,
      PayloadHeaderFieldSize("ipc_message.metadata", payload.ipc_message.metadata,
                             &header_size));

  ARROW_ASSIGN_OR_RAISE(auto header_buffer, AllocateBuffer(header_size, memory_pool));
  uint8_t* payload_header = header_buffer->mutable_data();

  payload_header = PackField(descriptor_size, payload.descriptor, payload_header);
  payload_header = PackField(app_metadata_size, payload.app_metadata, payload_header);
  payload_header =
      PackField(ipc_metadata_size, payload.ipc_message.metadata, payload_header);

  return PayloadHeaderFrame(std::move(header_buffer));
}
Status PayloadHeaderFrame::ToFlightData(internal::FlightData* data) {
  std::shared_ptr<Buffer> buffer = std::move(buffer_);

  // Unpack the descriptor
  uint32_t offset = 0;
  uint32_t size = BytesToUInt32Be(buffer->data());
  offset += 4;
  if (size != kMissingFieldSentinel) {
    if (static_cast<int64_t>(offset + size) > buffer->size()) {
      return Status::Invalid("Buffer is too small: expected ", offset + size,
                             " bytes but have ", buffer->size());
    }
    std::string_view desc(reinterpret_cast<const char*>(buffer->data() + offset), size);
    data->descriptor.reset(new FlightDescriptor());
    ARROW_ASSIGN_OR_RAISE(*data->descriptor, FlightDescriptor::Deserialize(desc));
    offset += size;
  } else {
    data->descriptor = nullptr;
  }

  // Unpack app_metadata
  size = BytesToUInt32Be(buffer->data() + offset);
  offset += 4;
  // While we properly handle zero-size vs nullptr metadata here, gRPC
  // doesn't (Protobuf doesn't differentiate between the two)
  if (size != kMissingFieldSentinel) {
    if (static_cast<int64_t>(offset + size) > buffer->size()) {
      return Status::Invalid("Buffer is too small: expected ", offset + size,
                             " bytes but have ", buffer->size());
    }
    data->app_metadata = SliceBuffer(buffer, offset, size);
    offset += size;
  } else {
    data->app_metadata = nullptr;
  }

  // Unpack the IPC header
  size = BytesToUInt32Be(buffer->data() + offset);
  offset += 4;
  if (size != kMissingFieldSentinel) {
    if (static_cast<int64_t>(offset + size) > buffer->size()) {
      return Status::Invalid("Buffer is too small: expected ", offset + size,
                             " bytes but have ", buffer->size());
    }
    data->metadata = SliceBuffer(std::move(buffer), offset, size);
  } else {
    data->metadata = nullptr;
  }
  data->body = nullptr;
  return Status::OK();
}

// pImpl the driver since async methods require a stable address
class UcpCallDriver::Impl {
 public:
#if defined(ARROW_FLIGHT_UCX_SEND_CONTIG)
  constexpr static bool kEnableContigSend = true;
#else
  constexpr static bool kEnableContigSend = false;
#endif

  Impl(std::shared_ptr<UcpWorker> worker, ucp_ep_h endpoint)
      : padding_bytes_({0, 0, 0, 0, 0, 0, 0, 0}),
        worker_(std::move(worker)),
        endpoint_(endpoint),
        read_memory_pool_(default_memory_pool()),
        write_memory_pool_(default_memory_pool()),
        memory_manager_(CPUDevice::Instance()->default_memory_manager()),
        name_("(unknown remote)"),
        counter_(0) {
#if defined(ARROW_FLIGHT_UCX_SEND_IOV_MAP)
    TryMapBuffer(worker_->context().get(), padding_bytes_.data(), padding_bytes_.size(),
                 UCS_MEMORY_TYPE_HOST, &padding_memh_p_);
#endif

    ucp_ep_attr_t attrs;
    std::memset(&attrs, 0, sizeof(attrs));
    attrs.field_mask =
        UCP_EP_ATTR_FIELD_LOCAL_SOCKADDR | UCP_EP_ATTR_FIELD_REMOTE_SOCKADDR;
    if (ucp_ep_query(endpoint_, &attrs) == UCS_OK) {
      std::string local_addr, remote_addr;
      ARROW_UNUSED(SockaddrToString(attrs.local_sockaddr).Value(&local_addr));
      ARROW_UNUSED(SockaddrToString(attrs.remote_sockaddr).Value(&remote_addr));
      name_ = "local:" + local_addr + ";remote:" + remote_addr;
    }
  }

  ~Impl() {
#if defined(ARROW_FLIGHT_UCX_SEND_IOV_MAP)
    TryUnmapBuffer(worker_->context().get(), padding_memh_p_);
#endif
  }

  arrow::Result<std::shared_ptr<Frame>> ReadNextFrame() {
    auto fut = ReadFrameAsync();
    while (!fut.is_finished()) MakeProgress();
    RETURN_NOT_OK(fut.status());
    return fut.MoveResult();
  }

  Future<std::shared_ptr<Frame>> ReadFrameAsync() {
    RETURN_NOT_OK(CheckClosed());

    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (ARROW_PREDICT_FALSE(!status_.ok())) return status_;

    // Expected value of "counter" field in the frame header
    const uint32_t counter_value = next_counter_++;
    auto it = frames_.find(counter_value);
    if (it != frames_.end()) {
      // Message already delivered, return it
      Future<std::shared_ptr<Frame>> fut = it->second;
      frames_.erase(it);
      return fut;
    }
    // Message not yet delivered, insert a future and wait
    auto pair = frames_.insert({counter_value, Future<std::shared_ptr<Frame>>::Make()});
    DCHECK(pair.second);
    return pair.first->second;
  }

  Status SendFrame(FrameType frame_type, const uint8_t* data, const int64_t size) {
    RETURN_NOT_OK(CheckClosed());

    void* request = nullptr;
    ucp_request_param_t request_param;
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    request_param.flags = UCP_AM_SEND_FLAG_REPLY;

    // Send frame header
    FrameHeader header;
    RETURN_NOT_OK(header.Set(frame_type, counter_++, size));
    if (size == 0) {
      // UCX appears to crash on zero-byte payloads
      request = ucp_am_send_nbx(endpoint_, kUcpAmHandlerId, header.data(), header.size(),
                                padding_bytes_.data(),
                                /*size=*/1, &request_param);
    } else {
      request = ucp_am_send_nbx(endpoint_, kUcpAmHandlerId, header.data(), header.size(),
                                data, size, &request_param);
    }
    RETURN_NOT_OK(CompleteRequestBlocking("ucp_am_send_nbx", request));

    return Status::OK();
  }

  Future<> SendFrameAsync(FrameType frame_type, std::unique_ptr<Buffer> buffer) {
    RETURN_NOT_OK(CheckClosed());

    ucp_request_param_t request_param;
    std::memset(&request_param, 0, sizeof(request_param));
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE |
                                 UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_USER_DATA;
    request_param.cb.send = AmSendCallback;
    request_param.datatype = ucp_dt_make_contig(1);
    request_param.flags = UCP_AM_SEND_FLAG_REPLY;

    const int64_t size = buffer->size();
    if (size == 0) {
      // UCX appears to crash on zero-byte payloads
      ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(1, write_memory_pool_));
    }

    std::unique_ptr<PendingContigSend> pending_send(new PendingContigSend());
    RETURN_NOT_OK(pending_send->header.Set(frame_type, counter_++, size));
    pending_send->ipc_message = std::move(buffer);
    pending_send->driver = this;
    pending_send->completed = Future<>::Make();
    pending_send->memh_p = nullptr;

    request_param.user_data = pending_send.release();
    {
      auto* pending_send = reinterpret_cast<PendingContigSend*>(request_param.user_data);

      void* request = ucp_am_send_nbx(
          endpoint_, kUcpAmHandlerId, pending_send->header.data(),
          pending_send->header.size(),
          reinterpret_cast<void*>(pending_send->ipc_message->mutable_data()),
          static_cast<size_t>(pending_send->ipc_message->size()), &request_param);
      if (!request) {
        // Request completed immediately
        delete pending_send;
        return Status::OK();
      } else if (UCS_PTR_IS_ERR(request)) {
        delete pending_send;
        return FromUcsStatus("ucp_am_send_nbx", UCS_PTR_STATUS(request));
      }
      return pending_send->completed;
    }
  }

  Future<> SendFlightPayload(const FlightPayload& payload) {
    static const int64_t kMaxBatchSize = std::numeric_limits<int32_t>::max();
    RETURN_NOT_OK(CheckClosed());

    if (payload.ipc_message.body_length > kMaxBatchSize) {
      return Status::Invalid("Cannot send record batches exceeding 2GiB yet");
    }

    {
      ARROW_ASSIGN_OR_RAISE(auto frame,
                            PayloadHeaderFrame::Make(payload, write_memory_pool_));
      RETURN_NOT_OK(SendFrame(FrameType::kPayloadHeader, frame.data(), frame.size()));
    }

    if (!ipc::Message::HasBody(payload.ipc_message.type)) {
      return Status::OK();
    }

    // While IOV (scatter-gather) might seem like it avoids a memcpy,
    // profiling shows that at least for the TCP/SHM/RDMA transports,
    // UCX just does a memcpy internally. Furthermore, on the receiver
    // side, a sender-side IOV send prevents optimizations based on
    // mapped buffers (UCX will memcpy to the destination buffer
    // regardless of whether it's mapped or not).

    // If all buffers are on the CPU, concatenate them ourselves and
    // do a regular send to avoid this. Else, use IOV and let UCX
    // figure out what to do.

    // Weirdness: UCX prefers TCP over shared memory for CONTIG? We
    // can avoid this by setting UCX_RNDV_THRESH=inf, this will make
    // UCX prefer shared memory again. However, we still want to avoid
    // the CONTIG path when shared memory is available, because the
    // total amount of time spent in memcpy is greater than using IOV
    // and letting UCX handle it.

    // Consider: if we can figure out how to make IOV always as fast
    // as CONTIG, we can just send the metadata fields as part of the
    // IOV payload and avoid having to send two distinct messages.

    bool all_cpu = true;
    int32_t total_buffers = 0;
    for (const auto& buffer : payload.ipc_message.body_buffers) {
      if (!buffer || buffer->size() == 0) continue;
      all_cpu = all_cpu && buffer->is_cpu();
      total_buffers++;

      // Arrow IPC requires that we align buffers to 8 byte boundary
      const auto remainder = static_cast<int>(
          bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
      if (remainder) total_buffers++;
    }

    ucp_request_param_t request_param;
    std::memset(&request_param, 0, sizeof(request_param));
    request_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE |
                                 UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_USER_DATA;
    request_param.cb.send = AmSendCallback;
    request_param.flags = UCP_AM_SEND_FLAG_REPLY;

    std::unique_ptr<PendingAmSend> pending_send;
    void* send_data = nullptr;
    size_t send_size = 0;

    if (!all_cpu) {
      request_param.op_attr_mask =
          request_param.op_attr_mask | UCP_OP_ATTR_FIELD_MEMORY_TYPE;
      // XXX: UCX doesn't appear to autodetect this correctly if we
      // use UNKNOWN
      request_param.memory_type = UCS_MEMORY_TYPE_CUDA;
    }

    if (kEnableContigSend && all_cpu) {
      // CONTIG - concatenate buffers into one before sending

      // TODO(ARROW-16126): this needs to be pipelined since it can be expensive.
      // Preliminary profiling shows ~5% overhead just from mapping the buffer
      // alone (on Infiniband; it seems to be trivial for shared memory)
      request_param.datatype = ucp_dt_make_contig(1);
      pending_send = std::make_unique<PendingContigSend>();
      auto* pending_contig = reinterpret_cast<PendingContigSend*>(pending_send.get());

      const int64_t body_length = std::max<int64_t>(payload.ipc_message.body_length, 1);
      ARROW_ASSIGN_OR_RAISE(pending_contig->ipc_message,
                            AllocateBuffer(body_length, write_memory_pool_));
      TryMapBuffer(worker_->context().get(), *pending_contig->ipc_message,
                   &pending_contig->memh_p);

      uint8_t* ipc_message = pending_contig->ipc_message->mutable_data();
      if (payload.ipc_message.body_length == 0) {
        std::memset(ipc_message, '\0', 1);
      }

      for (const auto& buffer : payload.ipc_message.body_buffers) {
        if (!buffer || buffer->size() == 0) continue;

        std::memcpy(ipc_message, buffer->data(), buffer->size());
        ipc_message += buffer->size();

        const auto remainder = static_cast<int>(
            bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
        if (remainder) {
          std::memset(ipc_message, 0, remainder);
          ipc_message += remainder;
        }
      }

      send_data = reinterpret_cast<void*>(pending_contig->ipc_message->mutable_data());
      send_size = static_cast<size_t>(pending_contig->ipc_message->size());
    } else {
      // IOV - let UCX use scatter-gather path
      request_param.datatype = UCP_DATATYPE_IOV;
      pending_send = std::make_unique<PendingIovSend>();
      auto* pending_iov = reinterpret_cast<PendingIovSend*>(pending_send.get());

      pending_iov->payload = payload;
      pending_iov->iovs.resize(total_buffers);
      ucp_dt_iov_t* iov = pending_iov->iovs.data();
#if defined(ARROW_FLIGHT_UCX_SEND_IOV_MAP)
      // XXX: this seems to have no benefits in tests so far
      pending_iov->memh_ps.resize(total_buffers);
      ucp_mem_h* memh_p = pending_iov->memh_ps.data();
#endif
      for (const auto& buffer : payload.ipc_message.body_buffers) {
        if (!buffer || buffer->size() == 0) continue;

        iov->buffer = const_cast<void*>(reinterpret_cast<const void*>(buffer->address()));
        iov->length = buffer->size();
        ++iov;

#if defined(ARROW_FLIGHT_UCX_SEND_IOV_MAP)
        TryMapBuffer(worker_->context().get(), *buffer, memh_p);
        memh_p++;
#endif

        const auto remainder = static_cast<int>(
            bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
        if (remainder) {
          iov->buffer =
              const_cast<void*>(reinterpret_cast<const void*>(padding_bytes_.data()));
          iov->length = remainder;
          ++iov;
        }
      }

      if (total_buffers == 0) {
        // UCX cannot handle zero-byte payloads
        pending_iov->iovs.resize(1);
        pending_iov->iovs[0].buffer =
            const_cast<void*>(reinterpret_cast<const void*>(padding_bytes_.data()));
        pending_iov->iovs[0].length = 1;
      }

      send_data = pending_iov->iovs.data();
      send_size = pending_iov->iovs.size();
    }

    DCHECK(send_data) << "Payload cannot be nullptr";
    DCHECK_GT(send_size, 0) << "Payload cannot be empty";

    RETURN_NOT_OK(pending_send->header.Set(FrameType::kPayloadBody, counter_++,
                                           payload.ipc_message.body_length));
    pending_send->driver = this;
    pending_send->completed = Future<>::Make();

    request_param.user_data = pending_send.release();
    {
      auto* pending_send = reinterpret_cast<PendingAmSend*>(request_param.user_data);

      void* request = ucp_am_send_nbx(
          endpoint_, kUcpAmHandlerId, pending_send->header.data(),
          pending_send->header.size(), send_data, send_size, &request_param);
      if (!request) {
        // Request completed immediately
        delete pending_send;
        return Status::OK();
      } else if (UCS_PTR_IS_ERR(request)) {
        delete pending_send;
        return FromUcsStatus("ucp_am_send_nbx", UCS_PTR_STATUS(request));
      }
      return pending_send->completed;
    }
  }

  Status Close() {
    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (!endpoint_) return Status::OK();

    for (auto& item : frames_) {
      item.second.MarkFinished(Status::Cancelled("UcpCallDriver is being closed"));
    }
    frames_.clear();

    void* request = ucp_ep_close_nb(endpoint_, UCP_EP_CLOSE_MODE_FLUSH);
    ucs_status_t status = UCS_OK;
    std::string origin = "ucp_ep_close_nb";
    if (UCS_PTR_IS_ERR(request)) {
      status = UCS_PTR_STATUS(request);
    } else if (UCS_PTR_IS_PTR(request)) {
      origin = "ucp_request_check_status";
      while ((status = ucp_request_check_status(request)) == UCS_INPROGRESS) {
        MakeProgress();
      }
      ucp_request_free(request);
    } else {
      DCHECK(!request);
    }

    endpoint_ = nullptr;
    if (status != UCS_OK && !IsIgnorableDisconnectError(status)) {
      return FromUcsStatus(origin, status);
    }
    return Status::OK();
  }

  void MakeProgress() { ucp_worker_progress(worker_->get()); }

  void Push(std::shared_ptr<Frame> frame) {
    std::unique_lock<std::mutex> guard(frame_mutex_);
    if (ARROW_PREDICT_FALSE(!status_.ok())) return;
    auto pair = frames_.insert({frame->counter, frame});
    if (!pair.second) {
      // Not inserted, because ReadFrameAsync was called for this
      // frame counter value and the client is already waiting on
      // it. Complete the existing future.
      pair.first->second.MarkFinished(std::move(frame));
      frames_.erase(pair.first);
    }
    // Otherwise, we inserted the frame, meaning the client was not
    // currently waiting for that frame counter value
  }

  void Push(Status status) {
    std::unique_lock<std::mutex> guard(frame_mutex_);
    status_ = std::move(status);
    for (auto& item : frames_) {
      // Push(Frame) may push a complete frame, in which case the
      // future is already complete - just skip it
      if (item.second.is_finished()) continue;
      item.second.MarkFinished(status_);
    }
    frames_.clear();
  }

  ucs_status_t RecvActiveMessage(const void* header, size_t header_length, void* data,
                                 const size_t data_length,
                                 const ucp_am_recv_param_t* param) {
    auto maybe_status =
        RecvActiveMessageImpl(header, header_length, data, data_length, param);
    if (!maybe_status.ok()) {
      Push(maybe_status.status());
      return UCS_OK;
    }
    return maybe_status.MoveValueUnsafe();
  }

  const std::shared_ptr<MemoryManager>& memory_manager() const { return memory_manager_; }
  void set_memory_manager(std::shared_ptr<MemoryManager> memory_manager) {
    if (memory_manager) {
      memory_manager_ = std::move(memory_manager);
    } else {
      memory_manager_ = CPUDevice::Instance()->default_memory_manager();
    }
  }
  void set_read_memory_pool(MemoryPool* pool) {
    read_memory_pool_ = pool ? pool : default_memory_pool();
  }
  void set_write_memory_pool(MemoryPool* pool) {
    write_memory_pool_ = pool ? pool : default_memory_pool();
  }
  const std::string& peer() const { return name_; }

 private:
  class PendingAmSend {
   public:
    virtual ~PendingAmSend() = default;
    UcpCallDriver::Impl* driver;
    Future<> completed;
    FrameHeader header;
  };

  class PendingContigSend : public PendingAmSend {
   public:
    std::unique_ptr<Buffer> ipc_message;
    ucp_mem_h memh_p;

    virtual ~PendingContigSend() {
      TryUnmapBuffer(driver->worker_->context().get(), memh_p);
    }
  };

  class PendingIovSend : public PendingAmSend {
   public:
    FlightPayload payload;
    std::vector<ucp_dt_iov_t> iovs;
#if defined(ARROW_FLIGHT_UCX_SEND_IOV_MAP)
    std::vector<ucp_mem_h> memh_ps;

    virtual ~PendingIovSend() {
      for (ucp_mem_h memh_p : memh_ps) {
        TryUnmapBuffer(driver->worker_->context().get(), memh_p);
      }
    }
#endif
  };

  struct PendingAmRecv {
    UcpCallDriver::Impl* driver;
    std::shared_ptr<Frame> frame;
    ucp_mem_h memh_p;

    PendingAmRecv(UcpCallDriver::Impl* driver_, std::shared_ptr<Frame> frame_)
        : driver(driver_), frame(std::move(frame_)) {}

    ~PendingAmRecv() { TryUnmapBuffer(driver->worker_->context().get(), memh_p); }
  };

  static void AmSendCallback(void* request, ucs_status_t status, void* user_data) {
    auto* pending_send = reinterpret_cast<PendingAmSend*>(user_data);
    if (status == UCS_OK) {
      pending_send->completed.MarkFinished();
    } else {
      pending_send->completed.MarkFinished(FromUcsStatus("ucp_am_send_nbx", status));
    }
    // TODO(ARROW-16126): delete should occur on a background thread if there's
    // mapped buffers, since unmapping can be nontrivial and we don't want to block
    // the thread doing UCX work. (Borrow the Rust transfer-and-drop pattern.)
    delete pending_send;
    ucp_request_free(request);
  }

  static void AmRecvCallback(void* request, ucs_status_t status, size_t length,
                             void* user_data) {
    auto* pending_recv = reinterpret_cast<PendingAmRecv*>(user_data);
    ucp_request_free(request);
    if (status != UCS_OK) {
      pending_recv->driver->Push(
          FromUcsStatus("ucp_am_recv_data_nbx (callback)", status));
    } else {
      pending_recv->driver->Push(std::move(pending_recv->frame));
    }
    delete pending_recv;
  }

  arrow::Result<ucs_status_t> RecvActiveMessageImpl(const void* header,
                                                    size_t header_length, void* data,
                                                    const size_t data_length,
                                                    const ucp_am_recv_param_t* param) {
    DCHECK(param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP);

    if (data_length > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      return Status::Invalid("Cannot allocate buffer greater than 2 GiB, requested: ",
                             data_length);
    }

    ARROW_ASSIGN_OR_RAISE(auto frame, Frame::ParseHeader(header, header_length));
    if (data_length < frame->size) {
      return Status::IOError("Expected frame of ", frame->size, " bytes, but got only ",
                             data_length);
    }

    if ((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_DATA) &&
        (memory_manager_->is_cpu() || frame->type != FrameType::kPayloadBody)) {
      // Zero-copy path. UCX-allocated buffer must be freed later.

      // XXX: this buffer can NOT be freed until AFTER we return from
      // this handler. Otherwise, UCX won't have fully set up its
      // internal data structures (allocated just before the buffer)
      // and we'll crash when we free the buffer. Effectively: we can
      // never use Then/AddCallback on a Future<> from ReadFrameAsync,
      // because we might run the callback synchronously (which might
      // free the buffer) when we call Push here.
      frame->buffer = std::make_unique<UcxDataBuffer>(worker_, data, data_length);
      Push(std::move(frame));
      return UCS_INPROGRESS;
    }

    if ((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_DATA) ||
        (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)) {
      // Rendezvous protocol (RNDV), or unpack to destination (DATA).

      // We want to map/pin/register the buffer for faster transfer
      // where possible. (It gets unmapped in ~PendingAmRecv.)
      // TODO(ARROW-16126): This takes non-trivial time, so return
      // UCS_INPROGRESS, kick off the allocation in the background,
      // and recv the data later (is it allowed to call
      // ucp_am_recv_data_nbx asynchronously?).
      if (frame->type == FrameType::kPayloadBody) {
        ARROW_ASSIGN_OR_RAISE(frame->buffer,
                              memory_manager_->AllocateBuffer(data_length));
      } else {
        ARROW_ASSIGN_OR_RAISE(frame->buffer,
                              AllocateBuffer(data_length, read_memory_pool_));
      }

      PendingAmRecv* pending_recv = new PendingAmRecv(this, std::move(frame));
      TryMapBuffer(worker_->context().get(), *pending_recv->frame->buffer,
                   &pending_recv->memh_p);

      ucp_request_param_t recv_param;
      recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                UCP_OP_ATTR_FIELD_MEMORY_TYPE |
                                UCP_OP_ATTR_FIELD_USER_DATA;
      recv_param.cb.recv_am = AmRecvCallback;
      recv_param.user_data = pending_recv;
      recv_param.memory_type = InferMemoryType(*pending_recv->frame->buffer);

      void* dest =
          reinterpret_cast<void*>(pending_recv->frame->buffer->mutable_address());
      void* request =
          ucp_am_recv_data_nbx(worker_->get(), data, dest, data_length, &recv_param);
      if (UCS_PTR_IS_ERR(request)) {
        delete pending_recv;
        return FromUcsStatus("ucp_am_recv_data_nbx", UCS_PTR_STATUS(request));
      } else if (!request) {
        // Request completed instantly
        Push(std::move(pending_recv->frame));
        delete pending_recv;
      }
      return UCS_OK;
    } else {
      // Data will be freed after callback returns - copy to buffer
      if (memory_manager_->is_cpu() || frame->type != FrameType::kPayloadBody) {
        ARROW_ASSIGN_OR_RAISE(frame->buffer,
                              AllocateBuffer(data_length, read_memory_pool_));
        std::memcpy(frame->buffer->mutable_data(), data, data_length);
      } else {
        ARROW_ASSIGN_OR_RAISE(
            frame->buffer,
            MemoryManager::CopyNonOwned(Buffer(reinterpret_cast<uint8_t*>(data),
                                               static_cast<int64_t>(data_length)),
                                        memory_manager_));
      }
      Push(std::move(frame));
      return UCS_OK;
    }
  }

  Status CompleteRequestBlocking(const std::string& context, void* request) {
    if (UCS_PTR_IS_ERR(request)) {
      return FromUcsStatus(context, UCS_PTR_STATUS(request));
    } else if (UCS_PTR_IS_PTR(request)) {
      while (true) {
        auto status = ucp_request_check_status(request);
        if (status == UCS_OK) {
          break;
        } else if (status != UCS_INPROGRESS) {
          ucp_request_release(request);
          return FromUcsStatus("ucp_request_check_status", status);
        }
        MakeProgress();
      }
      ucp_request_free(request);
    } else {
      // Send was completed instantly
      DCHECK(!request);
    }
    return Status::OK();
  }

  Status CheckClosed() {
    if (!endpoint_) {
      return Status::Invalid("UcpCallDriver is closed");
    }
    return Status::OK();
  }

  const std::array<uint8_t, 8> padding_bytes_;
#if defined(ARROW_FLIGHT_UCX_SEND_IOV_MAP)
  ucp_mem_h padding_memh_p_;
#endif

  std::shared_ptr<UcpWorker> worker_;
  ucp_ep_h endpoint_;
  MemoryPool* read_memory_pool_;
  MemoryPool* write_memory_pool_;
  std::shared_ptr<MemoryManager> memory_manager_;

  // Internal name for logging/tracing
  std::string name_;
  // Counter used to reorder messages
  uint32_t counter_ = 0;

  std::mutex frame_mutex_;
  Status status_;
  std::unordered_map<uint32_t, Future<std::shared_ptr<Frame>>> frames_;
  uint32_t next_counter_ = 0;
};

UcpCallDriver::UcpCallDriver(std::shared_ptr<UcpWorker> worker, ucp_ep_h endpoint)
    : impl_(new Impl(std::move(worker), endpoint)) {}
UcpCallDriver::UcpCallDriver(UcpCallDriver&&) = default;
UcpCallDriver& UcpCallDriver::operator=(UcpCallDriver&&) = default;
UcpCallDriver::~UcpCallDriver() = default;

arrow::Result<std::shared_ptr<Frame>> UcpCallDriver::ReadNextFrame() {
  return impl_->ReadNextFrame();
}

Future<std::shared_ptr<Frame>> UcpCallDriver::ReadFrameAsync() {
  return impl_->ReadFrameAsync();
}

Status UcpCallDriver::ExpectFrameType(const Frame& frame, FrameType type) {
  if (frame.type != type) {
    return Status::IOError("Expected frame type ", static_cast<int32_t>(type),
                           ", but got frame type ", static_cast<int32_t>(frame.type));
  }
  return Status::OK();
}

Status UcpCallDriver::StartCall(const std::string& method) {
  std::vector<std::pair<std::string, std::string>> headers;
  headers.emplace_back(kHeaderMethod, method);
  ARROW_ASSIGN_OR_RAISE(auto frame, HeadersFrame::Make(headers));
  auto buffer = std::move(frame).GetBuffer();
  RETURN_NOT_OK(impl_->SendFrame(FrameType::kHeaders, buffer->data(), buffer->size()));
  return Status::OK();
}

Future<> UcpCallDriver::SendFlightPayload(const FlightPayload& payload) {
  return impl_->SendFlightPayload(payload);
}

Status UcpCallDriver::SendFrame(FrameType frame_type, const uint8_t* data,
                                const int64_t size) {
  return impl_->SendFrame(frame_type, data, size);
}

Future<> UcpCallDriver::SendFrameAsync(FrameType frame_type,
                                       std::unique_ptr<Buffer> buffer) {
  return impl_->SendFrameAsync(frame_type, std::move(buffer));
}

Status UcpCallDriver::Close() { return impl_->Close(); }

void UcpCallDriver::MakeProgress() { impl_->MakeProgress(); }

ucs_status_t UcpCallDriver::RecvActiveMessage(const void* header, size_t header_length,
                                              void* data, const size_t data_length,
                                              const ucp_am_recv_param_t* param) {
  return impl_->RecvActiveMessage(header, header_length, data, data_length, param);
}

const std::shared_ptr<MemoryManager>& UcpCallDriver::memory_manager() const {
  return impl_->memory_manager();
}

void UcpCallDriver::set_memory_manager(std::shared_ptr<MemoryManager> memory_manager) {
  impl_->set_memory_manager(std::move(memory_manager));
}
void UcpCallDriver::set_read_memory_pool(MemoryPool* pool) {
  impl_->set_read_memory_pool(pool);
}
void UcpCallDriver::set_write_memory_pool(MemoryPool* pool) {
  impl_->set_write_memory_pool(pool);
}
const std::string& UcpCallDriver::peer() const { return impl_->peer(); }

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow

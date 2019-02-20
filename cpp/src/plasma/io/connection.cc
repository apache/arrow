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

#include "plasma/io/connection.h"

#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/logging.h"
#include "plasma/fling.h"
#include "plasma/plasma_generated.h"
#include "plasma/protocol.h"

// TODO(pcm): Replace our own custom message header (message type,
// message length, plasma protocol verion) with one that is serialized
// using flatbuffers.
constexpr int64_t kPlasmaProtocolVersion = 0x0000000000000000;

namespace plasma {
namespace io {

using flatbuf::MessageType;

Status asio_to_arrow_status(const std::error_code& ec) {
  if (!ec) {
    return Status::OK();
  }
  if (ec.value() == EPIPE || ec.value() == EBADF || ec.value() == ECONNRESET) {
    ARROW_LOG(WARNING) << "Received SIGPIPE, BAD FILE DESCRIPTOR, or ECONNRESET when "
                          "processing a message. The client on the other end may "
                          "have hung up.";
  }
  return Status::IOError("Error code = ", strerror(ec.value()));
}

struct AsyncMessageWriteBuffer : public AsyncWriteBuffer {
  AsyncMessageWriteBuffer(int64_t version, int64_t type, int64_t length,
                          const uint8_t* message, AsyncWriteCallback callback)
      : write_version(version), write_type(type), write_length(length) {
    write_message.resize(length);
    write_message.assign(message, message + length);
    handler = callback;
  }

  void ToBuffers(std::vector<asio::const_buffer>& message_buffers) override {
    message_buffers.push_back(asio::buffer(&write_version, sizeof(write_version)));
    message_buffers.push_back(asio::buffer(&write_type, sizeof(write_type)));
    message_buffers.push_back(asio::buffer(&write_length, sizeof(write_length)));
    message_buffers.push_back(asio::buffer(write_message));
  }

  int64_t write_version;
  int64_t write_type;
  uint64_t write_length;
  std::vector<uint8_t> write_message;
};

std::shared_ptr<ServerConnection> ServerConnection::shared_from_this() {
  return std::static_pointer_cast<ServerConnection>(PlasmaConnection::shared_from_this());
}

std::shared_ptr<ServerConnection> ServerConnection::Create(PlasmaStream&& stream) {
  std::shared_ptr<ServerConnection> self(new ServerConnection(std::move(stream)));
  return self;
}

Status ServerConnection::ReadMessage(int64_t type, std::vector<uint8_t>* message) {
  int64_t read_version, read_type, read_length;
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<asio::mutable_buffer> header;
  header.push_back(asio::buffer(&read_version, sizeof(read_version)));
  header.push_back(asio::buffer(&read_type, sizeof(read_type)));
  header.push_back(asio::buffer(&read_length, sizeof(read_length)));

  auto ec = PlasmaConnection::ReadBuffer(header);
  if (ec) {
    return asio_to_arrow_status(ec);
  }
  // If there was no error, make sure the protocol version matches.
  if (read_version != kPlasmaProtocolVersion) {
    return Status::ProtocolError(
        "Expected Plasma message protocol version: ", kPlasmaProtocolVersion,
        ", got protocol version: ", read_version);
  }
  if (type != read_type) {
    if (read_type == static_cast<int64_t>(MessageType::PlasmaDisconnectClient)) {
      // Disconnected by client.
      return Status::IOError("The other side disconnected.");
    }
    return Status::IOError("Connection corrupted. Expected message type: ", type,
                           "; got message type: ", read_type,
                           ". Check logs or dmesg for previous errors.");
  }
  // Create read buffer.
  message->resize(read_length);
  auto buffer = asio::buffer(*message);
  // Wait for the message to be read.
  return asio_to_arrow_status(PlasmaConnection::ReadBuffer(buffer));
}

Status ServerConnection::WriteMessage(int64_t type, int64_t length,
                                      const uint8_t* message) {
  PlasmaConnection::sync_writes_ += 1;
  PlasmaConnection::bytes_written_ += length;

  std::vector<asio::const_buffer> message_buffers;
  auto write_version = kPlasmaProtocolVersion;
  message_buffers.push_back(asio::buffer(&write_version, sizeof(write_version)));
  message_buffers.push_back(asio::buffer(&type, sizeof(type)));
  message_buffers.push_back(asio::buffer(&length, sizeof(length)));
  message_buffers.push_back(asio::buffer(message, length));
  return asio_to_arrow_status(PlasmaConnection::WriteBuffer(message_buffers));
}

void ServerConnection::WriteMessageAsync(int64_t type, int64_t length,
                                         const uint8_t* message,
                                         const AsyncWriteCallback& handler) {
  auto write_buffer = std::unique_ptr<io::AsyncWriteBuffer>(new AsyncMessageWriteBuffer(
      kPlasmaProtocolVersion, type, length, message, handler));
  PlasmaConnection::WriteBufferAsync(std::move(write_buffer));
}

Status ServerConnection::RecvFd(int* fd) {
  *fd = recv_fd(GetNativeHandle());
  ARROW_CHECK(*fd);
  return Status::OK();
}

Status ServerConnection::Disconnect() {
  if (!IsOpen()) {
    ARROW_LOG(WARNING) << "The client is not connected. 'Disconnect()' is ignored.";
    return Status::OK();
  }
  // Write the disconnection message.
  auto status =
      WriteMessage(static_cast<int64_t>(MessageType::PlasmaDisconnectClient), 0, NULLPTR);
  Close();  // Close the stream anyway.
  return status;
}

Status ServerConnection::ReadNotificationMessage(std::unique_ptr<uint8_t[]>& message) {
  int64_t size;
  auto ec = ReadBuffer(asio::mutable_buffer(&size, sizeof(size)));
  if (ec) {
    // The other side has closed the socket.
    ARROW_LOG(DEBUG) << "Socket has been closed, or some other error has occurred.";
    return asio_to_arrow_status(ec);
  }
  message.reset(new uint8_t[size]);
  ec = ReadBuffer(asio::mutable_buffer(message.get(), size));
  if (ec) {
    // The other side has closed the socket.
    ARROW_LOG(DEBUG) << "Socket has been closed, or some other error has occurred.";
    return asio_to_arrow_status(ec);
  }
  return Status::OK();
}

ServerConnection::ServerConnection(PlasmaStream&& stream)
    : PlasmaConnection(std::move(stream)) {}

std::shared_ptr<ClientConnection> ClientConnection::Create(
    PlasmaStream&& stream, MessageHandler& message_handler,
    const std::string& debug_label) {
  return std::shared_ptr<ClientConnection>(
      new ClientConnection(std::move(stream), message_handler, debug_label));
}

ClientConnection::ClientConnection(PlasmaStream&& stream, MessageHandler& message_handler,
                                   const std::string& debug_label)
    : ServerConnection(std::move(stream)),
      debug_label_(debug_label),
      message_handler_(message_handler) {}

std::shared_ptr<ClientConnection> ClientConnection::shared_from_this() {
  return std::static_pointer_cast<ClientConnection>(ServerConnection::shared_from_this());
}

void ClientConnection::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<asio::mutable_buffer> header{
      asio::buffer(&read_version_, sizeof(read_version_)),
      asio::buffer(&read_type_, sizeof(read_type_)),
      asio::buffer(&read_length_, sizeof(read_length_))};

  asio::async_read(ServerConnection::stream_, header,
                   std::bind(&ClientConnection::ProcessMessageHeader, shared_from_this(),
                             std::placeholders::_1));  // Ignore byte_transferred
}

void ClientConnection::ProcessMessageHeader(const std::error_code& error) {
  if (error) {
    // If there was an error, disconnect the client.
    ProcessError(error);
    return;
  }

  // If there was no error, make sure the protocol version matches.
  // TODO(suquark): Don't let server die here.
  ARROW_CHECK(read_version_ == kPlasmaProtocolVersion);
  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  ServerConnection::bytes_read_ += read_length_;
  // Wait for the message to be read.
  asio::async_read(ServerConnection::stream_, asio::buffer(read_message_),
                   std::bind(&ClientConnection::ProcessMessageBody, shared_from_this(),
                             std::placeholders::_1));
}

void ClientConnection::ProcessMessageBody(const std::error_code& error) {
  if (error) {
    ProcessError(error);
    return;
  }
  auto start = std::chrono::system_clock::now();
  ProcessMessage(read_type_, read_length_, read_message_.data());
  auto end = std::chrono::system_clock::now();
  auto interval = std::chrono::duration<double, std::milli>(end - start);
  if (interval.count() > 100.0) {
    ARROW_LOG(WARNING) << "[" << debug_label_ << "]ProcessMessage with type "
                       << read_type_ << " took " << interval.count() << " ms.";
  }
}

void ClientConnection::ProcessError(const std::error_code& ec) {
  ARROW_LOG(ERROR)
      << "Failed when processing message. Disconnecting the client. Error code = " << ec;
  // If there was an error, disconnect the client.
  PlasmaConnection::Close();
}

void ClientConnection::ProcessMessage(int64_t type, int64_t length, const uint8_t* data) {
  message_handler_(shared_from_this(), type, length, data);
}

struct AsyncObjectNotificationWriteBuffer : public AsyncWriteBuffer {
  static std::unique_ptr<AsyncObjectNotificationWriteBuffer> MakeDeletion(
      const ObjectID& object_id) {
    auto message = new std::vector<uint8_t>();
    SerializeObjectDeletionNotification(object_id, message);
    return std::unique_ptr<AsyncObjectNotificationWriteBuffer>(
        new AsyncObjectNotificationWriteBuffer(message));
  }

  static std::unique_ptr<AsyncObjectNotificationWriteBuffer> MakeReady(
      const ObjectID& object_id, const ObjectTableEntry& entry) {
    auto message = new std::vector<uint8_t>();
    SerializeObjectSealedNotification(object_id, entry, message);
    return std::unique_ptr<AsyncObjectNotificationWriteBuffer>(
        new AsyncObjectNotificationWriteBuffer(message));
  }

  void ToBuffers(std::vector<asio::const_buffer>& message_buffers) override {
    message_buffers.push_back(asio::buffer(&size, sizeof(size)));
    message_buffers.push_back(asio::buffer(*notification_msg));
  }

  std::unique_ptr<std::vector<uint8_t>> notification_msg;
  int64_t size;

 protected:
  explicit AsyncObjectNotificationWriteBuffer(std::vector<uint8_t>* message) {
    // Serialize the object.
    notification_msg.reset(message);
    size = message->size();
    handler = [](const asio::error_code& status) {
      auto errno_ = status.value();
      if (!errno_) {
        return;
      }
      if (errno_ == EAGAIN || errno_ == EWOULDBLOCK || errno_ == EINTR) {
        ARROW_LOG(DEBUG) << "The socket's send buffer is full, so we are caching this "
                            "notification and will send it later.";
        ARROW_LOG(WARNING) << "Blocked unexpectly when sending message async.";
      } else {
        ARROW_LOG(WARNING) << "Failed to send notification to client.";
        if (errno_ == EPIPE) {
          // TODO(suquark): We could probably close the socket here.
        }
      }
    };
  }
};

Status ClientConnection::SendFd(int fd) {
  ARROW_CHECK(send_fd(GetNativeHandle(), fd));
  return Status::OK();
}

void ClientConnection::SendObjectDeletionAsync(const ObjectID& object_id) {
  auto raw_ptr = AsyncObjectNotificationWriteBuffer::MakeDeletion(object_id).release();
  auto write_buffer =
      std::unique_ptr<io::AsyncWriteBuffer>(static_cast<io::AsyncWriteBuffer*>(raw_ptr));
  // Attempt to send a notification about this object ID.
  WriteBufferAsync(std::move(write_buffer));
}

void ClientConnection::SendObjectReadyAsync(const ObjectID& object_id,
                                            const ObjectTableEntry& entry) {
  auto raw_ptr =
      AsyncObjectNotificationWriteBuffer::MakeReady(object_id, entry).release();
  auto write_buffer =
      std::unique_ptr<io::AsyncWriteBuffer>(static_cast<io::AsyncWriteBuffer*>(raw_ptr));
  // Attempt to send a notification about this object ID.
  WriteBufferAsync(std::move(write_buffer));
}

}  // namespace io
}  // namespace plasma

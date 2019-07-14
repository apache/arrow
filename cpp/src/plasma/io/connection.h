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

#ifndef PLASMA_IO_CONNECTION_H
#define PLASMA_IO_CONNECTION_H

#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "plasma/common.h"
#include "plasma/io/basic_connection.h"

namespace plasma {
namespace io {

using arrow::Status;

using PlasmaConnection = Connection<PlasmaStream>;

Status asio_to_arrow_status(const error_code& ec);

/// A generic type representing a client connection to a server. This typename
/// can be used to write messages synchronously to the server.
class ServerConnection : public PlasmaConnection {
 public:
  std::shared_ptr<ServerConnection> shared_from_this();

  /// Allocate a new server connection.
  ///
  /// \param stream A reference to the server stream.
  /// \return std::shared_ptr<ServerConnection>.
  static std::shared_ptr<ServerConnection> Create(PlasmaStream&& stream);

  /// Write a message to the client.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer.
  /// \return Status.
  Status WriteMessage(int64_t type, int64_t length, const uint8_t* message);

  /// Read a message from the client.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param message A pointer to the message vector.
  /// \return Status.
  Status ReadMessage(int64_t type, std::vector<uint8_t>* message);

  /// Write a message to the client asynchronously.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer.
  /// \param handler A callback to run on write completion.
  void WriteMessageAsync(int64_t type, int64_t length, const uint8_t* message,
                         const AsyncWriteCallback& handler);

  Status RecvFd(int* fd);

  Status Disconnect();

  Status ReadNotificationMessage(std::unique_ptr<uint8_t[]>& message);

 protected:
  /// A private constructor for a server connection.
  explicit ServerConnection(PlasmaStream&& stream);
};

class ClientConnection;
using MessageHandler = std::function<void(std::shared_ptr<ClientConnection>, int64_t type,
                                          int64_t length, const uint8_t*)>;

/// A generic type representing a client connection on a server. In addition to
/// writing messages to the client, like in ServerConnection, this typename can
/// also be used to process messages asynchronously from client.
class ClientConnection : public ServerConnection {
 public:
  /// Allocate a new node client connection.
  ///
  /// \param stream The client stream.
  /// \param message_handler A reference to the message handler.
  /// \return std::shared_ptr<ClientConnection>.
  static std::shared_ptr<ClientConnection> Create(PlasmaStream&& stream,
                                                  MessageHandler& message_handler);

  std::shared_ptr<ClientConnection> shared_from_this();

  /// Listen for and process messages from the client connection. Once a
  /// message has been fully received, the client manager's
  /// ProcessClientMessage handler will be called.
  void ProcessMessages();

  Status SendFd(int fd);

  inline bool ObjectIDExists(const ObjectID& object_id) {
    return object_ids.find(object_id) != object_ids.end();
  }

  inline void RemoveObjectID(const ObjectID& object_id) { object_ids.erase(object_id); }

  inline int RemoveObjectIDIfExists(const ObjectID& object_id) {
    auto it = object_ids.find(object_id);
    if (it != object_ids.end()) {
      object_ids.erase(it);
      // Return 1 to indicate that the client was removed.
      return 1;
    } else {
      // Return 0 to indicate that the client was not removed.
      return 0;
    }
  }

  /// Send notifications about sealed objects to the subscribers. This is called
  /// in SealObject.
  void SendObjectReadyAsync(const ObjectID& object_id, const ObjectTableEntry& entry);

  /// Send notifications about evicted objects to the subscribers.
  void SendObjectDeletionAsync(const ObjectID& object_id);

  /// Object ids that are used by this client.
  std::unordered_set<ObjectID> object_ids;
  /// File descriptors that are used by this client.
  std::unordered_set<int> used_fds;

 private:
  /// Process the message header from the client.
  /// \param ec The returned error code.
  void ProcessMessageHeader(const error_code& ec);

  /// Process the message body from the client.
  /// \param ec The returned error code.
  void ProcessMessageBody(const error_code& ec);

  /// Process the message from the client.
  /// \param type The type of the message.
  /// \param length The length of the message.
  /// \param data The data buffer of the message.
  void ProcessMessage(int64_t type, int64_t length, const uint8_t* data);

  /// Process an error from reading the message from the client.
  /// \param status The status code.
  void ProcessError(const Status& status);

  /// A private constructor for a node client connection.
  ClientConnection(PlasmaStream&& stream, MessageHandler& message_handler);

  /// The handler for a message from the client.
  MessageHandler message_handler_;

  int64_t read_version_;
  int64_t read_type_;
  uint64_t read_length_;

  /// Buffers for the current message being read from the client.
  std::vector<uint8_t> read_message_;
};

}  // namespace io
}  // namespace plasma

#endif  // PLASMA_IO_CONNECTION_H

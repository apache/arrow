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

#ifndef PLASMA_IO_BASIC_CONNECTION_H
#define PLASMA_IO_BASIC_CONNECTION_H

#ifndef ASIO_STANDALONE
#define ASIO_STANDALONE
#endif

#include <deque>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <boost/system/error_code.hpp>
#include <boost/asio.hpp>

namespace asio = boost::asio;

using error_code = boost::system::error_code;

namespace plasma {
namespace io {

enum class AsyncWriteCallbackCode {
  OK,
  DISCONNECT,
  UNKNOWN_ERROR,
};

using AsyncWriteCallback = std::function<AsyncWriteCallbackCode(const error_code&)>;
// TODO(suquark): Change it according to the platform.
using PlasmaStream = asio::basic_stream_socket<asio::local::stream_protocol>;
using PlasmaAcceptor = asio::local::stream_protocol::acceptor;

/// Create a local acceptor depends on the platform.
PlasmaAcceptor CreateLocalAcceptor(asio::io_context& io_context, const std::string& name);

/// Create a local stream depends on the platform.
Status CreateLocalStream(const std::string& name, PlasmaStream* result);

/// A message that is queued for writing asynchronously.
struct AsyncWriteBuffer {
  virtual void ToBuffers(std::vector<asio::const_buffer>& message_buffers) = 0;
  virtual ~AsyncWriteBuffer() {}
  inline AsyncWriteCallbackCode Handle(const error_code& ec) { return handler_(ec); }

 protected:
  AsyncWriteCallback handler_;
};

template <typename T>
class Connection : public std::enable_shared_from_this<Connection<T>> {
 public:
  explicit Connection(T&& stream);

  ~Connection();

  /// Read a buffer from this connection.
  ///
  /// \param buffer The output buffer.
  error_code ReadBuffer(const asio::mutable_buffer& buffer);

  /// Read buffers from this connection.
  ///
  /// \param buffer The output vector of buffers.
  error_code ReadBuffer(const std::vector<asio::mutable_buffer>& buffer);

  /// Write a buffer to this connection.
  ///
  /// \param buffer The buffer.
  error_code WriteBuffer(const asio::const_buffer& buffer);

  /// Write buffers to this connection.
  ///
  /// \param buffer The vector of buffers.
  error_code WriteBuffer(const std::vector<asio::const_buffer>& buffer);

  /// Write buffers to this connection async.
  ///
  /// \param write_buffer The buffer to write async.
  void WriteBufferAsync(std::unique_ptr<AsyncWriteBuffer> write_buffer);

  /// Whether the stream is open.
  inline bool IsOpen() { return stream_.is_open(); }

  /// Shuts down the stream for this connection.
  void Close();

  /// Get the native handle from the stream.
  inline int GetNativeHandle() { return stream_.native_handle(); }

  /// Get the debug string.
  std::string DebugString() const;

 protected:
  /// The stream that supports most asio protocols (read, read_some, write,
  /// write_some, async_read, async_write, async_read_some, async_write_some).
  T stream_;

  /// Whether we are in the middle of an async write.
  bool async_write_in_flight_;

  /// Max number of messages to write out at once.
  const int async_write_max_messages_;

  /// List of pending messages to write.
  std::deque<std::unique_ptr<AsyncWriteBuffer>> async_write_queue_;

  /// Count of sync messages sent total.
  int64_t sync_writes_ = 0;

  /// Count of async messages sent total.
  int64_t async_writes_ = 0;

  /// Count of bytes sent total.
  int64_t bytes_written_ = 0;

  /// Count of bytes read total.
  int64_t bytes_read_ = 0;

 private:
  /// Asynchronously flushes the write queue. While async writes are running, the flag
  /// async_write_in_flight_ will be set. This should only be called when no async writes
  /// are currently in flight.
  void DoAsyncWrites();
};

}  // namespace io
}  // namespace plasma

#endif  // PLASMA_IO_BASIC_CONNECTION_H

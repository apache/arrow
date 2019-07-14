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

#include "plasma/io/basic_connection.h"
#include "arrow/util/logging.h"

#include <chrono>
#include <memory>
#include <thread>
#include <utility>

namespace plasma {
namespace io {

/// Connect a Unix local domain socket.
///
/// \param socket The socket to connect.
/// \param socket_name The name/path of the socket.
/// \return Status.
error_code UnixDomainSocketConnect(asio::local::stream_protocol::socket& socket,
                                        const std::string& socket_name) {
  asio::local::stream_protocol::endpoint endpoint(socket_name);
  error_code ec;
  socket.connect(endpoint, ec);
  if (ec) {
    // Close the socket if the connect failed.
    error_code close_error;
    socket.close(close_error);
  }
  return ec;
}

PlasmaStream CreateLocalStream(asio::io_context& io_context, const std::string& name) {
  // TODO(suquark): May be use "kNumConnectAttempts" and "kConnectTimeoutMs"?
  constexpr int num_retries = 50;
  constexpr int timeout_ms = 100;
  ARROW_CHECK(!name.empty());
#ifndef _WIN32
  asio::basic_stream_socket<asio::local::stream_protocol> socket(io_context);
  for (int i = 0; i < num_retries; i++) {
    error_code ec = UnixDomainSocketConnect(socket, name);
    if (!ec) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
    if (i > 0) {
      ARROW_LOG(ERROR) << "Retrying to connect to socket for pathname " << name
                       << " (num_attempts = " << i << ", num_retries = " << num_retries
                       << ")";
    }
  }
  return socket;
#else
// For windows: https://stackoverflow.com/questions/1236460/c-using-windows-named-pipes
#error "Windows has not been supported."
#endif
}

PlasmaAcceptor CreateLocalAcceptor(asio::io_context& io_context,
                                   const std::string& name) {
#ifndef _WIN32
  return PlasmaAcceptor(io_context, asio::local::stream_protocol::endpoint(name));
#else
// For windows: https://stackoverflow.com/questions/1236460/c-using-windows-named-pipes
#error "Windows has not been supported."
#endif
}

template <class T>
Connection<T>::Connection(T&& stream)
    : stream_(std::move(stream)),
      async_write_in_flight_(false),
      async_write_max_messages_(1),
      async_write_queue_() {}

template <class T>
Connection<T>::~Connection() {
  // If there are any pending messages, invoke their callbacks with an IOError status.
  for (const auto& write_buffer : async_write_queue_) {
    write_buffer->Handle(
        error_code(static_cast<int>(boost::system::errc::io_error), boost::system::system_category()));
  }
}

template <class T>
error_code Connection<T>::ReadBuffer(const asio::mutable_buffer& buffer) {
  error_code ec;
  // Loop until all bytes are read while handling interrupts.
  uint64_t bytes_remaining = asio::buffer_size(buffer);
  uint64_t position = 0;
  while (bytes_remaining != 0) {
    size_t bytes_read =
        stream_.read_some(asio::buffer(buffer + position, bytes_remaining), ec);
    position += bytes_read;
    bytes_remaining -= bytes_read;
    if (ec.value() == EINTR) {
      continue;
    } else if (ec) {
      return ec;
    }
  }
  return error_code();
}

template <class T>
error_code Connection<T>::ReadBuffer(
    const std::vector<asio::mutable_buffer>& buffer) {
  // Loop until all bytes are read while handling interrupts.
  for (const auto& b : buffer) {
    auto ec = ReadBuffer(b);
    if (ec) return ec;
  }
  return error_code();
}

/// Write a buffer to this connection.
///
/// \param buffer The buffer.
template <class T>
error_code Connection<T>::WriteBuffer(const asio::const_buffer& buffer) {
  error_code error;
  // Loop until all bytes are written while handling interrupts.
  // When profiling with pprof, unhandled interrupts were being sent by the profiler to
  // the raylet process, which was causing synchronous reads and writes to fail.
  uint64_t bytes_remaining = asio::buffer_size(buffer);
  uint64_t position = 0;
  while (bytes_remaining != 0) {
    size_t bytes_written =
        stream_.write_some(asio::buffer(buffer + position, bytes_remaining), error);
    position += bytes_written;
    bytes_remaining -= bytes_written;
    if (error.value() == EINTR) {
      continue;
    } else if (error) {
      return error;
    }
  }
  return error_code();
}

template <class T>
error_code Connection<T>::WriteBuffer(
    const std::vector<asio::const_buffer>& buffer) {
  error_code error;
  // Loop until all bytes are written while handling interrupts.
  // When profiling with pprof, unhandled interrupts were being sent by the profiler to
  // the raylet process, which was causing synchronous reads and writes to fail.
  for (const auto& b : buffer) {
    error = WriteBuffer(b);
    if (error) {
      return error;
    }
  }
  return error_code();
}

template <class T>
void Connection<T>::WriteBufferAsync(std::unique_ptr<AsyncWriteBuffer> write_buffer) {
  async_writes_ += 1;
  auto size = async_write_queue_.size();
  auto size_is_power_of_two = (size & (size - 1)) == 0;
  if (size > 1000 && size_is_power_of_two) {
    ARROW_LOG(WARNING) << "Connection has " << size << " buffered async writes";
  }
  async_write_queue_.push_back(std::move(write_buffer));
  if (!async_write_in_flight_) {
    DoAsyncWrites();
  }
}

// Shuts down socket for this connection.
template <class T>
void Connection<T>::Close() {
  error_code ec;
  stream_.close(ec);
}

template <class T>
std::string Connection<T>::DebugString() const {
  std::stringstream result;
  result << "\n- bytes read: " << bytes_read_;
  result << "\n- bytes written: " << bytes_written_;
  result << "\n- num async writes: " << async_writes_;
  result << "\n- num sync writes: " << sync_writes_;
  result << "\n- writing: " << async_write_in_flight_;
  result << "\n- pending async messages: " << async_write_queue_.size();
  return result.str();
}

template <class T>
void Connection<T>::DoAsyncWrites() {
  // Make sure we were not writing to the socket.
  ARROW_CHECK(!async_write_in_flight_);
  async_write_in_flight_ = true;

  // Do an async write of everything currently in the queue to the socket.
  std::vector<asio::const_buffer> message_buffers;
  int num_messages = 0;
  for (const auto& write_buffer : async_write_queue_) {
    write_buffer->ToBuffers(message_buffers);
    num_messages++;
    if (num_messages >= async_write_max_messages_) {
      break;
    }
  }

  // Ensure lambda holds a reference to this.
  auto this_ptr = this->shared_from_this();
  asio::async_write(stream_, message_buffers,
                    [this, this_ptr, num_messages](const error_code& ec,
                                                   size_t bytes_transferred) {
                      bytes_written_ += bytes_transferred;
                      bool close_connection = false;
                      // Call the handlers for the written messages.
                      for (int i = 0; i < num_messages; i++) {
                        auto write_buffer = std::move(async_write_queue_.front());
                        auto return_code = write_buffer->Handle(ec);
                        if (return_code != AsyncWriteCallbackCode::OK) {
                          close_connection = true;
                        }
                        async_write_queue_.pop_front();  // release object
                      }
                      // We finished writing, so mark that we're no longer doing an
                      // async write.
                      async_write_in_flight_ = false;
                      if (close_connection) {
                        Close();
                        return;
                      }
                      // If there is more to write, try to write the rest.
                      if (!async_write_queue_.empty()) {
                        DoAsyncWrites();
                      }
                    });
}

// We have to fill the template of all possible types.
template class Connection<PlasmaStream>;

}  // namespace io
}  // namespace plasma

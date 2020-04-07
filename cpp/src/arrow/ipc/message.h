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

// C++ object model and user API for interprocess schema messaging

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace io {

class FileInterface;
class InputStream;
class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {

struct IpcWriteOptions;

enum class MetadataVersion : char {
  /// 0.1.0
  V1,

  /// 0.2.0
  V2,

  /// 0.3.0 to 0.7.1
  V3,

  /// >= 0.8.0
  V4
};

// Read interface classes. We do not fully deserialize the flatbuffers so that
// individual fields metadata can be retrieved from very large schema without
//

/// \class Message
/// \brief An IPC message including metadata and body
class ARROW_EXPORT Message {
 public:
  enum Type { NONE, SCHEMA, DICTIONARY_BATCH, RECORD_BATCH, TENSOR, SPARSE_TENSOR };

  /// \brief Construct message, but do not validate
  ///
  /// Use at your own risk; Message::Open has more metadata validation
  Message(std::shared_ptr<Buffer> metadata, std::shared_ptr<Buffer> body);

  ~Message();

  /// \brief Create and validate a Message instance from two buffers
  ///
  /// \param[in] metadata a buffer containing the Flatbuffer metadata
  /// \param[in] body a buffer containing the message body, which may be null
  /// \return the created message
  static Result<std::unique_ptr<Message>> Open(std::shared_ptr<Buffer> metadata,
                                               std::shared_ptr<Buffer> body);

  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(const std::shared_ptr<Buffer>& metadata,
                     const std::shared_ptr<Buffer>& body, std::unique_ptr<Message>* out) {
    return Open(metadata, body).Value(out);
  }

  /// \brief Read message body and create Message given Flatbuffer metadata
  /// \param[in] metadata containing a serialized Message flatbuffer
  /// \param[in] stream an InputStream
  /// \return the created Message
  ///
  /// \note If stream supports zero-copy, this is zero-copy
  static Result<std::unique_ptr<Message>> ReadFrom(std::shared_ptr<Buffer> metadata,
                                                   io::InputStream* stream);

  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status ReadFrom(std::shared_ptr<Buffer> metadata, io::InputStream* stream,
                         std::unique_ptr<Message>* out) {
    return ReadFrom(std::move(metadata), stream).Value(out);
  }

  /// \brief Read message body from position in file, and create Message given
  /// the Flatbuffer metadata
  /// \param[in] offset the position in the file where the message body starts.
  /// \param[in] metadata containing a serialized Message flatbuffer
  /// \param[in] file the seekable file interface to read from
  /// \return the created Message
  ///
  /// \note If file supports zero-copy, this is zero-copy
  static Result<std::unique_ptr<Message>> ReadFrom(const int64_t offset,
                                                   std::shared_ptr<Buffer> metadata,
                                                   io::RandomAccessFile* file);

  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status ReadFrom(const int64_t offset, std::shared_ptr<Buffer> metadata,
                         io::RandomAccessFile* file, std::unique_ptr<Message>* out) {
    return ReadFrom(offset, std::move(metadata), file).Value(out);
  }

  /// \brief Return true if message type and contents are equal
  ///
  /// \param other another message
  /// \return true if contents equal
  bool Equals(const Message& other) const;

  /// \brief the Message metadata
  ///
  /// \return buffer
  std::shared_ptr<Buffer> metadata() const;

  /// \brief Custom metadata serialized in metadata Flatbuffer. Returns nullptr
  /// when none set
  const std::shared_ptr<const KeyValueMetadata>& custom_metadata() const;

  /// \brief the Message body, if any
  ///
  /// \return buffer is null if no body
  std::shared_ptr<Buffer> body() const;

  /// \brief The expected body length according to the metadata, for
  /// verification purposes
  int64_t body_length() const;

  /// \brief The Message type
  Type type() const;

  /// \brief The Message metadata version
  MetadataVersion metadata_version() const;

  const void* header() const;

  /// \brief Write length-prefixed metadata and body to output stream
  ///
  /// \param[in] file output stream to write to
  /// \param[in] options IPC writing options including alignment
  /// \param[out] output_length the number of bytes written
  /// \return Status
  Status SerializeTo(io::OutputStream* file, const IpcWriteOptions& options,
                     int64_t* output_length) const;

  /// \brief Return true if the Message metadata passes Flatbuffer validation
  bool Verify() const;

  /// \brief Whether a given message type needs a body.
  static bool HasBody(Type type) { return type != NONE && type != SCHEMA; }

 private:
  // Hide serialization details from user API
  class MessageImpl;
  std::unique_ptr<MessageImpl> impl_;

  ARROW_DISALLOW_COPY_AND_ASSIGN(Message);
};

ARROW_EXPORT std::string FormatMessageType(Message::Type type);

/// \brief Abstract interface for a sequence of messages
/// \since 0.5.0
class ARROW_EXPORT MessageReader {
 public:
  virtual ~MessageReader() = default;

  /// \brief Create MessageReader that reads from InputStream
  static std::unique_ptr<MessageReader> Open(io::InputStream* stream);

  /// \brief Create MessageReader that reads from owned InputStream
  static std::unique_ptr<MessageReader> Open(
      const std::shared_ptr<io::InputStream>& owned_stream);

  /// \brief Read next Message from the interface
  ///
  /// \return an arrow::ipc::Message instance
  virtual Result<std::unique_ptr<Message>> ReadNextMessage() = 0;

  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  Status ReadNextMessage(std::unique_ptr<Message>* message) {
    return ReadNextMessage().Value(message);
  }
};

/// \brief Read encapsulated RPC message from position in file
///
/// Read a length-prefixed message flatbuffer starting at the indicated file
/// offset. If the message has a body with non-zero length, it will also be
/// read
///
/// The metadata_length includes at least the length prefix and the flatbuffer
///
/// \param[in] offset the position in the file where the message starts. The
/// first 4 bytes after the offset are the message length
/// \param[in] metadata_length the total number of bytes to read from file
/// \param[in] file the seekable file interface to read from
/// \return the message read
ARROW_EXPORT
Result<std::unique_ptr<Message>> ReadMessage(const int64_t offset,
                                             const int32_t metadata_length,
                                             io::RandomAccessFile* file);

/// \brief Advance stream to an 8-byte offset if its position is not a multiple
/// of 8 already
/// \param[in] stream an input stream
/// \param[in] alignment the byte multiple for the metadata prefix, usually 8
/// or 64, to ensure the body starts on a multiple of that alignment
/// \return Status
ARROW_EXPORT
Status AlignStream(io::InputStream* stream, int32_t alignment = 8);

/// \brief Advance stream to an 8-byte offset if its position is not a multiple
/// of 8 already
/// \param[in] stream an output stream
/// \param[in] alignment the byte multiple for the metadata prefix, usually 8
/// or 64, to ensure the body starts on a multiple of that alignment
/// \return Status
ARROW_EXPORT
Status AlignStream(io::OutputStream* stream, int32_t alignment = 8);

/// \brief Return error Status if file position is not a multiple of the
/// indicated alignment
ARROW_EXPORT
Status CheckAligned(io::FileInterface* stream, int32_t alignment = 8);

/// \brief Read encapsulated IPC message (metadata and body) from InputStream
///
/// Returns null if there are not enough bytes available or the
/// message length is 0 (e.g. EOS in a stream)
///
/// \param[in] stream an input stream
/// \param[in] pool an optional MemoryPool to copy metadata on the CPU, if required
/// \return Message
ARROW_EXPORT
Result<std::unique_ptr<Message>> ReadMessage(io::InputStream* stream,
                                             MemoryPool* pool = default_memory_pool());

/// Write encapsulated IPC message Does not make assumptions about
/// whether the stream is aligned already. Can write legacy (pre
/// version 0.15.0) IPC message if option set
///
/// continuation: 0xFFFFFFFF
/// message_size: int32
/// message: const void*
/// padding
///
///
/// \param[in] message a buffer containing the metadata to write
/// \param[in] options IPC writing options, including alignment and
/// legacy message support
/// \param[in,out] file the OutputStream to write to
/// \param[out] message_length the total size of the payload written including
/// padding
/// \return Status
Status WriteMessage(const Buffer& message, const IpcWriteOptions& options,
                    io::OutputStream* file, int32_t* message_length);

// ----------------------------------------------------------------------
// Deprecated APIs

ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
ARROW_EXPORT
Status ReadMessage(const int64_t offset, const int32_t metadata_length,
                   io::RandomAccessFile* file, std::unique_ptr<Message>* message);

ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
ARROW_EXPORT
Status ReadMessage(io::InputStream* stream, std::unique_ptr<Message>* message);

}  // namespace ipc
}  // namespace arrow

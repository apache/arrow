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

// Buffered stream implementations

#ifndef ARROW_IO_BUFFERED_H
#define ARROW_IO_BUFFERED_H

#include <cstdint>
#include <memory>

#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace io {

class ARROW_EXPORT BufferedOutputStream : public OutputStream {
 public:
  ~BufferedOutputStream() override;

  /// \brief Create a buffered output stream wrapping the given output stream.
  /// \param[in] raw another OutputStream
  /// \param[in] buffer_size the size of the temporary buffer. Allocates from
  /// the default memory pool
  /// \param[out] out the created BufferedOutputStream
  /// \return Status
  static Status Create(std::shared_ptr<OutputStream> raw, int64_t buffer_size,
                       std::shared_ptr<BufferedOutputStream>* out);

  /// \brief Resize internal buffer
  /// \param[in] new_buffer_size the new buffer size
  /// \return Status
  Status SetBufferSize(int64_t new_buffer_size);

  /// \brief Return the current size of the internal buffer
  int64_t buffer_size() const;

  // OutputStream interface

  /// \brief Close the buffered output stream.  This implicitly closes the
  /// underlying raw output stream.
  Status Close() override;
  bool closed() const override;

  Status Tell(int64_t* position) const override;
  // Write bytes to the stream. Thread-safe
  Status Write(const void* data, int64_t nbytes) override;

  Status Flush() override;

  /// \brief Return the underlying raw output stream.
  std::shared_ptr<OutputStream> raw() const;

 private:
  explicit BufferedOutputStream(std::shared_ptr<OutputStream> raw);

  class ARROW_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_BUFFERED_H

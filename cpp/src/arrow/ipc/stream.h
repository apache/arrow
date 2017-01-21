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

// Implement Arrow streaming binary format

#ifndef ARROW_IPC_STREAM_H
#define ARROW_IPC_STREAM_H

#include <cstdint>
#include <memory>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
struct Field;
class RecordBatch;
class Schema;
class Status;

namespace io {

class OutputStream;
class ReadableFileInterface;

}  // namespace io

namespace ipc {

class FileBlock;

class ARROW_EXPORT BaseStreamWriter {
 public:
  ~BaseStreamWriter();
  virtual Status WriteRecordBatch(const RecordBatch& batch) = 0;
  virtual Status Close() = 0;

 protected:
  BaseStreamWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema);

  virtual Status Start() = 0;

  Status CheckStarted();
  Status UpdatePosition();

  Status WriteRecordBatch(const RecordBatch& batch, FileBlock* block);

  // Adds padding bytes if necessary to ensure all memory blocks are written on
  // 8-byte boundaries.
  Status Align();

  // Write data and update position
  Status Write(const uint8_t* data, int64_t nbytes);

  // Write and align
  Status WriteAligned(const uint8_t* data, int64_t nbytes);

  io::OutputStream* sink_;
  std::shared_ptr<Schema> schema_;
  int64_t position_;
  bool started_;
};

class ARROW_EXPORT StreamWriter : public BaseStreamWriter {
 public:
  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
      std::shared_ptr<StreamWriter>* out);

  Status WriteRecordBatch(const RecordBatch& batch) override;
  Status Close() override;

 private:
  using BaseStreamWriter::BaseStreamWriter;

  Status Start() override;
};

class ARROW_EXPORT BaseStreamReader {
 public:
  ~BaseStreamReader();
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_STREAM_H

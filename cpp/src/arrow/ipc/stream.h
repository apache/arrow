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

class InputStream;
class OutputStream;

}  // namespace io

namespace ipc {

struct FileBlock;
class Message;

class ARROW_EXPORT StreamWriter {
 public:
  virtual ~StreamWriter();

  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
      std::shared_ptr<StreamWriter>* out);

  virtual Status WriteRecordBatch(const RecordBatch& batch);
  virtual Status Close();

 protected:
  StreamWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema);

  virtual Status Start();

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

class ARROW_EXPORT StreamReader {
 public:
  ~StreamReader();

  // Open an stream.
  static Status Open(const std::shared_ptr<io::InputStream>& stream,
      std::shared_ptr<StreamReader>* reader);

  std::shared_ptr<Schema> schema() const;

  // Returned batch is nullptr when end of stream reached
  Status GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch);

 private:
  explicit StreamReader(const std::shared_ptr<io::InputStream>& stream);

  Status ReadSchema();

  Status ReadNextMessage(std::shared_ptr<Message>* message);

  std::shared_ptr<io::InputStream> stream_;
  std::shared_ptr<Schema> schema_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_STREAM_H

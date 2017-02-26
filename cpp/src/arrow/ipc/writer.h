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
#include <vector>

#include "arrow/ipc/metadata.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
struct Field;
class MemoryPool;
class RecordBatch;
class Schema;
class Status;

namespace io {

class OutputStream;

}  // namespace io

namespace ipc {

class ARROW_EXPORT StreamWriter {
 public:
  virtual ~StreamWriter() = default;

  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
      std::shared_ptr<StreamWriter>* out);

  virtual Status WriteRecordBatch(const RecordBatch& batch);

  /// Perform any logic necessary to finish the stream. User is responsible for
  /// closing the actual OutputStream
  virtual Status Close();

  // In some cases, writing may require memory allocation. We use the default
  // memory pool, but provide the option to override
  void set_memory_pool(MemoryPool* pool);

 protected:
  StreamWriter();
  class ARROW_NO_EXPORT StreamWriterImpl;
  std::unique_ptr<StreamWriterImpl> impl_;
};

Status WriteFileFooter(const Schema& schema, const std::vector<FileBlock>& dictionaries,
    const std::vector<FileBlock>& record_batches, DictionaryMemo* dictionary_memo,
    io::OutputStream* out);

class ARROW_EXPORT FileWriter : public StreamWriter {
 public:
  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
      std::shared_ptr<FileWriter>* out);

  Status WriteRecordBatch(const RecordBatch& batch) override;
  Status Close() override;

 private:
  FileWriter();
  class ARROW_NO_EXPORT FileWriterImpl;
  std::unique_ptr<FileWriterImpl> impl_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_STREAM_H

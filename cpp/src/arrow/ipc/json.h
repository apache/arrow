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

// Implement Arrow JSON serialization format

#ifndef ARROW_IPC_JSON_H
#define ARROW_IPC_JSON_H

#include <memory>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/visibility.h"

namespace arrow {

class MemoryPool;

namespace io {

class OutputStream;
class ReadableFileInterface;

}  // namespace io

namespace ipc {

class ARROW_EXPORT JsonWriter {
 public:
  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
      std::shared_ptr<FileWriter>* out);

  // TODO(wesm): Write dictionaries

  Status WriteRecordBatch(
      const std::vector<std::shared_ptr<Array>>& columns, int32_t num_rows);

  Status Close();

 private:
  JsonWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema);

  io::OutputStream* sink_;
  std::shared_ptr<Schema> schema_;

  // Hide RapidJSON details from public API
  class JsonWriterImpl;
  std::unique_ptr<JsonWriterImpl> impl_;
};

class ARROW_EXPORT JsonReader {
 public:
  static Status Open(MemoryPool* pool,
      const std::shared_ptr<io::ReadableFileInterface>& file,
      std::shared_ptr<JsonReader>* reader);

  // Use the default memory pool
  static Status Open(const std::shared_ptr<io::ReadableFileInterface>& file,
      std::shared_ptr<JsonReader>* reader);

  std::shared_ptr<Schema> schema() const;

  int num_record_batches() const;

  // Read a record batch from the file
  Status GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch);

 private:
  explicit JsonReader(const std::shared_ptr<io::ReadableFileInterface>& file);

  std::shared_ptr<io::ReadableFileInterface> file_;
  std::shared_ptr<Schema> schema_;

  // Hide RapidJSON details from public API
  class JsonReaderImpl;
  std::unique_ptr<JsonReaderImpl> impl_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_FILE_H

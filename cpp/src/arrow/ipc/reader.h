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

// Implement Arrow file layout for IPC/RPC purposes and short-lived storage

#ifndef ARROW_IPC_FILE_H
#define ARROW_IPC_FILE_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/ipc/metadata.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class RecordBatch;
class Schema;
class Status;

namespace io {

class InputStream;
class ReadableFileInterface;

}  // namespace io

namespace ipc {

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
  StreamReader();

  class ARROW_NO_EXPORT StreamReaderImpl;
  std::unique_ptr<StreamReaderImpl> impl_;
};

class ARROW_EXPORT FileReader {
 public:
  ~FileReader();

  // Open a file-like object that is assumed to be self-contained; i.e., the
  // end of the file interface is the end of the Arrow file. Note that there
  // can be any amount of data preceding the Arrow-formatted data, because we
  // need only locate the end of the Arrow file stream to discover the metadata
  // and then proceed to read the data into memory.
  static Status Open(const std::shared_ptr<io::ReadableFileInterface>& file,
      std::shared_ptr<FileReader>* reader);

  // If the file is embedded within some larger file or memory region, you can
  // pass the absolute memory offset to the end of the file (which contains the
  // metadata footer). The metadata must have been written with memory offsets
  // relative to the start of the containing file
  //
  // @param file: the data source
  // @param footer_offset: the position of the end of the Arrow "file"
  static Status Open(const std::shared_ptr<io::ReadableFileInterface>& file,
      int64_t footer_offset, std::shared_ptr<FileReader>* reader);

  /// The schema includes any dictionaries
  std::shared_ptr<Schema> schema() const;

  int num_record_batches() const;

  MetadataVersion::type version() const;

  // Read a record batch from the file. Does not copy memory if the input
  // source supports zero-copy.
  //
  // TODO(wesm): Make the copy/zero-copy behavior configurable (e.g. provide an
  // "always copy" option)
  Status GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch);

 private:
  FileReader();

  class ARROW_NO_EXPORT FileReaderImpl;
  std::unique_ptr<FileReaderImpl> impl_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_FILE_H

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

/// Public API for the "Feather" file format, originally created at
/// http://github.com/wesm/feather

#ifndef ARROW_IPC_FEATHER_H
#define ARROW_IPC_FEATHER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"

namespace arrow {

class Buffer;
class Column;
class Status;

namespace io {

class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {
namespace feather {

static constexpr const int kFeatherVersion = 2;

// ----------------------------------------------------------------------
// Metadata accessor classes

class ARROW_EXPORT TableReader {
 public:
  TableReader();
  ~TableReader();

  Status Open(const std::shared_ptr<io::RandomAccessFile>& source);

  static Status OpenFile(const std::string& abspath, std::unique_ptr<TableReader>* out);

  // Optional table description
  //
  // This does not return a const std::string& because a string has to be
  // copied from the flatbuffer to be able to return a non-flatbuffer type
  std::string GetDescription() const;
  bool HasDescription() const;

  int version() const;

  int64_t num_rows() const;
  int64_t num_columns() const;

  std::string GetColumnName(int i) const;

  Status GetColumn(int i, std::shared_ptr<Column>* out);

 private:
  class ARROW_NO_EXPORT TableReaderImpl;
  std::unique_ptr<TableReaderImpl> impl_;
};

class ARROW_EXPORT TableWriter {
 public:
  ~TableWriter();

  static Status Open(
      const std::shared_ptr<io::OutputStream>& stream, std::unique_ptr<TableWriter>* out);

  static Status OpenFile(const std::string& abspath, std::unique_ptr<TableWriter>* out);

  void SetDescription(const std::string& desc);
  void SetNumRows(int64_t num_rows);

  Status Append(const std::string& name, const Array& values);

  // We are done, write the file metadata and footer
  Status Finalize();

 private:
  TableWriter();
  class ARROW_NO_EXPORT TableWriterImpl;
  std::unique_ptr<TableWriterImpl> impl_;
};

}  // namespace feather
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_FEATHER_H

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

#pragma once

#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include "parquet/exception.h"

namespace parquet {

/// \brief The path and filesystem where an actual file is located.
class PARQUET_EXPORT FilePath {
 public:
  FilePath(std::string path, std::shared_ptr<::arrow::fs::FileSystem> filesystem)
      : file_info_(std::move(path)), filesystem_(filesystem) {}

  /// \brief The directory base name (component before the file base name
  std::string dir_name() const { return file_info_.dir_name(); }

  /// \brief The file base name (component after the last directory separator)
  std::string base_name() const { return file_info_.base_name(); }

  /// The full file path in the filesystem
  const std::string& path() const { return file_info_.path(); }
  void set_path(std::string path) { file_info_.set_path(path); }

  /// \brief Return the filesystem.
  const std::shared_ptr<::arrow::fs::FileSystem>& filesystem() const {
    return filesystem_;
  }

  /// \brief Get an OutputStream which wraps this file source.
  ::arrow::Result<std::shared_ptr<::arrow::io::OutputStream>> OpenWriteable() const {
    return filesystem_->OpenOutputStream(file_info_.path());
  }

  /// \brief Open an input file for random access reading.
  ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> OpenReadable() const {
    return filesystem_->OpenInputFile(file_info_.path());
  }

  /// \brief Delete a file.
  void DeleteFile() {
    if (filesystem_->DeleteFile(file_info_.path()) != ::arrow::Status::OK()) {
      throw ParquetException("Failed to delete file " + file_info_.path());
    }
  }

  /// \brief Move / rename a file or directory.
  void Move(const std::string& src, const std::string& dest) {
    if (filesystem_->Move(src, dest) != ::arrow::Status::OK()) {
      throw ParquetException("Failed to rename file " + file_info_.path());
    }
  }

 private:
  ::arrow::fs::FileInfo file_info_;
  std::shared_ptr<::arrow::fs::FileSystem> filesystem_;
};

}  // namespace parquet

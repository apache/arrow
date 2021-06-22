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
  FilePath(const std::string& parent, const std::string& child,
           std::shared_ptr<::arrow::fs::FileSystem> filesystem)
      : parent_(parent), child_(child), filesystem_(filesystem) {}

  /// \brief Return the file parent.
  const std::string& parent() const { return parent_; }

  /// \brief Return the file name.
  const std::string& child() const { return child_; }

  /// \brief Set file name.
  void set_child(const std::string& child) { child_ = child; }

  /// \brief Return the filesystem.
  const std::shared_ptr<::arrow::fs::FileSystem>& filesystem() const {
    return filesystem_;
  }

  /// \brief Get an OutputStream which wraps this file source.
  ::arrow::Result<std::shared_ptr<::arrow::io::OutputStream>> OpenWriteable() const {
    return filesystem_->OpenOutputStream(parent_ + child_);
  }

  /// \brief Open an input file for random access reading.
  ::arrow::Result<std::shared_ptr<::arrow::io::RandomAccessFile>> OpenReadable() const {
    return filesystem_->OpenInputFile(parent_ + child_);
  }

  /// \brief Delete a file.
  void DeleteFile() {
    if (filesystem_->DeleteFile(parent_ + child_) != ::arrow::Status::OK()) {
      throw ParquetException("Failed to delete file " + parent_ + child_);
    }
  }

  /// \brief Move / rename a file or directory.
  void Move(const std::string& src, const std::string& dest) {
    if (filesystem_->Move(src, dest) != ::arrow::Status::OK()) {
      throw ParquetException("Failed to rename file " + parent_ + child_);
    }
  }

 private:
  // file path.
  std::string parent_;
  // File name.
  std::string child_;
  std::shared_ptr<::arrow::fs::FileSystem> filesystem_;
};

}  // namespace parquet

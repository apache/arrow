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

#include <memory>

#include "arrow/dataset/type_fwd.h"
#include "arrow/util/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace dataset {

class ARROW_EXPORT FileLocation {
 public:
  enum LocationType { REMOTE, IN_MEMORY };

  FileLocation(std::string path, fs::FileSystem* filesystem)
      : FileLocation(FileLocation::REMOTE),
        path_(std::move(path)),
        filesystem_(filesystem) {}

  FileLocation(std::shared_ptr<Buffer> buffer)
      : FileLocation(FileLocation::IN_MEMORY), buffer_(std::move(buffer)) {}

 private:
  FileLocation(LocationType type) : type_(type) {}

  FileLocation::type type_;

  //
  std::string path_;
  fs::FileSystem* filesystem_;

  std::shared_ptr<Buffer> buffer_;
};

/// \brief Base class for file scanning options
class ARROW_EXPORT FileScanOptions {
 public:
  virtual ~FileScanOptions() = default;

  /// \brief The name of the file format this options corresponds to
  virtual std::string file_type() const = 0;
};

/// \brief Base class for file writing options
class ARROW_EXPORT FileWriteOptions {
 public:
  virtual ~FileWriteOptions() = default;

  /// \brief The name of the file format this options corresponds to
  virtual std::string file_type() const = 0;
};

/// \brief Base class for file writing options
class ARROW_EXPORT FileFormat {
 public:
  virtual ~FileFormat() = default;

  virtual std::string name() const = 0;

  /// \brief Return true if the given file extension
  virtual bool IsValidExtension(const std::string& ext) const = 0;

  /// \brief Open a file for scanning
  virtual Status ScanFile(const std::string& path, const FileScanOptions& options,
                          fs::FileSystem* filesystem,
                          std::unique_ptr<ScanTaskIterator>* out) const = 0;
};

}  // namespace dataset
}  // namespace arrow

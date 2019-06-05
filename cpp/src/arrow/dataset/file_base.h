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
#include "arrow/util/compression.h"
#include "arrow/dataset/visibility.h"

namespace arrow {
namespace dataset {

/// \brief
class ARROW_DS_EXPORT FileSource {
 public:
  enum SourceType { PATH, BUFFER };

  FileSource(std::string path, fs::FileSystem* filesystem,
             Compression::type compression = Compression::UNCOMPRESSED)
      : FileSource(FileSource::PATH),
        path_(std::move(path)),
        filesystem_(filesystem) {}

  FileSource(std::shared_ptr<Buffer> buffer,
             Compression::type compression = Compression::UNCOMPRESSED)
      : FileSource(FileSource::BUFFER), buffer_(std::move(buffer)) {}

  SourceType type() const { return type_; }

 private:
  FileSource(SourceType type) : type_(type) {}
  SourceType type_;

  // PATH-based source
  std::string path_;
  fs::FileSystem* filesystem_;

  std::shared_ptr<Buffer> buffer_;
};

/// \brief Base class for file scanning options
class ARROW_DS_EXPORT FileScanOptions {
 public:
  virtual ~FileScanOptions() = default;

  /// \brief The name of the file format this options corresponds to
  virtual std::string file_type() const = 0;
};

/// \brief Base class for file writing options
class ARROW_DS_EXPORT FileWriteOptions {
 public:
  virtual ~FileWriteOptions() = default;

  /// \brief The name of the file format this options corresponds to
  virtual std::string file_type() const = 0;
};

/// \brief Base class for file format implementation
class ARROW_DS_EXPORT FileFormat {
 public:
  virtual ~FileFormat() = default;

  virtual std::string name() const = 0;

  /// \brief Return true if the given file extension
  virtual bool IsValidExtension(const std::string& ext) const = 0;

  /// \brief Open a file for scanning
  virtual Status ScanFile(const FileSource& location, const FileScanOptions& options,
                          std::shared_ptr<ScanContext> scan_context,
                          std::unique_ptr<ScanTaskIterator>* out) const = 0;
};

/// \brief A DataFragment that is stored in a file with a known format
class ARROW_DS_EXPORT FileBasedDataFragment : public DataFragment {
 public:
  FileBasedDataFragment(const FileSource& location,
                        std::shared_ptr<FileFormat> format);

  const FileSource& location() const { return location_; }
  std::shared_ptr<FileFormat> format() const { return format_; }

 protected:
  FileSource location_;
  std::shared_ptr<FileFormat> format_;
};

}  // namespace dataset
}  // namespace arrow

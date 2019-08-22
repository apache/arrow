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
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/dataset/writer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/io/file.h"
#include "arrow/util/compression.h"

namespace arrow {
namespace dataset {

/// \brief The path and filesystem where an actual file is located or a buffer which can
/// be read like a file
class ARROW_DS_EXPORT FileSource {
 public:
  enum SourceType { PATH, BUFFER };

  FileSource(std::string path, fs::FileSystem* filesystem,
             Compression::type compression = Compression::UNCOMPRESSED)
      : FileSource(FileSource::PATH, compression) {
    path_ = std::move(path);
    filesystem_ = filesystem;
  }

  FileSource(std::shared_ptr<Buffer> buffer,
             Compression::type compression = Compression::UNCOMPRESSED)
      : FileSource(FileSource::BUFFER, compression) {
    buffer_ = std::move(buffer);
  }

  bool operator==(const FileSource& other) const {
    if (type_ != other.type_) {
      return false;
    } else if (type_ == FileSource::PATH) {
      return path_ == other.path_ && filesystem_ == other.filesystem_;
    } else {
      return buffer_->Equals(*other.buffer_);
    }
  }

  /// \brief The kind of file, whether stored in a filesystem, memory
  /// resident, or other
  SourceType type() const { return type_; }

  /// \brief Return the type of raw compression on the file, if any
  Compression::type compression() const { return compression_; }

  /// \brief Return the file path, if any. Only valid when file source
  /// type is PATH
  std::string path() const { return path_; }

  /// \brief Return the filesystem, if any. Only valid when file
  /// source type is PATH
  fs::FileSystem* filesystem() const { return filesystem_; }

  /// \brief Return the buffer containing the file, if any. Only value
  /// when file source type is BUFFER
  std::shared_ptr<Buffer> buffer() const { return buffer_; }

  /// \brief Get a RandomAccessFile which views this file source
  Status Open(std::shared_ptr<arrow::io::RandomAccessFile>* out) const;

 private:
  explicit FileSource(SourceType type,
                      Compression::type compression = Compression::UNCOMPRESSED)
      : type_(type), compression_(compression) {}
  SourceType type_;
  Compression::type compression_;

  // PATH-based source
  std::string path_;
  fs::FileSystem* filesystem_ = NULLPTR;

  // BUFFER-based source
  std::shared_ptr<Buffer> buffer_;
};

/// \brief Base class for file scanning options
class ARROW_DS_EXPORT FileScanOptions : public ScanOptions {
 public:
  /// \brief The name of the file format this options corresponds to
  virtual std::string file_type() const = 0;
};

/// \brief Base class for file writing options
class ARROW_DS_EXPORT FileWriteOptions : public WriteOptions {
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
  virtual bool IsKnownExtension(const std::string& ext) const = 0;

  /// \brief Open a file for scanning
  virtual Status ScanFile(const FileSource& source,
                          std::shared_ptr<ScanOptions> scan_options,
                          std::shared_ptr<ScanContext> scan_context,
                          std::unique_ptr<ScanTaskIterator>* out) const = 0;

  /// \brief Open a fragment
  virtual Status MakeFragment(const FileSource& location,
                              std::shared_ptr<ScanOptions> opts,
                              std::unique_ptr<DataFragment>* out) = 0;
};

/// \brief A DataFragment that is stored in a file with a known format
class ARROW_DS_EXPORT FileBasedDataFragment : public DataFragment {
 public:
  FileBasedDataFragment(const FileSource& source, std::shared_ptr<FileFormat> format,
                        std::shared_ptr<ScanOptions> scan_options)
      : source_(source),
        format_(std::move(format)),
        scan_options_(std::move(scan_options)) {}

  Status Scan(std::shared_ptr<ScanContext> scan_context,
              std::unique_ptr<ScanTaskIterator>* out) override;

  const FileSource& source() const { return source_; }
  std::shared_ptr<FileFormat> format() const { return format_; }

  std::shared_ptr<ScanOptions> scan_options() const override { return scan_options_; }

 protected:
  FileSource source_;
  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<ScanOptions> scan_options_;
};

/// \brief A DataSource which takes files of one format from a directory
///
/// The directory is crawled upon construction (Make) and not updated afterward.
/// GetFragments() will not include files added after this DataDource is constructed and
/// will error if files are deleted/moved.
class ARROW_DS_EXPORT FileSystemBasedDataSource : public DataSource {
 public:
  static Status Make(fs::FileSystem* filesystem, const fs::Selector& selector,
                     std::shared_ptr<FileFormat> format,
                     std::shared_ptr<ScanOptions> scan_options,
                     std::unique_ptr<FileSystemBasedDataSource>* out);

  std::string type() const override { return "directory"; }

  std::unique_ptr<DataFragmentIterator> GetFragments(
      std::shared_ptr<ScanOptions> options) override;

 protected:
  FileSystemBasedDataSource(fs::FileSystem* filesystem, const fs::Selector& selector,
                            std::shared_ptr<FileFormat> format,
                            std::shared_ptr<ScanOptions> scan_options,
                            std::vector<fs::FileStats> stats);

  fs::FileSystem* filesystem_ = NULLPTR;
  fs::Selector selector_;
  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<ScanOptions> scan_options_;
  std::vector<fs::FileStats> stats_;
};

}  // namespace dataset
}  // namespace arrow

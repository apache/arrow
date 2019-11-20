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
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/dataset/writer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_tree.h"
#include "arrow/io/file.h"
#include "arrow/util/compression.h"
#include "arrow/util/variant.h"

namespace arrow {

namespace dataset {

class Filter;

/// \brief The path and filesystem where an actual file is located or a buffer which can
/// be read like a file
class ARROW_DS_EXPORT FileSource {
 public:
  enum SourceType { PATH, BUFFER };

  FileSource(std::string path, fs::FileSystem* filesystem,
             Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(PathAndFileSystem{std::move(path), filesystem}),
        compression_(compression) {}

  FileSource(std::shared_ptr<Buffer> buffer,
             Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(std::move(buffer)), compression_(compression) {}

  bool operator==(const FileSource& other) const {
    if (type() != other.type()) {
      return false;
    }

    if (type() == PATH) {
      return path() == other.path() && filesystem() == other.filesystem();
    }

    return buffer()->Equals(*other.buffer());
  }

  /// \brief The kind of file, whether stored in a filesystem, memory
  /// resident, or other
  SourceType type() const { return static_cast<SourceType>(impl_.index()); }

  /// \brief Return the type of raw compression on the file, if any
  Compression::type compression() const { return compression_; }

  /// \brief Return the file path, if any. Only valid when file source
  /// type is PATH
  const std::string& path() const {
    static std::string buffer_path = "<Buffer>";
    return type() == PATH ? util::get<PATH>(impl_).path : buffer_path;
  }

  /// \brief Return the filesystem, if any. Only non null when file
  /// source type is PATH
  fs::FileSystem* filesystem() const {
    return type() == PATH ? util::get<PATH>(impl_).filesystem : NULLPTR;
  }

  /// \brief Return the buffer containing the file, if any. Only value
  /// when file source type is BUFFER
  const std::shared_ptr<Buffer>& buffer() const {
    static std::shared_ptr<Buffer> path_buffer = NULLPTR;
    return type() == BUFFER ? util::get<BUFFER>(impl_) : path_buffer;
  }

  /// \brief Get a RandomAccessFile which views this file source
  Result<std::shared_ptr<arrow::io::RandomAccessFile>> Open() const;

 private:
  struct PathAndFileSystem {
    std::string path;
    fs::FileSystem* filesystem;
  };

  util::variant<PathAndFileSystem, std::shared_ptr<Buffer>> impl_;
  Compression::type compression_;
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

  /// \brief Indicate if the FileSource is supported/readable by this format.
  virtual Result<bool> IsSupported(const FileSource& source) const = 0;

  /// \brief Return the schema of the file if possible.
  virtual Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const = 0;

  /// \brief Open a file for scanning
  virtual Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                            ScanOptionsPtr options,
                                            ScanContextPtr context) const = 0;

  /// \brief Open a fragment
  virtual Result<DataFragmentPtr> MakeFragment(const FileSource& location,
                                               ScanOptionsPtr options) = 0;
};

/// \brief A DataFragment that is stored in a file with a known format
class ARROW_DS_EXPORT FileDataFragment : public DataFragment {
 public:
  FileDataFragment(const FileSource& source, FileFormatPtr format,
                   ScanOptionsPtr scan_options)
      : DataFragment(std::move(scan_options)),
        source_(source),
        format_(std::move(format)) {}

  Result<ScanTaskIterator> Scan(ScanContextPtr context) override;

  const FileSource& source() const { return source_; }
  FileFormatPtr format() const { return format_; }

 protected:
  FileSource source_;
  FileFormatPtr format_;
};

/// \brief A DataSource of FileBasedDataFragments.
class ARROW_DS_EXPORT FileSystemDataSource : public DataSource {
 public:
  /// \brief Create a FileSystemBasedDataSource with optional partitions.
  ///
  /// \param[in] filesystem the filesystem which files are from.
  /// \param[in] stats a list of files/directories to consume.
  /// \param[in] source_partition the top-level partition of the DataSource
  /// \param[in] partitions optional partitions attached to FileStats found in
  ///            `stats`.
  /// \param[in] format file format to create fragments from.
  ///
  /// The caller is not required to provide a complete coverage of nodes and
  /// partitions.
  static Result<DataSourcePtr> Make(fs::FileSystemPtr filesystem,
                                    fs::FileStatsVector stats,
                                    ExpressionPtr source_partition,
                                    PathPartitions partitions, FileFormatPtr format);

  // New Make: partition scheme is passed for application to stats.
  static Status Make(fs::FileSystem* filesystem, std::vector<fs::FileStats> stats,
                     std::shared_ptr<Expression> source_partition,
                     const std::shared_ptr<PartitionScheme>& scheme,
                     std::shared_ptr<FileFormat> format,
                     std::shared_ptr<DataSource>* out) {
    return Status::NotImplemented("");
  }

  std::string type() const override { return "filesystem_data_source"; }

 protected:
  DataFragmentIterator GetFragmentsImpl(ScanOptionsPtr options) override;

  FileSystemDataSource(fs::FileSystemPtr filesystem, fs::PathForest forest,
                       ExpressionPtr source_partition, PathPartitions partitions,
                       FileFormatPtr format);
  // New constructor: partition scheme has been applied to each of file_stats.
  // The corresponding fragment level partition information is now in
  // file_partitions.
  // FileSystemBasedDataSource(fs::FileSystem* filesystem,
  //                           std::shared_ptr<Expression> source_partition,
  //                           std::vector<fs::FileStats> file_stats,
  //                           ExpressionVector file_partitions,
  //                           std::shared_ptr<FileFormat> format);

  bool PartitionMatches(const fs::FileStats& stats, ExpressionPtr filter);

  fs::FileSystemPtr filesystem_;
  fs::PathForest forest_;
  PathPartitions partitions_;

  FileFormatPtr format_;
};

}  // namespace dataset
}  // namespace arrow

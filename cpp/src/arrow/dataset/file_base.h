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
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_forest.h"
#include "arrow/io/file.h"
#include "arrow/util/compression.h"
#include "arrow/util/variant.h"

namespace arrow {

namespace dataset {

/// \brief The path and filesystem where an actual file is located or a buffer which can
/// be read like a file
class ARROW_DS_EXPORT FileSource {
 public:
  // NOTE(kszucs): it'd be better to separate the BufferSource from FileSource
  enum DatasetType { PATH, BUFFER };

  FileSource(std::string path, fs::FileSystem* filesystem,
             Compression::type compression = Compression::UNCOMPRESSED,
             bool writable = true)
      : impl_(PathAndFileSystem{std::move(path), filesystem}),
        compression_(compression),
        writable_(writable) {}

  explicit FileSource(std::shared_ptr<Buffer> buffer,
                      Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(std::move(buffer)), compression_(compression) {}

  explicit FileSource(std::shared_ptr<ResizableBuffer> buffer,
                      Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(std::move(buffer)), compression_(compression), writable_(true) {}

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
  DatasetType type() const { return static_cast<DatasetType>(impl_.index()); }

  /// \brief Return the type of raw compression on the file, if any
  Compression::type compression() const { return compression_; }

  /// \brief Whether the this source may be opened writable
  bool writable() const { return writable_; }

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

  /// \brief Get an OutputStream which wraps this file source
  Result<std::shared_ptr<arrow::io::OutputStream>> OpenWritable() const;

 private:
  struct PathAndFileSystem {
    std::string path;
    fs::FileSystem* filesystem;
  };

  util::variant<PathAndFileSystem, std::shared_ptr<Buffer>> impl_;
  Compression::type compression_;
  bool writable_ = false;
};

/// \brief Base class for file format implementation
class ARROW_DS_EXPORT FileFormat : public std::enable_shared_from_this<FileFormat> {
 public:
  virtual ~FileFormat() = default;

  /// \brief The name identifying the kind of file format
  virtual std::string type_name() const = 0;

  /// \brief Return true if fragments of this format can benefit from parallel scanning.
  virtual bool splittable() const { return false; }

  /// \brief Indicate if the FileSource is supported/readable by this format.
  virtual Result<bool> IsSupported(const FileSource& source) const = 0;

  /// \brief Return the schema of the file if possible.
  virtual Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const = 0;

  /// \brief Open a file for scanning
  virtual Result<ScanTaskIterator> ScanFile(
      const FileSource& source, std::shared_ptr<ScanOptions> options,
      std::shared_ptr<ScanContext> context) const = 0;

  /// \brief Open a fragment
  virtual Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, std::shared_ptr<ScanOptions> options,
      std::shared_ptr<Expression> partition_expression);

  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, std::shared_ptr<ScanOptions> options);

  /// \brief Write a fragment. If the parent directory of destination does not exist, it
  /// will be created.
  virtual Result<std::shared_ptr<WriteTask>> WriteFragment(
      FileSource destination, std::shared_ptr<Fragment> fragment,
      std::shared_ptr<ScanContext> scan_context);  // FIXME(bkietz) make this pure virtual
};

/// \brief A Fragment that is stored in a file with a known format
class ARROW_DS_EXPORT FileFragment : public Fragment {
 public:
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanContext> context) override;

  std::string type_name() const override { return format_->type_name(); }
  bool splittable() const override { return format_->splittable(); }

  const FileSource& source() const { return source_; }
  const std::shared_ptr<FileFormat>& format() const { return format_; }

 protected:
  FileFragment(FileSource source, std::shared_ptr<FileFormat> format,
               std::shared_ptr<ScanOptions> scan_options,
               std::shared_ptr<Expression> partition_expression)
      : Fragment(std::move(scan_options), std::move(partition_expression)),
        source_(std::move(source)),
        format_(std::move(format)) {}

  FileSource source_;
  std::shared_ptr<FileFormat> format_;

  friend class FileFormat;
};

/// \brief A Dataset of FileFragments.
class ARROW_DS_EXPORT FileSystemDataset : public Dataset {
 public:
  /// \brief Create a FileSystemDataset.
  ///
  /// \param[in] schema the top-level schema of the DataDataset
  /// \param[in] root_partition the top-level partition of the DataDataset
  /// \param[in] format file format to create fragments from.
  /// \param[in] filesystem the filesystem which files are from.
  /// \param[in] infos a list of files/directories to consume.
  /// attach additional partition expressions to FileInfo found in `infos`.
  ///
  /// The caller is not required to provide a complete coverage of nodes and
  /// partitions.
  static Result<std::shared_ptr<FileSystemDataset>> Make(
      std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
      std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
      std::vector<fs::FileInfo> infos);

  /// \brief Create a FileSystemDataset with file-level partitions.
  ///
  /// \param[in] schema the top-level schema of the DataDataset
  /// \param[in] root_partition the top-level partition of the DataDataset
  /// \param[in] format file format to create fragments from.
  /// \param[in] filesystem the filesystem which files are from.
  /// \param[in] infos a list of files/directories to consume.
  /// \param[in] partitions partition information associated with `infos`.
  /// attach additional partition expressions to FileInfo found in `infos`.
  ///
  /// The caller is not required to provide a complete coverage of nodes and
  /// partitions.
  static Result<std::shared_ptr<FileSystemDataset>> Make(
      std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
      std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
      std::vector<fs::FileInfo> infos, ExpressionVector partitions);

  /// \brief Create a FileSystemDataset with file-level partitions.
  ///
  /// \param[in] schema the top-level schema of the DataDataset
  /// \param[in] root_partition the top-level partition of the DataDataset
  /// \param[in] format file format to create fragments from.
  /// \param[in] filesystem the filesystem which files are from.
  /// \param[in] forest a PathForest of files/directories to consume.
  /// \param[in] partitions partition information associated with `forest`.
  /// attach additional partition expressions to FileInfo found in `forest`.
  ///
  /// The caller is not required to provide a complete coverage of nodes and
  /// partitions.
  static Result<std::shared_ptr<FileSystemDataset>> Make(
      std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
      std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
      fs::PathForest forest, ExpressionVector partitions);

  /// \brief Write to a new format and filesystem location, preserving partitioning.
  ///
  /// \param[in] plan the WritePlan to execute.
  /// \param[in] scan_context context in which to scan fragments before writing.
  static Result<std::shared_ptr<FileSystemDataset>> Write(
      const WritePlan& plan, std::shared_ptr<ScanContext> scan_context);

  std::string type_name() const override { return "filesystem"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

  const std::shared_ptr<FileFormat>& format() const { return format_; }

  std::vector<std::string> files() const;

  const ExpressionVector& partitions() const { return partitions_; }

  std::string ToString() const;

 protected:
  FragmentIterator GetFragmentsImpl(std::shared_ptr<ScanOptions> options) override;

  FileSystemDataset(std::shared_ptr<Schema> schema,
                    std::shared_ptr<Expression> root_partition,
                    std::shared_ptr<FileFormat> format,
                    std::shared_ptr<fs::FileSystem> filesystem, fs::PathForest forest,
                    ExpressionVector file_partitions);

  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<fs::FileSystem> filesystem_;
  fs::PathForest forest_;
  ExpressionVector partitions_;
};

/// \brief Write a fragment to a single OutputStream.
class ARROW_DS_EXPORT WriteTask {
 public:
  virtual Status Execute() = 0;

  virtual ~WriteTask() = default;

  const FileSource& destination() const;
  const std::shared_ptr<FileFormat>& format() const { return format_; }

 protected:
  WriteTask(FileSource destination, std::shared_ptr<FileFormat> format)
      : destination_(std::move(destination)), format_(std::move(format)) {}

  Status CreateDestinationParentDir() const;

  FileSource destination_;
  std::shared_ptr<FileFormat> format_;
};

/// \brief A declarative plan for writing fragments to a partitioned directory structure.
class ARROW_DS_EXPORT WritePlan {
 public:
  /// The partitioning with which paths were generated
  std::shared_ptr<Partitioning> partitioning;

  /// The schema of the Dataset which will be written
  std::shared_ptr<Schema> schema;

  /// The format into which fragments will be written
  std::shared_ptr<FileFormat> format;

  /// The FileSystem and base directory for partitioned writing
  std::shared_ptr<fs::FileSystem> filesystem;
  std::string partition_base_dir;

  using FragmentOrPartitionExpression =
      util::variant<std::shared_ptr<Expression>, std::shared_ptr<Fragment>>;

  /// If fragment_or_partition_expressions[i] is a Fragment, that Fragment will be
  /// written to paths[i]. If it is an Expression, a directory representing that partition
  /// expression will be created at paths[i] instead.
  std::vector<FragmentOrPartitionExpression> fragment_or_partition_expressions;
  std::vector<std::string> paths;
};

}  // namespace dataset
}  // namespace arrow

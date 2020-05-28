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

// This API is EXPERIMENTAL.

#pragma once

#include <functional>
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

namespace arrow {

namespace dataset {

/// \brief The path and filesystem where an actual file is located or a buffer which can
/// be read like a file
class ARROW_DS_EXPORT FileSource {
 public:
  FileSource(std::string path, std::shared_ptr<fs::FileSystem> filesystem,
             Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(PathAndFileSystem{std::move(path), std::move(filesystem)}),
        compression_(compression) {}

  explicit FileSource(std::shared_ptr<Buffer> buffer,
                      Compression::type compression = Compression::UNCOMPRESSED)
      : source_kind_(BUFFER), buffer_(std::move(buffer)), compression_(compression) {}

  using CustomOpen = std::function<Result<std::shared_ptr<io::RandomAccessFile>>()>;
  explicit FileSource(CustomOpen open) : impl_(std::move(open)) {}

  using CustomOpenWithCompression =
      std::function<Result<std::shared_ptr<io::RandomAccessFile>>(Compression::type)>;
  explicit FileSource(CustomOpenWithCompression open_with_compression,
                      Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(std::bind(std::move(open_with_compression), compression)),
        compression_(compression) {}

  explicit FileSource(std::shared_ptr<io::RandomAccessFile> file,
                      Compression::type compression = Compression::UNCOMPRESSED)
      : impl_([=] { return ToResult(file); }), compression_(compression) {}

  FileSource() : impl_(CustomOpen{&InvalidOpen}) {}

  /// \brief Return the type of raw compression on the file, if any
  Compression::type compression() const { return compression_; }

  /// \brief Return the file path, if any. Only valid when file source wraps a path.
  const std::string& path() const {
    if (IsPath()) {
      return util::get<PathAndFileSystem>(impl_).path;
    }
    if (IsBuffer()) {
      static std::string no_path = "<Buffer>";
      return no_path;
    } else {
      static std::string no_path = "<CustomOpen>";
      return no_path;
    }
  }

  /// \brief Return the filesystem, if any. Otherwise returns nullptr
  const std::shared_ptr<fs::FileSystem>& filesystem() const {
    if (!IsPath()) {
      static std::shared_ptr<fs::FileSystem> no_fs = NULLPTR;
      return no_fs;
    }
    return util::get<PathAndFileSystem>(impl_).filesystem;
  }

  /// \brief Return the buffer containing the file, if any. Only value
  /// when file source type is BUFFER
  const std::shared_ptr<Buffer>& buffer() const {
    if (!IsBuffer()) {
      static std::shared_ptr<Buffer> no_buffer = NULLPTR;
      return no_buffer;
    }
    return util::get<std::shared_ptr<Buffer>>(impl_);
  }

  /// \brief Get a RandomAccessFile which views this file source
  Result<std::shared_ptr<io::RandomAccessFile>> Open() const;

 private:
  static Result<std::shared_ptr<io::RandomAccessFile>> InvalidOpen() {
    return Status::Invalid("Called Open() on an uninitialized FileSource");
  }

  bool IsPath() const { return util::holds_alternative<PathAndFileSystem>(impl_); }

  bool IsBuffer() const {
    return util::holds_alternative<std::shared_ptr<Buffer>>(impl_);
  }

  struct PathAndFileSystem {
    std::string path;
    std::shared_ptr<fs::FileSystem> filesystem;
  };

  util::variant<PathAndFileSystem, std::shared_ptr<Buffer>, CustomOpen> impl_;
  Compression::type compression_ = Compression::UNCOMPRESSED;
};

/// \brief The path and filesystem where an actual file is located or a buffer which can
/// be written to like a file
class ARROW_DS_EXPORT WritableFileSource {
 public:
  WritableFileSource(std::string path, std::shared_ptr<fs::FileSystem> filesystem,
                     Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(PathAndFileSystem{std::move(path), std::move(filesystem)}),
        compression_(compression) {}

  explicit WritableFileSource(std::shared_ptr<ResizableBuffer> buffer,
                              Compression::type compression = Compression::UNCOMPRESSED)
      : impl_(std::move(buffer)), compression_(compression) {}

  /// \brief Return the type of raw compression on the file, if any
  Compression::type compression() const { return compression_; }

  /// \brief Return the file path, if any. Only valid when file source wraps a path.
  const std::string& path() const {
    if (IsPath()) {
      return util::get<PathAndFileSystem>(impl_).path;
    }
    static std::string no_path = "<Buffer>";
    return no_path;
  }

  /// \brief Return the filesystem, if any. Otherwise returns nullptr
  const std::shared_ptr<fs::FileSystem>& filesystem() const {
    if (!IsPath()) {
      static std::shared_ptr<fs::FileSystem> no_fs = NULLPTR;
      return no_fs;
    }
    return util::get<PathAndFileSystem>(impl_).filesystem;
  }

  /// \brief Return the buffer containing the file, if any. Otherwise returns nullptr
  const std::shared_ptr<ResizableBuffer>& buffer() const {
    if (!IsBuffer()) {
      static std::shared_ptr<ResizableBuffer> no_buffer = NULLPTR;
      return no_buffer;
    }
    return util::get<std::shared_ptr<ResizableBuffer>>(impl_);
  }

  /// \brief Get an OutputStream which wraps this file source
  Result<std::shared_ptr<arrow::io::OutputStream>> Open() const;

 private:
  bool IsPath() const { return util::holds_alternative<PathAndFileSystem>(impl_); }

  bool IsBuffer() const {
    return util::holds_alternative<std::shared_ptr<ResizableBuffer>>(impl_);
  }

  struct PathAndFileSystem {
    std::string path;
    std::shared_ptr<fs::FileSystem> filesystem;
  };

  util::variant<PathAndFileSystem, std::shared_ptr<ResizableBuffer>> impl_;
  Compression::type compression_;
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
      FileSource source, std::shared_ptr<Expression> partition_expression);

  Result<std::shared_ptr<FileFragment>> MakeFragment(FileSource source);

  /// \brief Write a fragment. If the parent directory of destination does not exist, it
  /// will be created.
  virtual Result<std::shared_ptr<WriteTask>> WriteFragment(
      WritableFileSource destination, std::shared_ptr<Fragment> fragment,
      std::shared_ptr<ScanOptions> options,
      std::shared_ptr<ScanContext> scan_context);  // FIXME(bkietz) make this pure virtual
};

/// \brief A Fragment that is stored in a file with a known format
class ARROW_DS_EXPORT FileFragment : public Fragment {
 public:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchema() override;

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                std::shared_ptr<ScanContext> context) override;

  std::string type_name() const override { return format_->type_name(); }
  bool splittable() const override { return format_->splittable(); }

  const FileSource& source() const { return source_; }
  const std::shared_ptr<FileFormat>& format() const { return format_; }

 protected:
  FileFragment(FileSource source, std::shared_ptr<FileFormat> format,
               std::shared_ptr<Expression> partition_expression)
      : Fragment(std::move(partition_expression)),
        source_(std::move(source)),
        format_(std::move(format)) {}

  FileSource source_;
  std::shared_ptr<FileFormat> format_;

  friend class FileFormat;
};

/// \brief A Dataset of FileFragments.
///
/// A FileSystemDataset is composed of one or more FileFragment. The fragments
/// are independent and don't need to share the same format and/or filesystem.
class ARROW_DS_EXPORT FileSystemDataset : public Dataset {
 public:
  /// \brief Create a FileSystemDataset.
  ///
  /// \param[in] schema the schema of the dataset
  /// \param[in] root_partition the partition expression of the dataset
  /// \param[in] format the format of each FileFragment.
  /// \param[in] fragments list of fragments to create the dataset from
  ///
  /// Note that all fragment must be of `FileFragment` type. The type are
  /// erased to simplify callers.
  ///
  /// \return A constructed dataset.
  static Result<std::shared_ptr<FileSystemDataset>> Make(
      std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
      std::shared_ptr<FileFormat> format,
      std::vector<std::shared_ptr<FileFragment>> fragments);

  /// \brief Write to a new format and filesystem location, preserving partitioning.
  ///
  /// \param[in] plan the WritePlan to execute.
  /// \param[in] scan_options options in which to scan fragments
  /// \param[in] scan_context context in which to scan fragments before writing.
  static Result<std::shared_ptr<FileSystemDataset>> Write(
      const WritePlan& plan, std::shared_ptr<ScanOptions> scan_options,
      std::shared_ptr<ScanContext> scan_context);

  /// \brief Return the type name of the dataset.
  std::string type_name() const override { return "filesystem"; }

  /// \brief Replace the schema of the dataset.
  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

  /// \brief Return the path of files.
  std::vector<std::string> files() const;

  /// \brief Return the format.
  const std::shared_ptr<FileFormat>& format() const { return format_; }

  std::string ToString() const;

 protected:
  FragmentIterator GetFragmentsImpl(std::shared_ptr<Expression> predicate) override;

  FileSystemDataset(std::shared_ptr<Schema> schema,
                    std::shared_ptr<Expression> root_partition,
                    std::shared_ptr<FileFormat> format,
                    std::vector<std::shared_ptr<FileFragment>> fragments);

  std::shared_ptr<FileFormat> format_;
  std::vector<std::shared_ptr<FileFragment>> fragments_;
};

/// \brief Write a fragment to a single OutputStream.
class ARROW_DS_EXPORT WriteTask {
 public:
  virtual Status Execute() = 0;

  virtual ~WriteTask() = default;

  const WritableFileSource& destination() const;
  const std::shared_ptr<FileFormat>& format() const { return format_; }

 protected:
  WriteTask(WritableFileSource destination, std::shared_ptr<FileFormat> format)
      : destination_(std::move(destination)), format_(std::move(format)) {}

  Status CreateDestinationParentDir() const;

  WritableFileSource destination_;
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

  class FragmentOrPartitionExpression {
   public:
    enum Kind { EXPRESSION, FRAGMENT };

    explicit FragmentOrPartitionExpression(std::shared_ptr<Expression> partition_expr)
        : kind_(EXPRESSION), partition_expr_(std::move(partition_expr)) {}

    explicit FragmentOrPartitionExpression(std::shared_ptr<Fragment> fragment)
        : kind_(FRAGMENT), fragment_(std::move(fragment)) {}

    Kind kind() const { return kind_; }

    const std::shared_ptr<Expression>& partition_expr() const { return partition_expr_; }
    const std::shared_ptr<Fragment>& fragment() const { return fragment_; }

   private:
    Kind kind_;
    std::shared_ptr<Expression> partition_expr_;
    std::shared_ptr<Fragment> fragment_;
  };

  /// If fragment_or_partition_expressions[i] is a Fragment, that Fragment will be
  /// written to paths[i]. If it is an Expression, a directory representing that partition
  /// expression will be created at paths[i] instead.
  std::vector<FragmentOrPartitionExpression> fragment_or_partition_expressions;
  std::vector<std::string> paths;
};

}  // namespace dataset
}  // namespace arrow

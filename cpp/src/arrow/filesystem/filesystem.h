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

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/compare.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

// The Windows API defines macros from *File resolving to either
// *FileA or *FileW.  Need to undo them.
#ifdef _WIN32
#ifdef DeleteFile
#undef DeleteFile
#endif
#ifdef CopyFile
#undef CopyFile
#endif
#endif

namespace arrow {

namespace io {

class InputStream;
class LatencyGenerator;
class OutputStream;
class RandomAccessFile;

}  // namespace io

namespace fs {

// A system clock time point expressed as a 64-bit (or more) number of
// nanoseconds since the epoch.
using TimePoint =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

/// \brief FileSystem entry type
enum class ARROW_EXPORT FileType : int8_t {
  /// Entry does not exist
  NonExistent,
  /// Entry exists but its type is unknown
  ///
  /// This can designate a special file such as a Unix socket or character
  /// device, or Windows NUL / CON / ...
  Unknown,
  /// Entry is a regular file
  File,
  /// Entry is a directory
  Directory
};

ARROW_EXPORT std::string ToString(FileType);

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, FileType);

static const int64_t kNoSize = -1;
static const TimePoint kNoTime = TimePoint(TimePoint::duration(-1));

/// \brief FileSystem entry stats
struct ARROW_EXPORT FileStats : public util::EqualityComparable<FileStats> {
  FileStats() = default;
  FileStats(FileStats&&) = default;
  FileStats& operator=(FileStats&&) = default;
  FileStats(const FileStats&) = default;
  FileStats& operator=(const FileStats&) = default;

  /// The file type
  FileType type() const { return type_; }
  void set_type(FileType type) { type_ = type; }

  /// The full file path in the filesystem
  const std::string& path() const { return path_; }
  void set_path(std::string path) { path_ = std::move(path); }

  /// The file base name (component after the last directory separator)
  std::string base_name() const;

  // The directory base name (component before the file base name).
  std::string dir_name() const;

  /// The size in bytes, if available
  ///
  /// Only regular files are guaranteed to have a size.
  int64_t size() const { return size_; }
  void set_size(int64_t size) { size_ = size; }

  /// The file extension (excluding the dot)
  std::string extension() const;

  /// The time of last modification, if available
  TimePoint mtime() const { return mtime_; }
  void set_mtime(TimePoint mtime) { mtime_ = mtime; }

  bool IsFile() const { return type_ == FileType::File; }
  bool IsDirectory() const { return type_ == FileType::Directory; }

  bool Equals(const FileStats& other) const {
    return type() == other.type() && path() == other.path() && size() == other.size() &&
           mtime() == other.mtime();
  }

  std::string ToString() const;

  /// Function object implementing less-than comparison and hashing
  /// by path, to support sorting stats, using them as keys, and other
  /// interactions with the STL.
  struct ByPath {
    bool operator()(const FileStats& l, const FileStats& r) const {
      return l.path() < r.path();
    }

    size_t operator()(const FileStats& s) const {
      return std::hash<std::string>{}(s.path());
    }
  };

 protected:
  FileType type_ = FileType::Unknown;
  std::string path_;
  int64_t size_ = kNoSize;
  TimePoint mtime_ = kNoTime;
};

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const FileStats&);

/// \brief File selector for filesystem APIs
struct ARROW_EXPORT FileSelector {
  /// The directory in which to select files.
  /// If the path exists but doesn't point to a directory, this should be an error.
  std::string base_dir;
  /// The behavior if `base_dir` doesn't exist in the filesystem.  If false,
  /// an error is returned.  If true, an empty selection is returned.
  bool allow_non_existent = false;
  /// Whether to recurse into subdirectories.
  bool recursive = false;
  /// The maximum number of subdirectories to recurse into.
  int32_t max_recursion = INT32_MAX;

  FileSelector() {}
};

/// \brief Abstract file system API
class ARROW_EXPORT FileSystem {
 public:
  virtual ~FileSystem();

  virtual std::string type_name() const = 0;

  /// Get statistics for the given target.
  ///
  /// Any symlink is automatically dereferenced, recursively.
  /// A non-existing or unreachable file returns an Ok status and
  /// has a FileType of value NonExistent.  An error status indicates
  /// a truly exceptional condition (low-level I/O error, etc.).
  virtual Result<FileStats> GetTargetStats(const std::string& path) = 0;
  /// Same, for many targets at once.
  virtual Result<std::vector<FileStats>> GetTargetStats(
      const std::vector<std::string>& paths);
  /// Same, according to a selector.
  ///
  /// The selector's base directory will not be part of the results, even if
  /// it exists.
  /// If it doesn't exist, see `FileSelector::allow_non_existent`.
  virtual Result<std::vector<FileStats>> GetTargetStats(const FileSelector& select) = 0;

  /// Create a directory and subdirectories.
  ///
  /// This function succeeds if the directory already exists.
  virtual Status CreateDir(const std::string& path, bool recursive = true) = 0;

  /// Delete a directory and its contents, recursively.
  virtual Status DeleteDir(const std::string& path) = 0;

  /// Delete a directory's contents, recursively.
  ///
  /// Like DeleteDir, but doesn't delete the directory itself.
  /// Passing an empty path ("") will wipe the entire filesystem tree.
  virtual Status DeleteDirContents(const std::string& path) = 0;

  /// Delete a file.
  virtual Status DeleteFile(const std::string& path) = 0;
  /// Delete many files.
  ///
  /// The default implementation issues individual delete operations in sequence.
  virtual Status DeleteFiles(const std::vector<std::string>& paths);

  /// Move / rename a file or directory.
  ///
  /// If the destination exists:
  /// - if it is a non-empty directory, an error is returned
  /// - otherwise, if it has the same type as the source, it is replaced
  /// - otherwise, behavior is unspecified (implementation-dependent).
  virtual Status Move(const std::string& src, const std::string& dest) = 0;

  /// Copy a file.
  ///
  /// If the destination exists and is a directory, an error is returned.
  /// Otherwise, it is replaced.
  virtual Status CopyFile(const std::string& src, const std::string& dest) = 0;

  /// Open an input stream for sequential reading.
  virtual Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) = 0;

  /// Open an input file for random access reading.
  virtual Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) = 0;

  /// Open an output stream for sequential writing.
  ///
  /// If the target already exists, existing data is truncated.
  virtual Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path) = 0;

  /// Open an output stream for appending.
  ///
  /// If the target doesn't exist, a new empty file is created.
  virtual Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path) = 0;
};

/// \brief A FileSystem implementation that delegates to another
/// implementation after prepending a fixed base path.
///
/// This is useful to expose a logical view of a subtree of a filesystem,
/// for example a directory in a LocalFileSystem.
/// This works on abstract paths, i.e. paths using forward slashes and
/// and a single root "/".  Windows paths are not guaranteed to work.
/// This makes no security guarantee.  For example, symlinks may allow to
/// "escape" the subtree and access other parts of the underlying filesystem.
class ARROW_EXPORT SubTreeFileSystem : public FileSystem {
 public:
  explicit SubTreeFileSystem(const std::string& base_path,
                             std::shared_ptr<FileSystem> base_fs);
  ~SubTreeFileSystem() override;

  std::string type_name() const override { return "subtree"; }

  /// \cond FALSE
  using FileSystem::GetTargetStats;
  /// \endcond
  Result<FileStats> GetTargetStats(const std::string& path) override;
  Result<std::vector<FileStats>> GetTargetStats(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;
  Status DeleteDirContents(const std::string& path) override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;
  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path) override;

 protected:
  const std::string base_path_;
  std::shared_ptr<FileSystem> base_fs_;

  std::string PrependBase(const std::string& s) const;
  Status PrependBaseNonEmpty(std::string* s) const;
  Status StripBase(const std::string& s, std::string* out) const;
  Status FixStats(FileStats* st) const;
};

/// \brief A FileSystem implementation that delegates to another
/// implementation but inserts latencies at various points.
class ARROW_EXPORT SlowFileSystem : public FileSystem {
 public:
  SlowFileSystem(std::shared_ptr<FileSystem> base_fs,
                 std::shared_ptr<io::LatencyGenerator> latencies);
  SlowFileSystem(std::shared_ptr<FileSystem> base_fs, double average_latency);
  SlowFileSystem(std::shared_ptr<FileSystem> base_fs, double average_latency,
                 int32_t seed);

  std::string type_name() const override { return "slow"; }

  using FileSystem::GetTargetStats;
  Result<FileStats> GetTargetStats(const std::string& path) override;
  Result<std::vector<FileStats>> GetTargetStats(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;
  Status DeleteDirContents(const std::string& path) override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;
  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path) override;
  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path) override;

 protected:
  std::shared_ptr<FileSystem> base_fs_;
  std::shared_ptr<io::LatencyGenerator> latencies_;
};

/// \defgroup filesystem-factories Functions for creating FileSystem instances
///
/// @{

/// \brief Create a new FileSystem by URI
///
/// A scheme-less URI is considered a local filesystem path.
/// Recognized schemes are "file", "mock" and "hdfs".
///
/// \param[in] uri a URI-based path, ex: file:///some/local/path
/// \param[out] out_path (optional) Path inside the filesystem.
/// \return out_fs FileSystem instance.
ARROW_EXPORT
Result<std::shared_ptr<FileSystem>> FileSystemFromUri(const std::string& uri,
                                                      std::string* out_path = NULLPTR);

/// @}

/// \brief Create a new FileSystem by URI
///
/// A scheme-less URI is considered a local filesystem path.
/// Recognized schemes are "file", "mock" and "hdfs".
///
/// \param[in] uri a URI-based path, ex: file:///some/local/path
/// \param[out] out_fs FileSystem instance.
/// \param[out] out_path (optional) Path inside the filesystem.
/// \return Status
ARROW_DEPRECATED("Use Result-returning version")
Status FileSystemFromUri(const std::string& uri, std::shared_ptr<FileSystem>* out_fs,
                         std::string* out_path = NULLPTR);

}  // namespace fs
}  // namespace arrow

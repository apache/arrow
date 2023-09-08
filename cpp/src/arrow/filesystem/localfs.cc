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

#include <chrono>
#include <cstring>
#include <memory>
#include <sstream>
#include <utility>

#ifdef _WIN32
#include "arrow/util/windows_compatibility.h"
#else
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#endif

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/type_fwd.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/file.h"
#include "arrow/io/type_fwd.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/io_util.h"
#include "arrow/util/uri.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {
namespace fs {

using ::arrow::internal::IOErrorFromErrno;
#ifdef _WIN32
using ::arrow::internal::IOErrorFromWinError;
#endif
using ::arrow::internal::NativePathString;
using ::arrow::internal::PlatformFilename;

namespace {

Status ValidatePath(std::string_view s) {
  if (internal::IsLikelyUri(s)) {
    return Status::Invalid("Expected a local filesystem path, got a URI: '", s, "'");
  }
  return Status::OK();
}

Result<std::string> DoNormalizePath(std::string path) {
  RETURN_NOT_OK(ValidatePath(path));
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  return fn.ToString();
}

#ifdef _WIN32

std::string NativeToString(const NativePathString& ns) {
  PlatformFilename fn(ns);
  return fn.ToString();
}

TimePoint ToTimePoint(FILETIME ft) {
  // Hundreds of nanoseconds between January 1, 1601 (UTC) and the Unix epoch.
  static constexpr int64_t kFileTimeEpoch = 11644473600LL * 10000000;

  int64_t hundreds = (static_cast<int64_t>(ft.dwHighDateTime) << 32) + ft.dwLowDateTime -
                     kFileTimeEpoch;  // hundreds of ns since Unix epoch
  std::chrono::nanoseconds ns_count(100 * hundreds);
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
}

FileInfo FileInformationToFileInfo(const BY_HANDLE_FILE_INFORMATION& information) {
  FileInfo info;
  if (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
    info.set_type(FileType::Directory);
    info.set_size(kNoSize);
  } else {
    // Regular file
    info.set_type(FileType::File);
    info.set_size((static_cast<int64_t>(information.nFileSizeHigh) << 32) +
                  information.nFileSizeLow);
  }
  info.set_mtime(ToTimePoint(information.ftLastWriteTime));
  return info;
}

Result<FileInfo> StatFile(const std::wstring& path) {
  HANDLE h;
  std::string bytes_path = NativeToString(path);
  FileInfo info;

  /* Inspired by CPython, see Modules/posixmodule.c */
  h = CreateFileW(path.c_str(), FILE_READ_ATTRIBUTES, /* desired access */
                  0,                                  /* share mode */
                  NULL,                               /* security attributes */
                  OPEN_EXISTING,
                  /* FILE_FLAG_BACKUP_SEMANTICS is required to open a directory */
                  FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS, NULL);

  if (h == INVALID_HANDLE_VALUE) {
    DWORD err = GetLastError();
    if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) {
      info.set_path(bytes_path);
      info.set_type(FileType::NotFound);
      info.set_mtime(kNoTime);
      info.set_size(kNoSize);
      return info;
    } else {
      return IOErrorFromWinError(GetLastError(), "Failed querying information for path '",
                                 bytes_path, "'");
    }
  }
  BY_HANDLE_FILE_INFORMATION information;
  if (!GetFileInformationByHandle(h, &information)) {
    CloseHandle(h);
    return IOErrorFromWinError(GetLastError(), "Failed querying information for path '",
                               bytes_path, "'");
  }
  CloseHandle(h);
  info = FileInformationToFileInfo(information);
  info.set_path(bytes_path);
  return info;
}

#else  // POSIX systems

TimePoint ToTimePoint(const struct timespec& s) {
  std::chrono::nanoseconds ns_count(static_cast<int64_t>(s.tv_sec) * 1000000000 +
                                    static_cast<int64_t>(s.tv_nsec));
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
}

FileInfo StatToFileInfo(const struct stat& s) {
  FileInfo info;
  if (S_ISREG(s.st_mode)) {
    info.set_type(FileType::File);
    info.set_size(static_cast<int64_t>(s.st_size));
  } else if (S_ISDIR(s.st_mode)) {
    info.set_type(FileType::Directory);
    info.set_size(kNoSize);
  } else {
    info.set_type(FileType::Unknown);
    info.set_size(kNoSize);
  }
#ifdef __APPLE__
  // macOS doesn't use the POSIX-compliant spelling
  info.set_mtime(ToTimePoint(s.st_mtimespec));
#else
  info.set_mtime(ToTimePoint(s.st_mtim));
#endif
  return info;
}

Result<FileInfo> StatFile(const std::string& path) {
  FileInfo info;
  struct stat s;
  int r = stat(path.c_str(), &s);
  if (r == -1) {
    if (errno == ENOENT || errno == ENOTDIR || errno == ELOOP) {
      info.set_type(FileType::NotFound);
      info.set_mtime(kNoTime);
      info.set_size(kNoSize);
    } else {
      return IOErrorFromErrno(errno, "Failed stat()ing path '", path, "'");
    }
  } else {
    info = StatToFileInfo(s);
  }
  info.set_path(path);
  return info;
}

#endif

Status StatSelector(const PlatformFilename& dir_fn, const FileSelector& select,
                    int32_t nesting_depth, std::vector<FileInfo>* out) {
  auto result = ListDir(dir_fn);
  if (!result.ok()) {
    auto status = result.status();
    if (select.allow_not_found && status.IsIOError()) {
      ARROW_ASSIGN_OR_RAISE(bool exists, FileExists(dir_fn));
      if (!exists) {
        return Status::OK();
      }
    }
    return status;
  }

  for (const auto& child_fn : *result) {
    PlatformFilename full_fn = dir_fn.Join(child_fn);
    ARROW_ASSIGN_OR_RAISE(FileInfo info, StatFile(full_fn.ToNative()));
    if (info.type() != FileType::NotFound) {
      out->push_back(std::move(info));
    }
    if (nesting_depth < select.max_recursion && select.recursive &&
        info.type() == FileType::Directory) {
      RETURN_NOT_OK(StatSelector(full_fn, select, nesting_depth + 1, out));
    }
  }
  return Status::OK();
}

}  // namespace

LocalFileSystemOptions LocalFileSystemOptions::Defaults() {
  return LocalFileSystemOptions();
}

bool LocalFileSystemOptions::Equals(const LocalFileSystemOptions& other) const {
  return use_mmap == other.use_mmap && directory_readahead == other.directory_readahead &&
         file_info_batch_size == other.file_info_batch_size;
}

Result<LocalFileSystemOptions> LocalFileSystemOptions::FromUri(
    const ::arrow::internal::Uri& uri, std::string* out_path) {
  if (!uri.username().empty() || !uri.password().empty()) {
    return Status::Invalid("Unsupported username or password in local URI: '",
                           uri.ToString(), "'");
  }
  std::string path;
  const auto host = uri.host();
  if (!host.empty()) {
#ifdef _WIN32
    std::stringstream ss;
    ss << "//" << host << "/" << internal::RemoveLeadingSlash(uri.path());
    *out_path =
        std::string(internal::RemoveTrailingSlash(ss.str(), /*preserve_root=*/true));
#else
    return Status::Invalid("Unsupported hostname in non-Windows local URI: '",
                           uri.ToString(), "'");
#endif
  } else {
    *out_path =
        std::string(internal::RemoveTrailingSlash(uri.path(), /*preserve_root=*/true));
  }

  // TODO handle use_mmap option
  return LocalFileSystemOptions();
}

LocalFileSystem::LocalFileSystem(const io::IOContext& io_context)
    : FileSystem(io_context), options_(LocalFileSystemOptions::Defaults()) {}

LocalFileSystem::LocalFileSystem(const LocalFileSystemOptions& options,
                                 const io::IOContext& io_context)
    : FileSystem(io_context), options_(options) {}

LocalFileSystem::~LocalFileSystem() {}

Result<std::string> LocalFileSystem::NormalizePath(std::string path) {
  return DoNormalizePath(std::move(path));
}

Result<std::string> LocalFileSystem::PathFromUri(const std::string& uri_string) const {
#ifdef _WIN32
  auto authority_handling = internal::AuthorityHandlingBehavior::kWindows;
#else
  auto authority_handling = internal::AuthorityHandlingBehavior::kDisallow;
#endif
  return internal::PathFromUriHelper(uri_string, {"file"}, /*accept_local_paths=*/true,
                                     authority_handling);
}

bool LocalFileSystem::Equals(const FileSystem& other) const {
  if (other.type_name() != type_name()) {
    return false;
  } else {
    const auto& localfs = ::arrow::internal::checked_cast<const LocalFileSystem&>(other);
    return options_.Equals(localfs.options());
  }
}

Result<FileInfo> LocalFileSystem::GetFileInfo(const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  return StatFile(fn.ToNative());
}

Result<std::vector<FileInfo>> LocalFileSystem::GetFileInfo(const FileSelector& select) {
  RETURN_NOT_OK(ValidatePath(select.base_dir));
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(select.base_dir));
  std::vector<FileInfo> results;
  RETURN_NOT_OK(StatSelector(fn, select, 0, &results));
  return results;
}

namespace {

/// Workhorse for streaming async implementation of `GetFileInfo`
/// (`GetFileInfoGenerator`).
///
/// There are two variants of async discovery functions suported:
/// 1. `DiscoverDirectoryFiles`, which parallelizes traversal of individual directories
///    so that each directory results are yielded as a separate `FileInfoGenerator` via
///    an underlying `DiscoveryImplIterator`, which delivers items in chunks (default size
///    is 1K items).
/// 2. `DiscoverDirectoriesFlattened`, which forwards execution to the
///    `DiscoverDirectoryFiles`, with the difference that the results from individual
///    sub-directory iterators are merged into the single FileInfoGenerator stream.
///
/// The implementation makes use of additional attributes in `LocalFileSystemOptions`,
/// such as `directory_readahead`, which can be used to tune algorithm
/// behavior and adjust how many directories can be processed in parallel.
/// This option is disabled by default, so that individual directories are processed
/// in serial manner via `MakeConcatenatedGenerator` under the hood.
class AsyncStatSelector {
 public:
  using FileInfoGeneratorProducer = PushGenerator<FileInfoGenerator>::Producer;

  /// Discovery state, which is shared among all `DiscoveryImplGenerator`:s,
  /// spawned by a single discovery operation (`DiscoverDirectoryFiles()`).
  ///
  /// The sole purpose of this struct is to handle automatic closing the
  /// producer side of the resulting `FileInfoGenerator`. I.e. the producer
  /// is kept alive until all discovery iterators are exhausted, in which case
  /// `producer.Close()` is called automatically when ref-count for the state
  /// reaches zero (which is equivalent to finishing the file discovery
  /// process).
  struct DiscoveryState {
    FileInfoGeneratorProducer producer;

    explicit DiscoveryState(FileInfoGeneratorProducer p) : producer(std::move(p)) {}
    ~DiscoveryState() { producer.Close(); }
  };

  /// The main procedure to start async streaming discovery using a given `FileSelector`.
  ///
  /// The result is a two-level generator, i.e. "generator of FileInfoGenerator:s",
  /// where each individual generator represents an FileInfo item stream from coming an
  /// individual sub-directory under the selector's `base_dir`.
  static Result<AsyncGenerator<FileInfoGenerator>> DiscoverDirectoryFiles(
      FileSelector selector, LocalFileSystemOptions fs_opts,
      const io::IOContext& io_context) {
    PushGenerator<FileInfoGenerator> file_gen;

    ARROW_ASSIGN_OR_RAISE(
        auto base_dir, arrow::internal::PlatformFilename::FromString(selector.base_dir));
    ARROW_RETURN_NOT_OK(DoDiscovery(std::move(base_dir), 0, std::move(selector),
                                    std::make_shared<DiscoveryState>(file_gen.producer()),
                                    io_context, fs_opts.file_info_batch_size));

    return file_gen;
  }

  /// Version of `DiscoverDirectoryFiles` which flattens the stream of generators
  /// into a single FileInfoGenerator stream.
  /// Makes use of `LocalFileSystemOptions::directory_readahead` to determine how much
  /// readahead should happen.
  static arrow::Result<FileInfoGenerator> DiscoverDirectoriesFlattened(
      FileSelector selector, LocalFileSystemOptions fs_opts,
      const io::IOContext& io_context) {
    int32_t dir_readahead = fs_opts.directory_readahead;
    ARROW_ASSIGN_OR_RAISE(
        auto part_gen,
        DiscoverDirectoryFiles(std::move(selector), std::move(fs_opts), io_context));
    return dir_readahead > 1
               ? MakeSequencedMergedGenerator(std::move(part_gen), dir_readahead)
               : MakeConcatenatedGenerator(std::move(part_gen));
  }

 private:
  /// The class, which implements iterator interface to traverse a given
  /// directory at the fixed nesting depth, and possibly recurses into
  /// sub-directories (if specified by the selector), spawning more
  /// `DiscoveryImplIterators`, which feed their data into a single producer.
  class DiscoveryImplIterator {
    const PlatformFilename dir_fn_;
    const int32_t nesting_depth_;
    const FileSelector selector_;
    const uint32_t file_info_batch_size_;

    const io::IOContext& io_context_;
    std::shared_ptr<DiscoveryState> discovery_state_;
    FileInfoVector current_chunk_;
    std::vector<PlatformFilename> child_fns_;
    size_t idx_ = 0;
    bool initialized_ = false;

   public:
    DiscoveryImplIterator(PlatformFilename dir_fn, int32_t nesting_depth,
                          FileSelector selector,
                          std::shared_ptr<DiscoveryState> discovery_state,
                          const io::IOContext& io_context, uint32_t file_info_batch_size)
        : dir_fn_(std::move(dir_fn)),
          nesting_depth_(nesting_depth),
          selector_(std::move(selector)),
          file_info_batch_size_(file_info_batch_size),
          io_context_(io_context),
          discovery_state_(std::move(discovery_state)) {}

    /// Pre-initialize the iterator by listing directory contents and caching
    /// in the current instance.
    Status Initialize() {
      auto result = arrow::internal::ListDir(dir_fn_);
      if (!result.ok()) {
        auto status = result.status();
        if (selector_.allow_not_found && status.IsIOError()) {
          ARROW_ASSIGN_OR_RAISE(bool exists, FileExists(dir_fn_));
          if (!exists) {
            return Status::OK();
          }
        }
        return status;
      }
      child_fns_ = result.MoveValueUnsafe();

      const size_t dirent_count = child_fns_.size();
      current_chunk_.reserve(dirent_count >= file_info_batch_size_ ? file_info_batch_size_
                                                                   : dirent_count);

      initialized_ = true;
      return Status::OK();
    }

    Result<FileInfoVector> Next() {
      if (!initialized_) {
        auto init = Initialize();
        if (!init.ok()) {
          return Finish(init);
        }
      }
      while (idx_ < child_fns_.size()) {
        auto full_fn = dir_fn_.Join(child_fns_[idx_++]);
        auto res = StatFile(full_fn.ToNative());
        if (!res.ok()) {
          return Finish(res.status());
        }

        auto info = res.MoveValueUnsafe();

        // Try to recurse into subdirectories, if needed.
        if (info.type() == FileType::Directory &&
            nesting_depth_ < selector_.max_recursion && selector_.recursive) {
          auto status = DoDiscovery(std::move(full_fn), nesting_depth_ + 1, selector_,
                                    discovery_state_, io_context_, file_info_batch_size_);
          if (!status.ok()) {
            return Finish(status);
          }
        }
        // Everything is ok. Add the item to the current chunk of data.
        current_chunk_.emplace_back(std::move(info));
        // Keep `current_chunk_` as large, as `batch_size_`.
        // Otherwise, yield the complete chunk to the caller.
        if (current_chunk_.size() == file_info_batch_size_) {
          FileInfoVector yield_vec = std::move(current_chunk_);
          const size_t items_left = child_fns_.size() - idx_;
          current_chunk_.reserve(
              items_left >= file_info_batch_size_ ? file_info_batch_size_ : items_left);
          return yield_vec;
        }
      }  // while (idx_ < child_fns_.size())

      // Flush out remaining items
      if (!current_chunk_.empty()) {
        return std::move(current_chunk_);
      }
      return Finish();
    }

   private:
    /// Release reference to shared discovery state and return iteration end
    /// marker to indicate that this iterator is exhausted.
    Result<FileInfoVector> Finish(Status status = Status::OK()) {
      discovery_state_.reset();
      ARROW_RETURN_NOT_OK(status);
      return IterationEnd<FileInfoVector>();
    }
  };

  /// Create an instance of  `DiscoveryImplIterator` under the hood for the
  /// specified directory, wrap it in the `BackgroundGenerator`  and feed
  /// the results to the main producer queue.
  ///
  /// Each `DiscoveryImplIterator` maintains a reference to `DiscoveryState`,
  /// which simply wraps the producer to keep it alive for the lifetime
  /// of this iterator. When all references to `DiscoveryState` are invalidated,
  /// the producer is closed automatically.
  static Status DoDiscovery(const PlatformFilename& dir_fn, int32_t nesting_depth,
                            FileSelector selector,
                            std::shared_ptr<DiscoveryState> discovery_state,
                            const io::IOContext& io_context,
                            int32_t file_info_batch_size) {
    ARROW_RETURN_IF(discovery_state->producer.is_closed(),
                    arrow::Status::Cancelled("Discovery cancelled"));

    // Note, that here we use `MakeTransferredGenerator()` with the same
    // target executor (io executor) as the current iterator is running on.
    //
    // This is done on purpose, since typically the user of
    // `GetFileInfoGenerator()` would want to perform some more IO on the
    // produced results (e.g. read the files, examine metadata etc.).
    // So, it is preferable to execute the attached continuations on the same
    // executor, which belongs to the IO thread pool.
    ARROW_ASSIGN_OR_RAISE(
        auto gen,
        MakeBackgroundGenerator(Iterator<FileInfoVector>(DiscoveryImplIterator(
                                    std::move(dir_fn), nesting_depth, std::move(selector),
                                    discovery_state, io_context, file_info_batch_size)),
                                io_context.executor()));
    gen = MakeTransferredGenerator(std::move(gen), io_context.executor());
    ARROW_RETURN_IF(!discovery_state->producer.Push(std::move(gen)),
                    arrow::Status::Cancelled("Discovery cancelled"));
    return arrow::Status::OK();
  }
};

}  // anonymous namespace

FileInfoGenerator LocalFileSystem::GetFileInfoGenerator(const FileSelector& select) {
  auto path_status = ValidatePath(select.base_dir);
  if (!path_status.ok()) {
    return MakeFailingGenerator<FileInfoVector>(path_status);
  }
  auto fileinfo_gen =
      AsyncStatSelector::DiscoverDirectoriesFlattened(select, options(), io_context_);
  if (!fileinfo_gen.ok()) {
    return MakeFailingGenerator<FileInfoVector>(fileinfo_gen.status());
  }
  return fileinfo_gen.MoveValueUnsafe();
}

Status LocalFileSystem::CreateDir(const std::string& path, bool recursive) {
  RETURN_NOT_OK(ValidatePath(path));
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  if (recursive) {
    return ::arrow::internal::CreateDirTree(fn).status();
  } else {
    return ::arrow::internal::CreateDir(fn).status();
  }
}

Status LocalFileSystem::DeleteDir(const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  auto st = ::arrow::internal::DeleteDirTree(fn, /*allow_not_found=*/false).status();
  if (!st.ok()) {
    // TODO Status::WithPrefix()?
    std::stringstream ss;
    ss << "Cannot delete directory '" << path << "': " << st.message();
    return st.WithMessage(ss.str());
  }
  return Status::OK();
}

Status LocalFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  RETURN_NOT_OK(ValidatePath(path));
  if (internal::IsEmptyPath(path)) {
    return internal::InvalidDeleteDirContents(path);
  }
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  auto st = ::arrow::internal::DeleteDirContents(fn, missing_dir_ok).status();
  if (!st.ok()) {
    std::stringstream ss;
    ss << "Cannot delete directory contents in '" << path << "': " << st.message();
    return st.WithMessage(ss.str());
  }
  return Status::OK();
}

Status LocalFileSystem::DeleteRootDirContents() {
  return Status::Invalid("LocalFileSystem::DeleteRootDirContents is strictly forbidden");
}

Status LocalFileSystem::DeleteFile(const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  return ::arrow::internal::DeleteFile(fn, /*allow_not_found=*/false).status();
}

Status LocalFileSystem::Move(const std::string& src, const std::string& dest) {
  RETURN_NOT_OK(ValidatePath(src));
  RETURN_NOT_OK(ValidatePath(dest));
  ARROW_ASSIGN_OR_RAISE(auto sfn, PlatformFilename::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto dfn, PlatformFilename::FromString(dest));

#ifdef _WIN32
  if (!MoveFileExW(sfn.ToNative().c_str(), dfn.ToNative().c_str(),
                   MOVEFILE_REPLACE_EXISTING)) {
    return IOErrorFromWinError(GetLastError(), "Failed renaming '", sfn.ToString(),
                               "' to '", dfn.ToString(), "'");
  }
#else
  if (rename(sfn.ToNative().c_str(), dfn.ToNative().c_str()) == -1) {
    return IOErrorFromErrno(errno, "Failed renaming '", sfn.ToString(), "' to '",
                            dfn.ToString(), "'");
  }
#endif
  return Status::OK();
}

Status LocalFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  RETURN_NOT_OK(ValidatePath(src));
  RETURN_NOT_OK(ValidatePath(dest));
  ARROW_ASSIGN_OR_RAISE(auto sfn, PlatformFilename::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto dfn, PlatformFilename::FromString(dest));
  // XXX should we use fstat() to compare inodes?
  if (sfn.ToNative() == dfn.ToNative()) {
    return Status::OK();
  }

#ifdef _WIN32
  if (!CopyFileW(sfn.ToNative().c_str(), dfn.ToNative().c_str(),
                 FALSE /* bFailIfExists */)) {
    return IOErrorFromWinError(GetLastError(), "Failed copying '", sfn.ToString(),
                               "' to '", dfn.ToString(), "'");
  }
  return Status::OK();
#else
  ARROW_ASSIGN_OR_RAISE(auto is, OpenInputStream(src));
  ARROW_ASSIGN_OR_RAISE(auto os, OpenOutputStream(dest));
  RETURN_NOT_OK(internal::CopyStream(is, os, 1024 * 1024 /* chunk_size */, io_context()));
  RETURN_NOT_OK(os->Close());
  return is->Close();
#endif
}

namespace {

template <typename InputStreamType>
Result<std::shared_ptr<InputStreamType>> OpenInputStreamGeneric(
    const std::string& path, const LocalFileSystemOptions& options,
    const io::IOContext& io_context) {
  RETURN_NOT_OK(ValidatePath(path));
  if (options.use_mmap) {
    return io::MemoryMappedFile::Open(path, io::FileMode::READ);
  } else {
    return io::ReadableFile::Open(path, io_context.pool());
  }
}

}  // namespace

Result<std::shared_ptr<io::InputStream>> LocalFileSystem::OpenInputStream(
    const std::string& path) {
  return OpenInputStreamGeneric<io::InputStream>(path, options_, io_context());
}

Result<std::shared_ptr<io::RandomAccessFile>> LocalFileSystem::OpenInputFile(
    const std::string& path) {
  return OpenInputStreamGeneric<io::RandomAccessFile>(path, options_, io_context());
}

namespace {

Result<std::shared_ptr<io::OutputStream>> OpenOutputStreamGeneric(const std::string& path,
                                                                  bool truncate,
                                                                  bool append) {
  RETURN_NOT_OK(ValidatePath(path));
  ARROW_ASSIGN_OR_RAISE(auto fn, PlatformFilename::FromString(path));
  const bool write_only = true;
  ARROW_ASSIGN_OR_RAISE(
      auto fd, ::arrow::internal::FileOpenWritable(fn, write_only, truncate, append));
  int raw_fd = fd.Detach();
  auto maybe_stream = io::FileOutputStream::Open(raw_fd);
  if (!maybe_stream.ok()) {
    ARROW_UNUSED(::arrow::internal::FileClose(raw_fd));
  }
  return maybe_stream;
}

}  // namespace

Result<std::shared_ptr<io::OutputStream>> LocalFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  bool truncate = true;
  bool append = false;
  return OpenOutputStreamGeneric(path, truncate, append);
}

Result<std::shared_ptr<io::OutputStream>> LocalFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  bool truncate = false;
  bool append = true;
  return OpenOutputStreamGeneric(path, truncate, append);
}

}  // namespace fs
}  // namespace arrow

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

#include <dirent.h>
#include <sys/dirent.h>
#include <chrono>
#include <cstring>
#include <memory>
#include <sstream>
#include <stack>
#include <utility>
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"

#ifdef _WIN32
#include "arrow/util/windows_compatibility.h"
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstdio>
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
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"
#include "arrow/util/windows_fixup.h"

namespace arrow::fs {

using ::arrow::internal::ErrnoMessage;
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

/// TODO(bryce): docs
class DirIterator {
 public:
  virtual ~DirIterator() {}
  /// \pre !Done()
  virtual std::string name() const = 0;
  /// \pre !Done()
  virtual bool is_directory() const = 0;
  /// \pre !Done()
  virtual Status Next() = 0;
  virtual bool Done() const = 0;
};

class UnixDirIterator : public DirIterator {
 public:
  UnixDirIterator(DIR* directory, struct dirent* entry) {
    directory_ = directory;
    entry_ = entry;
  }
  ARROW_DISALLOW_COPY_AND_ASSIGN(UnixDirIterator);
  ~UnixDirIterator() {
    if (!directory_) {
      return;
    }
    if (closedir(directory_) != 0) {
      ARROW_LOG(WARNING) << "Cannot close directory handle: " << ErrnoMessage(errno);
    }
  }
  std::string name() const override {
    DCHECK(!Done());
    return std::string{entry_->d_name};
  }

  bool is_directory() const override {
    DCHECK(!Done());
    return entry_->d_type == DT_DIR;
  }

  Status Next() override {
    DCHECK(!Done());
    struct dirent* entry = readdir(directory_);

    if (entry == nullptr) {
      if (errno != 0) {
        return IOErrorFromErrno(errno, "Cannot list directory");
      }
      entry_ = nullptr;
      return Status::OK();
    }
    entry_ = entry;

    return SkipSpecial();
  }

  bool Done() const override { return !directory_ || !entry_; }

  Status SkipSpecial() {
    DCHECK(!Done());
    if (strcmp(entry_->d_name, ".") == 0 || strcmp(entry_->d_name, "..") == 0) {
      return Next();
    }
    return Status::OK();
  }

  static Result<std::unique_ptr<DirIterator>> Open(const PlatformFilename& path,
                                                   bool allow_not_found) {
    DIR* dir = opendir(path.ToNative().c_str());

    if (dir == nullptr) {
      if (allow_not_found) {
        ARROW_ASSIGN_OR_RAISE(bool exists, FileExists(path));
        if (!exists) {
          return std::make_unique<UnixDirIterator>(nullptr, nullptr);
        }
      }
      return IOErrorFromErrno(errno, "Cannot list directory '", path.ToString(), "'.");
    }
    errno = 0;
    dirent* entry = readdir(dir);

    if (entry == nullptr) {
      if (errno != 0) {
        return IOErrorFromErrno(errno, "Cannot list directory '", path.ToString(), "'.");
      }
      return std::make_unique<UnixDirIterator>(dir, nullptr);
    }

    auto dir_it = std::make_unique<UnixDirIterator>(dir, entry);
    ARROW_RETURN_NOT_OK(dir_it->SkipSpecial());
    return dir_it;
  }

 private:
  DIR* directory_;
  struct dirent* entry_;
};

ARROW_NOINLINE Status ListDir(const PlatformFilename& base_dir, bool allow_not_found,
               const std::function<Status(const DirIterator&)>& callback) {
  ARROW_ASSIGN_OR_RAISE(auto dir_it, UnixDirIterator::Open(base_dir, allow_not_found));

  while (!dir_it->Done()) {
    ARROW_RETURN_NOT_OK(callback(*dir_it));
    auto status = dir_it->Next();

    if (!status.ok()) {
      if (status.IsIOError()) {
        auto detail = status.detail();
        std::string detail_str;
        if (detail) {
          detail_str = detail->ToString();
        }
        return Status::IOError(status.message(), ": '", base_dir.ToString(), "'. Detail: ", detail_str);
      }

      return status;
    }
  }

  return Status::OK();
}

Status GetFileInfoImpl(const FileSelector& select, std::vector<FileInfo>* results) {
  RETURN_NOT_OK(ValidatePath(select.base_dir));
  ARROW_ASSIGN_OR_RAISE(auto base_dir, PlatformFilename::FromString(select.base_dir));

  std::stack<FileSelector> stack;
  stack.push(select);
  const bool recursive = select.recursive;
  const bool needs_extended_file_info = select.needs_extended_file_info;

  while (!stack.empty()) {
    auto current = stack.top();
    stack.pop();

    // #ifdef _WIN32
    // TODO: Return a not-ok-Result on WIN32 if Windows UWP/Extended/LongPath
    // TODO: Make sure I understand the difference between a PlatformFilename and not
    // if (select.base_dir.rfind("//?/", 0) && select.needs_extended_file_info) {
    //   return Status::Invalid("FileSelector with needs_extended_file_info is ");
    // }
    // #endif

    // RETURN_NOT_OK(StatSelector(fn, select, 0, &results));
    ARROW_ASSIGN_OR_RAISE(auto base_dir, PlatformFilename::FromString(current.base_dir));
    auto status = ListDir(base_dir, current.allow_not_found, [&](const DirIterator& dir_entry) {
      // TODO: Next step, we just changed this to properly propagate the directory
      // this should fix the tests.
      ARROW_ASSIGN_OR_RAISE(auto current_base_dir,
                            PlatformFilename::FromString(current.base_dir));
      ARROW_ASSIGN_OR_RAISE(PlatformFilename full_fn,
                            current_base_dir.Join(dir_entry.name()));

      std::string full_fn_string = full_fn.ToString();

      auto file_type = FileType::File;
      if (dir_entry.is_directory()) {
        file_type = FileType::Directory;
        if (recursive && current.max_recursion > 0) {
          FileSelector dir_selector;
          dir_selector.base_dir = full_fn_string;
          dir_selector.recursive = current.recursive;
          dir_selector.max_recursion = current.max_recursion - 1;
          // TODO: Document why we're doing this
          dir_selector.allow_not_found = false;
          dir_selector.needs_extended_file_info = needs_extended_file_info;

          stack.push(dir_selector);
        }
      }

      if (needs_extended_file_info) {
        ARROW_ASSIGN_OR_RAISE(FileInfo stat_info, StatFile(full_fn.ToNative()));
        results->push_back(std::move(stat_info));
      } else {
        auto& info = results->emplace_back(std::move(full_fn_string), file_type);
        info.set_size(kNoSize);
        info.set_mtime(kNoTime);
      }

      return Status::OK();
    });

    ARROW_RETURN_NOT_OK(status);
  }

  return Status::OK();
}

}  // namespace

LocalFileSystemOptions LocalFileSystemOptions::Defaults() { return {}; }

bool LocalFileSystemOptions::Equals(const LocalFileSystemOptions& other) const {
  return use_mmap == other.use_mmap && directory_readahead == other.directory_readahead &&
         file_info_batch_size == other.file_info_batch_size;
}

Result<LocalFileSystemOptions> LocalFileSystemOptions::FromUri(
    const ::arrow::util::Uri& uri, std::string* out_path) {
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

  LocalFileSystemOptions options;
  ARROW_ASSIGN_OR_RAISE(auto params, uri.query_items());
  for (const auto& [key, value] : params) {
    if (key == "use_mmap") {
      if (value.empty()) {
        options.use_mmap = true;
        continue;
      } else {
        ARROW_ASSIGN_OR_RAISE(options.use_mmap, ::arrow::internal::ParseBoolean(value));
      }
      break;
    }
  }
  return options;
}

LocalFileSystem::LocalFileSystem(const io::IOContext& io_context)
    : FileSystem(io_context), options_(LocalFileSystemOptions::Defaults()) {}

LocalFileSystem::LocalFileSystem(const LocalFileSystemOptions& options,
                                 const io::IOContext& io_context)
    : FileSystem(io_context), options_(options) {}

LocalFileSystem::~LocalFileSystem() = default;

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

Result<std::string> LocalFileSystem::MakeUri(std::string path) const {
  ARROW_ASSIGN_OR_RAISE(path, DoNormalizePath(std::move(path)));
  return "file://" + path + (options_.use_mmap ? "?use_mmap" : "");
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

// TODO: Next step is to make an auxiliary function this calls but takes
// the base dir and the results as a pointer and continues filling in it
// recursively
Result<std::vector<FileInfo>> LocalFileSystem::GetFileInfo(const FileSelector& select) {
  std::vector<FileInfo> results;
  ARROW_RETURN_NOT_OK(GetFileInfoImpl(select, &results));
  return results;
}

namespace {

/// Workhorse for streaming async implementation of `GetFileInfo`
/// (`GetFileInfoGenerator`).
///
/// There are two variants of async discovery functions supported:
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

    if (selector.needs_extended_file_info || true) {
      ARROW_RETURN_NOT_OK(
          DoDiscovery(std::move(base_dir), 0, std::move(selector),
                      std::make_shared<DiscoveryState>(file_gen.producer()), io_context,
                      fs_opts.file_info_batch_size));
    } else {
      // TODO: We need to deal with this but just not now
      // ARROW_RETURN_NOT_OK(
      //     DoDiscoveryStdFilesystem(std::move(base_dir), 0, std::move(selector),
      //                              std::make_shared<DiscoveryState>(file_gen.producer()),
      //                              io_context, fs_opts.file_info_batch_size));
    }

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
    std::vector<FileInfo> child_fis_;
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
      auto status =
          ListDir(dir_fn_, selector_.allow_not_found, [this](const DirIterator& dir_entry) {
            ARROW_ASSIGN_OR_RAISE(PlatformFilename full_fn,
                                  dir_fn_.Join(dir_entry.name()));

            // Ours
            auto file_type = FileType::File;
            if (dir_entry.is_directory()) {
              file_type = FileType::Directory;
            }
            auto& info = child_fis_.emplace_back(full_fn.ToString(), file_type);
            info.set_size(kNoSize);
            info.set_mtime(kNoTime);

            // Older
            child_fns_.push_back(std::move(full_fn));
            return Status::OK();
          });
      ARROW_RETURN_NOT_OK(status);

      const size_t dirent_count = child_fis_.size();
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
      while (idx_ < child_fis_.size()) {
        auto child_file_info = child_fis_[idx_++];
        ARROW_ASSIGN_OR_RAISE(auto full_fn,
                              PlatformFilename::FromString(child_file_info.path()));
        if (selector_.needs_extended_file_info) {
          auto res = StatFile(full_fn.ToNative());
          if (!res.ok()) {
            return Finish(res.status());
          }
          auto info = res.MoveValueUnsafe();
          child_file_info.set_size(info.size());
          child_file_info.set_mtime(info.mtime());
        }

        // Try to recurse into subdirectories, if needed.
        if (child_file_info.type() == FileType::Directory &&
            nesting_depth_ < selector_.max_recursion && selector_.recursive) {
          auto status = DoDiscovery(std::move(full_fn), nesting_depth_ + 1, selector_,
                                    discovery_state_, io_context_, file_info_batch_size_);
          if (!status.ok()) {
            return Finish(status);
          }
        }
        // Everything is ok. Add the item to the current chunk of data.
        current_chunk_.push_back(std::move(child_file_info));
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
                                    dir_fn, nesting_depth, std::move(selector),
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
  if (rename(sfn.ToNative().c_str(), dfn.ToNative().c_str()) != 0) {
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

static Result<std::shared_ptr<fs::FileSystem>> LocalFileSystemFactory(
    const arrow::util::Uri& uri, const io::IOContext& io_context, std::string* out_path) {
  std::string path;
  ARROW_ASSIGN_OR_RAISE(auto options, LocalFileSystemOptions::FromUri(uri, &path));
  if (out_path != nullptr) {
    *out_path = std::move(path);
  }
  return std::make_shared<LocalFileSystem>(options, io_context);
}

FileSystemRegistrar kLocalFileSystemModule[]{
    ARROW_REGISTER_FILESYSTEM("file", LocalFileSystemFactory, {}),
    ARROW_REGISTER_FILESYSTEM("local", LocalFileSystemFactory, {}),
};

}  // namespace arrow::fs

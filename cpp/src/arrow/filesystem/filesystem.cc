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

#include <sstream>
#include <utility>

#include "arrow/util/config.h"

#include "arrow/filesystem/filesystem.h"
#ifdef ARROW_HDFS
#include "arrow/filesystem/hdfs.h"
#endif
#ifdef ARROW_GCS
#include "arrow/filesystem/gcsfs.h"
#endif
#ifdef ARROW_S3
#include "arrow/filesystem/s3fs.h"
#endif
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/slow.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/parallel.h"
#include "arrow/util/uri.h"
#include "arrow/util/vector.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {

using internal::checked_pointer_cast;
using internal::TaskHints;
using internal::Uri;
using io::internal::SubmitIO;

namespace fs {

using internal::ConcatAbstractPath;
using internal::EnsureTrailingSlash;
using internal::GetAbstractPathParent;
using internal::kSep;
using internal::ParseFileSystemUri;
using internal::RemoveLeadingSlash;
using internal::RemoveTrailingSlash;
using internal::ToSlashes;

std::string ToString(FileType ftype) {
  switch (ftype) {
    case FileType::NotFound:
      return "not-found";
    case FileType::Unknown:
      return "unknown";
    case FileType::File:
      return "file";
    case FileType::Directory:
      return "directory";
    default:
      ARROW_LOG(FATAL) << "Invalid FileType value: " << static_cast<int>(ftype);
      return "???";
  }
}

// For googletest
ARROW_EXPORT std::ostream& operator<<(std::ostream& os, FileType ftype) {
#define FILE_TYPE_CASE(value_name)                  \
  case FileType::value_name:                        \
    os << "FileType::" ARROW_STRINGIFY(value_name); \
    break;

  switch (ftype) {
    FILE_TYPE_CASE(NotFound)
    FILE_TYPE_CASE(Unknown)
    FILE_TYPE_CASE(File)
    FILE_TYPE_CASE(Directory)
    default:
      ARROW_LOG(FATAL) << "Invalid FileType value: " << static_cast<int>(ftype);
  }

#undef FILE_TYPE_CASE
  return os;
}

std::string FileInfo::base_name() const {
  return internal::GetAbstractPathParent(path_).second;
}

std::string FileInfo::dir_name() const {
  return internal::GetAbstractPathParent(path_).first;
}

// Debug helper
std::string FileInfo::ToString() const {
  std::stringstream os;
  os << *this;
  return os.str();
}

std::ostream& operator<<(std::ostream& os, const FileInfo& info) {
  return os << "FileInfo(" << info.type() << ", " << info.path() << ", " << info.size()
            << ", " << info.mtime().time_since_epoch().count() << ")";
}

std::string FileInfo::extension() const {
  return internal::GetAbstractPathExtension(path_);
}

//////////////////////////////////////////////////////////////////////////
// FileSystem default method implementations

FileSystem::~FileSystem() {}

Result<std::string> FileSystem::NormalizePath(std::string path) { return path; }

Result<std::vector<FileInfo>> FileSystem::GetFileInfo(
    const std::vector<std::string>& paths) {
  std::vector<FileInfo> res;
  res.reserve(paths.size());
  for (const auto& path : paths) {
    ARROW_ASSIGN_OR_RAISE(FileInfo info, GetFileInfo(path));
    res.push_back(std::move(info));
  }
  return res;
}

namespace {

template <typename DeferredFunc>
auto FileSystemDefer(FileSystem* fs, bool synchronous, DeferredFunc&& func)
    -> decltype(DeferNotOk(
        fs->io_context().executor()->Submit(func, std::shared_ptr<FileSystem>{}))) {
  auto self = fs->shared_from_this();
  if (synchronous) {
    return std::forward<DeferredFunc>(func)(std::move(self));
  }
  return DeferNotOk(io::internal::SubmitIO(
      fs->io_context(), std::forward<DeferredFunc>(func), std::move(self)));
}

}  // namespace

Future<std::vector<FileInfo>> FileSystem::GetFileInfoAsync(
    const std::vector<std::string>& paths) {
  return FileSystemDefer(
      this, default_async_is_sync_,
      [paths](std::shared_ptr<FileSystem> self) { return self->GetFileInfo(paths); });
}

FileInfoGenerator FileSystem::GetFileInfoGenerator(const FileSelector& select) {
  auto fut = FileSystemDefer(
      this, default_async_is_sync_,
      [select](std::shared_ptr<FileSystem> self) { return self->GetFileInfo(select); });
  return MakeSingleFutureGenerator(std::move(fut));
}

Status FileSystem::DeleteFiles(const std::vector<std::string>& paths) {
  Status st = Status::OK();
  for (const auto& path : paths) {
    st &= DeleteFile(path);
  }
  return st;
}

namespace {

Status ValidateInputFileInfo(const FileInfo& info) {
  if (info.type() == FileType::NotFound) {
    return internal::PathNotFound(info.path());
  }
  if (info.type() != FileType::File && info.type() != FileType::Unknown) {
    return internal::NotAFile(info.path());
  }
  return Status::OK();
}

}  // namespace

Future<> FileSystem::DeleteDirContentsAsync(const std::string& path,
                                            bool missing_dir_ok) {
  return FileSystemDefer(this, default_async_is_sync_,
                         [path, missing_dir_ok](std::shared_ptr<FileSystem> self) {
                           return self->DeleteDirContents(path, missing_dir_ok);
                         });
}

Result<std::shared_ptr<io::InputStream>> FileSystem::OpenInputStream(
    const FileInfo& info) {
  RETURN_NOT_OK(ValidateInputFileInfo(info));
  return OpenInputStream(info.path());
}

Result<std::shared_ptr<io::RandomAccessFile>> FileSystem::OpenInputFile(
    const FileInfo& info) {
  RETURN_NOT_OK(ValidateInputFileInfo(info));
  return OpenInputFile(info.path());
}

Future<std::shared_ptr<io::InputStream>> FileSystem::OpenInputStreamAsync(
    const std::string& path) {
  return FileSystemDefer(
      this, default_async_is_sync_,
      [path](std::shared_ptr<FileSystem> self) { return self->OpenInputStream(path); });
}

Future<std::shared_ptr<io::InputStream>> FileSystem::OpenInputStreamAsync(
    const FileInfo& info) {
  RETURN_NOT_OK(ValidateInputFileInfo(info));
  return FileSystemDefer(
      this, default_async_is_sync_,
      [info](std::shared_ptr<FileSystem> self) { return self->OpenInputStream(info); });
}

Future<std::shared_ptr<io::RandomAccessFile>> FileSystem::OpenInputFileAsync(
    const std::string& path) {
  return FileSystemDefer(
      this, default_async_is_sync_,
      [path](std::shared_ptr<FileSystem> self) { return self->OpenInputFile(path); });
}

Future<std::shared_ptr<io::RandomAccessFile>> FileSystem::OpenInputFileAsync(
    const FileInfo& info) {
  RETURN_NOT_OK(ValidateInputFileInfo(info));
  return FileSystemDefer(
      this, default_async_is_sync_,
      [info](std::shared_ptr<FileSystem> self) { return self->OpenInputFile(info); });
}

Result<std::shared_ptr<io::OutputStream>> FileSystem::OpenOutputStream(
    const std::string& path) {
  return OpenOutputStream(path, std::shared_ptr<const KeyValueMetadata>{});
}

Result<std::shared_ptr<io::OutputStream>> FileSystem::OpenAppendStream(
    const std::string& path) {
  return OpenAppendStream(path, std::shared_ptr<const KeyValueMetadata>{});
}

Result<std::string> FileSystem::PathFromUri(const std::string& uri_string) const {
  return Status::NotImplemented("PathFromUri is not yet supported on this filesystem");
}

//////////////////////////////////////////////////////////////////////////
// SubTreeFileSystem implementation

namespace {

Status ValidateSubPath(std::string_view s) {
  if (internal::IsLikelyUri(s)) {
    return Status::Invalid("Expected a filesystem path, got a URI: '", s, "'");
  }
  return Status::OK();
}

}  // namespace

SubTreeFileSystem::SubTreeFileSystem(const std::string& base_path,
                                     std::shared_ptr<FileSystem> base_fs)
    : FileSystem(base_fs->io_context()),
      base_path_(NormalizeBasePath(base_path, base_fs).ValueOrDie()),
      base_fs_(base_fs) {}

SubTreeFileSystem::~SubTreeFileSystem() {}

Result<std::string> SubTreeFileSystem::NormalizeBasePath(
    std::string base_path, const std::shared_ptr<FileSystem>& base_fs) {
  ARROW_ASSIGN_OR_RAISE(base_path, base_fs->NormalizePath(std::move(base_path)));
  return EnsureTrailingSlash(std::move(base_path));
}

bool SubTreeFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& subfs = ::arrow::internal::checked_cast<const SubTreeFileSystem&>(other);
  return base_path_ == subfs.base_path_ && base_fs_->Equals(subfs.base_fs_);
}

Result<std::string> SubTreeFileSystem::PrependBase(const std::string& s) const {
  RETURN_NOT_OK(ValidateSubPath(s));
  if (s.empty()) {
    return base_path_;
  } else {
    return ConcatAbstractPath(base_path_, s);
  }
}

Result<std::string> SubTreeFileSystem::PrependBaseNonEmpty(const std::string& s) const {
  RETURN_NOT_OK(ValidateSubPath(s));
  if (s.empty()) {
    return Status::IOError("Empty path");
  } else {
    return ConcatAbstractPath(base_path_, s);
  }
}

Result<std::string> SubTreeFileSystem::StripBase(const std::string& s) const {
  auto len = base_path_.length();
  // Note base_path_ ends with a slash (if not empty)
  if (s.length() >= len && s.substr(0, len) == base_path_) {
    return s.substr(len);
  } else {
    return Status::UnknownError("Underlying filesystem returned path '", s,
                                "', which is not a subpath of '", base_path_, "'");
  }
}

Status SubTreeFileSystem::FixInfo(FileInfo* info) const {
  ARROW_ASSIGN_OR_RAISE(auto fixed_path, StripBase(info->path()));
  info->set_path(std::move(fixed_path));
  return Status::OK();
}

Result<std::string> SubTreeFileSystem::NormalizePath(std::string path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBase(path));
  ARROW_ASSIGN_OR_RAISE(auto normalized, base_fs_->NormalizePath(real_path));
  return StripBase(std::move(normalized));
}

Result<FileInfo> SubTreeFileSystem::GetFileInfo(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBase(path));
  ARROW_ASSIGN_OR_RAISE(FileInfo info, base_fs_->GetFileInfo(real_path));
  RETURN_NOT_OK(FixInfo(&info));
  return info;
}

Result<std::vector<FileInfo>> SubTreeFileSystem::GetFileInfo(const FileSelector& select) {
  auto selector = select;
  ARROW_ASSIGN_OR_RAISE(selector.base_dir, PrependBase(selector.base_dir));
  ARROW_ASSIGN_OR_RAISE(auto infos, base_fs_->GetFileInfo(selector));
  for (auto& info : infos) {
    RETURN_NOT_OK(FixInfo(&info));
  }
  return infos;
}

FileInfoGenerator SubTreeFileSystem::GetFileInfoGenerator(const FileSelector& select) {
  auto selector = select;
  auto maybe_base_dir = PrependBase(selector.base_dir);
  if (!maybe_base_dir.ok()) {
    return MakeFailingGenerator<std::vector<FileInfo>>(maybe_base_dir.status());
  }
  selector.base_dir = *std::move(maybe_base_dir);
  auto gen = base_fs_->GetFileInfoGenerator(selector);

  auto self = checked_pointer_cast<SubTreeFileSystem>(shared_from_this());

  std::function<Result<std::vector<FileInfo>>(const std::vector<FileInfo>& infos)>
      fix_infos = [self](std::vector<FileInfo> infos) -> Result<std::vector<FileInfo>> {
    for (auto& info : infos) {
      RETURN_NOT_OK(self->FixInfo(&info));
    }
    return infos;
  };
  return MakeMappedGenerator(gen, fix_infos);
}

Status SubTreeFileSystem::CreateDir(const std::string& path, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->CreateDir(real_path, recursive);
}

Status SubTreeFileSystem::DeleteDir(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->DeleteDir(real_path);
}

Status SubTreeFileSystem::DeleteDirContents(const std::string& path,
                                            bool missing_dir_ok) {
  if (internal::IsEmptyPath(path)) {
    return internal::InvalidDeleteDirContents(path);
  }
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBase(path));
  return base_fs_->DeleteDirContents(real_path, missing_dir_ok);
}

Status SubTreeFileSystem::DeleteRootDirContents() {
  if (base_path_.empty()) {
    return base_fs_->DeleteRootDirContents();
  } else {
    return base_fs_->DeleteDirContents(base_path_);
  }
}

Status SubTreeFileSystem::DeleteFile(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->DeleteFile(real_path);
}

Status SubTreeFileSystem::Move(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto real_src, PrependBaseNonEmpty(src));
  ARROW_ASSIGN_OR_RAISE(auto real_dest, PrependBaseNonEmpty(dest));
  return base_fs_->Move(real_src, real_dest);
}

Status SubTreeFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto real_src, PrependBaseNonEmpty(src));
  ARROW_ASSIGN_OR_RAISE(auto real_dest, PrependBaseNonEmpty(dest));
  return base_fs_->CopyFile(real_src, real_dest);
}

Result<std::shared_ptr<io::InputStream>> SubTreeFileSystem::OpenInputStream(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->OpenInputStream(real_path);
}

Result<std::shared_ptr<io::InputStream>> SubTreeFileSystem::OpenInputStream(
    const FileInfo& info) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(info.path()));
  FileInfo new_info(info);
  new_info.set_path(std::move(real_path));
  return base_fs_->OpenInputStream(new_info);
}

Future<std::shared_ptr<io::InputStream>> SubTreeFileSystem::OpenInputStreamAsync(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->OpenInputStreamAsync(real_path);
}

Future<std::shared_ptr<io::InputStream>> SubTreeFileSystem::OpenInputStreamAsync(
    const FileInfo& info) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(info.path()));
  FileInfo new_info(info);
  new_info.set_path(std::move(real_path));
  return base_fs_->OpenInputStreamAsync(new_info);
}

Result<std::shared_ptr<io::RandomAccessFile>> SubTreeFileSystem::OpenInputFile(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->OpenInputFile(real_path);
}

Result<std::shared_ptr<io::RandomAccessFile>> SubTreeFileSystem::OpenInputFile(
    const FileInfo& info) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(info.path()));
  FileInfo new_info(info);
  new_info.set_path(std::move(real_path));
  return base_fs_->OpenInputFile(new_info);
}

Future<std::shared_ptr<io::RandomAccessFile>> SubTreeFileSystem::OpenInputFileAsync(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->OpenInputFileAsync(real_path);
}

Future<std::shared_ptr<io::RandomAccessFile>> SubTreeFileSystem::OpenInputFileAsync(
    const FileInfo& info) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(info.path()));
  FileInfo new_info(info);
  new_info.set_path(std::move(real_path));
  return base_fs_->OpenInputFileAsync(new_info);
}

Result<std::shared_ptr<io::OutputStream>> SubTreeFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->OpenOutputStream(real_path, metadata);
}

Result<std::shared_ptr<io::OutputStream>> SubTreeFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto real_path, PrependBaseNonEmpty(path));
  return base_fs_->OpenAppendStream(real_path, metadata);
}

Result<std::string> SubTreeFileSystem::PathFromUri(const std::string& uri_string) const {
  return base_fs_->PathFromUri(uri_string);
}

//////////////////////////////////////////////////////////////////////////
// SlowFileSystem implementation

SlowFileSystem::SlowFileSystem(std::shared_ptr<FileSystem> base_fs,
                               std::shared_ptr<io::LatencyGenerator> latencies)
    : FileSystem(base_fs->io_context()), base_fs_(base_fs), latencies_(latencies) {}

SlowFileSystem::SlowFileSystem(std::shared_ptr<FileSystem> base_fs,
                               double average_latency)
    : FileSystem(base_fs->io_context()),
      base_fs_(base_fs),
      latencies_(io::LatencyGenerator::Make(average_latency)) {}

SlowFileSystem::SlowFileSystem(std::shared_ptr<FileSystem> base_fs,
                               double average_latency, int32_t seed)
    : FileSystem(base_fs->io_context()),
      base_fs_(base_fs),
      latencies_(io::LatencyGenerator::Make(average_latency, seed)) {}

bool SlowFileSystem::Equals(const FileSystem& other) const { return this == &other; }

Result<std::string> SlowFileSystem::PathFromUri(const std::string& uri_string) const {
  return base_fs_->PathFromUri(uri_string);
}

Result<FileInfo> SlowFileSystem::GetFileInfo(const std::string& path) {
  latencies_->Sleep();
  return base_fs_->GetFileInfo(path);
}

Result<std::vector<FileInfo>> SlowFileSystem::GetFileInfo(const FileSelector& selector) {
  latencies_->Sleep();
  return base_fs_->GetFileInfo(selector);
}

Status SlowFileSystem::CreateDir(const std::string& path, bool recursive) {
  latencies_->Sleep();
  return base_fs_->CreateDir(path, recursive);
}

Status SlowFileSystem::DeleteDir(const std::string& path) {
  latencies_->Sleep();
  return base_fs_->DeleteDir(path);
}

Status SlowFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  latencies_->Sleep();
  return base_fs_->DeleteDirContents(path, missing_dir_ok);
}

Status SlowFileSystem::DeleteRootDirContents() {
  latencies_->Sleep();
  return base_fs_->DeleteRootDirContents();
}

Status SlowFileSystem::DeleteFile(const std::string& path) {
  latencies_->Sleep();
  return base_fs_->DeleteFile(path);
}

Status SlowFileSystem::Move(const std::string& src, const std::string& dest) {
  latencies_->Sleep();
  return base_fs_->Move(src, dest);
}

Status SlowFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  latencies_->Sleep();
  return base_fs_->CopyFile(src, dest);
}

Result<std::shared_ptr<io::InputStream>> SlowFileSystem::OpenInputStream(
    const std::string& path) {
  latencies_->Sleep();
  ARROW_ASSIGN_OR_RAISE(auto stream, base_fs_->OpenInputStream(path));
  return std::make_shared<io::SlowInputStream>(stream, latencies_);
}

Result<std::shared_ptr<io::InputStream>> SlowFileSystem::OpenInputStream(
    const FileInfo& info) {
  latencies_->Sleep();
  ARROW_ASSIGN_OR_RAISE(auto stream, base_fs_->OpenInputStream(info));
  return std::make_shared<io::SlowInputStream>(stream, latencies_);
}

Result<std::shared_ptr<io::RandomAccessFile>> SlowFileSystem::OpenInputFile(
    const std::string& path) {
  latencies_->Sleep();
  ARROW_ASSIGN_OR_RAISE(auto file, base_fs_->OpenInputFile(path));
  return std::make_shared<io::SlowRandomAccessFile>(file, latencies_);
}

Result<std::shared_ptr<io::RandomAccessFile>> SlowFileSystem::OpenInputFile(
    const FileInfo& info) {
  latencies_->Sleep();
  ARROW_ASSIGN_OR_RAISE(auto file, base_fs_->OpenInputFile(info));
  return std::make_shared<io::SlowRandomAccessFile>(file, latencies_);
}

Result<std::shared_ptr<io::OutputStream>> SlowFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  latencies_->Sleep();
  // XXX Should we have a SlowOutputStream that waits on Flush() and Close()?
  return base_fs_->OpenOutputStream(path, metadata);
}

Result<std::shared_ptr<io::OutputStream>> SlowFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  latencies_->Sleep();
  return base_fs_->OpenAppendStream(path, metadata);
}

Status CopyFiles(const std::vector<FileLocator>& sources,
                 const std::vector<FileLocator>& destinations,
                 const io::IOContext& io_context, int64_t chunk_size, bool use_threads) {
  if (sources.size() != destinations.size()) {
    return Status::Invalid("Trying to copy ", sources.size(), " files into ",
                           destinations.size(), " paths.");
  }

  auto copy_one_file = [&](int i) {
    if (sources[i].filesystem->Equals(destinations[i].filesystem)) {
      return sources[i].filesystem->CopyFile(sources[i].path, destinations[i].path);
    }

    ARROW_ASSIGN_OR_RAISE(auto source,
                          sources[i].filesystem->OpenInputStream(sources[i].path));
    ARROW_ASSIGN_OR_RAISE(const auto metadata, source->ReadMetadata());

    ARROW_ASSIGN_OR_RAISE(auto destination, destinations[i].filesystem->OpenOutputStream(
                                                destinations[i].path, metadata));
    RETURN_NOT_OK(internal::CopyStream(source, destination, chunk_size, io_context));
    return destination->Close();
  };

  return ::arrow::internal::OptionalParallelFor(
      use_threads, static_cast<int>(sources.size()), std::move(copy_one_file),
      io_context.executor());
}

Status CopyFiles(const std::shared_ptr<FileSystem>& source_fs,
                 const FileSelector& source_sel,
                 const std::shared_ptr<FileSystem>& destination_fs,
                 const std::string& destination_base_dir, const io::IOContext& io_context,
                 int64_t chunk_size, bool use_threads) {
  ARROW_ASSIGN_OR_RAISE(auto source_infos, source_fs->GetFileInfo(source_sel));
  if (source_infos.empty()) {
    return Status::OK();
  }

  std::vector<FileLocator> sources, destinations;
  std::vector<std::string> dirs;

  for (const FileInfo& source_info : source_infos) {
    auto relative = internal::RemoveAncestor(source_sel.base_dir, source_info.path());
    if (!relative.has_value()) {
      return Status::Invalid("GetFileInfo() yielded path '", source_info.path(),
                             "', which is outside base dir '", source_sel.base_dir, "'");
    }

    auto destination_path =
        internal::ConcatAbstractPath(destination_base_dir, std::string(*relative));

    if (source_info.IsDirectory()) {
      dirs.push_back(destination_path);
    } else if (source_info.IsFile()) {
      sources.push_back({source_fs, source_info.path()});
      destinations.push_back({destination_fs, destination_path});
    }
  }

  auto create_one_dir = [&](int i) { return destination_fs->CreateDir(dirs[i]); };

  dirs = internal::MinimalCreateDirSet(std::move(dirs));
  RETURN_NOT_OK(::arrow::internal::OptionalParallelFor(
      use_threads, static_cast<int>(dirs.size()), std::move(create_one_dir),
      io_context.executor()));

  return CopyFiles(sources, destinations, io_context, chunk_size, use_threads);
}

namespace {

Result<std::shared_ptr<FileSystem>> FileSystemFromUriReal(const Uri& uri,
                                                          const std::string& uri_string,
                                                          const io::IOContext& io_context,
                                                          std::string* out_path) {
  const auto scheme = uri.scheme();

  if (scheme == "file") {
    std::string path;
    ARROW_ASSIGN_OR_RAISE(auto options, LocalFileSystemOptions::FromUri(uri, &path));
    if (out_path != nullptr) {
      *out_path = path;
    }
    return std::make_shared<LocalFileSystem>(options, io_context);
  }
  if (scheme == "gs" || scheme == "gcs") {
#ifdef ARROW_GCS
    ARROW_ASSIGN_OR_RAISE(auto options, GcsOptions::FromUri(uri, out_path));
    return GcsFileSystem::Make(options, io_context);
#else
    return Status::NotImplemented("Got GCS URI but Arrow compiled without GCS support");
#endif
  }

  if (scheme == "hdfs" || scheme == "viewfs") {
#ifdef ARROW_HDFS
    ARROW_ASSIGN_OR_RAISE(auto options, HdfsOptions::FromUri(uri));
    if (out_path != nullptr) {
      *out_path = uri.path();
    }
    ARROW_ASSIGN_OR_RAISE(auto hdfs, HadoopFileSystem::Make(options, io_context));
    return hdfs;
#else
    return Status::NotImplemented("Got HDFS URI but Arrow compiled without HDFS support");
#endif
  }
  if (scheme == "s3") {
#ifdef ARROW_S3
    RETURN_NOT_OK(EnsureS3Initialized());
    ARROW_ASSIGN_OR_RAISE(auto options, S3Options::FromUri(uri, out_path));
    ARROW_ASSIGN_OR_RAISE(auto s3fs, S3FileSystem::Make(options, io_context));
    return s3fs;
#else
    return Status::NotImplemented("Got S3 URI but Arrow compiled without S3 support");
#endif
  }

  if (scheme == "mock") {
    // MockFileSystem does not have an absolute / relative path distinction,
    // normalize path by removing leading slash.
    if (out_path != nullptr) {
      *out_path = std::string(RemoveLeadingSlash(uri.path()));
    }
    return std::make_shared<internal::MockFileSystem>(internal::CurrentTimePoint(),
                                                      io_context);
  }

  return Status::Invalid("Unrecognized filesystem type in URI: ", uri_string);
}

}  // namespace

Result<std::shared_ptr<FileSystem>> FileSystemFromUri(const std::string& uri_string,
                                                      std::string* out_path) {
  return FileSystemFromUri(uri_string, io::default_io_context(), out_path);
}

Result<std::shared_ptr<FileSystem>> FileSystemFromUri(const std::string& uri_string,
                                                      const io::IOContext& io_context,
                                                      std::string* out_path) {
  ARROW_ASSIGN_OR_RAISE(auto fsuri, ParseFileSystemUri(uri_string));
  return FileSystemFromUriReal(fsuri, uri_string, io_context, out_path);
}

Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(const std::string& uri_string,
                                                            std::string* out_path) {
  return FileSystemFromUriOrPath(uri_string, io::default_io_context(), out_path);
}

Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
    const std::string& uri_string, const io::IOContext& io_context,
    std::string* out_path) {
  if (internal::DetectAbsolutePath(uri_string)) {
    // Normalize path separators
    if (out_path != nullptr) {
      *out_path =
          std::string(RemoveTrailingSlash(ToSlashes(uri_string), /*preserve_root=*/true));
    }
    return std::make_shared<LocalFileSystem>();
  }
  return FileSystemFromUri(uri_string, io_context, out_path);
}

Status FileSystemFromUri(const std::string& uri, std::shared_ptr<FileSystem>* out_fs,
                         std::string* out_path) {
  return FileSystemFromUri(uri, out_path).Value(out_fs);
}

Status Initialize(const FileSystemGlobalOptions& options) {
  internal::global_options = options;
  return Status::OK();
}

}  // namespace fs
}  // namespace arrow

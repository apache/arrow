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
#include <unordered_map>
#include <utility>

#include "arrow/filesystem/hdfs.h"
#include "arrow/filesystem/hdfs_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {

using internal::ErrnoFromStatus;
using internal::ParseValue;
using util::Uri;

namespace fs {

using internal::HdfsOutputStream;
using internal::HdfsPathInfo;
using internal::HdfsReadableFile;

#define CHECK_FAILURE(RETURN_VALUE, WHAT)                       \
  do {                                                          \
    if (RETURN_VALUE == -1) {                                   \
      return IOErrorFromErrno(errno, "HDFS ", WHAT, " failed"); \
    }                                                           \
  } while (0)

static constexpr int kDefaultHdfsBufferSize = 1 << 16;

using internal::GetAbstractPathParent;
using internal::MakeAbstractPathRelative;
using internal::RemoveLeadingSlash;

// ----------------------------------------------------------------------
// HDFS client

// TODO(wesm): this could throw std::bad_alloc in the course of copying strings
// into the path info object
static void SetPathInfo(const hdfsFileInfo* input, HdfsPathInfo* out) {
  out->kind = input->mKind == kObjectKindFile ? arrow::fs::FileType::File
                                              : arrow::fs::FileType::Directory;
  out->name = std::string(input->mName);
  out->owner = std::string(input->mOwner);
  out->group = std::string(input->mGroup);

  out->last_access_time = static_cast<int32_t>(input->mLastAccess);
  out->last_modified_time = static_cast<int32_t>(input->mLastMod);
  out->size = static_cast<int64_t>(input->mSize);

  out->replication = input->mReplication;
  out->block_size = input->mBlockSize;

  out->permissions = input->mPermissions;
}

namespace {

Status GetPathInfoFailed(const std::string& path) {
  return IOErrorFromErrno(errno, "Calling GetPathInfo for '", path, "' failed");
}

}  // namespace

class HadoopFileSystem::Impl {
 public:
  Impl(HdfsOptions options, const io::IOContext& io_context)
      : options_(std::move(options)),
        io_context_(io_context),
        driver_(NULLPTR),
        port_(0),
        client_(NULLPTR) {}

  ~Impl() { ARROW_WARN_NOT_OK(Close(), "Failed to disconnect hdfs client"); }

  Status Init() {
    const HdfsConnectionConfig* config = &options_.connection_config;
    RETURN_NOT_OK(ConnectLibHdfs(&driver_));

    if (!driver_) {
      return Status::Invalid("Failed to initialize HDFS driver");
    }

    // connect to HDFS with the builder object
    hdfsBuilder* builder = driver_->NewBuilder();
    if (!config->host.empty()) {
      driver_->BuilderSetNameNode(builder, config->host.c_str());
    }
    driver_->BuilderSetNameNodePort(builder, static_cast<tPort>(config->port));
    if (!config->user.empty()) {
      driver_->BuilderSetUserName(builder, config->user.c_str());
    }
    if (!config->kerb_ticket.empty()) {
      driver_->BuilderSetKerbTicketCachePath(builder, config->kerb_ticket.c_str());
    }

    for (const auto& kv : config->extra_conf) {
      int ret = driver_->BuilderConfSetStr(builder, kv.first.c_str(), kv.second.c_str());
      CHECK_FAILURE(ret, "confsetstr");
    }

    driver_->BuilderSetForceNewInstance(builder);
    client_ = driver_->BuilderConnect(builder);

    if (client_ == nullptr) {
      return Status::IOError("HDFS connection failed");
    }

    namenode_host_ = config->host;
    port_ = config->port;
    user_ = config->user;
    kerb_ticket_ = config->kerb_ticket;

    return Status::OK();
  }

  Status Close() {
    if (client_ == nullptr) {
      return Status::OK();  // already closed
    }

    if (!driver_) {
      return Status::Invalid("driver_ is null in Close()");
    }

    int ret = driver_->Disconnect(client_);
    CHECK_FAILURE(ret, "hdfsFS::Disconnect");

    client_ = nullptr;  // prevent double-disconnect

    return Status::OK();
  }

  HdfsOptions options() const { return options_; }

  Result<FileInfo> GetFileInfo(const std::string& path) {
    // It has unfortunately been a frequent logic error to pass URIs down
    // to GetFileInfo (e.g. ARROW-10264).  Unlike other filesystems, HDFS
    // silently accepts URIs but returns different results than if given the
    // equivalent in-filesystem paths.  Instead of raising cryptic errors
    // later, notify the underlying problem immediately.
    if (path.substr(0, 5) == "hdfs:") {
      return Status::Invalid("GetFileInfo must not be passed a URI, got: ", path);
    }
    FileInfo info;
    HdfsPathInfo path_info;
    auto status = GetPathInfoStatus(path, &path_info);
    info.set_path(path);
    if (status.IsIOError()) {
      info.set_type(FileType::NotFound);
      return info;
    }

    PathInfoToFileInfo(path_info, &info);
    return info;
  }

  bool Exists(const std::string& path) {
    // hdfsExists does not distinguish between RPC failure and the file not
    // existing
    int ret = driver_->Exists(client_, path.c_str());
    return ret == 0;
  }

  Status StatSelector(const std::string& wd, const std::string& path,
                      const FileSelector& select, int nesting_depth,
                      std::vector<FileInfo>* out) {
    std::vector<HdfsPathInfo> children;
    Status st = ListDirectory(path, &children);
    if (!st.ok()) {
      if (select.allow_not_found) {
        ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(path));
        if (info.type() == FileType::NotFound) {
          return Status::OK();
        }
      }
      return st;
    }
    for (const auto& child_path_info : children) {
      // HDFS returns an absolute "URI" here, need to extract path relative to wd
      // XXX: unfortunately, this is not a real URI as special characters
      // are not %-escaped... hence parsing it as URI would fail.
      std::string child_path;
      if (!wd.empty()) {
        if (child_path_info.name.substr(0, wd.length()) != wd) {
          return Status::IOError("HDFS returned path '", child_path_info.name,
                                 "' that is not a child of '", wd, "'");
        }
        child_path = child_path_info.name.substr(wd.length());
      } else {
        child_path = child_path_info.name;
      }

      FileInfo info;
      info.set_path(child_path);
      PathInfoToFileInfo(child_path_info, &info);
      const bool is_dir = info.type() == FileType::Directory;
      out->push_back(std::move(info));
      if (is_dir && select.recursive && nesting_depth < select.max_recursion) {
        RETURN_NOT_OK(StatSelector(wd, child_path, select, nesting_depth + 1, out));
      }
    }
    return Status::OK();
  }

  Status GetWorkingDirectory(std::string* out) {
    char buffer[2048];
    if (driver_->GetWorkingDirectory(client_, buffer, sizeof(buffer) - 1) == nullptr) {
      return IOErrorFromErrno(errno, "HDFS GetWorkingDirectory failed");
    }
    *out = buffer;
    return Status::OK();
  }

  Result<std::vector<FileInfo>> GetFileInfo(const FileSelector& select) {
    // See GetFileInfo(const std::string&) above.
    if (select.base_dir.substr(0, 5) == "hdfs:") {
      return Status::Invalid("FileSelector.base_dir must not be a URI, got: ",
                             select.base_dir);
    }
    std::vector<FileInfo> results;

    // Fetch working directory.
    // If select.base_dir is relative, we need to trim it from the start
    // of paths returned by ListDirectory.
    // If select.base_dir is absolute, we need to trim the "URI authority"
    // portion of the working directory.
    std::string wd;
    RETURN_NOT_OK(GetWorkingDirectory(&wd));

    if (!select.base_dir.empty() && select.base_dir.front() == '/') {
      // base_dir is absolute, only keep the URI authority portion.
      // As mentioned in StatSelector() above, the URI may contain unescaped
      // special chars and therefore may not be a valid URI, so we parse by hand.
      auto pos = wd.find("://");  // start of host:port portion
      if (pos == std::string::npos) {
        return Status::IOError("Unexpected HDFS working directory URI: ", wd);
      }
      pos = wd.find("/", pos + 3);  // end of host:port portion
      if (pos == std::string::npos) {
        return Status::IOError("Unexpected HDFS working directory URI: ", wd);
      }
      wd = wd.substr(0, pos);  // keep up until host:port (included)
    } else if (!wd.empty() && wd.back() != '/') {
      // For a relative lookup, trim leading slashes
      wd += '/';
    }

    if (!select.base_dir.empty()) {
      ARROW_ASSIGN_OR_RAISE(auto info, GetFileInfo(select.base_dir));
      if (info.type() == FileType::File) {
        return Status::IOError(
            "GetFileInfo expects base_dir of selector to be a directory, but '",
            select.base_dir, "' is a file");
      }
    }
    RETURN_NOT_OK(StatSelector(wd, select.base_dir, select, 0, &results));
    return results;
  }

  Status CreateDir(const std::string& path, bool recursive) {
    if (IsDirectory(path)) {
      return Status::OK();
    }
    if (!recursive) {
      const auto parent = GetAbstractPathParent(path).first;
      if (!parent.empty() && !IsDirectory(parent)) {
        return Status::IOError("Cannot create directory '", path,
                               "': parent is not a directory");
      }
    }
    RETURN_NOT_OK(MakeDirectory(path));
    return Status::OK();
  }

  Status MakeDirectory(const std::string& path) {
    if (IsDirectory(path)) {
      return Status::OK();
    }
    RETURN_NOT_OK(MakeDirectory(path));
    return Status::OK();
  }

  Status GetPathInfoStatus(const std::string& path, HdfsPathInfo* info) {
    hdfsFileInfo* entry = driver_->GetPathInfo(client_, path.c_str());

    if (entry == nullptr) {
      return GetPathInfoFailed(path);
    }

    SetPathInfo(entry, info);
    driver_->FreeFileInfo(entry, 1);

    return Status::OK();
  }

  Status Stat(const std::string& path, arrow::fs::FileInfo* file_info) {
    HdfsPathInfo info;
    RETURN_NOT_OK(GetPathInfoStatus(path, &info));

    file_info->set_size(info.size);
    file_info->set_type(info.kind);
    return Status::OK();
  }

  Status CheckForDirectory(const std::string& path, const char* action) {
    // Check existence of path, and that it's a directory
    HdfsPathInfo info;
    RETURN_NOT_OK(GetPathInfoStatus(path, &info));
    if (info.kind != FileType::Directory) {
      return Status::IOError("Cannot ", action, " directory '", path,
                             "': not a directory");
    }
    return Status::OK();
  }

  Status GetChildren(const std::string& path, std::vector<std::string>* listing) {
    std::vector<HdfsPathInfo> detailed_listing;
    RETURN_NOT_OK(ListDirectory(path, &detailed_listing));
    for (const auto& info : detailed_listing) {
      listing->push_back(info.name);
    }
    return Status::OK();
  }

  Status ListDirectory(const std::string& path, std::vector<HdfsPathInfo>* listing) {
    int num_entries = 0;
    errno = 0;
    hdfsFileInfo* entries = driver_->ListDirectory(client_, path.c_str(), &num_entries);

    if (entries == nullptr) {
      // If the directory is empty, entries is NULL but errno is 0. Non-zero
      // errno indicates error
      //
      // Note: errno is thread-local
      //
      // XXX(wesm): ARROW-2300; we found with Hadoop 2.6 that libhdfs would set
      // errno 2/ENOENT for empty directories. To be more robust to this we
      // double check this case
      if ((errno == 0) || (errno == ENOENT && Exists(path))) {
        num_entries = 0;
      } else {
        return IOErrorFromErrno(errno, "HDFS list directory failed");
      }
    }

    // Allocate additional space for elements
    int vec_offset = static_cast<int>(listing->size());
    listing->resize(vec_offset + num_entries);

    for (int i = 0; i < num_entries; ++i) {
      SetPathInfo(entries + i, &(*listing)[vec_offset + i]);
    }

    // Free libhdfs file info
    driver_->FreeFileInfo(entries, num_entries);

    return Status::OK();
  }

  Status DeleteDirectory(const std::string& path) {
    return Delete(path, /*recursive=*/true);
  }

  Status DeleteDir(const std::string& path) {
    RETURN_NOT_OK(CheckForDirectory(path, "delete"));
    return DeleteDirectory(path);
  }

  Status DeleteDirContents(const std::string& path, bool missing_dir_ok) {
    auto st = CheckForDirectory(path, "delete contents of");
    if (!st.ok()) {
      if (missing_dir_ok && ErrnoFromStatus(st) == ENOENT) {
        return Status::OK();
      }
      return st;
    }

    std::vector<std::string> file_list;
    RETURN_NOT_OK(GetChildren(path, &file_list));
    for (const auto& file : file_list) {
      RETURN_NOT_OK(Delete(file, /*recursive=*/true));
    }
    return Status::OK();
  }

  Status DeleteFile(const std::string& path) {
    if (IsDirectory(path)) {
      return Status::IOError("path is a directory");
    }
    RETURN_NOT_OK(Delete(path, /*recursive=*/false));
    return Status::OK();
  }

  Status Delete(const std::string& path, bool recursive) {
    int ret = driver_->Delete(client_, path.c_str(), static_cast<int>(recursive));
    CHECK_FAILURE(ret, "delete");
    return Status::OK();
  }

  Status Rename(const std::string& src, const std::string& dst) {
    int ret = driver_->Rename(client_, src.c_str(), dst.c_str());
    CHECK_FAILURE(ret, "Rename");
    return Status::OK();
  }

  Status Move(const std::string& src, const std::string& dest) {
    auto st = Rename(src, dest);
    if (st.IsIOError() && IsFile(src) && IsFile(dest)) {
      // Allow file -> file clobber
      RETURN_NOT_OK(Delete(dest, /*recursive=*/false));
      st = Rename(src, dest);
    }
    return st;
  }

  Status Copy(const std::string& src, const std::string& dst) {
    int ret = driver_->Copy(client_, src.c_str(), client_, dst.c_str());
    CHECK_FAILURE(ret, "Rename");
    return Status::OK();
  }

  Status CopyFile(const std::string& src, const std::string& dest) {
    return Copy(src, dest);
  }

  Status Chmod(const std::string& path, int mode) {
    int ret = driver_->Chmod(client_, path.c_str(), static_cast<short>(mode));  // NOLINT
    CHECK_FAILURE(ret, "Chmod");
    return Status::OK();
  }

  Status Chown(const std::string& path, const char* owner, const char* group) {
    int ret = driver_->Chown(client_, path.c_str(), owner, group);
    CHECK_FAILURE(ret, "Chown");
    return Status::OK();
  }

  Status OpenReadable(const std::string& path, int32_t buffer_size,
                      const io::IOContext& io_context,
                      std::shared_ptr<HdfsReadableFile>* file) {
    errno = 0;
    hdfsFile handle =
        driver_->OpenFile(client_, path.c_str(), O_RDONLY, buffer_size, 0, 0);

    if (handle == nullptr) {
      return IOErrorFromErrno(errno, "Opening HDFS file '", path, "' failed");
    }

    // std::make_shared does not work with private ctors
    std::shared_ptr<HdfsReadableFile> file_tmp;
    ARROW_ASSIGN_OR_RAISE(file_tmp, HdfsReadableFile::Make(path, buffer_size, io_context,
                                                           driver_, client_, handle));
    *file = std::move(file_tmp);
    return Status::OK();
  }

  Status OpenReadable(const std::string& path, int32_t buffer_size,
                      std::shared_ptr<HdfsReadableFile>* file) {
    return OpenReadable(path, buffer_size, io::default_io_context(), file);
  }

  Status OpenReadable(const std::string& path, std::shared_ptr<HdfsReadableFile>* file) {
    return OpenReadable(path, kDefaultHdfsBufferSize, io::default_io_context(), file);
  }

  Status OpenReadable(const std::string& path, const io::IOContext& io_context,
                      std::shared_ptr<HdfsReadableFile>* file) {
    return OpenReadable(path, kDefaultHdfsBufferSize, io_context, file);
  }

  Status OpenWritable(const std::string& path, bool append, int32_t buffer_size,
                      int16_t replication, int64_t default_block_size,
                      std::shared_ptr<HdfsOutputStream>* file) {
    int flags = O_WRONLY;
    if (append) flags |= O_APPEND;

    errno = 0;
    hdfsFile handle =
        driver_->OpenFile(client_, path.c_str(), flags, buffer_size, replication,
                          static_cast<tSize>(default_block_size));

    if (handle == nullptr) {
      return IOErrorFromErrno(errno, "Opening HDFS file '", path, "' failed");
    }

    // std::make_shared does not work with private ctors
    std::shared_ptr<HdfsOutputStream> file_tmp;
    ARROW_ASSIGN_OR_RAISE(
        file_tmp, HdfsOutputStream::Make(path, buffer_size, driver_, client_, handle));
    *file = std::move(file_tmp);
    return Status::OK();
  }

  Status OpenWritable(const std::string& path, bool append,
                      std::shared_ptr<HdfsOutputStream>* file) {
    return OpenWritable(path, append, 0, 0, 0, file);
  }

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const std::string& path) {
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(path));
    std::shared_ptr<HdfsReadableFile> file;
    RETURN_NOT_OK(OpenReadable(path, io_context_, &file));
    return file;
  }

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(const std::string& path) {
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(path));
    std::shared_ptr<HdfsReadableFile> file;
    RETURN_NOT_OK(OpenReadable(path, io_context_, &file));
    return file;
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(const std::string& path) {
    bool append = false;
    return OpenOutputStreamGeneric(path, append);
  }

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(const std::string& path) {
    bool append = true;
    return OpenOutputStreamGeneric(path, append);
  }

  Status GetCapacity(int64_t* nbytes) {
    tOffset ret = driver_->GetCapacity(client_);
    CHECK_FAILURE(ret, "GetCapacity");
    *nbytes = ret;
    return Status::OK();
  }

  Status GetUsed(int64_t* nbytes) {
    tOffset ret = driver_->GetUsed(client_);
    CHECK_FAILURE(ret, "GetUsed");
    *nbytes = ret;
    return Status::OK();
  }

 protected:
  const HdfsOptions options_;
  const io::IOContext io_context_;
  internal::LibHdfsShim* driver_;

  std::string namenode_host_;
  std::string user_;
  int port_;
  std::string kerb_ticket_;

  hdfsFS client_;

  void PathInfoToFileInfo(const HdfsPathInfo& info, FileInfo* out) {
    if (info.kind == FileType::Directory) {
      out->set_type(FileType::Directory);
      out->set_size(kNoSize);
    } else if (info.kind == FileType::File) {
      out->set_type(FileType::File);
      out->set_size(info.size);
    }
    out->set_mtime(ToTimePoint(info.last_modified_time));
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStreamGeneric(
      const std::string& path, bool append) {
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(path));
    std::shared_ptr<HdfsOutputStream> stream;
    RETURN_NOT_OK(OpenWritable(path, append, options_.buffer_size, options_.replication,
                               options_.default_block_size, &stream));
    return stream;
  }

  bool IsDirectory(const std::string& path) {
    HdfsPathInfo info;
    return GetPathInfo(path, &info) && info.kind == FileType::Directory;
  }

  bool IsFile(const std::string& path) {
    HdfsPathInfo info;
    return GetPathInfo(path, &info) && info.kind == FileType::File;
  }

  bool GetPathInfo(const std::string& path, HdfsPathInfo* info) {
    return GetPathInfoStatus(path, info).ok();
  }

  TimePoint ToTimePoint(int secs) {
    std::chrono::nanoseconds ns_count(static_cast<int64_t>(secs) * 1000000000);
    return TimePoint(std::chrono::duration_cast<TimePoint::duration>(ns_count));
  }
};

void HdfsOptions::ConfigureEndPoint(std::string host, int port) {
  connection_config.host = std::move(host);
  connection_config.port = port;
}

void HdfsOptions::ConfigureUser(std::string user_name) {
  connection_config.user = std::move(user_name);
}

void HdfsOptions::ConfigureKerberosTicketCachePath(std::string path) {
  connection_config.kerb_ticket = std::move(path);
}

void HdfsOptions::ConfigureReplication(int16_t replication) {
  this->replication = replication;
}

void HdfsOptions::ConfigureBufferSize(int32_t buffer_size) {
  this->buffer_size = buffer_size;
}

void HdfsOptions::ConfigureBlockSize(int64_t default_block_size) {
  this->default_block_size = default_block_size;
}

void HdfsOptions::ConfigureExtraConf(std::string key, std::string val) {
  connection_config.extra_conf.emplace(std::move(key), std::move(val));
}

bool HdfsOptions::Equals(const HdfsOptions& other) const {
  return (buffer_size == other.buffer_size && replication == other.replication &&
          default_block_size == other.default_block_size &&
          connection_config.host == other.connection_config.host &&
          connection_config.port == other.connection_config.port &&
          connection_config.user == other.connection_config.user &&
          connection_config.kerb_ticket == other.connection_config.kerb_ticket &&
          connection_config.extra_conf == other.connection_config.extra_conf);
}

Result<HdfsOptions> HdfsOptions::FromUri(const Uri& uri) {
  HdfsOptions options;

  std::unordered_map<std::string, std::string> options_map;
  ARROW_ASSIGN_OR_RAISE(const auto options_items, uri.query_items());
  for (const auto& kv : options_items) {
    options_map.emplace(kv.first, kv.second);
  }

  std::string host;
  host = uri.scheme() + "://" + uri.host();

  // configure endpoint
  const auto port = uri.port();
  if (port == -1) {
    // default port will be determined by hdfs FileSystem impl
    options.ConfigureEndPoint(host, 0);
  } else {
    options.ConfigureEndPoint(host, port);
  }

  // configure replication
  auto it = options_map.find("replication");
  if (it != options_map.end()) {
    const auto& v = it->second;
    int16_t replication;
    if (!ParseValue<Int16Type>(v.data(), v.size(), &replication)) {
      return Status::Invalid("Invalid value for option 'replication': '", v, "'");
    }
    options.ConfigureReplication(replication);
    options_map.erase(it);
  }

  // configure buffer_size
  it = options_map.find("buffer_size");
  if (it != options_map.end()) {
    const auto& v = it->second;
    int32_t buffer_size;
    if (!ParseValue<Int32Type>(v.data(), v.size(), &buffer_size)) {
      return Status::Invalid("Invalid value for option 'buffer_size': '", v, "'");
    }
    options.ConfigureBufferSize(buffer_size);
    options_map.erase(it);
  }

  // configure default_block_size
  it = options_map.find("default_block_size");
  if (it != options_map.end()) {
    const auto& v = it->second;
    int64_t default_block_size;
    if (!ParseValue<Int64Type>(v.data(), v.size(), &default_block_size)) {
      return Status::Invalid("Invalid value for option 'default_block_size': '", v, "'");
    }
    options.ConfigureBlockSize(default_block_size);
    options_map.erase(it);
  }

  // configure user
  it = options_map.find("user");
  if (it != options_map.end()) {
    const auto& user = it->second;
    options.ConfigureUser(user);
    options_map.erase(it);
  }

  // configure kerberos
  it = options_map.find("kerb_ticket");
  if (it != options_map.end()) {
    const auto& ticket = it->second;
    options.ConfigureKerberosTicketCachePath(ticket);
    options_map.erase(it);
  }

  // configure other options
  for (const auto& it : options_map) {
    options.ConfigureExtraConf(it.first, it.second);
  }

  return options;
}

Result<HdfsOptions> HdfsOptions::FromUri(const std::string& uri_string) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri);
}

HadoopFileSystem::HadoopFileSystem(const HdfsOptions& options,
                                   const io::IOContext& io_context)
    : FileSystem(io_context), impl_(new Impl{options, io_context_}) {
  default_async_is_sync_ = false;
}

HadoopFileSystem::~HadoopFileSystem() {}

Result<std::shared_ptr<HadoopFileSystem>> HadoopFileSystem::Make(
    const HdfsOptions& options, const io::IOContext& io_context) {
  std::shared_ptr<HadoopFileSystem> ptr(new HadoopFileSystem(options, io_context));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

Result<FileInfo> HadoopFileSystem::GetFileInfo(const std::string& path) {
  return impl_->GetFileInfo(path);
}

HdfsOptions HadoopFileSystem::options() const { return impl_->options(); }

bool HadoopFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& hdfs = ::arrow::internal::checked_cast<const HadoopFileSystem&>(other);
  return options().Equals(hdfs.options());
}

Result<std::string> HadoopFileSystem::PathFromUri(const std::string& uri_string) const {
  return internal::PathFromUriHelper(uri_string, {"hdfs", "viewfs"},
                                     /*accept_local_paths=*/false,
                                     internal::AuthorityHandlingBehavior::kIgnore);
}

Result<std::vector<FileInfo>> HadoopFileSystem::GetFileInfo(const FileSelector& select) {
  return impl_->GetFileInfo(select);
}

Status HadoopFileSystem::CreateDir(const std::string& path, bool recursive) {
  return impl_->CreateDir(path, recursive);
}

Status HadoopFileSystem::DeleteDir(const std::string& path) {
  return impl_->DeleteDir(path);
}

Status HadoopFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  if (internal::IsEmptyPath(path)) {
    return internal::InvalidDeleteDirContents(path);
  }
  return impl_->DeleteDirContents(path, missing_dir_ok);
}

Status HadoopFileSystem::DeleteRootDirContents() {
  return impl_->DeleteDirContents("", /*missing_dir_ok=*/false);
}

Status HadoopFileSystem::DeleteFile(const std::string& path) {
  return impl_->DeleteFile(path);
}

Status HadoopFileSystem::Move(const std::string& src, const std::string& dest) {
  return impl_->Move(src, dest);
}

Status HadoopFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return impl_->CopyFile(src, dest);
}

Result<std::shared_ptr<io::InputStream>> HadoopFileSystem::OpenInputStream(
    const std::string& path) {
  return impl_->OpenInputStream(path);
}

Result<std::shared_ptr<io::RandomAccessFile>> HadoopFileSystem::OpenInputFile(
    const std::string& path) {
  return impl_->OpenInputFile(path);
}

Result<std::shared_ptr<io::OutputStream>> HadoopFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return impl_->OpenOutputStream(path);
}

Result<std::shared_ptr<io::OutputStream>> HadoopFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return impl_->OpenAppendStream(path);
}

Status HadoopFileSystem::MakeDirectory(const std::string& path) {
  return impl_->MakeDirectory(path);
}

Status HadoopFileSystem::Delete(const std::string& path, bool recursive) {
  return impl_->Delete(path, recursive);
}

bool HadoopFileSystem::Exists(const std::string& path) { return impl_->Exists(path); }

Status HadoopFileSystem::GetPathInfoStatus(const std::string& path, HdfsPathInfo* info) {
  return impl_->GetPathInfoStatus(path, info);
}

Status HadoopFileSystem::GetCapacity(int64_t* nbytes) {
  return impl_->GetCapacity(nbytes);
}

Status HadoopFileSystem::GetUsed(int64_t* nbytes) { return impl_->GetUsed(nbytes); }

Status HadoopFileSystem::ListDirectory(const std::string& path,
                                       std::vector<HdfsPathInfo>* listing) {
  return impl_->ListDirectory(path, listing);
}

Status HadoopFileSystem::Chmod(const std::string& path, int mode) {
  return impl_->Chmod(path, mode);
}

Status HadoopFileSystem::Chown(const std::string& path, const char* owner,
                               const char* group) {
  return impl_->Chown(path, owner, group);
}

Status HadoopFileSystem::Rename(const std::string& src, const std::string& dst) {
  return impl_->Rename(src, dst);
}

Status HadoopFileSystem::Copy(const std::string& src, const std::string& dst) {
  return impl_->Copy(src, dst);
}

Status HadoopFileSystem::OpenReadable(const std::string& path, int32_t buffer_size,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return impl_->OpenReadable(path, buffer_size, file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path, int32_t buffer_size,
                                      const io::IOContext& io_context,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return impl_->OpenReadable(path, buffer_size, io_context, file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return impl_->OpenReadable(path, file);
}

Status HadoopFileSystem::OpenReadable(const std::string& path,
                                      const io::IOContext& io_context,
                                      std::shared_ptr<HdfsReadableFile>* file) {
  return impl_->OpenReadable(path, io_context, file);
}

Status HadoopFileSystem::OpenWritable(
    const std::string& path, bool append, int32_t buffer_size, int16_t replication,
    int64_t default_block_size,
    std::shared_ptr<internal::HdfsOutputStream>* file) {
  return impl_->OpenWritable(path, append, buffer_size, replication, default_block_size,
                             file);
}

Status HadoopFileSystem::OpenWritable(
    const std::string& path, bool append,
    std::shared_ptr<internal::HdfsOutputStream>* file) {
  return impl_->OpenWritable(path, append, file);
}

// ----------------------------------------------------------------------
// Allow public API users to check whether we are set up correctly

Status HaveLibHdfs() {
  internal::LibHdfsShim* driver;
  return internal::ConnectLibHdfs(&driver);
}

}  // namespace fs
}  // namespace arrow

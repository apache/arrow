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

#include "arrow/filesystem/azurefs.h"

#include <azure/identity/default_azure_credential.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_client.hpp>

#include "arrow/buffer.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {
namespace fs {

static const char kSep = '/';

// -----------------------------------------------------------------------
// AzureOptions Implementation

AzureOptions::AzureOptions() {}

bool AzureOptions::Equals(const AzureOptions& other) const {
  return (account_dfs_url == other.account_dfs_url &&
          account_blob_url == other.account_blob_url &&
          credentials_kind == other.credentials_kind);
}

Status AzureOptions::ConfigureAccountKeyCredentials(const std::string& account_name,
                                                    const std::string& account_key) {
  if (this->backend == AzureBackend::Azurite) {
    account_blob_url = "http://127.0.0.1:10000/" + account_name + "/";
    account_dfs_url = "http://127.0.0.1:10000/" + account_name + "/";
  } else {
    account_dfs_url = "https://" + account_name + ".dfs.core.windows.net/";
    account_blob_url = "https://" + account_name + ".blob.core.windows.net/";
  }
  storage_credentials_provider =
      std::make_shared<Azure::Storage::StorageSharedKeyCredential>(account_name,
                                                                   account_key);
  credentials_kind = AzureCredentialsKind::StorageCredentials;
  return Status::OK();
}
namespace {

struct AzurePath {
  std::string full_path;
  std::string container;
  std::string path_to_file;
  std::vector<std::string> path_to_file_parts;
  // An AzureFileSystem represents a single Azure storage account. AzurePath describes the
  // container within that storage account and path within that container.

  static Result<AzurePath> FromString(const std::string& s) {
    // Example expected string format: testcontainer/testdir/testfile.txt
    // container = testcontainer
    // path_to_file = testdir/testfile.txt
    // path_to_file_parts = [testdir, testfile.txt]
    if (internal::IsLikelyUri(s)) {
      return Status::Invalid(
          "Expected an Azure object path of the form 'container/path...', got a URI: '",
          s, "'");
    }
    auto src = internal::RemoveTrailingSlash(s);
    auto input_path = std::string(src.data());
    src = internal::RemoveLeadingSlash(src);
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", input_path, "')");
    }
    if (first_sep == std::string::npos) {
      return AzurePath{std::string(src), std::string(src), "", {}};
    }
    AzurePath path;
    path.full_path = std::string(src);
    path.container = std::string(src.substr(0, first_sep));
    path.path_to_file = std::string(src.substr(first_sep + 1));
    path.path_to_file_parts = internal::SplitAbstractPath(path.path_to_file);
    RETURN_NOT_OK(Validate(path));
    return path;
  }

  static Status Validate(const AzurePath& path) {
    auto status = internal::ValidateAbstractPathParts(path.path_to_file_parts);
    if (!status.ok()) {
      return Status::Invalid(status.message(), " in path ", path.full_path);
    } else {
      return status;
    }
  }

  AzurePath parent() const {
    DCHECK(has_parent());
    auto parent = AzurePath{"", container, "", path_to_file_parts};
    parent.path_to_file_parts.pop_back();
    parent.path_to_file = internal::JoinAbstractPath(parent.path_to_file_parts);
    if (parent.path_to_file.empty()) {
      parent.full_path = parent.container;
    } else {
      parent.full_path = parent.container + kSep + parent.path_to_file;
    }
    return parent;
  }

  bool has_parent() const { return !path_to_file.empty(); }

  bool empty() const { return container.empty() && path_to_file.empty(); }

  bool operator==(const AzurePath& other) const {
    return container == other.container && path_to_file == other.path_to_file;
  }
};

Status PathNotFound(const AzurePath& path) {
  return ::arrow::fs::internal::PathNotFound(path.full_path);
}

Status NotAFile(const AzurePath& path) {
  return ::arrow::fs::internal::NotAFile(path.full_path);
}

Status ValidateFilePath(const AzurePath& path) {
  if (path.container.empty()) {
    return PathNotFound(path);
  }

  if (path.container.empty() || path.path_to_file.empty()) {
    return NotAFile(path);
  }
  return Status::OK();
}

template <typename ObjectResult>
std::shared_ptr<const KeyValueMetadata> GetObjectMetadata(const ObjectResult& result) {
  auto md = std::make_shared<KeyValueMetadata>();
  for (auto prop : result) {
    md->Append(prop.first, prop.second);
  }
  return md;
}

class ObjectInputFile final : public io::RandomAccessFile {
 protected:
  std::shared_ptr<Azure::Storage::Blobs::BlobClient> blob_client_;
  const io::IOContext io_context_;
  AzurePath path_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;

 public:
  ObjectInputFile(std::shared_ptr<Azure::Storage::Blobs::BlobClient>& blob_client,
                  const io::IOContext& io_context, const AzurePath& path,
                  int64_t size = kNoSize)
      : blob_client_(std::move(blob_client)),
        io_context_(io_context),
        path_(path),
        content_length_(size) {}

  Status Init() {
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }
    try {
      auto properties = blob_client_->GetProperties();
      content_length_ = properties.Value.BlobSize;
      metadata_ = GetObjectMetadata(properties.Value.Metadata);
      return Status::OK();
    } catch (const Azure::Storage::StorageException& exception) {
      if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
        // Could be either container or blob not found.
        return ::arrow::fs::internal::PathNotFound(path_.full_path);
      }
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return Status::OK();
  }

  Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return Status::IOError("Cannot ", action, " past end of file");
    }
    return Status::OK();
  }

  // RandomAccessFile APIs

  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    return metadata_;
  }

  Future<std::shared_ptr<const KeyValueMetadata>> ReadMetadataAsync(
      const io::IOContext& io_context) override {
    return metadata_;
  }

  Status Close() override {
    blob_client_ = nullptr;
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, content_length_ - position);
    if (nbytes == 0) {
      return 0;
    }

    // Read the desired range of bytes
    Azure::Storage::Blobs::DownloadBlobToOptions download_options;
    Azure::Core::Http::HttpRange range;
    range.Offset = position;
    range.Length = nbytes;
    download_options.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    try {
      auto result =
          blob_client_
              ->DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, download_options)
              .Value;
      return result.ContentRange.Length.Value();
    } catch (const Azure::Storage::StorageException& exception) {
      return Status::IOError(exception.RawResponse->GetReasonPhrase());
    }
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(nbytes, io_context_.pool()));
    if (nbytes > 0) {
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            ReadAt(position, nbytes, buf->mutable_data()));
      DCHECK_LE(bytes_read, nbytes);
      RETURN_NOT_OK(buf->Resize(bytes_read));
    }
    return std::move(buf);
  }

  Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }
};

}  // namespace

// -----------------------------------------------------------------------
// AzureFilesystem Implementation

class AzureFileSystem::Impl {
 public:
  io::IOContext io_context_;
  std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> service_client_;
  AzureOptions options_;

  explicit Impl(AzureOptions options, io::IOContext io_context)
      : io_context_(io_context), options_(std::move(options)) {}

  Status Init() {
    service_client_ = std::make_shared<Azure::Storage::Blobs::BlobServiceClient>(
        options_.account_blob_url, options_.storage_credentials_provider);
    return Status::OK();
  }

  const AzureOptions& options() const { return options_; }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const std::string& s,
                                                         AzureFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
    RETURN_NOT_OK(ValidateFilePath(path));

    auto blob_client = std::make_shared<Azure::Storage::Blobs::BlobClient>(
        std::move(service_client_->GetBlobContainerClient(path.container)
                      .GetBlobClient(path.path_to_file)));

    auto ptr = std::make_shared<ObjectInputFile>(blob_client, fs->io_context(), path);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<ObjectInputFile>> OpenInputFile(const FileInfo& info,
                                                         AzureFileSystem* fs) {
    if (info.type() == FileType::NotFound) {
      return ::arrow::fs::internal::PathNotFound(info.path());
    }
    if (info.type() != FileType::File && info.type() != FileType::Unknown) {
      return ::arrow::fs::internal::NotAFile(info.path());
    }

    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(info.path()));
    RETURN_NOT_OK(ValidateFilePath(path));

    auto blob_client = std::make_shared<Azure::Storage::Blobs::BlobClient>(
        std::move(service_client_->GetBlobContainerClient(path.container)
                      .GetBlobClient(path.path_to_file)));

    auto ptr = std::make_shared<ObjectInputFile>(blob_client, fs->io_context(), path,
                                                 info.size());
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }
};

const AzureOptions& AzureFileSystem::options() const { return impl_->options(); }

bool AzureFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& azure_fs = ::arrow::internal::checked_cast<const AzureFileSystem&>(other);
  return options().Equals(azure_fs.options());
}

Result<FileInfo> AzureFileSystem::GetFileInfo(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<FileInfoVector> AzureFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::CreateDir(const std::string& path, bool recursive) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteDir(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::DeleteFile(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::Move(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> AzureFileSystem::OpenInputStream(
    const std::string& path) {
  return impl_->OpenInputFile(path, this);
}

Result<std::shared_ptr<io::InputStream>> AzureFileSystem::OpenInputStream(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureFileSystem::OpenInputFile(
    const std::string& path) {
  return impl_->OpenInputFile(path, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureFileSystem::OpenInputFile(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::OutputStream>> AzureFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> AzureFileSystem::OpenAppendStream(
    const std::string&, const std::shared_ptr<const KeyValueMetadata>&) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<AzureFileSystem>> AzureFileSystem::Make(
    const AzureOptions& options, const io::IOContext& io_context) {
  std::shared_ptr<AzureFileSystem> ptr(new AzureFileSystem(options, io_context));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

AzureFileSystem::AzureFileSystem(const AzureOptions& options,
                                 const io::IOContext& io_context)
    : FileSystem(io_context), impl_(std::make_unique<Impl>(options, io_context)) {
  default_async_is_sync_ = false;
}

}  // namespace fs
}  // namespace arrow

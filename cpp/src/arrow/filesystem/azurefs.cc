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

#include "arrow/result.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace fs {

struct AzurePath {
  std::string full_path;
  std::string container;
  std::string path_to_file;
  std::vector<std::string> path_to_file_parts;

  static Result<AzurePath> FromString(const std::string& s) {
    // https://synapsemladlsgen2.dfs.core.windows.net/synapsemlfs/testdir/testfile.txt
    // container = synapsemlfs
    // account_name = synapsemladlsgen2
    // path_to_file = testdir/testfile.txt
    // path_to_file_parts = [testdir, testfile.txt]

    // Expected input here => s = synapsemlfs/testdir/testfile.txt,
    // http://127.0.0.1/accountName/pathToBlob
    auto src = internal::RemoveTrailingSlash(s);
    if (arrow::internal::StartsWith(src, "https://127.0.0.1") || arrow::internal::StartsWith(src, "http://127.0.0.1")) {
      RETURN_NOT_OK(FromLocalHostString(&src));
    }
    auto input_path = std::string(src.data());
    if (internal::IsLikelyUri(input_path)) {
      RETURN_NOT_OK(ExtractBlobPath(&input_path));
      src = std::string_view(input_path);
    }
    src = internal::RemoveLeadingSlash(src);
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::IOError("Path cannot start with a separator ('", input_path, "')");
    }
    if (first_sep == std::string::npos) {
      return AzurePath{std::string(src), std::string(src), "", {}};
    }
    AzurePath path;
    path.full_path = std::string(src);
    path.container = std::string(src.substr(0, first_sep));
    path.path_to_file = std::string(src.substr(first_sep + 1));
    path.path_to_file_parts = internal::SplitAbstractPath(path.path_to_file);
    RETURN_NOT_OK(Validate(&path));
    return path;
  }

  static Status FromLocalHostString(std::string_view* src) {
    // src = http://127.0.0.1:10000/accountName/pathToBlob
    Uri uri;
    RETURN_NOT_OK(uri.Parse(src->data()));
    *src = internal::RemoveLeadingSlash(uri.path());
    if (src->empty()) {
      return Status::IOError("Missing account name in Azure Blob Storage URI");
    }
    auto first_sep = src->find_first_of(kSep);
    if (first_sep != std::string::npos) {
      *src = src->substr(first_sep + 1);
    } else {
      *src = "";
    }
    return Status::OK();
  }

  // Removes scheme, host and port from the uri
  static Status ExtractBlobPath(std::string* src) {
    Uri uri;
    RETURN_NOT_OK(uri.Parse(*src));
    *src = uri.path();
    return Status::OK();
  }

  static Status Validate(const AzurePath* path) {
    auto status = internal::ValidateAbstractPathParts(path->path_to_file_parts);
    if (!status.ok()) {
      return Status::Invalid(status.message(), " in path ", path->full_path);
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

// -----------------------------------------------------------------------
// AzureOptions Implementation

AzureOptions::AzureOptions() {}

bool AzureOptions::Equals(const AzureOptions& other) const {
  return (account_dfs_url == other.account_dfs_url &&
          account_blob_url == other.account_blob_url &&
          credentials_kind == other.credentials_kind);
}


class ObjectInputFile final : public io::RandomAccessFile {
 public:
  ObjectInputFile(
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient>& path_client,
      std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient>& file_client,
      const io::IOContext& io_context, const AzurePath& path, int64_t size = kNoSize)
      : path_client_(std::move(path_client)),
        file_client_(std::move(file_client)),
        io_context_(io_context),
        path_(path),
        content_length_(size) {}

  Status Init() {
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }
    try {
      auto properties = path_client_->GetProperties();
      if (properties.Value.IsDirectory) {
        return ::arrow::fs::internal::NotAFile(path_.full_path);
      }
      content_length_ = properties.Value.FileSize;
      metadata_ = GetObjectMetadata(properties.Value.Metadata);
      return Status::OK();
    } catch (const Azure::Storage::StorageException& exception) {
      return ::arrow::fs::internal::PathNotFound(path_.full_path);
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
    path_client_ = nullptr;
    file_client_ = nullptr;
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
          file_client_
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

 protected:
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakePathClient> path_client_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeFileClient> file_client_;
  const io::IOContext io_context_;
  AzurePath path_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

// -----------------------------------------------------------------------
// AzureFilesystem Implementation

class AzureFileSystem::Impl {
 public:
  io::IOContext io_context_;
  bool is_hierarchical_namespace_enabled_;
  AzureOptions options_;

  explicit Impl(AzureOptions options, io::IOContext io_context)
      : io_context_(io_context), options_(std::move(options)) {}

  Status Init() {
    // TODO: GH-18014 Delete this once we have a proper implementation. This just
    // initializes a pointless Azure blob service client with a fake endpoint to ensure
    // the build will fail if the Azure SDK build is broken.
    auto default_credential = std::make_shared<Azure::Identity::DefaultAzureCredential>();
    auto service_client = Azure::Storage::Blobs::BlobServiceClient(
        "http://fake-blob-storage-endpoint", default_credential);
    if (options_.backend == AzureBackend::Azurite) {
      // gen1Client_->GetAccountInfo().Value.IsHierarchicalNamespaceEnabled
      // throws error in azurite
      is_hierarchical_namespace_enabled_ = false;
    }
    return Status::OK();
  }

  const AzureOptions& options() const { return options_; }
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
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> AzureFileSystem::OpenInputStream(
    const FileInfo& info) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureFileSystem::OpenInputFile(
    const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureFileSystem::OpenInputFile(
    const FileInfo& info) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
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

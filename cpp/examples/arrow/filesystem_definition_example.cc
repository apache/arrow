// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/filesystem_library.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>
#include <arrow/util/uri.h>

// Demonstrate registering a user-defined Arrow FileSystem outside
// of the Arrow source tree.

using arrow::Result;
using arrow::Status;
namespace io = arrow::io;
namespace fs = arrow::fs;

class ExampleFileSystem : public fs::FileSystem {
 public:
  explicit ExampleFileSystem(const io::IOContext& io_context)
      : fs::FileSystem{io_context} {}

  // This is a mock filesystem whose root directory contains a single file.
  // All operations which would mutate will simply raise an error.
  static constexpr std::string_view kPath = "example_file";
  static constexpr std::string_view kContents = "hello world";
  static fs::FileInfo info() {
    fs::FileInfo info;
    info.set_path(std::string{kPath});
    info.set_type(fs::FileType::File);
    info.set_size(kContents.size());
    return info;
  }

  static Status NotFound(std::string_view path) {
    return Status::IOError("Path does not exist '", path, "'");
  }

  static Status NoMutation() {
    return Status::IOError("operations which would mutate are not permitted");
  }

  Result<std::string> PathFromUri(const std::string& uri_string) const override {
    ARROW_ASSIGN_OR_RAISE(auto uri, arrow::util::Uri::FromString(uri_string));
    return uri.path();
  }

  std::string type_name() const override { return "example"; }

  bool Equals(const FileSystem& other) const override {
    return type_name() == other.type_name();
  }

  /// \cond FALSE
  using FileSystem::CreateDir;
  using FileSystem::DeleteDirContents;
  using FileSystem::GetFileInfo;
  using FileSystem::OpenAppendStream;
  using FileSystem::OpenOutputStream;
  /// \endcond

  Result<fs::FileInfo> GetFileInfo(const std::string& path) override {
    if (path == kPath) {
      return info();
    }
    return NotFound(path);
  }

  Result<std::vector<fs::FileInfo>> GetFileInfo(const fs::FileSelector& select) override {
    if (select.base_dir == "/" || select.base_dir == "") {
      return std::vector<fs::FileInfo>{info()};
    }
    if (select.allow_not_found) {
      return std::vector<fs::FileInfo>{};
    }
    return NotFound(select.base_dir);
  }

  Status CreateDir(const std::string& path, bool recursive) override {
    return NoMutation();
  }

  Status DeleteDir(const std::string& path) override { return NoMutation(); }

  Status DeleteDirContents(const std::string& path, bool missing_dir_ok) override {
    return NoMutation();
  }

  Status DeleteRootDirContents() override { return NoMutation(); }

  Status DeleteFile(const std::string& path) override { return NoMutation(); }

  Status Move(const std::string& src, const std::string& dest) override {
    return NoMutation();
  }

  Status CopyFile(const std::string& src, const std::string& dest) override {
    return NoMutation();
  }

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override {
    return OpenInputFile(path);
  }

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override {
    if (path == kPath) {
      return io::BufferReader::FromString(std::string{kContents});
    }
    return NotFound(path);
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override {
    return NoMutation();
  }

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) override {
    return NoMutation();
  }
};

auto kExampleFileSystemModule = ARROW_REGISTER_FILESYSTEM(
    "example",
    [](const arrow::util::Uri& uri, const io::IOContext& io_context,
       std::string* out_path) -> Result<std::shared_ptr<fs::FileSystem>> {
      auto fs = std::make_shared<ExampleFileSystem>(io_context);
      if (out_path) {
        ARROW_ASSIGN_OR_RAISE(*out_path, fs->PathFromUri(uri.ToString()));
      }
      return fs;
    },
    {});

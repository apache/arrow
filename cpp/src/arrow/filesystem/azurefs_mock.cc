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

#include "arrow/filesystem/azurefs_mock.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"
#include "arrow/util/variant.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {
namespace fs {
namespace internal {

namespace {

Status ValidatePath(util::string_view s) {
  if (internal::IsLikelyUri(s)) {
    return Status::Invalid("Expected a filesystem path, got a URI: '", s, "'");
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////
// Filesystem structure

class Entry;

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

    // Expected input here => s = /synapsemlfs/testdir/testfile.txt
    auto src = internal::RemoveTrailingSlash(s);
    if (src.starts_with("https:") || src.starts_with("http::")) {
      RemoveSchemeFromUri(src);
    }
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
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

  static void RemoveSchemeFromUri(nonstd::sv_lite::string_view& s) {
    auto first = s.find(".core.windows.net");
    s = s.substr(first + 18, s.length());
  }

  static Status Validate(const AzurePath* path) {
    auto result = internal::ValidateAbstractPathParts(path->path_to_file_parts);
    if (!result.ok()) {
      return Status::Invalid(result.message(), " in path ", path->full_path);
    } else {
      return result;
    }
  }

  AzurePath parent() const {
    DCHECK(!path_to_file_parts.empty());
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

struct File {
  TimePoint mtime;
  std::string name;
  std::string path;
  std::shared_ptr<Buffer> data;
  std::shared_ptr<const KeyValueMetadata> metadata;

  File(TimePoint mtime, std::string name, std::string path)
      : mtime(mtime), name(std::move(name)), path(std::move(path)) {}

  int64_t size() const { return data ? data->size() : 0; }

  explicit operator util::string_view() const {
    if (data) {
      return util::string_view(*data);
    } else {
      return "";
    }
  }
};

struct Directory {
  std::string name;
  std::string path;
  TimePoint mtime;
  std::map<std::string, std::unique_ptr<Entry>> entries;

  Directory(std::string name, std::string path, TimePoint mtime)
      : name(std::move(name)), path(std::move(path)), mtime(mtime) {}
  Directory(Directory&& other) noexcept
      : name(std::move(other.name)),
        path(other.path),
        mtime(other.mtime),
        entries(std::move(other.entries)) {}

  Directory& operator=(Directory&& other) noexcept {
    name = std::move(other.name);
    mtime = other.mtime;
    path = std::move(other.path);
    entries = std::move(other.entries);
    return *this;
  }

  Entry* Find(const std::string& s) {
    auto it = entries.find(s);
    if (it != entries.end()) {
      return it->second.get();
    } else {
      return nullptr;
    }
  }

  bool CreateEntry(const std::string& s, std::unique_ptr<Entry> entry) {
    DCHECK(!s.empty());
    auto p = entries.emplace(s, std::move(entry));
    return p.second;
  }

  void AssignEntry(const std::string& s, std::unique_ptr<Entry> entry) {
    DCHECK(!s.empty());
    entries[s] = std::move(entry);
  }

  bool DeleteEntry(const std::string& s) { return entries.erase(s) > 0; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Directory);
};

// A filesystem entry
using EntryBase = util::Variant<std::nullptr_t, File, Directory>;

class Entry : public EntryBase {
 public:
  Entry(Entry&&) = default;
  Entry& operator=(Entry&&) = default;
  explicit Entry(Directory&& v) : EntryBase(std::move(v)) {}
  explicit Entry(File&& v) : EntryBase(std::move(v)) {}

  bool is_dir() const { return util::holds_alternative<Directory>(*this); }

  bool is_file() const { return util::holds_alternative<File>(*this); }

  Directory& as_dir() { return util::get<Directory>(*this); }

  File& as_file() { return util::get<File>(*this); }

  // Get info for this entry.  Note the path() property isn't set.
  FileInfo GetInfo() {
    FileInfo info;
    if (is_dir()) {
      Directory& dir = as_dir();
      info.set_type(FileType::Directory);
      info.set_mtime(dir.mtime);
      info.set_path(dir.path);
    } else {
      DCHECK(is_file());
      File& file = as_file();
      info.set_type(FileType::File);
      info.set_mtime(file.mtime);
      info.set_size(file.size());
      info.set_path(file.path);
    }
    return info;
  }

  // Get info for this entry, knowing the parent path.
  FileInfo GetInfo(const std::string& base_path) {
    FileInfo info;
    if (is_dir()) {
      Directory& dir = as_dir();
      info.set_type(FileType::Directory);
      info.set_mtime(dir.mtime);
      info.set_path(ConcatAbstractPath(base_path, dir.name));
    } else {
      DCHECK(is_file());
      File& file = as_file();
      info.set_type(FileType::File);
      info.set_mtime(file.mtime);
      info.set_size(file.size());
      info.set_path(ConcatAbstractPath(base_path, file.name));
    }
    return info;
  }

  // Set the entry name
  void SetName(const std::string& name) {
    if (is_dir()) {
      as_dir().name = name;
    } else {
      DCHECK(is_file());
      as_file().name = name;
    }
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Entry);
};

////////////////////////////////////////////////////////////////////////////
// Streams

class MockFSOutputStream : public io::OutputStream {
 public:
  MockFSOutputStream(File* file, MemoryPool* pool)
      : file_(file), builder_(pool), closed_(false) {}

  ~MockFSOutputStream() override = default;

  // Implement the OutputStream interface
  Status Close() override {
    if (!closed_) {
      RETURN_NOT_OK(builder_.Finish(&file_->data));
      closed_ = true;
    }
    return Status::OK();
  }

  Status Abort() override {
    if (!closed_) {
      // MockFSOutputStream is mainly used for debugging and testing, so
      // mark an aborted file's contents explicitly.
      std::stringstream ss;
      ss << "MockFSOutputStream aborted after " << file_->size() << " bytes written";
      file_->data = Buffer::FromString(ss.str());
      closed_ = true;
    }
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    if (closed_) {
      return Status::Invalid("Invalid operation on closed stream");
    }
    return builder_.length();
  }

  Status Write(const void* data, int64_t nbytes) override {
    if (closed_) {
      return Status::Invalid("Invalid operation on closed stream");
    }
    return builder_.Append(data, nbytes);
  }

 protected:
  File* file_;
  BufferBuilder builder_;
  bool closed_;
};

class MockFSInputStream : public io::BufferReader {
 public:
  explicit MockFSInputStream(const File& file)
      : io::BufferReader(file.data), metadata_(file.metadata) {}

  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    return metadata_;
  }

 protected:
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

}  // namespace

////////////////////////////////////////////////////////////////////////////
// MockAzureFileSystem implementation

class MockAzureFileSystem::Impl {
 public:
  TimePoint current_time;
  MemoryPool* pool;

  // The root directory
  Entry root;
  std::mutex mutex;

  Impl(TimePoint current_time, MemoryPool* pool)
      : current_time(current_time), pool(pool), root(Directory("", "", current_time)) {}

  std::unique_lock<std::mutex> lock_guard() {
    return std::unique_lock<std::mutex>(mutex);
  }

  Directory& RootDir() { return root.as_dir(); }

  template <typename It>
  Entry* FindEntry(It it, It end, size_t* nconsumed) {
    size_t consumed = 0;
    Entry* entry = &root;

    for (; it != end; ++it) {
      const std::string& part = *it;
      DCHECK(entry->is_dir());
      Entry* child = entry->as_dir().Find(part);
      if (child == nullptr) {
        // Partial find only
        break;
      }
      ++consumed;
      entry = child;
      if (entry->is_file()) {
        // Cannot go any further
        break;
      }
      // Recurse
    }
    *nconsumed = consumed;
    return entry;
  }

  // Find an entry, allowing partial matching
  Entry* FindEntry(const std::vector<std::string>& parts, size_t* nconsumed) {
    return FindEntry(parts.begin(), parts.end(), nconsumed);
  }

  // Find an entry, only full matching allowed
  Entry* FindEntry(const std::vector<std::string>& parts) {
    size_t consumed;
    auto entry = FindEntry(parts, &consumed);
    return (consumed == parts.size()) ? entry : nullptr;
  }

  // Find the parent entry, only full matching allowed
  Entry* FindParent(const std::vector<std::string>& parts) {
    if (parts.size() == 0) {
      return nullptr;
    }
    size_t consumed;
    auto entry = FindEntry(parts.begin(), --parts.end(), &consumed);
    return (consumed == parts.size() - 1) ? entry : nullptr;
  }

  bool CheckFile(const std::string& prefix, const Directory& dir,
                 const MockFileInfo& expected) {
    std::string path = prefix + dir.name;
    if (!path.empty()) {
      path += "/";
    }
    for (const auto& pair : dir.entries) {
      Entry* child = pair.second.get();
      if (child->is_file()) {
        auto& file = child->as_file();
        if ((path + file.name) == expected.full_path) {
          if (util::string_view(file) == expected.data) {
            return true;
          }
        }
      }
    }
    bool res = false;
    for (const auto& pair : dir.entries) {
      Entry* child = pair.second.get();
      if (child->is_dir()) {
        res = res || CheckFile(path, child->as_dir(), expected);
      }
    }
    return res;
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path, bool append,
      const std::shared_ptr<const KeyValueMetadata>& metadata) {
    auto parts = SplitAbstractPath(path);
    RETURN_NOT_OK(ValidateAbstractPathParts(parts));

    Entry* parent = FindParent(parts);
    if (parent == nullptr || !parent->is_dir()) {
      return PathNotFound(path);
    }
    // Find the file in the parent dir, or create it
    const auto& name = parts.back();
    Entry* child = parent->as_dir().Find(name);
    File* file;
    if (child == nullptr) {
      child = new Entry(File(current_time, name, path));
      parent->as_dir().AssignEntry(name, std::unique_ptr<Entry>(child));
      file = &child->as_file();
      file->path = path;
    } else if (child->is_file()) {
      file = &child->as_file();
      file->mtime = current_time;
      file->path = path;
    } else {
      return NotAFile(path);
    }
    file->metadata = metadata;
    auto ptr = std::make_shared<MockFSOutputStream>(file, pool);
    if (append && file->data) {
      RETURN_NOT_OK(ptr->Write(file->data->data(), file->data->size()));
    }
    return ptr;
  }

  Result<std::shared_ptr<io::BufferReader>> OpenInputReader(const std::string& path) {
    auto parts = SplitAbstractPath(path);
    RETURN_NOT_OK(ValidateAbstractPathParts(parts));

    Entry* entry = FindEntry(parts);
    if (entry == nullptr) {
      return PathNotFound(path);
    }
    if (!entry->is_file()) {
      return NotAFile(path);
    }
    return std::make_shared<MockFSInputStream>(entry->as_file());
  }

  // Create a container. Successful if container already exists.
  Status CreateContainer(const std::string& container) {
    auto parts = SplitAbstractPath(container);
    size_t consumed;
    Entry* entry = FindEntry(parts, &consumed);
    if (consumed != 0) {
      return Status::OK();
    }
    std::unique_ptr<Entry> child(
        new Entry(Directory(container, container, current_time)));
    child.get();
    bool inserted = entry->as_dir().CreateEntry(container, std::move(child));
    DCHECK(inserted);
    return Status::OK();
  }

  // Tests to see if a container exists
  Result<bool> ContainerExists(const std::string& container) {
    auto parts = SplitAbstractPath(container);
    size_t consumed;
    FindEntry(parts, &consumed);
    if (consumed != 0) {
      return true;
    }
    return false;
  }

  Result<bool> DirExists(const std::string& s) {
    auto parts = SplitAbstractPath(s);
    Entry* entry = FindEntry(parts);
    if (entry == nullptr || !entry->is_dir()) {
      return false;
    }
    return true;
  }

  Result<bool> FileExists(const std::string& s) {
    auto parts = SplitAbstractPath(s);
    Entry* entry = FindEntry(parts);
    if (entry == nullptr || !entry->is_file()) {
      return false;
    }
    return true;
  }

  Status CreateEmptyDir(const std::string& container,
                        const std::vector<std::string>& path) {
    std::vector<std::string> parts = path;
    parts.insert(parts.begin(), container);
    size_t consumed;
    Entry* entry = FindEntry(parts, &consumed);
    if (!entry->is_dir()) {
      auto file_path = JoinAbstractPath(parts.begin(), parts.begin() + consumed);
      return Status::IOError("Cannot create directory: ", "ancestor '", file_path,
                             "' is not a directory");
    }
    std::string str;
    for (size_t i = 0; i < consumed; ++i) {
      str += parts[i];
      if ((i + 1) < consumed) {
        str += "/";
      }
    }
    for (size_t i = consumed; i < parts.size(); ++i) {
      const auto& name = parts[i];
      str += "/";
      str += name;
      std::unique_ptr<Entry> child(new Entry(Directory(name, str, current_time)));
      Entry* child_ptr = child.get();
      bool inserted = entry->as_dir().CreateEntry(name, std::move(child));
      DCHECK(inserted);
      entry = child_ptr;
    }
    return Status::OK();
  }

  Status DeleteContainer(const std::string& container, Directory& rootDir) {
    auto child = rootDir.Find(container);
    if (child == nullptr) {
      return Status::OK();
    }
    bool deleted = rootDir.DeleteEntry(container);
    DCHECK(deleted);
    return Status::OK();
  }

  Status DeleteDir(const std::string& container, const std::vector<std::string>& path,
                   const std::string& path_to_dir) {
    std::vector<std::string> parts = path;
    parts.insert(parts.begin(), container);

    Entry* parent = FindParent(parts);
    if (parent == nullptr || !parent->is_dir()) {
      return PathNotFound(path_to_dir);
    }
    Directory& parent_dir = parent->as_dir();
    auto child = parent_dir.Find(parts.back());
    if (child == nullptr) {
      return PathNotFound(path_to_dir);
    }
    if (!child->is_dir()) {
      return NotADir(path_to_dir);
    }

    bool deleted = parent_dir.DeleteEntry(parts.back());
    DCHECK(deleted);
    return Status::OK();
  }

  Status DeleteFile(const std::string& container, const std::vector<std::string>& path,
                    const std::string& path_to_file) {
    if (path.empty()) {
      return Status::IOError("Cannot delete File, Invalid File Path");
    }
    std::vector<std::string> parts = path;
    parts.insert(parts.begin(), container);

    Entry* parent = FindParent(parts);
    if (parent == nullptr || !parent->is_dir()) {
      return PathNotFound(path_to_file);
    }
    Directory& parent_dir = parent->as_dir();
    auto child = parent_dir.Find(parts.back());
    if (child == nullptr) {
      return PathNotFound(path_to_file);
    }
    if (!child->is_file()) {
      return NotAFile(path_to_file);
    }
    bool deleted = parent_dir.DeleteEntry(parts.back());
    DCHECK(deleted);
    return Status::OK();
  }

  Status ListPaths(const std::string& container, const std::string& path,
                   std::vector<std::string>* childrenDirs,
                   std::vector<std::string>* childrenFiles,
                   const bool allow_not_found = false) {
    auto parts = SplitAbstractPath(path);
    parts.insert(parts.begin(), container);
    Entry* entry = FindEntry(parts);
    Directory& base_dir = entry->as_dir();
    try {
      for (const auto& pair : base_dir.entries) {
        Entry* child = pair.second.get();
        if (child->is_dir()) {
          childrenDirs->push_back(child->GetInfo().path());
        }
        if (child->is_file()) {
          childrenFiles->push_back(child->GetInfo().path());
        }
      }
    } catch (std::exception const& e) {
      if (!allow_not_found) {
        return Status::IOError("Path does not exists");
      }
    }
    return Status::OK();
  }

  Status Walk(const FileSelector& select, const std::string& container,
              const std::string& path, int nesting_depth, std::vector<FileInfo>* out) {
    std::vector<std::string> childrenDirs;
    std::vector<std::string> childrenFiles;

    Status st =
        ListPaths(container, path, &childrenDirs, &childrenFiles, select.allow_not_found);
    if (!st.ok()) {
      return st;
    }

    for (const auto& childFile : childrenFiles) {
      FileInfo info;
      auto parts = SplitAbstractPath(childFile);
      Entry* entry = FindEntry(parts);
      info = entry->GetInfo();
      out->push_back(std::move(info));
    }
    for (const auto& childDir : childrenDirs) {
      FileInfo info;
      auto parts = SplitAbstractPath(childDir);
      Entry* entry = FindEntry(parts);
      if (entry == nullptr) {
        return Status::OK();
      }
      info = entry->GetInfo();
      out->push_back(std::move(info));
      if (select.recursive && nesting_depth < select.max_recursion) {
        const auto src = internal::RemoveTrailingSlash(childDir);
        auto first_sep = src.find_first_of("/");
        std::string s = std::string(src.substr(first_sep + 1));
        RETURN_NOT_OK(Walk(select, container, s, nesting_depth + 1, out));
      }
    }
    return Status::OK();
  }

  Status DeleteDirContents(const std::string& container, const std::string& path,
                           const std::vector<std::string>& path_to_file_parts) {
    std::vector<std::string> childrenDirs;
    std::vector<std::string> childrenFiles;

    Status st = ListPaths(container, path, &childrenDirs, &childrenFiles);
    if (!st.ok()) {
      return st;
    }
    for (const auto& childFile : childrenFiles) {
      ARROW_ASSIGN_OR_RAISE(auto filePath, AzurePath::FromString(childFile));
      DeleteFile(filePath.container, filePath.path_to_file_parts, filePath.full_path);
    }
    for (const auto& childDir : childrenDirs) {
      ARROW_ASSIGN_OR_RAISE(auto dirPath, AzurePath::FromString(childDir));
      DeleteDir(dirPath.container, dirPath.path_to_file_parts, dirPath.full_path);
    }
    return Status::OK();
  }

  Result<std::vector<std::string>> ListContainers(const Directory& base_dir) {
    std::vector<std::string> containers;
    for (const auto& pair : base_dir.entries) {
      Entry* child = pair.second.get();
      containers.push_back(child->GetInfo().path());
    }
    return containers;
  }
};

MockAzureFileSystem::~MockAzureFileSystem() = default;

MockAzureFileSystem::MockAzureFileSystem(TimePoint current_time,
                                         const io::IOContext& io_context) {
  impl_ = std::unique_ptr<Impl>(new Impl(current_time, io_context.pool()));
}

bool MockAzureFileSystem::Equals(const FileSystem& other) const { return this == &other; }

Status MockAzureFileSystem::CreateDir(const std::string& s, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  if (path.empty()) {
    return Status::IOError("Cannot create directory, root path given");
  }
  if ((impl_->FileExists(path.full_path)).ValueOrDie()) {
    return Status::IOError("Cannot create directory, file exists at path");
  }
  if (path.path_to_file.empty()) {
    // Create container
    return impl_->CreateContainer(path.container);
  }
  if (recursive) {
    // Ensure container exists
    ARROW_ASSIGN_OR_RAISE(bool container_exists, impl_->ContainerExists(path.container));
    if (!container_exists) {
      RETURN_NOT_OK(impl_->CreateContainer(path.container));
    }
    std::vector<std::string> parent_path_to_file;

    for (const auto& part : path.path_to_file_parts) {
      parent_path_to_file.push_back(part);
      RETURN_NOT_OK(impl_->CreateEmptyDir(path.container, parent_path_to_file));
    }
    return Status::OK();
  } else {
    // Check parent dir exists
    if (path.has_parent()) {
      AzurePath parent_path = path.parent();
      if (parent_path.path_to_file.empty()) {
        auto exists = impl_->ContainerExists(parent_path.container);
        if (!(exists.ValueOrDie())) {
          return Status::IOError("Cannot create directory '", path.full_path,
                                 "': parent directory does not exist");
        }
      } else {
        auto exists = impl_->DirExists(parent_path.full_path);
        if (!(exists.ValueOrDie())) {
          return Status::IOError("Cannot create directory '", path.full_path,
                                 "': parent directory does not exist");
        }
      }
    }
    return impl_->CreateEmptyDir(path.container, path.path_to_file_parts);
  }
}

Status MockAzureFileSystem::DeleteDir(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  if (path.empty()) {
    return Status::NotImplemented("Cannot delete all Azure Containers");
  }
  if (path.path_to_file.empty()) {
    return impl_->DeleteContainer(path.container, impl_->RootDir());
  }
  if ((impl_->FileExists(path.full_path)).ValueOrDie()) {
    return Status::IOError("Cannot delete directory, file exists at path");
  }
  return impl_->DeleteDir(path.container, path.path_to_file_parts, path.full_path);
}

Status MockAzureFileSystem::DeleteDirContents(const std::string& s, bool missing_dir_ok) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  if (path.empty()) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return Status::IOError("Invalid path provided");
  }

  if (path.path_to_file.empty() &&
      !(impl_->ContainerExists(path.container).ValueOrDie())) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return Status::IOError("Invalid path provided1");
  }

  if (impl_->FileExists(path.full_path).ValueOrDie()) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return Status::IOError("Invalid path provided2");
  }

  if (!(path.path_to_file.empty()) && !(impl_->DirExists(path.full_path).ValueOrDie())) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return Status::IOError("Invalid path provided3");
  }

  return impl_->DeleteDirContents(path.container, path.path_to_file,
                                  path.path_to_file_parts);
}

Status MockAzureFileSystem::DeleteRootDirContents() {
  auto guard = impl_->lock_guard();

  impl_->RootDir().entries.clear();
  return Status::OK();
}

Status MockAzureFileSystem::DeleteFile(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  return impl_->DeleteFile(path.container, path.path_to_file_parts, path.full_path);
}

Result<FileInfo> MockAzureFileSystem::GetFileInfo(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  FileInfo info;
  info.set_path(s);

  if (path.empty()) {
    // It's the root path ""
    info.set_type(FileType::Directory);
    return info;
  } else if (path.path_to_file.empty()) {
    // It's a container
    ARROW_ASSIGN_OR_RAISE(bool container_exists, impl_->ContainerExists(path.container));
    if (!container_exists) {
      info.set_type(FileType::NotFound);
      return info;
    }
    info.set_type(FileType::Directory);
    return info;
  } else {
    // It's an object
    ARROW_ASSIGN_OR_RAISE(bool file_exists, impl_->FileExists(path.full_path));
    if (file_exists) {
      // "File" object found
      std::vector<std::string> parts = path.path_to_file_parts;
      parts.insert(parts.begin(), path.container);
      Entry* entry = impl_->FindEntry(parts);
      info = entry->GetInfo();
      info.set_path(s);
      return info;
    }
    // Not found => perhaps it's a "directory"
    auto is_dir = impl_->DirExists(path.full_path);
    if (is_dir.ValueOrDie()) {
      info.set_type(FileType::Directory);
    } else {
      info.set_type(FileType::NotFound);
    }
    return info;
  }
}

Result<FileInfoVector> MockAzureFileSystem::GetFileInfo(const FileSelector& select) {
  ARROW_ASSIGN_OR_RAISE(auto base_path, AzurePath::FromString(select.base_dir));

  FileInfoVector results;

  if (base_path.empty()) {
    // List all containers
    ARROW_ASSIGN_OR_RAISE(auto containers, impl_->ListContainers(impl_->RootDir()));
    for (const auto& container : containers) {
      FileInfo info;
      auto parts = SplitAbstractPath(container);
      Entry* entry = impl_->FindEntry(parts);
      info = entry->GetInfo();
      info.set_path(container);
      results.push_back(std::move(info));
      if (select.recursive) {
        RETURN_NOT_OK(impl_->Walk(select, container, "", 0, &results));
      }
    }
    return results;
  }

  if (base_path.path_to_file.empty() &&
      !(impl_->ContainerExists(base_path.container).ValueOrDie())) {
    if (!select.allow_not_found) {
      return Status::IOError("Invalid path provided");
    }
    return results;
  }

  if (impl_->FileExists(base_path.full_path).ValueOrDie()) {
    return Status::IOError("Invalid path provided");
  }

  if (!(base_path.path_to_file.empty()) &&
      !(impl_->DirExists(base_path.full_path).ValueOrDie())) {
    if (!select.allow_not_found) {
      return Status::IOError("Invalid path provided");
    }
    return results;
  }

  // Nominal case -> walk a single container
  RETURN_NOT_OK(
      impl_->Walk(select, base_path.container, base_path.path_to_file, 0, &results));
  return results;
}

namespace {

// Helper for binary operations (move, copy)
struct BinaryOp {
  std::vector<std::string> src_parts;
  std::vector<std::string> dest_parts;
  Directory& src_dir;
  Directory& dest_dir;
  std::string src_name;
  std::string dest_name;
  Entry* src_entry;
  Entry* dest_entry;

  template <typename OpFunc>
  static Status Run(MockAzureFileSystem::Impl* impl, const std::string& src,
                    const std::string& dest, OpFunc&& op_func) {
    RETURN_NOT_OK(ValidatePath(src));
    RETURN_NOT_OK(ValidatePath(dest));
    auto src_parts = SplitAbstractPath(src);
    auto dest_parts = SplitAbstractPath(dest);
    RETURN_NOT_OK(ValidateAbstractPathParts(src_parts));
    RETURN_NOT_OK(ValidateAbstractPathParts(dest_parts));

    auto guard = impl->lock_guard();

    // Both source and destination must have valid parents
    Entry* src_parent = impl->FindParent(src_parts);
    if (src_parent == nullptr || !src_parent->is_dir()) {
      return PathNotFound(src);
    }
    Entry* dest_parent = impl->FindParent(dest_parts);
    if (dest_parent == nullptr || !dest_parent->is_dir()) {
      return PathNotFound(dest);
    }
    Directory& src_dir = src_parent->as_dir();
    Directory& dest_dir = dest_parent->as_dir();
    DCHECK_GE(src_parts.size(), 1);
    DCHECK_GE(dest_parts.size(), 1);
    const auto& src_name = src_parts.back();
    const auto& dest_name = dest_parts.back();

    BinaryOp op{std::move(src_parts),
                std::move(dest_parts),
                src_dir,
                dest_dir,
                src_name,
                dest_name,
                src_dir.Find(src_name),
                dest_dir.Find(dest_name)};

    return op_func(std::move(op));
  }
};

}  // namespace

Status MockAzureFileSystem::Move(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto src_path, AzurePath::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto dest_path, AzurePath::FromString(dest));

  if (src_path == dest_path) {
    return Status::OK();
  }
  return BinaryOp::Run(impl_.get(), src, dest, [&](const BinaryOp& op) -> Status {
    if (op.src_entry == nullptr) {
      return PathNotFound(src);
    }
    if (op.dest_entry != nullptr) {
      if (op.dest_entry->is_file() && op.src_entry->is_dir()) {
        return Status::IOError("Cannot replace destination '", dest,
                               "', which is a file, with directory '", src, "'");
      }
      if (op.dest_entry->is_dir() && op.src_entry->is_file()) {
        return Status::IOError("Cannot replace destination '", dest,
                               "', which is a directory, with file '", src, "'");
      }
      if (op.dest_entry->is_dir() && op.dest_entry->as_dir().entries.size() != 0) {
        return Status::IOError("Cannot replace destination '", dest,
                               "', destination not empty");
      }
    }
    if (op.src_parts.size() < op.dest_parts.size()) {
      // Check if dest is a child of src
      auto p =
          std::mismatch(op.src_parts.begin(), op.src_parts.end(), op.dest_parts.begin());
      if (p.first == op.src_parts.end()) {
        return Status::IOError("Cannot move '", src, "' into child path '", dest, "'");
      }
    }
    auto path = src_path.path_to_file_parts;
    std::unique_ptr<Entry> new_entry(new Entry(std::move(*op.src_entry)));
    new_entry->SetName(op.dest_name);
    bool deleted = op.src_dir.DeleteEntry(op.src_name);
    DCHECK(deleted);
    op.dest_dir.AssignEntry(op.dest_name, std::move(new_entry));
    return Status::OK();
  });
}

Status MockAzureFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return BinaryOp::Run(impl_.get(), src, dest, [&](const BinaryOp& op) -> Status {
    if (op.src_entry == nullptr) {
      return PathNotFound(src);
    }
    if (!op.src_entry->is_file()) {
      return NotAFile(src);
    }
    if (op.dest_parts.size() == 1) {
      return Status::IOError("Cannot copy destination '", dest,
                             "', which is a container");
    }
    if (op.dest_entry != nullptr) {
      if (op.dest_entry->is_file() && op.src_entry->is_dir()) {
        return Status::IOError("Cannot copy destination '", dest,
                               "', which is a file, with directory '", src, "'");
      }
      if (op.dest_entry->is_dir() && op.src_entry->is_file()) {
        return Status::IOError("Cannot copy destination '", dest,
                               "', which is a directory, with file '", src, "'");
      }
    }
    if (op.dest_entry != nullptr && op.dest_entry->is_dir()) {
      return Status::IOError("Cannot replace destination '", dest,
                             "', which is a directory");
    }

    // Copy original entry, fix its name
    std::unique_ptr<Entry> new_entry(new Entry(File(op.src_entry->as_file())));
    new_entry->SetName(op.dest_name);
    op.dest_dir.AssignEntry(op.dest_name, std::move(new_entry));
    return Status::OK();
  });
}

Result<std::shared_ptr<io::InputStream>> MockAzureFileSystem::OpenInputStream(
    const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  if (path.empty()) {
    return Status::IOError("Invalid path provided");
  }
  if (!(impl_->FileExists(s)).ValueOrDie()) {
    return Status::IOError("Invalid path provided");
  }
  return impl_->OpenInputReader(s);
}

Result<std::shared_ptr<io::RandomAccessFile>> MockAzureFileSystem::OpenInputFile(
    const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  if (path.empty()) {
    return Status::IOError("Invalid path provided");
  }
  if (!(impl_->FileExists(s)).ValueOrDie()) {
    return Status::IOError("Invalid path provided");
  }
  return impl_->OpenInputReader(s);
}

Result<std::shared_ptr<io::OutputStream>> MockAzureFileSystem::OpenOutputStream(
    const std::string& s, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  if (path.empty() || path.path_to_file.empty()) {
    return Status::IOError("Invalid path provided");
  }
  if (impl_->DirExists(s).ValueOrDie()) {
    return Status::IOError("Invalid path provided");
  }
  if (path.has_parent()) {
    AzurePath parent_path = path.parent();
    if (parent_path.path_to_file.empty()) {
      if (!impl_->ContainerExists(parent_path.container).ValueOrDie()) {
        return Status::IOError("Cannot write to file '", path.full_path,
                               "': parent directory does not exist");
      }
    } else {
      auto exists = impl_->DirExists(parent_path.full_path);
      if (!(exists.ValueOrDie())) {
        return Status::IOError("Cannot write to file '", path.full_path,
                               "': parent directory does not exist");
      }
    }
  }
  return impl_->OpenOutputStream(s, /*append=*/false, metadata);
}

Result<std::shared_ptr<io::OutputStream>> MockAzureFileSystem::OpenAppendStream(
    const std::string& s, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  return impl_->OpenOutputStream(s, /*append=*/true, metadata);
}

bool MockAzureFileSystem::CheckFile(const MockFileInfo& expected) {
  auto guard = impl_->lock_guard();

  return impl_->CheckFile("", impl_->RootDir(), expected);
}

Status MockAzureFileSystem::CreateFile(const std::string& path,
                                       util::string_view contents, bool recursive) {
  RETURN_NOT_OK(ValidatePath(path));
  auto parent = fs::internal::GetAbstractPathParent(path).first;

  if (parent != "") {
    RETURN_NOT_OK(CreateDir(parent, recursive));
  }

  ARROW_ASSIGN_OR_RAISE(auto file, OpenOutputStream(path));
  RETURN_NOT_OK(file->Write(contents));
  return file->Close();
}

Result<std::shared_ptr<FileSystem>> MockAzureFileSystem::Make(
    TimePoint current_time, const std::vector<FileInfo>& infos) {
  auto fs = std::make_shared<MockAzureFileSystem>(current_time);
  for (const auto& info : infos) {
    switch (info.type()) {
      case FileType::Directory:
        RETURN_NOT_OK(fs->CreateDir(info.path(), /*recursive*/ true));
        break;
      case FileType::File:
        RETURN_NOT_OK(fs->CreateFile(info.path(), "", /*recursive*/ true));
        break;
      default:
        break;
    }
  }

  return fs;
}
}  // namespace internal
}  // namespace fs
}  // namespace arrow

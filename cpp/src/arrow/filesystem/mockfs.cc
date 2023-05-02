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

#include <algorithm>
#include <iterator>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
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
#include "arrow/util/windows_fixup.h"

namespace arrow {
namespace fs {
namespace internal {

namespace {

Status ValidatePath(std::string_view s) {
  if (internal::IsLikelyUri(s)) {
    return Status::Invalid("Expected a filesystem path, got a URI: '", s, "'");
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////
// Filesystem structure

class Entry;

struct File {
  TimePoint mtime;
  std::string name;
  std::shared_ptr<Buffer> data;
  std::shared_ptr<const KeyValueMetadata> metadata;

  File(TimePoint mtime, std::string name) : mtime(mtime), name(std::move(name)) {}

  int64_t size() const { return data ? data->size() : 0; }

  explicit operator std::string_view() const {
    if (data) {
      return std::string_view(*data);
    } else {
      return "";
    }
  }
};

struct Directory {
  std::string name;
  TimePoint mtime;
  std::map<std::string, std::unique_ptr<Entry>> entries;

  Directory(std::string name, TimePoint mtime) : name(std::move(name)), mtime(mtime) {}
  Directory(Directory&& other) noexcept
      : name(std::move(other.name)),
        mtime(other.mtime),
        entries(std::move(other.entries)) {}

  Directory& operator=(Directory&& other) noexcept {
    name = std::move(other.name);
    mtime = other.mtime;
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
using EntryBase = std::variant<std::nullptr_t, File, Directory>;

class Entry : public EntryBase {
 public:
  Entry(Entry&&) = default;
  Entry& operator=(Entry&&) = default;
  explicit Entry(Directory&& v) : EntryBase(std::move(v)) {}
  explicit Entry(File&& v) : EntryBase(std::move(v)) {}

  bool is_dir() const { return std::holds_alternative<Directory>(*this); }

  bool is_file() const { return std::holds_alternative<File>(*this); }

  Directory& as_dir() { return std::get<Directory>(*this); }

  File& as_file() { return std::get<File>(*this); }

  // Get info for this entry.  Note the path() property isn't set.
  FileInfo GetInfo() {
    FileInfo info;
    if (is_dir()) {
      Directory& dir = as_dir();
      info.set_type(FileType::Directory);
      info.set_mtime(dir.mtime);
    } else {
      DCHECK(is_file());
      File& file = as_file();
      info.set_type(FileType::File);
      info.set_mtime(file.mtime);
      info.set_size(file.size());
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

std::ostream& operator<<(std::ostream& os, const MockDirInfo& di) {
  return os << "'" << di.full_path << "' [mtime=" << di.mtime.time_since_epoch().count()
            << "]";
}

std::ostream& operator<<(std::ostream& os, const MockFileInfo& di) {
  return os << "'" << di.full_path << "' [mtime=" << di.mtime.time_since_epoch().count()
            << ", size=" << di.data.length() << "]";
}

////////////////////////////////////////////////////////////////////////////
// MockFileSystem implementation

class MockFileSystem::Impl {
 public:
  TimePoint current_time;
  MemoryPool* pool;

  // The root directory
  Entry root;
  std::mutex mutex;

  Impl(TimePoint current_time, MemoryPool* pool)
      : current_time(current_time), pool(pool), root(Directory("", current_time)) {}

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

  void GatherInfos(const FileSelector& select, const std::string& base_path,
                   const Directory& base_dir, int32_t nesting_depth,
                   std::vector<FileInfo>* infos) {
    for (const auto& pair : base_dir.entries) {
      Entry* child = pair.second.get();
      infos->push_back(child->GetInfo(base_path));
      if (select.recursive && nesting_depth < select.max_recursion && child->is_dir()) {
        Directory& child_dir = child->as_dir();
        std::string child_path = infos->back().path();
        GatherInfos(select, std::move(child_path), child_dir, nesting_depth + 1, infos);
      }
    }
  }

  void DumpDirs(const std::string& prefix, const Directory& dir,
                std::vector<MockDirInfo>* out) {
    std::string path = prefix + dir.name;
    if (!path.empty()) {
      out->push_back({path, dir.mtime});
      path += "/";
    }
    for (const auto& pair : dir.entries) {
      Entry* child = pair.second.get();
      if (child->is_dir()) {
        DumpDirs(path, child->as_dir(), out);
      }
    }
  }

  void DumpFiles(const std::string& prefix, const Directory& dir,
                 std::vector<MockFileInfo>* out) {
    std::string path = prefix + dir.name;
    if (!path.empty()) {
      path += "/";
    }
    for (const auto& pair : dir.entries) {
      Entry* child = pair.second.get();
      if (child->is_file()) {
        auto& file = child->as_file();
        out->push_back({path + file.name, file.mtime, std::string_view(file)});
      } else if (child->is_dir()) {
        DumpFiles(path, child->as_dir(), out);
      }
    }
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path, bool append,
      const std::shared_ptr<const KeyValueMetadata>& metadata) {
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(path));
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
      child = new Entry(File(current_time, name));
      parent->as_dir().AssignEntry(name, std::unique_ptr<Entry>(child));
      file = &child->as_file();
    } else if (child->is_file()) {
      file = &child->as_file();
      file->mtime = current_time;
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
    ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(path));
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
};

MockFileSystem::~MockFileSystem() = default;

MockFileSystem::MockFileSystem(TimePoint current_time, const io::IOContext& io_context) {
  impl_ = std::make_unique<Impl>(current_time, io_context.pool());
}

bool MockFileSystem::Equals(const FileSystem& other) const { return this == &other; }

Result<std::string> MockFileSystem::PathFromUri(const std::string& uri_string) const {
  ARROW_ASSIGN_OR_RAISE(
      std::string parsed_path,
      internal::PathFromUriHelper(uri_string, {"mock"}, /*accept_local_paths=*/true,
                                  internal::AuthorityHandlingBehavior::kDisallow));
  return std::string(internal::RemoveLeadingSlash(parsed_path));
}

Status MockFileSystem::CreateDir(const std::string& path, bool recursive) {
  RETURN_NOT_OK(ValidatePath(path));
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  auto guard = impl_->lock_guard();

  size_t consumed;
  Entry* entry = impl_->FindEntry(parts, &consumed);
  if (!entry->is_dir()) {
    auto file_path = JoinAbstractPath(parts.begin(), parts.begin() + consumed);
    return Status::IOError("Cannot create directory '", path, "': ", "ancestor '",
                           file_path, "' is not a directory");
  }
  if (!recursive && (parts.size() - consumed) > 1) {
    return Status::IOError("Cannot create directory '", path,
                           "': ", "parent does not exist");
  }
  for (size_t i = consumed; i < parts.size(); ++i) {
    const auto& name = parts[i];
    std::unique_ptr<Entry> child(new Entry(Directory(name, impl_->current_time)));
    Entry* child_ptr = child.get();
    bool inserted = entry->as_dir().CreateEntry(name, std::move(child));
    // No race condition on insertion is possible, as all operations are locked
    DCHECK(inserted);
    entry = child_ptr;
  }
  return Status::OK();
}

Status MockFileSystem::DeleteDir(const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  auto guard = impl_->lock_guard();

  Entry* parent = impl_->FindParent(parts);
  if (parent == nullptr || !parent->is_dir()) {
    return PathNotFound(path);
  }
  Directory& parent_dir = parent->as_dir();
  auto child = parent_dir.Find(parts.back());
  if (child == nullptr) {
    return PathNotFound(path);
  }
  if (!child->is_dir()) {
    return NotADir(path);
  }

  bool deleted = parent_dir.DeleteEntry(parts.back());
  DCHECK(deleted);
  return Status::OK();
}

Status MockFileSystem::DeleteDirContents(const std::string& path, bool missing_dir_ok) {
  RETURN_NOT_OK(ValidatePath(path));
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  auto guard = impl_->lock_guard();

  if (parts.empty()) {
    // Wipe filesystem
    return internal::InvalidDeleteDirContents(path);
  }

  Entry* entry = impl_->FindEntry(parts);
  if (entry == nullptr) {
    if (missing_dir_ok) {
      return Status::OK();
    }
    return PathNotFound(path);
  }
  if (!entry->is_dir()) {
    return NotADir(path);
  }
  entry->as_dir().entries.clear();
  return Status::OK();
}

Status MockFileSystem::DeleteRootDirContents() {
  auto guard = impl_->lock_guard();

  impl_->RootDir().entries.clear();
  return Status::OK();
}

Status MockFileSystem::DeleteFile(const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  auto guard = impl_->lock_guard();

  Entry* parent = impl_->FindParent(parts);
  if (parent == nullptr || !parent->is_dir()) {
    return PathNotFound(path);
  }
  Directory& parent_dir = parent->as_dir();
  auto child = parent_dir.Find(parts.back());
  if (child == nullptr) {
    return PathNotFound(path);
  }
  if (!child->is_file()) {
    return NotAFile(path);
  }
  bool deleted = parent_dir.DeleteEntry(parts.back());
  DCHECK(deleted);
  return Status::OK();
}

Result<FileInfo> MockFileSystem::GetFileInfo(const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  auto guard = impl_->lock_guard();

  FileInfo info;
  Entry* entry = impl_->FindEntry(parts);
  if (entry == nullptr) {
    info.set_type(FileType::NotFound);
  } else {
    info = entry->GetInfo();
  }
  info.set_path(path);
  return info;
}

Result<FileInfoVector> MockFileSystem::GetFileInfo(const FileSelector& selector) {
  RETURN_NOT_OK(ValidatePath(selector.base_dir));
  auto parts = SplitAbstractPath(selector.base_dir);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  auto guard = impl_->lock_guard();

  FileInfoVector results;

  Entry* base_dir = impl_->FindEntry(parts);
  if (base_dir == nullptr) {
    // Base directory does not exist
    if (selector.allow_not_found) {
      return results;
    } else {
      return PathNotFound(selector.base_dir);
    }
  }
  if (!base_dir->is_dir()) {
    return NotADir(selector.base_dir);
  }

  impl_->GatherInfos(selector, selector.base_dir, base_dir->as_dir(), 0, &results);
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
  static Status Run(MockFileSystem::Impl* impl, const std::string& src,
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

Status MockFileSystem::Move(const std::string& src, const std::string& dest) {
  return BinaryOp::Run(impl_.get(), src, dest, [&](const BinaryOp& op) -> Status {
    if (op.src_entry == nullptr) {
      return PathNotFound(src);
    }
    if (op.dest_entry != nullptr) {
      if (op.dest_entry->is_dir()) {
        return Status::IOError("Cannot replace destination '", dest,
                               "', which is a directory");
      }
      if (op.dest_entry->is_file() && op.src_entry->is_dir()) {
        return Status::IOError("Cannot replace destination '", dest,
                               "', which is a file, with directory '", src, "'");
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

    // Move original entry, fix its name
    std::unique_ptr<Entry> new_entry(new Entry(std::move(*op.src_entry)));
    new_entry->SetName(op.dest_name);
    bool deleted = op.src_dir.DeleteEntry(op.src_name);
    DCHECK(deleted);
    op.dest_dir.AssignEntry(op.dest_name, std::move(new_entry));
    return Status::OK();
  });
}

Status MockFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return BinaryOp::Run(impl_.get(), src, dest, [&](const BinaryOp& op) -> Status {
    if (op.src_entry == nullptr) {
      return PathNotFound(src);
    }
    if (!op.src_entry->is_file()) {
      return NotAFile(src);
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

Result<std::shared_ptr<io::InputStream>> MockFileSystem::OpenInputStream(
    const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  auto guard = impl_->lock_guard();

  return impl_->OpenInputReader(path);
}

Result<std::shared_ptr<io::RandomAccessFile>> MockFileSystem::OpenInputFile(
    const std::string& path) {
  RETURN_NOT_OK(ValidatePath(path));
  auto guard = impl_->lock_guard();

  return impl_->OpenInputReader(path);
}

Result<std::shared_ptr<io::OutputStream>> MockFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  RETURN_NOT_OK(ValidatePath(path));
  auto guard = impl_->lock_guard();

  return impl_->OpenOutputStream(path, /*append=*/false, metadata);
}

Result<std::shared_ptr<io::OutputStream>> MockFileSystem::OpenAppendStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  ARROW_RETURN_NOT_OK(internal::AssertNoTrailingSlash(path));
  RETURN_NOT_OK(ValidatePath(path));
  auto guard = impl_->lock_guard();

  return impl_->OpenOutputStream(path, /*append=*/true, metadata);
}

std::vector<MockDirInfo> MockFileSystem::AllDirs() {
  auto guard = impl_->lock_guard();

  std::vector<MockDirInfo> result;
  impl_->DumpDirs("", impl_->RootDir(), &result);
  return result;
}

std::vector<MockFileInfo> MockFileSystem::AllFiles() {
  auto guard = impl_->lock_guard();

  std::vector<MockFileInfo> result;
  impl_->DumpFiles("", impl_->RootDir(), &result);
  return result;
}

Status MockFileSystem::CreateFile(const std::string& path, std::string_view contents,
                                  bool recursive) {
  RETURN_NOT_OK(ValidatePath(path));
  auto parent = fs::internal::GetAbstractPathParent(path).first;

  if (parent != "") {
    RETURN_NOT_OK(CreateDir(parent, recursive));
  }

  ARROW_ASSIGN_OR_RAISE(auto file, OpenOutputStream(path));
  RETURN_NOT_OK(file->Write(contents));
  return file->Close();
}

Result<std::shared_ptr<FileSystem>> MockFileSystem::Make(
    TimePoint current_time, const std::vector<FileInfo>& infos) {
  auto fs = std::make_shared<MockFileSystem>(current_time);
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

FileInfoGenerator MockAsyncFileSystem::GetFileInfoGenerator(const FileSelector& select) {
  auto maybe_infos = GetFileInfo(select);
  if (maybe_infos.ok()) {
    // Return the FileInfo entries one by one
    const auto& infos = *maybe_infos;
    std::vector<FileInfoVector> chunks(infos.size());
    std::transform(infos.begin(), infos.end(), chunks.begin(),
                   [](const FileInfo& info) { return FileInfoVector{info}; });
    return MakeVectorGenerator(std::move(chunks));
  } else {
    return MakeFailingGenerator(maybe_infos);
  }
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

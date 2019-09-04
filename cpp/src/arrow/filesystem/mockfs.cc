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
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/logging.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace fs {
namespace internal {

namespace {

Status PathNotFound(const std::string& path) {
  return Status::IOError("Path does not exist '", path, "'");
}

Status NotADir(const std::string& path) {
  return Status::IOError("Not a directory: '", path, "'");
}

Status NotAFile(const std::string& path) {
  return Status::IOError("Not a regular file: '", path, "'");
}

////////////////////////////////////////////////////////////////////////////
// Filesystem structure

class Entry;

struct File {
  TimePoint mtime;
  std::string name;
  std::string data;

  File(TimePoint mtime, const std::string& name) : mtime(mtime), name(name) {}

  int64_t size() const { return static_cast<int64_t>(data.length()); }
};

struct Directory {
  std::string name;
  TimePoint mtime;
  std::map<std::string, std::unique_ptr<Entry>> entries;

  Directory(const std::string& name, TimePoint mtime) : name(name), mtime(mtime) {}
  Directory(Directory&&) = default;

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
using EntryBase = util::variant<File, Directory>;

class Entry : public EntryBase {
 public:
  Entry(Entry&&) = default;
  explicit Entry(Directory&& v) : EntryBase(std::move(v)) {}
  explicit Entry(File&& v) : EntryBase(std::move(v)) {}

  bool is_dir() const { return util::holds_alternative<Directory>(*this); }

  bool is_file() const { return util::holds_alternative<File>(*this); }

  Directory& as_dir() { return util::get<Directory>(*this); }

  File& as_file() { return util::get<File>(*this); }

  // Get stats for this entry.  Note the path() property isn't set.
  FileStats GetStats() {
    FileStats st;
    if (is_dir()) {
      Directory& dir = as_dir();
      st.set_type(FileType::Directory);
      st.set_mtime(dir.mtime);
    } else {
      DCHECK(is_file());
      File& file = as_file();
      st.set_type(FileType::File);
      st.set_mtime(file.mtime);
      st.set_size(file.size());
    }
    return st;
  }

  // Get stats for this entry, knowing the parent path.
  FileStats GetStats(const std::string& base_path) {
    FileStats st;
    if (is_dir()) {
      Directory& dir = as_dir();
      st.set_type(FileType::Directory);
      st.set_mtime(dir.mtime);
      st.set_path(ConcatAbstractPath(base_path, dir.name));
    } else {
      DCHECK(is_file());
      File& file = as_file();
      st.set_type(FileType::File);
      st.set_mtime(file.mtime);
      st.set_size(file.size());
      st.set_path(ConcatAbstractPath(base_path, file.name));
    }
    return st;
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
  explicit MockFSOutputStream(File* file) : file_(file), closed_(false) {}

  ~MockFSOutputStream() override {}

  // Implement the OutputStream interface
  Status Close() override {
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Status Tell(int64_t* position) const override {
    if (closed_) {
      return Status::Invalid("Invalid operation on closed stream");
    }
    *position = file_->size();
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) override {
    if (closed_) {
      return Status::Invalid("Invalid operation on closed stream");
    }
    file_->data.append(reinterpret_cast<const char*>(data), static_cast<size_t>(nbytes));
    return Status::OK();
  }

 protected:
  File* file_;
  bool closed_;
};

}  // namespace

std::ostream& operator<<(std::ostream& os, const DirInfo& di) {
  return os << "'" << di.full_path << "' [mtime=" << di.mtime.time_since_epoch().count()
            << "]";
}

std::ostream& operator<<(std::ostream& os, const FileInfo& di) {
  return os << "'" << di.full_path << "' [mtime=" << di.mtime.time_since_epoch().count()
            << ", size=" << di.data.length() << "]";
}

////////////////////////////////////////////////////////////////////////////
// MockFileSystem implementation

class MockFileSystem::Impl {
 public:
  TimePoint current_time;
  // The root directory
  Entry root;

  explicit Impl(TimePoint current_time)
      : current_time(current_time), root(Directory("", current_time)) {}

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

  void GatherStats(const std::string& base_path, Directory& base_dir, bool recursive,
                   std::vector<FileStats>* stats) {
    for (const auto& pair : base_dir.entries) {
      Entry* child = pair.second.get();
      stats->push_back(child->GetStats(base_path));
      if (recursive && child->is_dir()) {
        Directory& child_dir = child->as_dir();
        std::string child_path = stats->back().path();
        GatherStats(std::move(child_path), child_dir, recursive, stats);
      }
    }
  }

  void DumpDirs(const std::string& prefix, Directory& dir, std::vector<DirInfo>* out) {
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

  void DumpFiles(const std::string& prefix, Directory& dir, std::vector<FileInfo>* out) {
    std::string path = prefix + dir.name;
    if (!path.empty()) {
      path += "/";
    }
    for (const auto& pair : dir.entries) {
      Entry* child = pair.second.get();
      if (child->is_file()) {
        auto& file = child->as_file();
        out->push_back({path + file.name, file.mtime, file.data});
      } else if (child->is_dir()) {
        DumpFiles(path, child->as_dir(), out);
      }
    }
  }

  Status OpenOutputStream(const std::string& path, bool append,
                          std::shared_ptr<io::OutputStream>* out) {
    auto parts = SplitAbstractPath(path);
    RETURN_NOT_OK(ValidateAbstractPathParts(parts));

    Entry* parent = FindParent(parts);
    if (parent == nullptr || !parent->is_dir()) {
      return PathNotFound(path);
    }
    // Find the file in the parent dir, or create it
    const auto& name = parts.back();
    Entry* child = parent->as_dir().Find(name);
    if (child == nullptr) {
      child = new Entry(File(current_time, name));
      parent->as_dir().AssignEntry(name, std::unique_ptr<Entry>(child));
    } else if (child->is_file()) {
      child->as_file().mtime = current_time;
      if (!append) {
        child->as_file().data.clear();
      }
    } else {
      return NotAFile(path);
    }
    *out = std::make_shared<MockFSOutputStream>(&child->as_file());
    return Status::OK();
  }

  Status OpenInputReader(const std::string& path,
                         std::shared_ptr<io::BufferReader>* out) {
    auto parts = SplitAbstractPath(path);
    RETURN_NOT_OK(ValidateAbstractPathParts(parts));

    Entry* entry = FindEntry(parts);
    if (entry == nullptr) {
      return PathNotFound(path);
    }
    if (!entry->is_file()) {
      return NotAFile(path);
    }
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(Buffer::FromString(entry->as_file().data, &buffer));
    *out = std::make_shared<io::BufferReader>(buffer);
    return Status::OK();
  }
};

MockFileSystem::~MockFileSystem() {}

MockFileSystem::MockFileSystem(TimePoint current_time) {
  impl_ = std::unique_ptr<Impl>(new Impl(current_time));
}

Status MockFileSystem::CreateDir(const std::string& path, bool recursive) {
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  size_t consumed;
  Entry* entry = impl_->FindEntry(parts, &consumed);
  if (!entry->is_dir()) {
    auto file_path = JoinAbstractPath(parts.begin(), parts.begin() + consumed);
    return Status::IOError("Cannot create directory '", path, "': ", "ancestor '",
                           file_path, "' is a regular file");
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
    DCHECK(inserted);
    entry = child_ptr;
  }
  return Status::OK();
}

Status MockFileSystem::DeleteDir(const std::string& path) {
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

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

Status MockFileSystem::DeleteDirContents(const std::string& path) {
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  if (parts.empty()) {
    // Wipe filesystem
    impl_->RootDir().entries.clear();
    return Status::OK();
  }

  Entry* entry = impl_->FindEntry(parts);
  if (entry == nullptr) {
    return PathNotFound(path);
  }
  if (!entry->is_dir()) {
    return NotADir(path);
  }
  entry->as_dir().entries.clear();
  return Status::OK();
}

Status MockFileSystem::DeleteFile(const std::string& path) {
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

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

Status MockFileSystem::GetTargetStats(const std::string& path, FileStats* out) {
  auto parts = SplitAbstractPath(path);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  Entry* entry = impl_->FindEntry(parts);
  if (entry == nullptr) {
    FileStats st;
    st.set_path(path);
    st.set_type(FileType::NonExistent);
    *out = std::move(st);
    return Status::OK();
  }
  *out = entry->GetStats();
  out->set_path(path);
  return Status::OK();
}

Status MockFileSystem::GetTargetStats(const Selector& selector,
                                      std::vector<FileStats>* out) {
  auto parts = SplitAbstractPath(selector.base_dir);
  RETURN_NOT_OK(ValidateAbstractPathParts(parts));

  out->clear();

  Entry* base_dir = impl_->FindEntry(parts);
  if (base_dir == nullptr) {
    // Base directory does not exist
    if (selector.allow_non_existent) {
      return Status::OK();
    } else {
      return PathNotFound(selector.base_dir);
    }
  }
  if (!base_dir->is_dir()) {
    return NotADir(selector.base_dir);
  }

  impl_->GatherStats(selector.base_dir, base_dir->as_dir(), selector.recursive, out);
  return Status::OK();
}

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
    auto src_parts = SplitAbstractPath(src);
    auto dest_parts = SplitAbstractPath(dest);
    RETURN_NOT_OK(ValidateAbstractPathParts(src_parts));
    RETURN_NOT_OK(ValidateAbstractPathParts(dest_parts));

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

Status MockFileSystem::OpenInputStream(const std::string& path,
                                       std::shared_ptr<io::InputStream>* out) {
  std::shared_ptr<io::BufferReader> reader;
  RETURN_NOT_OK(impl_->OpenInputReader(path, &reader));
  *out = std::move(reader);
  return Status::OK();
}

Status MockFileSystem::OpenInputFile(const std::string& path,
                                     std::shared_ptr<io::RandomAccessFile>* out) {
  std::shared_ptr<io::BufferReader> reader;
  RETURN_NOT_OK(impl_->OpenInputReader(path, &reader));
  *out = std::move(reader);
  return Status::OK();
}

Status MockFileSystem::OpenOutputStream(const std::string& path,
                                        std::shared_ptr<io::OutputStream>* out) {
  return impl_->OpenOutputStream(path, false /* append */, out);
}

Status MockFileSystem::OpenAppendStream(const std::string& path,
                                        std::shared_ptr<io::OutputStream>* out) {
  return impl_->OpenOutputStream(path, true /* append */, out);
}

std::vector<DirInfo> MockFileSystem::AllDirs() {
  std::vector<DirInfo> result;
  impl_->DumpDirs("", impl_->RootDir(), &result);
  return result;
}

std::vector<FileInfo> MockFileSystem::AllFiles() {
  std::vector<FileInfo> result;
  impl_->DumpFiles("", impl_->RootDir(), &result);
  return result;
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

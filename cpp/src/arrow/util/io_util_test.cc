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
#include <cerrno>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/windows_compatibility.h"

#ifdef _WIN32
#ifdef DeleteFile
#undef DeleteFile
#endif
#endif

namespace arrow {
namespace internal {

void AssertExists(const PlatformFilename& path) {
  bool exists = false;
  ASSERT_OK_AND_ASSIGN(exists, FileExists(path));
  ASSERT_TRUE(exists) << "Path '" << path.ToString() << "' doesn't exist";
}

void AssertNotExists(const PlatformFilename& path) {
  bool exists = true;
  ASSERT_OK_AND_ASSIGN(exists, FileExists(path));
  ASSERT_FALSE(exists) << "Path '" << path.ToString() << "' exists";
}

TEST(ErrnoFromStatus, Basics) {
  Status st;
  st = Status::OK();
  ASSERT_EQ(ErrnoFromStatus(st), 0);
  st = Status::KeyError("foo");
  ASSERT_EQ(ErrnoFromStatus(st), 0);
  st = Status::IOError("foo");
  ASSERT_EQ(ErrnoFromStatus(st), 0);
  st = StatusFromErrno(EINVAL, StatusCode::KeyError, "foo");
  ASSERT_EQ(ErrnoFromStatus(st), EINVAL);
  st = IOErrorFromErrno(EPERM, "foo");
  ASSERT_EQ(ErrnoFromStatus(st), EPERM);
  st = IOErrorFromErrno(6789, "foo");
  ASSERT_EQ(ErrnoFromStatus(st), 6789);
}

#if _WIN32
TEST(WinErrorFromStatus, Basics) {
  Status st;
  st = Status::OK();
  ASSERT_EQ(WinErrorFromStatus(st), 0);
  st = Status::KeyError("foo");
  ASSERT_EQ(WinErrorFromStatus(st), 0);
  st = Status::IOError("foo");
  ASSERT_EQ(WinErrorFromStatus(st), 0);
  st = StatusFromWinError(ERROR_FILE_NOT_FOUND, StatusCode::KeyError, "foo");
  ASSERT_EQ(WinErrorFromStatus(st), ERROR_FILE_NOT_FOUND);
  st = IOErrorFromWinError(ERROR_ACCESS_DENIED, "foo");
  ASSERT_EQ(WinErrorFromStatus(st), ERROR_ACCESS_DENIED);
  st = IOErrorFromWinError(6789, "foo");
  ASSERT_EQ(WinErrorFromStatus(st), 6789);
}
#endif

TEST(PlatformFilename, RoundtripAscii) {
  PlatformFilename fn;
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("a/b"));
  ASSERT_EQ(fn.ToString(), "a/b");
#if _WIN32
  ASSERT_EQ(fn.ToNative(), L"a\\b");
#else
  ASSERT_EQ(fn.ToNative(), "a/b");
#endif
}

TEST(PlatformFilename, RoundtripUtf8) {
  PlatformFilename fn;
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("h\xc3\xa9h\xc3\xa9"));
  ASSERT_EQ(fn.ToString(), "h\xc3\xa9h\xc3\xa9");
#if _WIN32
  ASSERT_EQ(fn.ToNative(), L"h\u00e9h\u00e9");
#else
  ASSERT_EQ(fn.ToNative(), "h\xc3\xa9h\xc3\xa9");
#endif
}

#if _WIN32
TEST(PlatformFilename, Separators) {
  PlatformFilename fn;
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("C:/foo/bar"));
  ASSERT_EQ(fn.ToString(), "C:/foo/bar");
  ASSERT_EQ(fn.ToNative(), L"C:\\foo\\bar");

  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("C:\\foo\\bar"));
  ASSERT_EQ(fn.ToString(), "C:/foo/bar");
  ASSERT_EQ(fn.ToNative(), L"C:\\foo\\bar");
}
#endif

TEST(PlatformFilename, Invalid) {
  std::string s = "foo";
  s += '\x00';
  ASSERT_RAISES(Invalid, PlatformFilename::FromString(s));
}

TEST(PlatformFilename, Join) {
  PlatformFilename fn, joined;
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("a/b"));
  ASSERT_OK_AND_ASSIGN(joined, fn.Join("c/d"));
  ASSERT_EQ(joined.ToString(), "a/b/c/d");
#if _WIN32
  ASSERT_EQ(joined.ToNative(), L"a\\b\\c\\d");
#else
  ASSERT_EQ(joined.ToNative(), "a/b/c/d");
#endif

  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("a/b/"));
  ASSERT_OK_AND_ASSIGN(joined, fn.Join("c/d"));
  ASSERT_EQ(joined.ToString(), "a/b/c/d");
#if _WIN32
  ASSERT_EQ(joined.ToNative(), L"a\\b\\c\\d");
#else
  ASSERT_EQ(joined.ToNative(), "a/b/c/d");
#endif

  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString(""));
  ASSERT_OK_AND_ASSIGN(joined, fn.Join("c/d"));
  ASSERT_EQ(joined.ToString(), "c/d");
#if _WIN32
  ASSERT_EQ(joined.ToNative(), L"c\\d");
#else
  ASSERT_EQ(joined.ToNative(), "c/d");
#endif

#if _WIN32
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("a\\b"));
  ASSERT_OK_AND_ASSIGN(joined, fn.Join("c\\d"));
  ASSERT_EQ(joined.ToString(), "a/b/c/d");
  ASSERT_EQ(joined.ToNative(), L"a\\b\\c\\d");

  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("a\\b\\"));
  ASSERT_OK_AND_ASSIGN(joined, fn.Join("c\\d"));
  ASSERT_EQ(joined.ToString(), "a/b/c/d");
  ASSERT_EQ(joined.ToNative(), L"a\\b\\c\\d");
#endif
}

TEST(PlatformFilename, JoinInvalid) {
  PlatformFilename fn;
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("a/b"));
  std::string s = "foo";
  s += '\x00';
  ASSERT_RAISES(Invalid, fn.Join(s));
}

TEST(PlatformFilename, Parent) {
  PlatformFilename fn;

  // Relative
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab/cd"));
  ASSERT_EQ(fn.ToString(), "ab/cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
#if _WIN32
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab/cd\\ef"));
  ASSERT_EQ(fn.ToString(), "ab/cd/ef");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab/cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
#endif

  // Absolute
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("/ab/cd/ef"));
  ASSERT_EQ(fn.ToString(), "/ab/cd/ef");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab/cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/");
#if _WIN32
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("\\ab\\cd/ef"));
  ASSERT_EQ(fn.ToString(), "/ab/cd/ef");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab/cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/");
#endif

  // Empty
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString(""));
  ASSERT_EQ(fn.ToString(), "");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "");

  // Multiple separators, relative
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab//cd///ef"));
  ASSERT_EQ(fn.ToString(), "ab//cd///ef");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab//cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
#if _WIN32
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab\\\\cd\\\\\\ef"));
  ASSERT_EQ(fn.ToString(), "ab//cd///ef");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab//cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab");
#endif

  // Multiple separators, absolute
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("//ab//cd///ef"));
  ASSERT_EQ(fn.ToString(), "//ab//cd///ef");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//ab//cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//");
#if _WIN32
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("\\\\ab\\cd\\ef"));
  ASSERT_EQ(fn.ToString(), "//ab/cd/ef");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//ab/cd");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//ab");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "//");
#endif

  // Trailing slashes
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("/ab/cd/ef/"));
  ASSERT_EQ(fn.ToString(), "/ab/cd/ef/");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab/cd");
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("/ab/cd/ef//"));
  ASSERT_EQ(fn.ToString(), "/ab/cd/ef//");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab/cd");
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab/"));
  ASSERT_EQ(fn.ToString(), "ab/");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab/");
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab//"));
  ASSERT_EQ(fn.ToString(), "ab//");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab//");
#if _WIN32
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("\\ab\\cd\\ef\\"));
  ASSERT_EQ(fn.ToString(), "/ab/cd/ef/");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab/cd");
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("\\ab\\cd\\ef\\\\"));
  ASSERT_EQ(fn.ToString(), "/ab/cd/ef//");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "/ab/cd");
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab\\"));
  ASSERT_EQ(fn.ToString(), "ab/");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab/");
  ASSERT_OK_AND_ASSIGN(fn, PlatformFilename::FromString("ab\\\\"));
  ASSERT_EQ(fn.ToString(), "ab//");
  fn = fn.Parent();
  ASSERT_EQ(fn.ToString(), "ab//");
#endif
}

TEST(CreateDirDeleteDir, Basics) {
  const std::string BASE = "xxx-io-util-test-dir";
  bool created, deleted;
  PlatformFilename parent, child;

  ASSERT_OK_AND_ASSIGN(parent, PlatformFilename::FromString(BASE));
  ASSERT_EQ(parent.ToString(), BASE);

  // Make sure the directory doesn't exist already
  ARROW_UNUSED(DeleteDirTree(parent));

  AssertNotExists(parent);

  ASSERT_OK_AND_ASSIGN(created, CreateDir(parent));
  ASSERT_TRUE(created);
  AssertExists(parent);
  ASSERT_OK_AND_ASSIGN(created, CreateDir(parent));
  ASSERT_FALSE(created);  // already exists
  AssertExists(parent);

  ASSERT_OK_AND_ASSIGN(child, PlatformFilename::FromString(BASE + "/some-child"));
  ASSERT_OK_AND_ASSIGN(created, CreateDir(child));
  ASSERT_TRUE(created);
  AssertExists(child);

  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirTree(parent));
  ASSERT_TRUE(deleted);
  AssertNotExists(parent);
  AssertNotExists(child);

  // Parent is deleted, cannot create child again
  ASSERT_RAISES(IOError, CreateDir(child));

  // It's not an error to call DeleteDirTree on a non-existent path.
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirTree(parent));
  ASSERT_FALSE(deleted);
  // ... unless asked so
  auto status = DeleteDirTree(parent, /*allow_non_existent=*/false).status();
  ASSERT_RAISES(IOError, status);
#ifdef _WIN32
  ASSERT_EQ(WinErrorFromStatus(status), ERROR_FILE_NOT_FOUND);
#else
  ASSERT_EQ(ErrnoFromStatus(status), ENOENT);
#endif
}

TEST(DeleteDirContents, Basics) {
  const std::string BASE = "xxx-io-util-test-dir2";
  bool created, deleted;
  PlatformFilename parent, child1, child2;

  ASSERT_OK_AND_ASSIGN(parent, PlatformFilename::FromString(BASE));
  ASSERT_EQ(parent.ToString(), BASE);

  // Make sure the directory doesn't exist already
  ARROW_UNUSED(DeleteDirTree(parent));

  AssertNotExists(parent);

  // Create the parent, a child dir and a child file
  ASSERT_OK_AND_ASSIGN(created, CreateDir(parent));
  ASSERT_TRUE(created);
  ASSERT_OK_AND_ASSIGN(child1, PlatformFilename::FromString(BASE + "/child-dir"));
  ASSERT_OK_AND_ASSIGN(child2, PlatformFilename::FromString(BASE + "/child-file"));
  ASSERT_OK_AND_ASSIGN(created, CreateDir(child1));
  ASSERT_TRUE(created);
  int fd = -1;
  ASSERT_OK_AND_ASSIGN(fd, FileOpenWritable(child2));
  ASSERT_OK(FileClose(fd));
  AssertExists(child1);
  AssertExists(child2);

  // Cannot call DeleteDirContents on a file
  ASSERT_RAISES(IOError, DeleteDirContents(child2));
  AssertExists(child2);

  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirContents(parent));
  ASSERT_TRUE(deleted);
  AssertExists(parent);
  AssertNotExists(child1);
  AssertNotExists(child2);
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirContents(parent));
  ASSERT_TRUE(deleted);
  AssertExists(parent);

  // It's not an error to call DeleteDirContents on a non-existent path.
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirContents(child1));
  ASSERT_FALSE(deleted);
  // ... unless asked so
  auto status = DeleteDirContents(child1, /*allow_non_existent=*/false).status();
  ASSERT_RAISES(IOError, status);
#ifdef _WIN32
  ASSERT_EQ(WinErrorFromStatus(status), ERROR_FILE_NOT_FOUND);
#else
  ASSERT_EQ(ErrnoFromStatus(status), ENOENT);
#endif
}

TEST(TemporaryDir, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  PlatformFilename fn;

  ASSERT_OK_AND_ASSIGN(temp_dir, TemporaryDir::Make("some-prefix-"));
  fn = temp_dir->path();
  // Path has a trailing separator, for convenience
  ASSERT_EQ(fn.ToString().back(), '/');
#if defined(_WIN32)
  ASSERT_EQ(fn.ToNative().back(), L'\\');
#else
  ASSERT_EQ(fn.ToNative().back(), '/');
#endif
  AssertExists(fn);
  ASSERT_NE(fn.ToString().find("some-prefix-"), std::string::npos);

  // Create child contents to check that they're cleaned up at the end
#if defined(_WIN32)
  PlatformFilename child(fn.ToNative() + L"some-child");
#else
  PlatformFilename child(fn.ToNative() + "some-child");
#endif
  ASSERT_OK(CreateDir(child));
  AssertExists(child);

  temp_dir.reset();
  AssertNotExists(fn);
  AssertNotExists(child);
}

TEST(CreateDirTree, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  PlatformFilename fn;
  bool created;

  ASSERT_OK_AND_ASSIGN(temp_dir, TemporaryDir::Make("io-util-test-"));

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/CD"));
  ASSERT_OK_AND_ASSIGN(created, CreateDirTree(fn));
  ASSERT_TRUE(created);
  ASSERT_OK_AND_ASSIGN(created, CreateDirTree(fn));
  ASSERT_FALSE(created);

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB"));
  ASSERT_OK_AND_ASSIGN(created, CreateDirTree(fn));
  ASSERT_FALSE(created);

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("EF"));
  ASSERT_OK_AND_ASSIGN(created, CreateDirTree(fn));
  ASSERT_TRUE(created);
}

TEST(ListDir, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  PlatformFilename fn;
  std::vector<PlatformFilename> entries;

  auto check_entries = [](const std::vector<PlatformFilename>& entries,
                          std::vector<std::string> expected) -> void {
    std::vector<std::string> actual(entries.size());
    std::transform(entries.begin(), entries.end(), actual.begin(),
                   [](const PlatformFilename& fn) { return fn.ToString(); });
    // Sort results for deterministic testing
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual, expected);
  };

  ASSERT_OK_AND_ASSIGN(temp_dir, TemporaryDir::Make("io-util-test-"));

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/CD"));
  ASSERT_OK(CreateDirTree(fn));
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/EF/GH"));
  ASSERT_OK(CreateDirTree(fn));
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/ghi.txt"));
  int fd = -1;
  ASSERT_OK_AND_ASSIGN(fd, FileOpenWritable(fn));
  ASSERT_OK(FileClose(fd));

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB"));
  ASSERT_OK_AND_ASSIGN(entries, ListDir(fn));
  ASSERT_EQ(entries.size(), 3);
  check_entries(entries, {"CD", "EF", "ghi.txt"});
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/EF/GH"));
  ASSERT_OK_AND_ASSIGN(entries, ListDir(fn));
  check_entries(entries, {});

  // Errors
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("non-existent"));
  ASSERT_RAISES(IOError, ListDir(fn));
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/ghi.txt"));
  ASSERT_RAISES(IOError, ListDir(fn));
}

TEST(DeleteFile, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  PlatformFilename fn;
  int fd;
  bool deleted;

  ASSERT_OK_AND_ASSIGN(temp_dir, TemporaryDir::Make("io-util-test-"));
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("test-file"));

  AssertNotExists(fn);
  ASSERT_OK_AND_ASSIGN(fd, FileOpenWritable(fn));
  ASSERT_OK(FileClose(fd));
  AssertExists(fn);
  ASSERT_OK_AND_ASSIGN(deleted, DeleteFile(fn));
  ASSERT_TRUE(deleted);
  AssertNotExists(fn);
  ASSERT_OK_AND_ASSIGN(deleted, DeleteFile(fn));
  ASSERT_FALSE(deleted);
  AssertNotExists(fn);
  auto status = DeleteFile(fn, /*allow_non_existent=*/false).status();
  ASSERT_RAISES(IOError, status);
#ifdef _WIN32
  ASSERT_EQ(WinErrorFromStatus(status), ERROR_FILE_NOT_FOUND);
#else
  ASSERT_EQ(ErrnoFromStatus(status), ENOENT);
#endif

  // Cannot call DeleteFile on directory
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("test-temp_dir"));
  ASSERT_OK(CreateDir(fn));
  AssertExists(fn);
  ASSERT_RAISES(IOError, DeleteFile(fn));
}

}  // namespace internal
}  // namespace arrow

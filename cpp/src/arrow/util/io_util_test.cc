// Licensed to the Apache Software Foundation (ASF) under one
// std::unique_ptr<TemporaryDir> temp_dir;
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
#include <atomic>
#include <cerrno>
#include <limits>
#include <sstream>
#include <vector>

#include <signal.h>

#ifndef _WIN32
#include <pthread.h>
#endif

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/windows_compatibility.h"
#include "arrow/util/windows_fixup.h"

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

void TouchFile(const PlatformFilename& path) {
  int fd = -1;
  ASSERT_OK_AND_ASSIGN(fd, FileOpenWritable(path));
  ASSERT_OK(FileClose(fd));
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

  st = CancelledFromSignal(SIGINT, "foo");
  ASSERT_EQ(ErrnoFromStatus(st), 0);

#ifdef _WIN32
  // If possible, a Windows error detail can be mapped to a corresponding
  // errno value.
  st = StatusFromWinError(ERROR_FILE_NOT_FOUND, StatusCode::KeyError, "foo");
  ASSERT_EQ(ErrnoFromStatus(st), ENOENT);
  st = StatusFromWinError(6789, StatusCode::KeyError, "foo");
  ASSERT_EQ(ErrnoFromStatus(st), 0);
#endif
}

TEST(SignalFromStatus, Basics) {
  Status st;
  st = Status::OK();
  ASSERT_EQ(SignalFromStatus(st), 0);
  st = Status::KeyError("foo");
  ASSERT_EQ(SignalFromStatus(st), 0);
  st = Status::Cancelled("foo");
  ASSERT_EQ(SignalFromStatus(st), 0);
  st = StatusFromSignal(SIGINT, StatusCode::KeyError, "foo");
  ASSERT_EQ(SignalFromStatus(st), SIGINT);
  ASSERT_EQ(st.ToString(),
            "Key error: foo. Detail: received signal " + std::to_string(SIGINT));
  st = CancelledFromSignal(SIGINT, "bar");
  ASSERT_EQ(SignalFromStatus(st), SIGINT);
  ASSERT_EQ(st.ToString(),
            "Cancelled: bar. Detail: received signal " + std::to_string(SIGINT));

  st = IOErrorFromErrno(EINVAL, "foo");
  ASSERT_EQ(SignalFromStatus(st), 0);
}

TEST(GetPageSize, Basics) {
  const auto page_size = GetPageSize();
  ASSERT_GE(page_size, 4096);
  // It's a power of 2
  ASSERT_EQ((page_size - 1) & page_size, 0);
}

TEST(MemoryAdviseWillNeed, Basics) {
  ASSERT_OK_AND_ASSIGN(auto buf1, AllocateBuffer(8192));
  ASSERT_OK_AND_ASSIGN(auto buf2, AllocateBuffer(1024 * 1024));

  const auto addr1 = buf1->mutable_data();
  const auto size1 = static_cast<size_t>(buf1->size());
  const auto addr2 = buf2->mutable_data();
  const auto size2 = static_cast<size_t>(buf2->size());

  ASSERT_OK(MemoryAdviseWillNeed({}));
  ASSERT_OK(MemoryAdviseWillNeed({{addr1, size1}, {addr2, size2}}));
  ASSERT_OK(MemoryAdviseWillNeed({{addr1 + 1, size1 - 1}, {addr2 + 4095, size2 - 4095}}));
  ASSERT_OK(MemoryAdviseWillNeed({{addr1, 13}, {addr2, 1}}));
  ASSERT_OK(MemoryAdviseWillNeed({{addr1, 0}, {addr2 + 1, 0}}));

  // Should probably fail
  // (but on Windows, MemoryAdviseWillNeed can be a no-op)
#ifndef _WIN32
  ASSERT_RAISES(IOError,
                MemoryAdviseWillNeed({{nullptr, std::numeric_limits<size_t>::max()}}));
#endif
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
  std::unique_ptr<TemporaryDir> temp_dir;
  ASSERT_OK_AND_ASSIGN(temp_dir, TemporaryDir::Make("deletedirtest-"));
  const std::string BASE =
      temp_dir->path().Join("xxx-io-util-test-dir2").ValueOrDie().ToString();
  bool created, deleted;
  PlatformFilename parent, child, child_file;

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

  ASSERT_OK_AND_ASSIGN(child_file, PlatformFilename::FromString(BASE + "/some-file"));
  TouchFile(child_file);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      IOError, ::testing::HasSubstr("non-directory entry exists"), CreateDir(child_file));

  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirTree(parent));
  ASSERT_TRUE(deleted);
  AssertNotExists(parent);
  AssertNotExists(child);

  // Parent is deleted, cannot create child again
  ASSERT_RAISES(IOError, CreateDir(child));

  // It's not an error to call DeleteDirTree on a nonexistent path.
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirTree(parent));
  ASSERT_FALSE(deleted);
  // ... unless asked so
  auto status = DeleteDirTree(parent, /*allow_not_found=*/false).status();
  ASSERT_RAISES(IOError, status);
#ifdef _WIN32
  ASSERT_EQ(WinErrorFromStatus(status), ERROR_FILE_NOT_FOUND);
#else
  ASSERT_EQ(ErrnoFromStatus(status), ENOENT);
#endif
}

TEST(DeleteDirContents, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  ASSERT_OK_AND_ASSIGN(temp_dir, TemporaryDir::Make("deletedirtest-"));
  const std::string BASE =
      temp_dir->path().Join("xxx-io-util-test-dir2").ValueOrDie().ToString();
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
  TouchFile(child2);
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

  // It's not an error to call DeleteDirContents on a nonexistent path.
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirContents(child1));
  ASSERT_FALSE(deleted);
  // ... unless asked so
  auto status = DeleteDirContents(child1, /*allow_not_found=*/false).status();
  ASSERT_RAISES(IOError, status);
#ifdef _WIN32
  ASSERT_EQ(WinErrorFromStatus(status), ERROR_FILE_NOT_FOUND);
#else
  ASSERT_EQ(ErrnoFromStatus(status), ENOENT);
#endif

  // Now actually delete the test directory
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirTree(parent));
  ASSERT_TRUE(deleted);
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

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/file"));
  TouchFile(fn);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      IOError, ::testing::HasSubstr("non-directory entry exists"), CreateDirTree(fn));

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/file/sub"));
  ASSERT_RAISES(IOError, CreateDirTree(fn));
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
  TouchFile(fn);

  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB"));
  ASSERT_OK_AND_ASSIGN(entries, ListDir(fn));
  ASSERT_EQ(entries.size(), 3);
  check_entries(entries, {"CD", "EF", "ghi.txt"});
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/EF/GH"));
  ASSERT_OK_AND_ASSIGN(entries, ListDir(fn));
  check_entries(entries, {});

  // Errors
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("nonexistent"));
  ASSERT_RAISES(IOError, ListDir(fn));
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("AB/ghi.txt"));
  ASSERT_RAISES(IOError, ListDir(fn));
}

TEST(DeleteFile, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  PlatformFilename fn;
  bool deleted;

  ASSERT_OK_AND_ASSIGN(temp_dir, TemporaryDir::Make("io-util-test-"));
  ASSERT_OK_AND_ASSIGN(fn, temp_dir->path().Join("test-file"));

  AssertNotExists(fn);
  TouchFile(fn);
  AssertExists(fn);
  ASSERT_OK_AND_ASSIGN(deleted, DeleteFile(fn));
  ASSERT_TRUE(deleted);
  AssertNotExists(fn);
  ASSERT_OK_AND_ASSIGN(deleted, DeleteFile(fn));
  ASSERT_FALSE(deleted);
  AssertNotExists(fn);
  auto status = DeleteFile(fn, /*allow_not_found=*/false).status();
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

#ifndef __APPLE__
TEST(FileUtils, LongPaths) {
  // ARROW-8477: check using long file paths under Windows (> 260 characters).
  bool created, deleted;
#ifdef _WIN32
  const char* kRegKeyName = R"(SYSTEM\CurrentControlSet\Control\FileSystem)";
  const char* kRegValueName = "LongPathsEnabled";
  DWORD value = 0;
  DWORD size = sizeof(value);
  LSTATUS status = RegGetValueA(HKEY_LOCAL_MACHINE, kRegKeyName, kRegValueName,
                                RRF_RT_REG_DWORD, NULL, &value, &size);
  bool test_long_paths = (status == ERROR_SUCCESS && value == 1);
  if (!test_long_paths) {
    ARROW_LOG(WARNING)
        << "Tests for accessing files with long path names have been disabled. "
        << "To enable these tests, set the value of " << kRegValueName
        << " in registry key \\HKEY_LOCAL_MACHINE\\" << kRegKeyName
        << " to 1 on the test host.";
    return;
  }
#endif

  const std::string BASE = "xxx-io-util-test-dir-long";
  PlatformFilename base_path, long_path, long_filename;
  int fd = -1;
  std::stringstream fs;
  fs << BASE;
  for (int i = 0; i < 64; ++i) {
    fs << "/123456789ABCDEF";
  }
  ASSERT_OK_AND_ASSIGN(base_path,
                       PlatformFilename::FromString(BASE));  // long_path length > 1024
  ASSERT_OK_AND_ASSIGN(
      long_path, PlatformFilename::FromString(fs.str()));  // long_path length > 1024
  ASSERT_OK_AND_ASSIGN(created, CreateDirTree(long_path));
  ASSERT_TRUE(created);
  AssertExists(long_path);
  ASSERT_OK_AND_ASSIGN(long_filename,
                       PlatformFilename::FromString(fs.str() + "/file.txt"));
  TouchFile(long_filename);
  AssertExists(long_filename);
  fd = -1;
  ASSERT_OK_AND_ASSIGN(fd, FileOpenReadable(long_filename));
  ASSERT_OK(FileClose(fd));
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirContents(long_path));
  ASSERT_TRUE(deleted);
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirTree(long_path));
  ASSERT_TRUE(deleted);

  // Now delete the whole test directory tree
  ASSERT_OK_AND_ASSIGN(deleted, DeleteDirTree(base_path));
  ASSERT_TRUE(deleted);
}
#endif

static std::atomic<int> signal_received;

static void handle_signal(int signum) {
  ReinstateSignalHandler(signum, &handle_signal);
  signal_received.store(signum);
}

TEST(SendSignal, Generic) {
  signal_received.store(0);
  SignalHandlerGuard guard(SIGINT, &handle_signal);

  ASSERT_EQ(signal_received.load(), 0);
  ASSERT_OK(SendSignal(SIGINT));
  BusyWait(1.0, [&]() { return signal_received.load() != 0; });
  ASSERT_EQ(signal_received.load(), SIGINT);

  // Re-try (exercise ReinstateSignalHandler)
  signal_received.store(0);
  ASSERT_OK(SendSignal(SIGINT));
  BusyWait(1.0, [&]() { return signal_received.load() != 0; });
  ASSERT_EQ(signal_received.load(), SIGINT);
}

TEST(SendSignal, ToThread) {
#ifdef _WIN32
  uint64_t dummy_thread_id = 42;
  ASSERT_RAISES(NotImplemented, SendSignalToThread(SIGINT, dummy_thread_id));
#else
  // Have to use a C-style cast because pthread_t can be a pointer *or* integer type
  uint64_t thread_id = (uint64_t)(pthread_self());  // NOLINT readability-casting
  signal_received.store(0);
  SignalHandlerGuard guard(SIGINT, &handle_signal);

  ASSERT_EQ(signal_received.load(), 0);
  ASSERT_OK(SendSignalToThread(SIGINT, thread_id));
  BusyWait(1.0, [&]() { return signal_received.load() != 0; });

  ASSERT_EQ(signal_received.load(), SIGINT);
#endif
}

TEST(Memory, GetRSS) {
#if defined(_WIN32)
  ASSERT_GT(GetCurrentRSS(), 0);
#elif defined(__APPLE__)
  ASSERT_GT(GetCurrentRSS(), 0);
#elif defined(__linux__)
  ASSERT_GT(GetCurrentRSS(), 0);
#else
  ASSERT_EQ(GetCurrentRSS(), 0);
#endif
}

}  // namespace internal
}  // namespace arrow

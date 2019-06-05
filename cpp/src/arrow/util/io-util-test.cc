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

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io-util.h"

namespace arrow {
namespace internal {

void AssertExists(const PlatformFilename& path) {
  bool exists = false;
  ASSERT_OK(FileExists(path, &exists));
  ASSERT_TRUE(exists) << "Path '" << path.ToString() << "' doesn't exist";
}

void AssertNotExists(const PlatformFilename& path) {
  bool exists = true;
  ASSERT_OK(FileExists(path, &exists));
  ASSERT_FALSE(exists) << "Path '" << path.ToString() << "' exists";
}

TEST(PlatformFilename, RoundtripAscii) {
  PlatformFilename fn;
  ASSERT_OK(PlatformFilename::FromString("a/b", &fn));
  ASSERT_EQ(fn.ToString(), "a/b");
#if _WIN32
  ASSERT_EQ(fn.ToNative(), L"a\\b");
#else
  ASSERT_EQ(fn.ToNative(), "a/b");
#endif
}

TEST(PlatformFilename, RoundtripUtf8) {
  PlatformFilename fn;
  ASSERT_OK(PlatformFilename::FromString("h\xc3\xa9h\xc3\xa9", &fn));
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
  ASSERT_OK(PlatformFilename::FromString("C:/foo/bar", &fn));
  ASSERT_EQ(fn.ToString(), "C:/foo/bar");
  ASSERT_EQ(fn.ToNative(), L"C:\\foo\\bar");

  ASSERT_OK(PlatformFilename::FromString("C:\\foo\\bar", &fn));
  ASSERT_EQ(fn.ToString(), "C:/foo/bar");
  ASSERT_EQ(fn.ToNative(), L"C:\\foo\\bar");
}
#endif

TEST(PlatformFilename, Invalid) {
  PlatformFilename fn;
  std::string s = "foo";
  s += '\x00';
  ASSERT_RAISES(Invalid, PlatformFilename::FromString(s, &fn));
}

TEST(CreateDirDeleteDir, Basics) {
  const std::string BASE = "xxx-io-util-test-dir";
  bool created, deleted;
  PlatformFilename parent, child;

  ASSERT_OK(PlatformFilename::FromString(BASE, &parent));
  ASSERT_EQ(parent.ToString(), BASE);

  // Make sure the directory doesn't exist already
  ARROW_UNUSED(DeleteDirTree(parent));

  AssertNotExists(parent);

  ASSERT_OK(CreateDir(parent, &created));
  ASSERT_TRUE(created);
  AssertExists(parent);
  ASSERT_OK(CreateDir(parent, &created));
  ASSERT_FALSE(created);  // already exists
  AssertExists(parent);

  ASSERT_OK(PlatformFilename::FromString(BASE + "/some-child", &child));
  ASSERT_OK(CreateDir(child, &created));
  ASSERT_TRUE(created);
  AssertExists(child);

  ASSERT_OK(DeleteDirTree(parent, &deleted));
  ASSERT_TRUE(deleted);
  AssertNotExists(parent);
  AssertNotExists(child);

  // Parent is deleted, cannot create child again
  ASSERT_RAISES(IOError, CreateDir(child, &created));

  // It's not an error to call DeleteDirTree on a non-existent path.
  ASSERT_OK(DeleteDirTree(parent, &deleted));
  ASSERT_FALSE(deleted);
}

TEST(TemporaryDir, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  PlatformFilename fn;

  ASSERT_OK(TemporaryDir::Make("some-prefix-", &temp_dir));
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

  ASSERT_OK(TemporaryDir::Make("io-util-test-", &temp_dir));

  ASSERT_OK(temp_dir->path().Join("AB/CD", &fn));
  ASSERT_OK(CreateDirTree(fn, &created));
  ASSERT_TRUE(created);
  ASSERT_OK(CreateDirTree(fn, &created));
  ASSERT_FALSE(created);

  ASSERT_OK(temp_dir->path().Join("AB", &fn));
  ASSERT_OK(CreateDirTree(fn, &created));
  ASSERT_FALSE(created);

  ASSERT_OK(temp_dir->path().Join("EF", &fn));
  ASSERT_OK(CreateDirTree(fn, &created));
  ASSERT_TRUE(created);
}

TEST(DeleteFile, Basics) {
  std::unique_ptr<TemporaryDir> temp_dir;
  PlatformFilename fn;
  int fd;
  bool deleted;

  ASSERT_OK(TemporaryDir::Make("io-util-test-", &temp_dir));
  ASSERT_OK(temp_dir->path().Join("test-file", &fn));

  AssertNotExists(fn);
  ASSERT_OK(FileOpenWritable(fn, true /* write_only */, true /* truncate */,
                             false /* append */, &fd));
  ASSERT_OK(FileClose(fd));
  AssertExists(fn);
  ASSERT_OK(DeleteFile(fn, &deleted));
  ASSERT_TRUE(deleted);
  AssertNotExists(fn);
  ASSERT_OK(DeleteFile(fn, &deleted));
  ASSERT_FALSE(deleted);
  AssertNotExists(fn);

  // Cannot call DeleteFile on directory
  ASSERT_OK(temp_dir->path().Join("test-temp_dir", &fn));
  ASSERT_OK(CreateDir(fn));
  AssertExists(fn);
  ASSERT_RAISES(IOError, DeleteFile(fn));
}

}  // namespace internal
}  // namespace arrow

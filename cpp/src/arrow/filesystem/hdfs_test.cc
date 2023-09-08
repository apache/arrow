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

#include "arrow/filesystem/hdfs.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <memory>
#include <sstream>
#include <string>

#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::ErrnoFromStatus;
using internal::Uri;

namespace fs {

TEST(TestHdfsOptions, FromUri) {
  HdfsOptions options;
  Uri uri;

  ASSERT_OK(uri.Parse("hdfs://localhost"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.replication, 3);
  ASSERT_EQ(options.connection_config.host, "hdfs://localhost");
  ASSERT_EQ(options.connection_config.port, 0);
  ASSERT_EQ(options.connection_config.user, "");

  ASSERT_OK(uri.Parse("hdfs://otherhost:9999/?replication=2&kerb_ticket=kerb.ticket"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.replication, 2);
  ASSERT_EQ(options.connection_config.kerb_ticket, "kerb.ticket");
  ASSERT_EQ(options.connection_config.host, "hdfs://otherhost");
  ASSERT_EQ(options.connection_config.port, 9999);
  ASSERT_EQ(options.connection_config.user, "");

  ASSERT_OK(uri.Parse("hdfs://otherhost:9999/?hdfs_token=hdfs_token_ticket"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.connection_config.host, "hdfs://otherhost");
  ASSERT_EQ(options.connection_config.port, 9999);
  ASSERT_EQ(options.connection_config.extra_conf["hdfs_token"], "hdfs_token_ticket");

  ASSERT_OK(uri.Parse("viewfs://other-nn/mypath/myfile"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.connection_config.host, "viewfs://other-nn");
  ASSERT_EQ(options.connection_config.port, 0);
  ASSERT_EQ(options.connection_config.user, "");
}

class HadoopFileSystemTestMixin {
 public:
  void MakeFileSystem() {
    const char* host = std::getenv("ARROW_HDFS_TEST_HOST");
    const char* port = std::getenv("ARROW_HDFS_TEST_PORT");
    const char* user = std::getenv("ARROW_HDFS_TEST_USER");

    std::string hdfs_host = host == nullptr ? "localhost" : std::string(host);
    int hdfs_port = port == nullptr ? 20500 : atoi(port);
    std::string hdfs_user = user == nullptr ? "root" : std::string(user);

    options_.ConfigureEndPoint(hdfs_host, hdfs_port);
    options_.ConfigureUser(hdfs_user);
    options_.ConfigureReplication(0);

    auto result = HadoopFileSystem::Make(options_);
    if (!result.ok()) {
      ARROW_LOG(INFO)
          << "HadoopFileSystem::Make failed, it is possible when we don't have "
             "proper driver on this node, err msg is "
          << result.status().ToString();
      loaded_driver_ = false;
      return;
    }
    loaded_driver_ = true;
    fs_ = *result;
  }

 protected:
  HdfsOptions options_;
  bool loaded_driver_ = false;
  std::shared_ptr<FileSystem> fs_;
};

class TestHadoopFileSystem : public ::testing::Test, public HadoopFileSystemTestMixin {
 public:
  void SetUp() override { MakeFileSystem(); }

  void TestFileSystemFromUri() {
    std::stringstream ss;
    ss << "hdfs://" << options_.connection_config.host << ":"
       << options_.connection_config.port << "/"
       << "?replication=0&user=" << options_.connection_config.user;

    std::shared_ptr<FileSystem> uri_fs;
    std::string path;
    ARROW_LOG(INFO) << "!!! uri = " << ss.str();
    ASSERT_OK_AND_ASSIGN(uri_fs, FileSystemFromUri(ss.str(), &path));
    ASSERT_EQ(path, "/");
    ASSERT_OK_AND_ASSIGN(path, uri_fs->PathFromUri(ss.str()));
    ASSERT_EQ(path, "/");

    // Sanity check
    ASSERT_OK(uri_fs->CreateDir("AB"));
    AssertFileInfo(uri_fs.get(), "AB", FileType::Directory);
    ASSERT_OK(uri_fs->DeleteDir("AB"));
    AssertFileInfo(uri_fs.get(), "AB", FileType::NotFound);
  }

  void TestGetFileInfo(const std::string& base_dir) {
    std::vector<FileInfo> infos;

    ASSERT_OK(fs_->CreateDir(base_dir + "AB"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/CD"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/EF"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/EF/GH"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/EF/GH/IJ"));
    CreateFile(fs_.get(), base_dir + "AB/data", "some data");

    // With single path
    FileInfo info;
    ASSERT_OK_AND_ASSIGN(info, fs_->GetFileInfo(base_dir + "AB"));
    AssertFileInfo(info, base_dir + "AB", FileType::Directory);
    ASSERT_OK_AND_ASSIGN(info, fs_->GetFileInfo(base_dir + "AB/data"));
    AssertFileInfo(info, base_dir + "AB/data", FileType::File, 9);

    // With selector
    FileSelector selector;
    selector.base_dir = base_dir + "AB";
    selector.recursive = false;

    ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(selector));
    ASSERT_EQ(infos.size(), 3);
    AssertFileInfo(infos[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileInfo(infos[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileInfo(infos[2], base_dir + "AB/data", FileType::File);

    selector.recursive = true;
    ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(selector));
    ASSERT_EQ(infos.size(), 5);
    AssertFileInfo(infos[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileInfo(infos[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileInfo(infos[2], base_dir + "AB/EF/GH", FileType::Directory);
    AssertFileInfo(infos[3], base_dir + "AB/EF/GH/IJ", FileType::Directory);
    AssertFileInfo(infos[4], base_dir + "AB/data", FileType::File, 9);

    selector.max_recursion = 0;
    ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(selector));
    ASSERT_EQ(infos.size(), 3);
    AssertFileInfo(infos[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileInfo(infos[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileInfo(infos[2], base_dir + "AB/data", FileType::File);

    selector.max_recursion = 1;
    ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(selector));
    ASSERT_EQ(infos.size(), 4);
    AssertFileInfo(infos[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileInfo(infos[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileInfo(infos[2], base_dir + "AB/EF/GH", FileType::Directory);
    AssertFileInfo(infos[3], base_dir + "AB/data", FileType::File);

    selector.base_dir = base_dir + "XYZ";
    selector.allow_not_found = true;
    ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(selector));
    ASSERT_EQ(infos.size(), 0);

    selector.allow_not_found = false;
    auto st = fs_->GetFileInfo(selector).status();
    ASSERT_RAISES(IOError, st);
    ASSERT_EQ(ErrnoFromStatus(st), ENOENT);

    ASSERT_OK(fs_->DeleteDir(base_dir + "AB"));
    AssertFileInfo(fs_.get(), base_dir + "AB", FileType::NotFound);
  }
};

#define SKIP_IF_NO_DRIVER()                        \
  if (!this->loaded_driver_) {                     \
    GTEST_SKIP() << "Driver not loaded, skipping"; \
  }

TEST_F(TestHadoopFileSystem, CreateDirDeleteDir) {
  SKIP_IF_NO_DRIVER();

  // recursive = true
  ASSERT_OK(this->fs_->CreateDir("AB/CD"));
  CreateFile(this->fs_.get(), "AB/CD/data", "some data");
  AssertFileInfo(this->fs_.get(), "AB", FileType::Directory);
  AssertFileInfo(this->fs_.get(), "AB/CD", FileType::Directory);
  AssertFileInfo(this->fs_.get(), "AB/CD/data", FileType::File, 9);

  ASSERT_OK(this->fs_->DeleteDir("AB"));
  AssertFileInfo(this->fs_.get(), "AB", FileType::NotFound);

  // recursive = false
  ASSERT_RAISES(IOError, this->fs_->CreateDir("AB/CD", /*recursive=*/false));
  ASSERT_OK(this->fs_->CreateDir("AB", /*recursive=*/false));
  ASSERT_OK(this->fs_->CreateDir("AB/CD", /*recursive=*/false));

  ASSERT_OK(this->fs_->DeleteDir("AB"));
  AssertFileInfo(this->fs_.get(), "AB", FileType::NotFound);

  auto st = this->fs_->DeleteDir("AB");
  ASSERT_RAISES(IOError, st);
  ASSERT_EQ(ErrnoFromStatus(st), ENOENT);
}

TEST_F(TestHadoopFileSystem, DeleteDirContents) {
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->fs_->CreateDir("AB/CD"));
  CreateFile(this->fs_.get(), "AB/CD/data", "some data");
  AssertFileInfo(this->fs_.get(), "AB", FileType::Directory);
  AssertFileInfo(this->fs_.get(), "AB/CD", FileType::Directory);
  AssertFileInfo(this->fs_.get(), "AB/CD/data", FileType::File, 9);

  ASSERT_OK(this->fs_->DeleteDirContents("AB"));
  AssertFileInfo(this->fs_.get(), "AB", FileType::Directory);
  AssertFileInfo(this->fs_.get(), "AB/CD", FileType::NotFound);
  AssertFileInfo(this->fs_.get(), "AB/CD/data", FileType::NotFound);

  ASSERT_OK(this->fs_->DeleteDirContents("AB"));
  AssertFileInfo(this->fs_.get(), "AB", FileType::Directory);
  ASSERT_OK(this->fs_->DeleteDir("AB"));

  ASSERT_OK(this->fs_->DeleteDirContents("DoesNotExist", /*missing_dir_ok=*/true));
  auto st = this->fs_->DeleteDirContents("DoesNotExist");
  ASSERT_RAISES(IOError, st);
  ASSERT_EQ(ErrnoFromStatus(st), ENOENT);
}

TEST_F(TestHadoopFileSystem, WriteReadFile) {
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->fs_->CreateDir("CD"));
  constexpr int kDataSize = 9;
  std::string file_name = "CD/abc";
  std::string data = "some data";
  std::shared_ptr<io::OutputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, this->fs_->OpenOutputStream(file_name));
  auto data_size = static_cast<int64_t>(data.size());
  ASSERT_OK(stream->Write(data.data(), data_size));
  ASSERT_OK(stream->Close());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, this->fs_->OpenInputFile(file_name));
  ASSERT_OK_AND_EQ(kDataSize, file->GetSize());
  uint8_t buffer[kDataSize];
  ASSERT_OK_AND_EQ(kDataSize, file->Read(kDataSize, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, data.c_str(), kDataSize));

  ASSERT_OK(this->fs_->DeleteDir("CD"));
}

TEST_F(TestHadoopFileSystem, GetFileInfoRelative) {
  // Test GetFileInfo() with relative paths
  SKIP_IF_NO_DRIVER();

  this->TestGetFileInfo("");
}

TEST_F(TestHadoopFileSystem, GetFileInfoAbsolute) {
  // Test GetFileInfo() with absolute paths
  SKIP_IF_NO_DRIVER();

  this->TestGetFileInfo("/");
}

TEST_F(TestHadoopFileSystem, RelativeVsAbsolutePaths) {
  SKIP_IF_NO_DRIVER();

  // XXX This test assumes the current working directory is not "/"

  ASSERT_OK(this->fs_->CreateDir("AB"));
  AssertFileInfo(this->fs_.get(), "AB", FileType::Directory);
  AssertFileInfo(this->fs_.get(), "/AB", FileType::NotFound);

  ASSERT_OK(this->fs_->CreateDir("/CD"));
  AssertFileInfo(this->fs_.get(), "/CD", FileType::Directory);
  AssertFileInfo(this->fs_.get(), "CD", FileType::NotFound);
}

TEST_F(TestHadoopFileSystem, MoveDir) {
  SKIP_IF_NO_DRIVER();

  FileInfo info;
  std::string directory_name_src = "AB";
  std::string directory_name_dest = "CD";
  ASSERT_OK(this->fs_->CreateDir(directory_name_src));
  ASSERT_OK_AND_ASSIGN(info, this->fs_->GetFileInfo(directory_name_src));
  AssertFileInfo(info, directory_name_src, FileType::Directory);

  // move file
  ASSERT_OK(this->fs_->Move(directory_name_src, directory_name_dest));
  ASSERT_OK_AND_ASSIGN(info, this->fs_->GetFileInfo(directory_name_src));
  ASSERT_TRUE(info.type() == FileType::NotFound);

  ASSERT_OK_AND_ASSIGN(info, this->fs_->GetFileInfo(directory_name_dest));
  AssertFileInfo(info, directory_name_dest, FileType::Directory);
  ASSERT_OK(this->fs_->DeleteDir(directory_name_dest));
}

TEST_F(TestHadoopFileSystem, FileSystemFromUri) {
  SKIP_IF_NO_DRIVER();

  this->TestFileSystemFromUri();
}

class TestHadoopFileSystemGeneric : public ::testing::Test,
                                    public HadoopFileSystemTestMixin,
                                    public GenericFileSystemTest {
 public:
  void SetUp() override {
    MakeFileSystem();
    SKIP_IF_NO_DRIVER();
    timestamp_ =
        static_cast<int64_t>(std::chrono::time_point_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now())
                                 .time_since_epoch()
                                 .count());
  }

 protected:
  bool allow_write_file_over_dir() const override { return true; }
  bool allow_move_dir_over_non_empty_dir() const override { return true; }
  bool have_implicit_directories() const override { return true; }
  bool allow_append_to_new_file() const override { return false; }

  std::shared_ptr<FileSystem> GetEmptyFileSystem() override {
    // Since the HDFS contents are kept persistently between test runs,
    // make sure each test gets a pristine fresh directory.
    std::stringstream ss;
    ss << "GenericTest" << timestamp_ << "-" << test_num_++;
    const auto subdir = ss.str();
    ARROW_EXPECT_OK(fs_->CreateDir(subdir));
    return std::make_shared<SubTreeFileSystem>(subdir, fs_);
  }

  static int test_num_;
  int64_t timestamp_;
};

int TestHadoopFileSystemGeneric::test_num_ = 1;

GENERIC_FS_TEST_FUNCTIONS(TestHadoopFileSystemGeneric);

}  // namespace fs
}  // namespace arrow

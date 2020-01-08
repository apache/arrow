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

#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "arrow/filesystem/hdfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::Uri;
using io::HdfsDriver;

namespace fs {

TEST(TestHdfsOptions, FromUri) {
  HdfsOptions options;
  internal::Uri uri;

  ASSERT_OK(uri.Parse("hdfs://localhost"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.replication, 3);
  ASSERT_EQ(options.connection_config.host, "hdfs://localhost");
  ASSERT_EQ(options.connection_config.port, 0);
  ASSERT_EQ(options.connection_config.user, "");
  ASSERT_EQ(options.connection_config.driver, HdfsDriver::LIBHDFS);

  ASSERT_OK(uri.Parse("hdfs://otherhost:9999/?use_hdfs3=0&replication=2"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.replication, 2);
  ASSERT_EQ(options.connection_config.host, "hdfs://otherhost");
  ASSERT_EQ(options.connection_config.port, 9999);
  ASSERT_EQ(options.connection_config.user, "");
  ASSERT_EQ(options.connection_config.driver, HdfsDriver::LIBHDFS);

  ASSERT_OK(uri.Parse("hdfs://otherhost:9999/?use_hdfs3=1&user=stevereich"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.replication, 3);
  ASSERT_EQ(options.connection_config.host, "otherhost");
  ASSERT_EQ(options.connection_config.port, 9999);
  ASSERT_EQ(options.connection_config.user, "stevereich");
  ASSERT_EQ(options.connection_config.driver, HdfsDriver::LIBHDFS3);

  ASSERT_OK(uri.Parse("viewfs://other-nn/mypath/myfile"));
  ASSERT_OK_AND_ASSIGN(options, HdfsOptions::FromUri(uri));
  ASSERT_EQ(options.connection_config.host, "viewfs://other-nn");
  ASSERT_EQ(options.connection_config.port, 0);
  ASSERT_EQ(options.connection_config.user, "");
  ASSERT_EQ(options.connection_config.driver, HdfsDriver::LIBHDFS);
}

struct JNIDriver {
  static HdfsDriver type;
};

struct PivotalDriver {
  static HdfsDriver type;
};

template <typename DRIVER>
class TestHadoopFileSystem : public ::testing::Test {
 public:
  void SetUp() override {
    const char* host = std::getenv("ARROW_HDFS_TEST_HOST");
    const char* port = std::getenv("ARROW_HDFS_TEST_PORT");
    const char* user = std::getenv("ARROW_HDFS_TEST_USER");

    std::string hdfs_host = host == nullptr ? "localhost" : std::string(host);
    int hdfs_port = port == nullptr ? 20500 : atoi(port);
    std::string hdfs_user = user == nullptr ? "root" : std::string(user);

    if (DRIVER::type == HdfsDriver::LIBHDFS) {
      use_hdfs3_ = false;
    } else {
      use_hdfs3_ = true;
    }

    options_.ConfigureEndPoint(hdfs_host, hdfs_port);
    options_.ConfigureHdfsUser(hdfs_user);
    options_.ConfigureHdfsDriver(use_hdfs3_);
    options_.ConfigureHdfsReplication(0);

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
    fs_ = std::make_shared<SubTreeFileSystem>("", *result);
  }

  void TestFileSystemFromUri() {
    std::stringstream ss;
    ss << "hdfs://" << options_.connection_config.host << ":"
       << options_.connection_config.port << "/"
       << "?replication=0&user=" << options_.connection_config.user;
    if (use_hdfs3_) {
      ss << "&use_hdfs3=1";
    }

    std::shared_ptr<FileSystem> uri_fs;
    std::string path;
    ARROW_LOG(INFO) << "!!! uri = " << ss.str();
    ASSERT_OK_AND_ASSIGN(uri_fs, FileSystemFromUri(ss.str(), &path));
    ASSERT_EQ(path, "/");

    // Sanity check
    ASSERT_OK(uri_fs->CreateDir("AB"));
    AssertFileStats(uri_fs.get(), "AB", FileType::Directory);
    ASSERT_OK(uri_fs->DeleteDir("AB"));
    AssertFileStats(uri_fs.get(), "AB", FileType::NonExistent);
  }

  void TestGetTargetStats(const std::string& base_dir) {
    std::vector<FileStats> stats;

    ASSERT_OK(fs_->CreateDir(base_dir + "AB"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/CD"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/EF"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/EF/GH"));
    ASSERT_OK(fs_->CreateDir(base_dir + "AB/EF/GH/IJ"));
    CreateFile(fs_.get(), base_dir + "AB/data", "some data");

    // With single path
    FileStats st;
    ASSERT_OK_AND_ASSIGN(st, fs_->GetTargetStats(base_dir + "AB"));
    AssertFileStats(st, base_dir + "AB", FileType::Directory);
    ASSERT_OK_AND_ASSIGN(st, fs_->GetTargetStats(base_dir + "AB/data"));
    AssertFileStats(st, base_dir + "AB/data", FileType::File, 9);

    // With selector
    FileSelector selector;
    selector.base_dir = base_dir + "AB";
    selector.recursive = false;

    ASSERT_OK_AND_ASSIGN(stats, fs_->GetTargetStats(selector));
    ASSERT_EQ(stats.size(), 3);
    AssertFileStats(stats[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileStats(stats[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileStats(stats[2], base_dir + "AB/data", FileType::File);

    selector.recursive = true;
    ASSERT_OK_AND_ASSIGN(stats, fs_->GetTargetStats(selector));
    ASSERT_EQ(stats.size(), 5);
    AssertFileStats(stats[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileStats(stats[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileStats(stats[2], base_dir + "AB/EF/GH", FileType::Directory);
    AssertFileStats(stats[3], base_dir + "AB/EF/GH/IJ", FileType::Directory);
    AssertFileStats(stats[4], base_dir + "AB/data", FileType::File, 9);

    selector.max_recursion = 0;
    ASSERT_OK_AND_ASSIGN(stats, fs_->GetTargetStats(selector));
    ASSERT_EQ(stats.size(), 3);
    AssertFileStats(stats[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileStats(stats[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileStats(stats[2], base_dir + "AB/data", FileType::File);

    selector.max_recursion = 1;
    ASSERT_OK_AND_ASSIGN(stats, fs_->GetTargetStats(selector));
    ASSERT_EQ(stats.size(), 4);
    AssertFileStats(stats[0], base_dir + "AB/CD", FileType::Directory);
    AssertFileStats(stats[1], base_dir + "AB/EF", FileType::Directory);
    AssertFileStats(stats[2], base_dir + "AB/EF/GH", FileType::Directory);
    AssertFileStats(stats[3], base_dir + "AB/data", FileType::File);

    selector.base_dir = base_dir + "XYZ";
    selector.allow_non_existent = true;
    ASSERT_OK_AND_ASSIGN(stats, fs_->GetTargetStats(selector));
    ASSERT_EQ(stats.size(), 0);

    selector.allow_non_existent = false;
    ASSERT_RAISES(IOError, fs_->GetTargetStats(selector));

    ASSERT_OK(fs_->DeleteDir(base_dir + "AB"));
    AssertFileStats(fs_.get(), base_dir + "AB", FileType::NonExistent);
  }

 protected:
  std::shared_ptr<FileSystem> fs_;
  bool use_hdfs3_;
  HdfsOptions options_;
  bool loaded_driver_ = false;
};

HdfsDriver JNIDriver::type = HdfsDriver::LIBHDFS;
HdfsDriver PivotalDriver::type = HdfsDriver::LIBHDFS3;

typedef ::testing::Types<JNIDriver, PivotalDriver> DriverTypes;

TYPED_TEST_CASE(TestHadoopFileSystem, DriverTypes);

#define SKIP_IF_NO_DRIVER()                           \
  if (!this->loaded_driver_) {                        \
    ARROW_LOG(INFO) << "Driver not loaded, skipping"; \
    return;                                           \
  }

TYPED_TEST(TestHadoopFileSystem, CreateDirDeleteDir) {
  SKIP_IF_NO_DRIVER();

  // recursive = true
  ASSERT_OK(this->fs_->CreateDir("AB/CD"));
  CreateFile(this->fs_.get(), "AB/CD/data", "some data");
  AssertFileStats(this->fs_.get(), "AB", FileType::Directory);
  AssertFileStats(this->fs_.get(), "AB/CD", FileType::Directory);
  AssertFileStats(this->fs_.get(), "AB/CD/data", FileType::File, 9);

  ASSERT_OK(this->fs_->DeleteDir("AB"));
  AssertFileStats(this->fs_.get(), "AB", FileType::NonExistent);

  // recursive = false
  ASSERT_RAISES(IOError, this->fs_->CreateDir("AB/CD", /*recursive=*/false));
  ASSERT_OK(this->fs_->CreateDir("AB", /*recursive=*/false));
  ASSERT_OK(this->fs_->CreateDir("AB/CD", /*recursive=*/false));

  ASSERT_OK(this->fs_->DeleteDir("AB"));
  AssertFileStats(this->fs_.get(), "AB", FileType::NonExistent);
  ASSERT_RAISES(IOError, this->fs_->DeleteDir("AB"));
}

TYPED_TEST(TestHadoopFileSystem, DeleteDirContents) {
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->fs_->CreateDir("AB/CD"));
  CreateFile(this->fs_.get(), "AB/CD/data", "some data");
  AssertFileStats(this->fs_.get(), "AB", FileType::Directory);
  AssertFileStats(this->fs_.get(), "AB/CD", FileType::Directory);
  AssertFileStats(this->fs_.get(), "AB/CD/data", FileType::File, 9);

  ASSERT_OK(this->fs_->DeleteDirContents("AB"));
  AssertFileStats(this->fs_.get(), "AB", FileType::Directory);
  AssertFileStats(this->fs_.get(), "AB/CD", FileType::NonExistent);
  AssertFileStats(this->fs_.get(), "AB/CD/data", FileType::NonExistent);

  ASSERT_OK(this->fs_->DeleteDirContents("AB"));
  AssertFileStats(this->fs_.get(), "AB", FileType::Directory);
  ASSERT_OK(this->fs_->DeleteDir("AB"));
}

TYPED_TEST(TestHadoopFileSystem, WriteReadFile) {
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

TYPED_TEST(TestHadoopFileSystem, GetTargetStatsRelative) {
  // Test GetTargetStats() with relative paths
  SKIP_IF_NO_DRIVER();

  this->TestGetTargetStats("");
}

TYPED_TEST(TestHadoopFileSystem, GetTargetStatsAbsolute) {
  // Test GetTargetStats() with absolute paths
  SKIP_IF_NO_DRIVER();

  this->TestGetTargetStats("/");
}

TYPED_TEST(TestHadoopFileSystem, RelativeVsAbsolutePaths) {
  SKIP_IF_NO_DRIVER();

  // XXX This test assumes the current working directory is not "/"

  ASSERT_OK(this->fs_->CreateDir("AB"));
  AssertFileStats(this->fs_.get(), "AB", FileType::Directory);
  AssertFileStats(this->fs_.get(), "/AB", FileType::NonExistent);

  ASSERT_OK(this->fs_->CreateDir("/CD"));
  AssertFileStats(this->fs_.get(), "/CD", FileType::Directory);
  AssertFileStats(this->fs_.get(), "CD", FileType::NonExistent);
}

TYPED_TEST(TestHadoopFileSystem, MoveDir) {
  SKIP_IF_NO_DRIVER();

  FileStats stat;
  std::string directory_name_src = "AB";
  std::string directory_name_dest = "CD";
  ASSERT_OK(this->fs_->CreateDir(directory_name_src));
  ASSERT_OK_AND_ASSIGN(stat, this->fs_->GetTargetStats(directory_name_src));
  AssertFileStats(stat, directory_name_src, FileType::Directory);

  // move file
  ASSERT_OK(this->fs_->Move(directory_name_src, directory_name_dest));
  ASSERT_OK_AND_ASSIGN(stat, this->fs_->GetTargetStats(directory_name_src));
  ASSERT_TRUE(stat.type() == FileType::NonExistent);

  ASSERT_OK_AND_ASSIGN(stat, this->fs_->GetTargetStats(directory_name_dest));
  AssertFileStats(stat, directory_name_dest, FileType::Directory);
  ASSERT_OK(this->fs_->DeleteDir(directory_name_dest));
}

TYPED_TEST(TestHadoopFileSystem, FileSystemFromUri) {
  SKIP_IF_NO_DRIVER();

  this->TestFileSystemFromUri();
}

}  // namespace fs
}  // namespace arrow

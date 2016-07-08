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

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

#include <boost/filesystem.hpp>  // NOLINT

#include "arrow/io/hdfs.h"
#include "arrow/test-util.h"
#include "arrow/util/status.h"

namespace arrow {
namespace io {

std::vector<uint8_t> RandomData(int64_t size) {
  std::vector<uint8_t> buffer(size);
  test::random_bytes(size, 0, buffer.data());
  return buffer;
}

class TestHdfsClient : public ::testing::Test {
 public:
  Status MakeScratchDir() {
    if (client_->Exists(scratch_dir_)) {
      RETURN_NOT_OK((client_->Delete(scratch_dir_, true)));
    }
    return client_->CreateDirectory(scratch_dir_);
  }

  Status WriteDummyFile(const std::string& path, const uint8_t* buffer, int64_t size,
      bool append = false, int buffer_size = 0, int replication = 0,
      int default_block_size = 0) {
    std::shared_ptr<HdfsWriteableFile> file;
    RETURN_NOT_OK(client_->OpenWriteable(
        path, append, buffer_size, replication, default_block_size, &file));

    RETURN_NOT_OK(file->Write(buffer, size));
    RETURN_NOT_OK(file->Close());

    return Status::OK();
  }

  std::string ScratchPath(const std::string& name) {
    std::stringstream ss;
    ss << scratch_dir_ << "/" << name;
    return ss.str();
  }

  std::string HdfsAbsPath(const std::string& relpath) {
    std::stringstream ss;
    ss << "hdfs://" << conf_.host << ":" << conf_.port << relpath;
    return ss.str();
  }

 protected:
  // Set up shared state between unit tests
  static void SetUpTestCase() {
    if (!ConnectLibHdfs().ok()) {
      std::cout << "Loading libhdfs failed, skipping tests gracefully" << std::endl;
      return;
    }

    loaded_libhdfs_ = true;

    const char* host = std::getenv("ARROW_HDFS_TEST_HOST");
    const char* port = std::getenv("ARROW_HDFS_TEST_PORT");
    const char* user = std::getenv("ARROW_HDFS_TEST_USER");

    ASSERT_TRUE(user) << "Set ARROW_HDFS_TEST_USER";

    conf_.host = host == nullptr ? "localhost" : host;
    conf_.user = user;
    conf_.port = port == nullptr ? 20500 : atoi(port);

    ASSERT_OK(HdfsClient::Connect(&conf_, &client_));
  }

  static void TearDownTestCase() {
    if (client_) {
      EXPECT_OK(client_->Delete(scratch_dir_, true));
      EXPECT_OK(client_->Disconnect());
    }
  }

  static bool loaded_libhdfs_;

  // Resources shared amongst unit tests
  static HdfsConnectionConfig conf_;
  static std::string scratch_dir_;
  static std::shared_ptr<HdfsClient> client_;
};

bool TestHdfsClient::loaded_libhdfs_ = false;
HdfsConnectionConfig TestHdfsClient::conf_ = HdfsConnectionConfig();

std::string TestHdfsClient::scratch_dir_ =
    boost::filesystem::unique_path("/tmp/arrow-hdfs/scratch-%%%%").native();

std::shared_ptr<HdfsClient> TestHdfsClient::client_ = nullptr;

#define SKIP_IF_NO_LIBHDFS()                          \
  if (!loaded_libhdfs_) {                             \
    std::cout << "No libhdfs, skipping" << std::endl; \
    return;                                           \
  }

TEST_F(TestHdfsClient, ConnectsAgain) {
  SKIP_IF_NO_LIBHDFS();

  std::shared_ptr<HdfsClient> client;
  ASSERT_OK(HdfsClient::Connect(&conf_, &client));
  ASSERT_OK(client->Disconnect());
}

TEST_F(TestHdfsClient, CreateDirectory) {
  SKIP_IF_NO_LIBHDFS();

  std::string path = ScratchPath("create-directory");

  if (client_->Exists(path)) { ASSERT_OK(client_->Delete(path, true)); }

  ASSERT_OK(client_->CreateDirectory(path));
  ASSERT_TRUE(client_->Exists(path));
  EXPECT_OK(client_->Delete(path, true));
  ASSERT_FALSE(client_->Exists(path));
}

TEST_F(TestHdfsClient, GetCapacityUsed) {
  SKIP_IF_NO_LIBHDFS();

  // Who knows what is actually in your DFS cluster, but expect it to have
  // positive used bytes and capacity
  int64_t nbytes = 0;
  ASSERT_OK(client_->GetCapacity(&nbytes));
  ASSERT_LT(0, nbytes);

  ASSERT_OK(client_->GetUsed(&nbytes));
  ASSERT_LT(0, nbytes);
}

TEST_F(TestHdfsClient, GetPathInfo) {
  SKIP_IF_NO_LIBHDFS();

  HdfsPathInfo info;

  ASSERT_OK(MakeScratchDir());

  // Directory info
  ASSERT_OK(client_->GetPathInfo(scratch_dir_, &info));
  ASSERT_EQ(ObjectType::DIRECTORY, info.kind);
  ASSERT_EQ(HdfsAbsPath(scratch_dir_), info.name);
  ASSERT_EQ(conf_.user, info.owner);

  // TODO(wesm): test group, other attrs

  auto path = ScratchPath("test-file");

  const int size = 100;

  std::vector<uint8_t> buffer = RandomData(size);

  ASSERT_OK(WriteDummyFile(path, buffer.data(), size));
  ASSERT_OK(client_->GetPathInfo(path, &info));

  ASSERT_EQ(ObjectType::FILE, info.kind);
  ASSERT_EQ(HdfsAbsPath(path), info.name);
  ASSERT_EQ(conf_.user, info.owner);
  ASSERT_EQ(size, info.size);
}

TEST_F(TestHdfsClient, AppendToFile) {
  SKIP_IF_NO_LIBHDFS();

  ASSERT_OK(MakeScratchDir());

  auto path = ScratchPath("test-file");
  const int size = 100;

  std::vector<uint8_t> buffer = RandomData(size);
  ASSERT_OK(WriteDummyFile(path, buffer.data(), size));

  // now append
  ASSERT_OK(WriteDummyFile(path, buffer.data(), size, true));

  HdfsPathInfo info;
  ASSERT_OK(client_->GetPathInfo(path, &info));
  ASSERT_EQ(size * 2, info.size);
}

TEST_F(TestHdfsClient, ListDirectory) {
  SKIP_IF_NO_LIBHDFS();

  const int size = 100;
  std::vector<uint8_t> data = RandomData(size);

  auto p1 = ScratchPath("test-file-1");
  auto p2 = ScratchPath("test-file-2");
  auto d1 = ScratchPath("test-dir-1");

  ASSERT_OK(MakeScratchDir());
  ASSERT_OK(WriteDummyFile(p1, data.data(), size));
  ASSERT_OK(WriteDummyFile(p2, data.data(), size / 2));
  ASSERT_OK(client_->CreateDirectory(d1));

  std::vector<HdfsPathInfo> listing;
  ASSERT_OK(client_->ListDirectory(scratch_dir_, &listing));

  // Do it again, appends!
  ASSERT_OK(client_->ListDirectory(scratch_dir_, &listing));

  ASSERT_EQ(6, static_cast<int>(listing.size()));

  // Argh, well, shouldn't expect the listing to be in any particular order
  for (size_t i = 0; i < listing.size(); ++i) {
    const HdfsPathInfo& info = listing[i];
    if (info.name == HdfsAbsPath(p1)) {
      ASSERT_EQ(ObjectType::FILE, info.kind);
      ASSERT_EQ(size, info.size);
    } else if (info.name == HdfsAbsPath(p2)) {
      ASSERT_EQ(ObjectType::FILE, info.kind);
      ASSERT_EQ(size / 2, info.size);
    } else if (info.name == HdfsAbsPath(d1)) {
      ASSERT_EQ(ObjectType::DIRECTORY, info.kind);
    } else {
      FAIL() << "Unexpected path: " << info.name;
    }
  }
}

TEST_F(TestHdfsClient, ReadableMethods) {
  SKIP_IF_NO_LIBHDFS();

  ASSERT_OK(MakeScratchDir());

  auto path = ScratchPath("test-file");
  const int size = 100;

  std::vector<uint8_t> data = RandomData(size);
  ASSERT_OK(WriteDummyFile(path, data.data(), size));

  std::shared_ptr<HdfsReadableFile> file;
  ASSERT_OK(client_->OpenReadable(path, &file));

  // Test GetSize -- move this into its own unit test if ever needed
  int64_t file_size;
  ASSERT_OK(file->GetSize(&file_size));
  ASSERT_EQ(size, file_size);

  uint8_t buffer[50];
  int32_t bytes_read = 0;

  ASSERT_OK(file->Read(50, &bytes_read, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, data.data(), 50));
  ASSERT_EQ(50, bytes_read);

  ASSERT_OK(file->Read(50, &bytes_read, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, data.data() + 50, 50));
  ASSERT_EQ(50, bytes_read);

  // EOF
  ASSERT_OK(file->Read(1, &bytes_read, buffer));
  ASSERT_EQ(0, bytes_read);

  // ReadAt to EOF
  ASSERT_OK(file->ReadAt(60, 100, &bytes_read, buffer));
  ASSERT_EQ(40, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, data.data() + 60, bytes_read));

  // Seek, Tell
  ASSERT_OK(file->Seek(60));

  int64_t position;
  ASSERT_OK(file->Tell(&position));
  ASSERT_EQ(60, position);
}

TEST_F(TestHdfsClient, RenameFile) {
  SKIP_IF_NO_LIBHDFS();

  ASSERT_OK(MakeScratchDir());

  auto src_path = ScratchPath("src-file");
  auto dst_path = ScratchPath("dst-file");
  const int size = 100;

  std::vector<uint8_t> data = RandomData(size);
  ASSERT_OK(WriteDummyFile(src_path, data.data(), size));

  ASSERT_OK(client_->Rename(src_path, dst_path));

  ASSERT_FALSE(client_->Exists(src_path));
  ASSERT_TRUE(client_->Exists(dst_path));
}

}  // namespace io
}  // namespace arrow

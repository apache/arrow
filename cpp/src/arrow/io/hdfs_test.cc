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

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/hdfs.h"
#include "arrow/io/hdfs_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace io {

std::vector<uint8_t> RandomData(int64_t size) {
  std::vector<uint8_t> buffer(size);
  random_bytes(size, 0, buffer.data());
  return buffer;
}

class TestHadoopFileSystem : public ::testing::Test {
 public:
  Status MakeScratchDir() {
    if (client_->Exists(scratch_dir_)) {
      RETURN_NOT_OK((client_->Delete(scratch_dir_, true)));
    }
    return client_->MakeDirectory(scratch_dir_);
  }

  Status WriteDummyFile(const std::string& path, const uint8_t* buffer, int64_t size,
                        bool append = false, int buffer_size = 0, int16_t replication = 0,
                        int default_block_size = 0) {
    std::shared_ptr<HdfsOutputStream> file;
    RETURN_NOT_OK(client_->OpenWritable(path, append, buffer_size, replication,
                                        default_block_size, &file));

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

  // Set up shared state between unit tests
  void SetUp() {
    internal::LibHdfsShim* driver_shim;

    client_ = nullptr;
    scratch_dir_ =
        (std::filesystem::temp_directory_path() / "arrow-hdfs/scratch-").string();
    int random_size = 4;
    scratch_dir_.resize(scratch_dir_.size() + random_size, '%');
    random_alnum(
        random_size, 0,
        reinterpret_cast<uint8_t*>(&scratch_dir_[scratch_dir_.size() - random_size]));

    loaded_driver_ = false;

    Status msg = ConnectLibHdfs(&driver_shim);
    if (!msg.ok()) {
      if (std::getenv("ARROW_HDFS_TEST_LIBHDFS_REQUIRE")) {
        FAIL() << "Loading libhdfs failed: " << msg.ToString();
      } else {
        std::cout << "Loading libhdfs failed, skipping tests gracefully: "
                  << msg.ToString() << std::endl;
      }
      return;
    }

    loaded_driver_ = true;

    const char* host = std::getenv("ARROW_HDFS_TEST_HOST");
    const char* port = std::getenv("ARROW_HDFS_TEST_PORT");
    const char* user = std::getenv("ARROW_HDFS_TEST_USER");

    ASSERT_TRUE(user != nullptr) << "Set ARROW_HDFS_TEST_USER";

    conf_.host = host == nullptr ? "localhost" : host;
    conf_.user = user;
    conf_.port = port == nullptr ? 20500 : atoi(port);

    ASSERT_OK(HadoopFileSystem::Connect(&conf_, &client_));
  }

  void TearDown() {
    if (client_) {
      if (client_->Exists(scratch_dir_)) {
        ARROW_EXPECT_OK(client_->Delete(scratch_dir_, true));
      }
      ARROW_EXPECT_OK(client_->Disconnect());
    }
  }

  HdfsConnectionConfig conf_;
  bool loaded_driver_;

  // Resources shared amongst unit tests
  std::string scratch_dir_;
  std::shared_ptr<HadoopFileSystem> client_;
};

#define SKIP_IF_NO_DRIVER()                        \
  if (!this->loaded_driver_) {                     \
    GTEST_SKIP() << "Driver not loaded, skipping"; \
  }

TEST_F(TestHadoopFileSystem, ConnectsAgain) {
  SKIP_IF_NO_DRIVER();

  std::shared_ptr<HadoopFileSystem> client;
  ASSERT_OK(HadoopFileSystem::Connect(&this->conf_, &client));
  ASSERT_OK(client->Disconnect());
}

TEST_F(TestHadoopFileSystem, MultipleClients) {
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->MakeScratchDir());

  std::shared_ptr<HadoopFileSystem> client1;
  std::shared_ptr<HadoopFileSystem> client2;
  ASSERT_OK(HadoopFileSystem::Connect(&this->conf_, &client1));
  ASSERT_OK(HadoopFileSystem::Connect(&this->conf_, &client2));
  ASSERT_OK(client1->Disconnect());

  // client2 continues to function after equivalent client1 has shutdown
  std::vector<HdfsPathInfo> listing;
  ASSERT_OK(client2->ListDirectory(this->scratch_dir_, &listing));
  ASSERT_OK(client2->Disconnect());
}

TEST_F(TestHadoopFileSystem, MakeDirectory) {
  SKIP_IF_NO_DRIVER();

  std::string path = this->ScratchPath("create-directory");

  if (this->client_->Exists(path)) {
    ASSERT_OK(this->client_->Delete(path, true));
  }

  ASSERT_OK(this->client_->MakeDirectory(path));
  ASSERT_TRUE(this->client_->Exists(path));
  std::vector<HdfsPathInfo> listing;
  ARROW_EXPECT_OK(this->client_->ListDirectory(path, &listing));
  ASSERT_EQ(0, listing.size());
  ARROW_EXPECT_OK(this->client_->Delete(path, true));
  ASSERT_FALSE(this->client_->Exists(path));
  ASSERT_RAISES(IOError, this->client_->ListDirectory(path, &listing));
}

TEST_F(TestHadoopFileSystem, GetCapacityUsed) {
  SKIP_IF_NO_DRIVER();

  // Who knows what is actually in your DFS cluster, but expect it to have
  // positive used bytes and capacity
  int64_t nbytes = 0;
  ASSERT_OK(this->client_->GetCapacity(&nbytes));
  ASSERT_LT(0, nbytes);

  ASSERT_OK(this->client_->GetUsed(&nbytes));
  ASSERT_LT(0, nbytes);
}

TEST_F(TestHadoopFileSystem, GetPathInfo) {
  SKIP_IF_NO_DRIVER();

  HdfsPathInfo info;

  ASSERT_OK(this->MakeScratchDir());

  // Directory info
  ASSERT_OK(this->client_->GetPathInfo(this->scratch_dir_, &info));
  ASSERT_EQ(ObjectType::DIRECTORY, info.kind);
  ASSERT_EQ(this->HdfsAbsPath(this->scratch_dir_), info.name);
  ASSERT_EQ(this->conf_.user, info.owner);

  // TODO(wesm): test group, other attrs

  auto path = this->ScratchPath("test-file");

  const int size = 100;

  std::vector<uint8_t> buffer = RandomData(size);

  ASSERT_OK(this->WriteDummyFile(path, buffer.data(), size));
  ASSERT_OK(this->client_->GetPathInfo(path, &info));

  ASSERT_EQ(ObjectType::FILE, info.kind);
  ASSERT_EQ(this->HdfsAbsPath(path), info.name);
  ASSERT_EQ(this->conf_.user, info.owner);
  ASSERT_EQ(size, info.size);
}

TEST_F(TestHadoopFileSystem, GetPathInfoNotExist) {
  // ARROW-2919: Test that the error message is reasonable
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->MakeScratchDir());
  auto path = this->ScratchPath("path-does-not-exist");

  HdfsPathInfo info;
  Status s = this->client_->GetPathInfo(path, &info);
  ASSERT_TRUE(s.IsIOError());

  const std::string error_message = s.ToString();

  // Check that the file path is found in the error message
  ASSERT_LT(error_message.find(path), std::string::npos);
}

TEST_F(TestHadoopFileSystem, AppendToFile) {
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->MakeScratchDir());

  auto path = this->ScratchPath("test-file");
  const int size = 100;

  std::vector<uint8_t> buffer = RandomData(size);
  ASSERT_OK(this->WriteDummyFile(path, buffer.data(), size));

  // now append
  ASSERT_OK(this->WriteDummyFile(path, buffer.data(), size, true));

  HdfsPathInfo info;
  ASSERT_OK(this->client_->GetPathInfo(path, &info));
  ASSERT_EQ(size * 2, info.size);
}

TEST_F(TestHadoopFileSystem, ListDirectory) {
  SKIP_IF_NO_DRIVER();

  const int size = 100;
  std::vector<uint8_t> data = RandomData(size);

  auto p1 = this->ScratchPath("test-file-1");
  auto p2 = this->ScratchPath("test-file-2");
  auto d1 = this->ScratchPath("test-dir-1");

  ASSERT_OK(this->MakeScratchDir());
  ASSERT_OK(this->WriteDummyFile(p1, data.data(), size));
  ASSERT_OK(this->WriteDummyFile(p2, data.data(), size / 2));
  ASSERT_OK(this->client_->MakeDirectory(d1));

  std::vector<HdfsPathInfo> listing;
  ASSERT_OK(this->client_->ListDirectory(this->scratch_dir_, &listing));

  // Do it again, appends!
  ASSERT_OK(this->client_->ListDirectory(this->scratch_dir_, &listing));

  ASSERT_EQ(6, static_cast<int>(listing.size()));

  // Argh, well, shouldn't expect the listing to be in any particular order
  for (size_t i = 0; i < listing.size(); ++i) {
    const HdfsPathInfo& info = listing[i];
    if (info.name == this->HdfsAbsPath(p1)) {
      ASSERT_EQ(ObjectType::FILE, info.kind);
      ASSERT_EQ(size, info.size);
    } else if (info.name == this->HdfsAbsPath(p2)) {
      ASSERT_EQ(ObjectType::FILE, info.kind);
      ASSERT_EQ(size / 2, info.size);
    } else if (info.name == this->HdfsAbsPath(d1)) {
      ASSERT_EQ(ObjectType::DIRECTORY, info.kind);
    } else {
      FAIL() << "Unexpected path: " << info.name;
    }
  }
}

TEST_F(TestHadoopFileSystem, ReadableMethods) {
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->MakeScratchDir());

  auto path = this->ScratchPath("test-file");
  const int size = 100;

  std::vector<uint8_t> data = RandomData(size);
  ASSERT_OK(this->WriteDummyFile(path, data.data(), size));

  std::shared_ptr<HdfsReadableFile> file;
  ASSERT_OK(this->client_->OpenReadable(path, &file));

  // Test GetSize -- move this into its own unit test if ever needed
  ASSERT_OK_AND_EQ(size, file->GetSize());

  uint8_t buffer[50];

  ASSERT_OK_AND_EQ(50, file->Read(50, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, data.data(), 50));

  ASSERT_OK_AND_EQ(50, file->Read(50, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, data.data() + 50, 50));

  // EOF
  ASSERT_OK_AND_EQ(0, file->Read(1, buffer));

  // ReadAt to EOF
  ASSERT_OK_AND_EQ(40, file->ReadAt(60, 100, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, data.data() + 60, 40));

  // Seek, Tell
  ASSERT_OK(file->Seek(60));
  ASSERT_OK_AND_EQ(60, file->Tell());
}

TEST_F(TestHadoopFileSystem, LargeFile) {
  SKIP_IF_NO_DRIVER();

  ASSERT_OK(this->MakeScratchDir());

  auto path = this->ScratchPath("test-large-file");
  const int size = 1000000;

  std::vector<uint8_t> data = RandomData(size);
  ASSERT_OK(this->WriteDummyFile(path, data.data(), size));

  std::shared_ptr<HdfsReadableFile> file;
  ASSERT_OK(this->client_->OpenReadable(path, &file));

  ASSERT_FALSE(file->closed());

  ASSERT_OK_AND_ASSIGN(auto buffer, AllocateBuffer(size));

  ASSERT_OK_AND_EQ(size, file->Read(size, buffer->mutable_data()));
  ASSERT_EQ(0, std::memcmp(buffer->data(), data.data(), size));

  // explicit buffer size
  std::shared_ptr<HdfsReadableFile> file2;
  ASSERT_OK(this->client_->OpenReadable(path, 1 << 18, &file2));

  ASSERT_OK_AND_ASSIGN(auto buffer2, AllocateBuffer(size));

  ASSERT_OK_AND_EQ(size, file2->Read(size, buffer2->mutable_data()));
  ASSERT_EQ(0, std::memcmp(buffer2->data(), data.data(), size));
}

TEST_F(TestHadoopFileSystem, RenameFile) {
  SKIP_IF_NO_DRIVER();
  ASSERT_OK(this->MakeScratchDir());

  auto src_path = this->ScratchPath("src-file");
  auto dst_path = this->ScratchPath("dst-file");
  const int size = 100;

  std::vector<uint8_t> data = RandomData(size);
  ASSERT_OK(this->WriteDummyFile(src_path, data.data(), size));

  ASSERT_OK(this->client_->Rename(src_path, dst_path));

  ASSERT_FALSE(this->client_->Exists(src_path));
  ASSERT_TRUE(this->client_->Exists(dst_path));
}

TEST_F(TestHadoopFileSystem, ChmodChown) {
  SKIP_IF_NO_DRIVER();
  ASSERT_OK(this->MakeScratchDir());

  auto path = this->ScratchPath("path-to-chmod");

  int16_t mode = 0755;
  const int size = 100;

  std::vector<uint8_t> data = RandomData(size);
  ASSERT_OK(this->WriteDummyFile(path, data.data(), size));

  HdfsPathInfo info;
  ASSERT_OK(this->client_->Chmod(path, mode));
  ASSERT_OK(this->client_->GetPathInfo(path, &info));
  ASSERT_EQ(mode, info.permissions);

  std::string owner = "hadoop";
  std::string group = "hadoop";
  ASSERT_OK(this->client_->Chown(path, owner.c_str(), group.c_str()));
  ASSERT_OK(this->client_->GetPathInfo(path, &info));
  ASSERT_EQ("hadoop", info.owner);
  ASSERT_EQ("hadoop", info.group);
}

TEST_F(TestHadoopFileSystem, ThreadSafety) {
  SKIP_IF_NO_DRIVER();
  ASSERT_OK(this->MakeScratchDir());

  auto src_path = this->ScratchPath("threadsafety");

  std::string data = "foobar";
  ASSERT_OK(this->WriteDummyFile(src_path, reinterpret_cast<const uint8_t*>(data.c_str()),
                                 static_cast<int64_t>(data.size())));

  std::shared_ptr<HdfsReadableFile> file;
  ASSERT_OK(this->client_->OpenReadable(src_path, &file));

  std::atomic<int> correct_count(0);
  int niter = 1000;

  auto ReadData = [&file, &correct_count, &data, &niter]() {
    for (int i = 0; i < niter; ++i) {
      std::shared_ptr<Buffer> buffer;
      if (i % 2 == 0) {
        ASSERT_OK_AND_ASSIGN(buffer, file->ReadAt(3, 3));
        if (0 == memcmp(data.c_str() + 3, buffer->data(), 3)) {
          correct_count += 1;
        }
      } else {
        ASSERT_OK_AND_ASSIGN(buffer, file->ReadAt(0, 4));
        if (0 == memcmp(data.c_str() + 0, buffer->data(), 4)) {
          correct_count += 1;
        }
      }
    }
  };

  std::thread thread1(ReadData);
  std::thread thread2(ReadData);
  std::thread thread3(ReadData);
  std::thread thread4(ReadData);

  thread1.join();
  thread2.join();
  thread3.join();
  thread4.join();

  ASSERT_EQ(niter * 4, correct_count);
}

}  // namespace io
}  // namespace arrow

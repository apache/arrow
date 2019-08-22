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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/api.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace dataset {

using fs::internal::GetAbstractPathExtension;
using internal::TemporaryDir;

TEST(FileSource, PathBased) {
  fs::LocalFileSystem localfs;

  std::string p1 = "/path/to/file.ext";
  std::string p2 = "/path/to/file.ext.gz";

  FileSource source1(p1, &localfs);
  FileSource source2(p2, &localfs, Compression::GZIP);

  ASSERT_EQ(p1, source1.path());
  ASSERT_EQ(&localfs, source1.filesystem());
  ASSERT_EQ(FileSource::PATH, source1.type());
  ASSERT_EQ(Compression::UNCOMPRESSED, source1.compression());

  ASSERT_EQ(p2, source2.path());
  ASSERT_EQ(&localfs, source2.filesystem());
  ASSERT_EQ(FileSource::PATH, source2.type());
  ASSERT_EQ(Compression::GZIP, source2.compression());

  // Test copy constructor and comparison
  FileSource source3 = source1;
  ASSERT_EQ(source1, source3);
}

TEST(FileSource, BufferBased) {
  std::string the_data = "this is the file contents";
  auto buf = std::make_shared<Buffer>(the_data);

  FileSource source1(buf);
  FileSource source2(buf, Compression::LZ4);

  ASSERT_EQ(FileSource::BUFFER, source1.type());
  ASSERT_TRUE(source1.buffer()->Equals(*buf));
  ASSERT_EQ(Compression::UNCOMPRESSED, source1.compression());

  ASSERT_EQ(FileSource::BUFFER, source2.type());
  ASSERT_TRUE(source2.buffer()->Equals(*buf));
  ASSERT_EQ(Compression::LZ4, source2.compression());
}

class TestFileSystemBasedDataSource : public FileSourceFixtureMixin {
 public:
  void SetUp() override {
    format_ = std::make_shared<DummyFileFormat>();

    ASSERT_OK(TemporaryDir::Make("test-fsdatasource-", &temp_dir_));
    local_fs_ = std::make_shared<fs::LocalFileSystem>();

    auto path = temp_dir_->path().ToString();
    fs_ = std::make_shared<fs::SubTreeFileSystem>(path, local_fs_);

    CreateEmptyFiles();
  }

  void CreateFile(std::string path, std::string contents) {
    auto parent = fs::internal::GetAbstractPathParent(path).first;
    if (parent != "") {
      ASSERT_OK(this->fs_->CreateDir(parent, true));
    }
    std::shared_ptr<io::OutputStream> file;
    ASSERT_OK(this->fs_->OpenOutputStream(path, &file));
    ASSERT_OK(file->Write(contents));
  }

  void CreateEmptyFiles() {
    for (auto path : {"a/b/c.dummy", "a/b/c/d.dummy", "a/b.dummy", "a.dummy"}) {
      CreateFile(path, "");
    }
  }

  void MakeDataSource() {
    ASSERT_OK(FileSystemBasedDataSource::Make(fs_.get(), selector_, format_,
                                              std::make_shared<ScanOptions>(), &source_));
  }

 protected:
  fs::Selector selector_;
  std::unique_ptr<FileSystemBasedDataSource> source_;
  std::shared_ptr<fs::LocalFileSystem> local_fs_;
  std::shared_ptr<fs::FileSystem> fs_;
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::shared_ptr<DummyFileFormat> format_;
};

TEST_F(TestFileSystemBasedDataSource, NonRecursive) {
  selector_.base_dir = "/";
  MakeDataSource();

  int count = 0;
  ASSERT_OK(source_->GetFragments({})->Visit([&](std::shared_ptr<DataFragment> fragment) {
    auto file_fragment = internal::checked_pointer_cast<FileBasedDataFragment>(fragment);
    ++count;
    auto extension =
        fs::internal::GetAbstractPathExtension(file_fragment->source().path());
    EXPECT_TRUE(format_->IsKnownExtension(extension));
    return Status::OK();
  }));

  ASSERT_EQ(count, 1);
}

TEST_F(TestFileSystemBasedDataSource, Recursive) {
  selector_.base_dir = "/";
  selector_.recursive = true;
  MakeDataSource();

  int count = 0;
  ASSERT_OK(source_->GetFragments({})->Visit([&](std::shared_ptr<DataFragment> fragment) {
    auto file_fragment = internal::checked_pointer_cast<FileBasedDataFragment>(fragment);
    ++count;
    auto extension =
        fs::internal::GetAbstractPathExtension(file_fragment->source().path());
    EXPECT_TRUE(format_->IsKnownExtension(extension));
    return Status::OK();
  }));

  ASSERT_EQ(count, 4);
}

TEST_F(TestFileSystemBasedDataSource, DeletedFile) {
  selector_.base_dir = "/";
  selector_.recursive = true;
  MakeDataSource();
  ASSERT_OK(this->fs_->DeleteFile("a/b.dummy"));

  ASSERT_RAISES(
      Invalid,
      source_->GetFragments({})->Visit([&](std::shared_ptr<DataFragment> fragment) {
        auto file_fragment =
            internal::checked_pointer_cast<FileBasedDataFragment>(fragment);
        auto extension =
            fs::internal::GetAbstractPathExtension(file_fragment->source().path());
        EXPECT_TRUE(format_->IsKnownExtension(extension));
        return Status::OK();
      }));
}

}  // namespace dataset
}  // namespace arrow

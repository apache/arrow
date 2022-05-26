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

#include "arrow/filesystem/azurefs_mock.h"

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "arrow/filesystem/test_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::Uri;

namespace fs {
namespace internal {

class TestAzureFileSystem : public ::testing::Test {
 public:
  TimePoint time_;
  std::shared_ptr<FileSystem> fs_;

  void MakeFileSystem() { fs_ = std::make_shared<MockAzureFileSystem>(time_); }

  void SetUp() override {
    time_ = TimePoint(TimePoint::duration(42));
    MakeFileSystem();
    fs_->CreateDir("container");
    fs_->CreateDir("empty-container");
    fs_->CreateDir("container2/newdir");
    fs_->CreateDir("container/emptydir");
    fs_->CreateDir("container/somedir");
    fs_->CreateDir("container/somedir/subdir");
    CreateFile(fs_.get(), "container/somedir/subdir/subfile", "sub data");
    CreateFile(fs_.get(), "container/somefile", "some data");
  }

  bool CheckFile(const MockFileInfo& expected) {
    return arrow::internal::checked_pointer_cast<MockAzureFileSystem>(fs_)->CheckFile(
        expected);
  }
};

TEST_F(TestAzureFileSystem, CreateDir) {
  // New container
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::Directory);

  // Existing container
  ASSERT_OK(fs_->CreateDir("container"));
  AssertFileInfo(fs_.get(), "container", FileType::Directory);

  ASSERT_RAISES(IOError, fs_->CreateDir(""));

  // New "directory", true
  AssertFileInfo(fs_.get(), "container/newdir", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("container/newdir", true));
  AssertFileInfo(fs_.get(), "container/newdir", FileType::Directory);

  // New "directory", false
  AssertFileInfo(fs_.get(), "container/newdir1", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("container/newdir1", false));
  AssertFileInfo(fs_.get(), "container/newdir1", FileType::Directory);

  // Existing "directory", true
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("container/somedir", true));
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);

  // Existing "directory", false
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("container/somedir", false));
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);

  // Existing "file", should fail
  ASSERT_RAISES(IOError, fs_->CreateDir("container/somefile"));

  // C/D/D
  AssertFileInfo(fs_.get(), "container/somedir/subdir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("container/somedir/subdir"));
  AssertFileInfo(fs_.get(), "container/somedir/subdir", FileType::Directory);

  auto res = fs_->OpenOutputStream("container/somedir/base.txt");
  res->get()->Write("Changed the data");

  // C/D/F
  AssertFileInfo(fs_.get(), "container/somedir/base.txt", FileType::File);
  ASSERT_RAISES(IOError, fs_->CreateDir("container/somedir/base.txt"));
  AssertFileInfo(fs_.get(), "container/somedir/base.txt", FileType::File);

  // New "directory",Parent dir not exists, false
  ASSERT_RAISES(IOError, fs_->CreateDir("container/newdir/newsub/newsubsub", false));

  // New "directory",Parent dir not exists, true
  ASSERT_OK(fs_->CreateDir("container/newdir/newsub/newsubsub", true));
  AssertFileInfo(fs_.get(), "container/newdir/newsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "container/newdir/newsub/newsubsub", FileType::Directory);

  // New "directory",Container not exists, false
  ASSERT_RAISES(IOError, fs_->CreateDir("container4/newdir", false));

  // New "directory",Container not exists, true
  ASSERT_OK(fs_->CreateDir("container4/newdir", true));
  AssertFileInfo(fs_.get(), "container4", FileType::Directory);
  AssertFileInfo(fs_.get(), "container4/newdir", FileType::Directory);
}

TEST_F(TestAzureFileSystem, DeleteDir) {
  FileSelector select;
  select.base_dir = "container2";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_OK(fs_->DeleteDir("empty-container"));
  AssertFileInfo(fs_.get(), "empty-container", FileType::NotFound);

  // Nonexistent Container
  ASSERT_OK(fs_->DeleteDir("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound);

  // root
  ASSERT_RAISES(NotImplemented, fs_->DeleteDir(""));

  // C/D
  ASSERT_OK(fs_->DeleteDir("container2/newdir"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // C/ND
  AssertFileInfo(fs_.get(), "container2/newdir1", FileType::NotFound);
  ASSERT_RAISES(IOError, fs_->DeleteDir("container2/newdir1"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // C/F
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somefile"));

  // C/D/D
  ASSERT_OK(fs_->DeleteDir("container/somedir/subdir"));

  // C/D/F
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somedir/subdir/subfile"));

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container6/somedir"));
}

TEST_F(TestAzureFileSystem, DeleteFile) {
  FileSelector select;
  select.base_dir = "container2";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container2"));

  // Nonexistent Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container5"));

  // root
  ASSERT_RAISES(IOError, fs_->DeleteFile(""));

  // C/D
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/emptydir"));

  // C/ND
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/emptydir1"));

  // C/F
  ASSERT_OK(fs_->DeleteFile("container/somefile"));

  // C/NF
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somefile"));

  // C/D/D
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somedir/subdir"));

  // C/D/F
  AssertFileInfo(fs_.get(), "container/somedir/subdir/subfile", FileType::File);
  ASSERT_OK(fs_->DeleteFile("container/somedir/subdir/subfile"));

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteFile("container7/somedir"));
}

TEST_F(TestAzureFileSystem, GetFileInfo) {
  // Containers
  AssertFileInfo(fs_.get(), "container", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-container", FileType::NotFound);

  AssertFileInfo(fs_.get(), "", FileType::Directory);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/emptydir", FileType::Directory);
  AssertFileInfo(fs_.get(), "container/emptydir1", FileType::NotFound);

  // "Files"
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File);
  AssertFileInfo(fs_.get(), "container/somefile1", FileType::NotFound);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/somedir/subdir", FileType::Directory);
  AssertFileInfo(fs_.get(), "container/somedir/subdir/subfile", FileType::File);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/somedir45/subdir", FileType::NotFound);
  AssertFileInfo(fs_.get(), "containe23r/somedir/subdir/subfile", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoSelector) {
  FileSelector select;
  std::vector<FileInfo> infos;

  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 3);

  // Nonexistent container
  select.base_dir = "nonexistent-container";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 3);

  // C/D
  select.base_dir = "container/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);

  // C/ND
  select.base_dir = "container/sgsgs";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // C/F
  select.base_dir = "container/somefile";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));

  // C/D/D
  select.base_dir = "container/somedir/subdir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);

  // C/F
  select.base_dir = "container/somedir/subdir/subfile";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));

  // C/ND/D
  select.base_dir = "container/ahsh/agsg";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // NC/D
  select.base_dir = "nonexistent-container/agshhs";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;
}

TEST_F(TestAzureFileSystem, Move) {
  ASSERT_RAISES(IOError, fs_->Move("container", "container/nshhd"));
  fs_->CreateDir("container/newdir/newsub/newsubsub", true);
  ASSERT_RAISES(IOError,
                fs_->Move("container/somedir/subdir", "container/newdir/newsub"));
  ASSERT_OK(fs_->Move("container/newdir/newsub", "container/emptydir"));
  ASSERT_OK(fs_->Move("container/emptydir", "container/emptydir1"));
  ASSERT_OK(fs_->Move("container/emptydir1", "container/emptydir"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/somefile"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/ahsh/gssjd"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir23", "container/base.txt"));
  ASSERT_OK(fs_->Move("container/somedir/subdir/subfile", "container/somefile"));
  ASSERT_OK(fs_->Move("container/somefile", "container/base.txt"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "container/ahsh/gssjd"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "containerqw/ghdj"));
  ASSERT_RAISES(IOError, fs_->Move("container/base2.txt", "container/gshh"));
}

TEST_F(TestAzureFileSystem, CopyFile) {
  // "File"
  ASSERT_RAISES(IOError, fs_->CopyFile("container", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir", "container/newfile"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container/somedir22/subdir", "container/newfile"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container23/somedir/subdir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container3435"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/somedir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/ahsj/ggws"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container27/hshj"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base2t.txt", "container27/hshj"));

  ASSERT_OK(fs_->CopyFile("container/somefile", "container/somedir/subdir/subfile"));
  ASSERT_OK(fs_->CopyFile("container/somefile", "container/somefile3"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container/somedir/subdir/subfile", "container3435"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", ""));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container/somedir/subdir/subfile", "container/somedir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                       "container/somedir/subdir"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container/somedir/subdir/subfile", "container/ahsj/ggws"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container/somedir/subdir/subfile", "container27/hshj"));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container/somedir/subdir/subfile", "container27/hshj"));
  ASSERT_OK(fs_->CopyFile("container/somedir/subdir/subfile", "container/somefile"));
  fs_->DeleteFile("container/somefile3");
  ASSERT_OK(fs_->CopyFile("container/somedir/subdir/subfile", "container/somefile3"));
}

TEST_F(TestAzureFileSystem, OpenInputStream) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buf;

  ASSERT_RAISES(IOError, fs_->OpenInputStream("container"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container263"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream(""));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/sjdjd"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/shjdj/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container526/somedir"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("container/somefile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(2));
  AssertBufferEqual(*buf, "so");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "ta");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "");

  CreateFile(fs_.get(), "container/subfile", "sub data");

  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("container/subfile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "sub data");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "");
  ASSERT_OK(stream->Close());
}

TEST_F(TestAzureFileSystem, OpenInputFile) {
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;

  ASSERT_RAISES(IOError, fs_->OpenInputFile("container"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container263"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile(""));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/sjdjd"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/shjdj/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container526/somedir"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile("container/somefile"));
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_ASSIGN(buf, file->Read(4));
  AssertBufferEqual(*buf, "some");
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_EQ(4, file->Tell());

  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(2, 5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_EQ(4, file->Tell());
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(5, 20));
  AssertBufferEqual(*buf, "data");
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(9, 20));
  AssertBufferEqual(*buf, "");

  char result[10];
  ASSERT_OK_AND_EQ(5, file->ReadAt(2, 5, &result));
  ASSERT_OK_AND_EQ(4, file->ReadAt(5, 20, &result));
  ASSERT_OK_AND_EQ(0, file->ReadAt(9, 0, &result));

  // Reading past end of file
  ASSERT_RAISES(IOError, file->ReadAt(10, 20));

  ASSERT_OK(file->Seek(5));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "da");
  ASSERT_OK(file->Seek(9));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "");
  // Seeking past end of file
  ASSERT_RAISES(IOError, file->Seek(10));
}

TEST_F(TestAzureFileSystem, OpenOutputStream) {
  std::shared_ptr<io::OutputStream> stream;

  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container263"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream(""));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/shjdj/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container526/somedir"));

  // Create new empty file
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(CheckFile({"container/newfile1", time_, ""}));

  // Create new file with 1 small write
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile2"));
  ASSERT_OK(stream->Write("some data"));
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(CheckFile({"container/newfile2", time_, "some data"}));

  // Create new file with 3 small writes
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile3"));
  ASSERT_OK(stream->Write("some "));
  ASSERT_OK(stream->Write(""));
  ASSERT_OK(stream->Write("new data"));
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(CheckFile({"container/newfile3", time_, "some new data"}));

  // Create new file with some large writes
  std::string s1, s2, s3, s4, s5, expected;
  s1 = random_string(6000000, /*seed =*/42);  // More than the 5 MB minimum part upload
  s2 = "xxx";
  s3 = random_string(6000000, 43);
  s4 = "zzz";
  s5 = random_string(600000, 44);
  expected = s1 + s2 + s3 + s4 + s5;
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile4"));
  for (auto input : {s1, s2, s3, s4, s5}) {
    ASSERT_OK(stream->Write(input));
    // Clobber source contents.  This shouldn't reflect in the data written.
    input.front() = 'x';
    input.back() = 'x';
  }
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(CheckFile({"container/newfile4", time_, expected}));

  // Overwrite
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Write("overwritten data"));
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(CheckFile({"container/newfile1", time_, "overwritten data"}));

  // Overwrite and make empty
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  ASSERT_TRUE(CheckFile({"container/newfile1", time_, ""}));
}

TEST_F(TestAzureFileSystem, DeleteDirContents) {
  FileSelector select;
  select.base_dir = "container2/newdir";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_OK(fs_->DeleteDirContents("container2"));
  AssertFileInfo(fs_.get(), "container2", FileType::Directory);

  // Nonexistent Container
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container3"));

  // root
  ASSERT_RAISES(IOError, fs_->DeleteDirContents(""));

  fs_->CreateDir("container2/newdir/subdir", true);

  // C/D
  ASSERT_OK(fs_->DeleteDirContents("container2/newdir"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // C/ND
  AssertFileInfo(fs_.get(), "container2/newdir1", FileType::NotFound);
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container2/newdir1"));

  // C/F
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container/somefile"));
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File);

  // C/D/D

  ASSERT_OK(fs_->DeleteDirContents("container/somedir/subdir"));
  select.base_dir = "container/somedir/subdir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container3/somedir"));
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

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

#include "arrow/filesystem/azurefs.h"

#include <azure/storage/files/datalake.hpp>
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
  std::shared_ptr<FileSystem> fs_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> gen2Client_;
  AzureOptions options_;

  void MakeFileSystem() { 
    const std::string& account_name = "devstoreaccount1";
    const std::string& account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    options_.account_blob_url = "http://127.0.0.1:10000/devstoreaccount1/";
    options_.account_dfs_url = "http://127.0.0.1:10000/devstoreaccount1/";
    options_.isTestEnabled = true;
    options_.storage_credentials_provider = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(account_name, account_key);
    options_.credentials_kind = AzureCredentialsKind::StorageCredentials;
    gen2Client_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakeServiceClient>(options_.account_dfs_url, options_.storage_credentials_provider);
    auto result = AzureBlobFileSystem::Make(options_);
    fs_ = *result;
  }

  void SetUp() override {
    MakeFileSystem();
    auto fileSystemClient = gen2Client_->GetFileSystemClient("container");
    fileSystemClient.CreateIfNotExists();
    fileSystemClient = gen2Client_->GetFileSystemClient("empty-container");
    fileSystemClient.CreateIfNotExists();
    auto fileClient = std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(options_.account_blob_url + "container/somefile", options_.storage_credentials_provider);
    std::string s = "some data";
    fileClient->UploadFrom(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&s[0])), s.size());
  }
  void TearDown() override {
    auto containers = gen2Client_->ListFileSystems();
    for(auto c:containers.FileSystems) {
      auto fileSystemClient = gen2Client_->GetFileSystemClient(c.Name);
      fileSystemClient.DeleteIfExists();
    }
  }
  void AssertObjectContents(Azure::Storage::Files::DataLake::DataLakeServiceClient* client, const std::string& container,
                          const std::string& path_to_file, const std::string& expected) {
    auto pathClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakePathClient>(client->GetUrl() + container + "/" + path_to_file, options_.storage_credentials_provider);
    auto size = pathClient_->GetProperties().Value.FileSize;
    if (size == 0) {
      return;
    }
    auto buf = AllocateResizableBuffer(size, fs_->io_context().pool());
    Azure::Storage::Blobs::DownloadBlobToOptions downloadOptions;
    Azure::Core::Http::HttpRange range;
    range.Offset = 0;
    range.Length = size;
    downloadOptions.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    auto fileClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(client->GetUrl() + container + "/" + path_to_file, options_.storage_credentials_provider);
    auto result = fileClient_->DownloadTo(reinterpret_cast<uint8_t*>(buf->get()->mutable_data()), size, downloadOptions).Value;
    buf->get()->Equals(Buffer(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&expected[0])), expected.size()));
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

  // Existing "file", should fail
  ASSERT_RAISES(IOError, fs_->CreateDir("container/somefile"));

  // directory, false
  ASSERT_RAISES(IOError, fs_->CreateDir("container/newdir/newsub/newsubsub", false));

  // directory, true
  ASSERT_RAISES(IOError, fs_->CreateDir("container/newdir/newsub/newsubsub", true));
}

TEST_F(TestAzureFileSystem, DeleteDir) {
  // Container
  ASSERT_OK(fs_->DeleteDir("container"));
  AssertFileInfo(fs_.get(), "container", FileType::NotFound); 

  // Nonexistent Container 
  ASSERT_OK(fs_->DeleteDir("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound); 

  // root 
  ASSERT_RAISES(NotImplemented, fs_->DeleteDir(""));

  // C/F
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somefile"));

  // C/NF
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somefile19"));
  
  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container3/somedir"));
}

TEST_F(TestAzureFileSystem, DeleteFile) {
  // Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container"));

  // Nonexistent Container 
  ASSERT_RAISES(IOError, fs_->DeleteFile("container5"));

  // root 
  ASSERT_RAISES(IOError, fs_->DeleteFile(""));
  
  // C/F
  ASSERT_OK(fs_->DeleteFile("container/somefile"));
  
  // C/NF
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somefile"));

  // C/D/D
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somedir/subdir"));

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container3/somedir"));
}

TEST_F(TestAzureFileSystem, GetFileInfo) {
  //Containers
  AssertFileInfo(fs_.get(), "container", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-container", FileType::NotFound);

  AssertFileInfo(fs_.get(), "", FileType::Directory); 

  auto res = fs_->OpenOutputStream("container/base.txt");
  ASSERT_OK(res->get()->Write("Base data"));

  // "Files"
  AssertFileInfo(fs_.get(), "container/base.txt", FileType::File);
  AssertFileInfo(fs_.get(), "container/base1.txt", FileType::NotFound);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/non-existentdir/subdir", FileType::NotFound);
  AssertFileInfo(fs_.get(), "nonexistent-container/somedir/subdir/subfile", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoSelector) { 
  FileSelector select;
  std::vector<FileInfo> infos;

  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);

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
  ASSERT_EQ(infos.size(), 2);

  // C/F
  select.base_dir = "container/somefile";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));

  // C/ND/D
  select.base_dir = "container/non-existentdir/non-existentsubdir";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // NC/D
  select.base_dir = "nonexistent-container/non-existentdir";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;
}

TEST_F(TestAzureFileSystem, Move) { 
  ASSERT_RAISES(IOError, fs_->Move("container", "container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->Move("container/somedir/subdir", "container/newdir/newsub"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/base.txt"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/non-existentdir/non-existentsubdir"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "nonexistent-container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir23", "container/base.txt"));
  auto res = fs_->OpenOutputStream("container/somefile");
  ASSERT_OK(res->get()->Write("Changed the data"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "container/somefile"));
  ASSERT_RAISES(IOError, fs_->Move("container/somefile", "container/base.txt"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "container/non-existentdir/non-existentsubdir"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "nonexistent-container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->Move("container/base2.txt", "container/non-existentdir"));
}

TEST_F(TestAzureFileSystem, CopyFile) {
  ASSERT_RAISES(IOError, fs_->CopyFile("container", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir22/subdir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("nonexistent-container/somedir/subdir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container3435"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/non-existentdir/non-existentsubdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "nonexistent-container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base2t.txt", "nonexistent-container/non-existentdir"));

  ASSERT_OK(fs_->CopyFile("container/somefile", "container/base.txt"));
  ASSERT_OK(fs_->CopyFile("container/base.txt", "container/somefile3"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container/somedir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container/non-existentdir/non-existentsubdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "nonexistent-container/non-existentdir"));
}

TEST_F(TestAzureFileSystem, OpenInputStream) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buf;

  ASSERT_RAISES(IOError, fs_->OpenInputStream("container"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream(""));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/non-existentdir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("nonexistent-container/somedir"));

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
}

TEST_F(TestAzureFileSystem, OpenInputFile) {
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;

  ASSERT_RAISES(IOError, fs_->OpenInputFile("container"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile(""));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/non-existentdir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("nonexistent-container/somedir"));

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
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream(""));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/non-existentdir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("nonexistent-container/somedir"));

  // Create new empty file
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2Client_.get(), "container", "newfile1", "");

  // Create new file with 1 small write
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile2"));
  ASSERT_OK(stream->Write("some data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2Client_.get(), "container", "newfile2", "some data");

  // Create new file with 3 small writes
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile3"));
  ASSERT_OK(stream->Write("some "));
  ASSERT_OK(stream->Write(""));
  ASSERT_OK(stream->Write("new data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2Client_.get(), "container", "newfile3", "some new data");

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
  AssertObjectContents(gen2Client_.get(), "container", "newfile4", expected);

  // Overwrite
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Write("overwritten data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2Client_.get(), "container", "newfile1", "overwritten data");

  // Overwrite and make empty
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2Client_.get(), "container", "newfile1", "");
}

TEST_F(TestAzureFileSystem, DeleteDirContents) {
  // Container
  ASSERT_OK(fs_->DeleteDirContents("container"));
  AssertFileInfo(fs_.get(), "container", FileType::Directory); 

  // Nonexistent Container 
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound); 

  // root 
  ASSERT_RAISES(IOError, fs_->DeleteDirContents(""));
  
  // C/F
  auto res = fs_->OpenOutputStream("container/somefile");
  ASSERT_OK(res->get()->Write("some data"));
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container/somefile"));
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File);

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container3/somedir"));
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
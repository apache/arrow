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

#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
#include <azure/storage/files/datalake.hpp>
#include <boost/process.hpp>
#include <chrono>
#include <thread>

#include "arrow/filesystem/test_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::Uri;

namespace fs {
namespace internal {

namespace bp = boost::process;

using ::arrow::internal::TemporaryDir;
using ::testing::IsEmpty;
using ::testing::NotNull;

class AzuriteEnv : public ::testing::Environment {
 public:
  AzuriteEnv() {
    account_name_ = "devstoreaccount1";
    account_key_ =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/"
        "KBHBeksoGMGw==";
    auto exe_path = bp::search_path("azurite");
    if (exe_path.empty()) {
      auto error = std::string("Could not find Azurite emulator.");
      status_ = Status::Invalid(error);
      return;
    }
    auto temp_dir_ = TemporaryDir::Make("azurefs-test-").ValueOrDie();
    server_process_ = bp::child(boost::this_process::environment(), exe_path, "--silent",
                                "--location", temp_dir_->path().ToString(), "--debug",
                                temp_dir_->path().ToString() + "/debug.log");
    if (!(server_process_.valid() && server_process_.running())) {
      auto error = "Could not start Azurite emulator.";
      server_process_.terminate();
      server_process_.wait();
      status_ = Status::Invalid(error);
      return;
    }
    status_ = Status::OK();
  }

  ~AzuriteEnv() override {
    server_process_.terminate();
    server_process_.wait();
  }

  const std::string& account_name() const { return account_name_; }
  const std::string& account_key() const { return account_key_; }
  const Status status() const { return status_; }

 private:
  std::string account_name_;
  std::string account_key_;
  bp::child server_process_;
  Status status_;
  std::unique_ptr<TemporaryDir> temp_dir_;
};

auto* azurite_env = ::testing::AddGlobalTestEnvironment(new AzuriteEnv);

AzuriteEnv* GetAzuriteEnv() {
  return ::arrow::internal::checked_cast<AzuriteEnv*>(azurite_env);
}

class TestAzureFileSystem : public ::testing::Test {
 public:
  std::shared_ptr<FileSystem> fs_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> gen2_client_;
  AzureOptions options_;

  void MakeFileSystem() {
    const std::string& account_name = GetAzuriteEnv()->account_name();
    const std::string& account_key = GetAzuriteEnv()->account_key();
    options_.is_azurite = true;
    options_.ConfigureAccountKeyCredentials(account_name, account_key);
    gen2_client_ =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakeServiceClient>(
            options_.account_dfs_url, options_.storage_credentials_provider);
    ASSERT_OK_AND_ASSIGN(fs_, AzureBlobFileSystem::Make(options_));
  }

  void SetUp() override {
    ASSERT_THAT(GetAzuriteEnv(), NotNull());
    ASSERT_OK(GetAzuriteEnv()->status());

    MakeFileSystem();
    auto file_system_client = gen2_client_->GetFileSystemClient("container");
    file_system_client.CreateIfNotExists();
    file_system_client = gen2_client_->GetFileSystemClient("empty-container");
    file_system_client.CreateIfNotExists();
    auto file_client =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(
            options_.account_blob_url + "container/somefile",
            options_.storage_credentials_provider);
    std::string s = "some data";
    file_client->UploadFrom(
        const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(s.data())), s.size());
  }

  void TearDown() override {
    auto containers = gen2_client_->ListFileSystems();
    for (auto container : containers.FileSystems) {
      auto file_system_client = gen2_client_->GetFileSystemClient(container.Name);
      file_system_client.DeleteIfExists();
    }
  }

  void AssertObjectContents(
      Azure::Storage::Files::DataLake::DataLakeServiceClient* client,
      const std::string& container, const std::string& path_to_file,
      const std::string& expected) {
    auto path_client =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakePathClient>(
            client->GetUrl() + container + "/" + path_to_file,
            options_.storage_credentials_provider);
    auto size = path_client->GetProperties().Value.FileSize;
    if (size == 0) {
      ASSERT_EQ(expected, "");
      return;
    }
    auto buf = AllocateBuffer(size, fs_->io_context().pool());
    Azure::Storage::Blobs::DownloadBlobToOptions download_options;
    Azure::Core::Http::HttpRange range;
    range.Offset = 0;
    range.Length = size;
    download_options.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    auto file_client =
        std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(
            client->GetUrl() + container + "/" + path_to_file,
            options_.storage_credentials_provider);
    auto result = file_client
                      ->DownloadTo(reinterpret_cast<uint8_t*>(buf->get()->mutable_data()),
                                   size, download_options)
                      .Value;
    auto buf_data = std::move(buf->get());
    auto expected_data = std::make_shared<Buffer>(
        reinterpret_cast<const uint8_t*>(expected.data()), expected.size());
    AssertBufferEqual(*buf_data, *expected_data);
  }
};

TEST_F(TestAzureFileSystem, FromUri) {
  Uri uri;

  // Public container
  ASSERT_OK(uri.Parse("https://testcontainer.dfs.core.windows.net/"));
  ASSERT_OK_AND_ASSIGN(auto options, AzureOptions::FromUri(uri));
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Anonymous);
  ASSERT_EQ(options.account_dfs_url, "https://testcontainer.dfs.core.windows.net/");

  // Sas Token
  ASSERT_OK(uri.Parse(
      "https://testblobcontainer.blob.core.windows.net/?dummy_sas_key=dummy_value"));
  ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromUri(uri));
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
  ASSERT_EQ(options.account_dfs_url, "https://testblobcontainer.dfs.core.windows.net/");
  ASSERT_EQ(options.account_blob_url, "https://testblobcontainer.blob.core.windows.net/");
  ASSERT_EQ(options.sas_token, "?dummy_sas_key=dummy_value");
}

TEST_F(TestAzureFileSystem, FromAccountKey) {
  auto options = AzureOptions::FromAccountKey(GetAzuriteEnv()->account_name(),
                                              GetAzuriteEnv()->account_key())
                     .ValueOrDie();
  ASSERT_EQ(options.credentials_kind,
            arrow::fs::AzureCredentialsKind::StorageCredentials);
  ASSERT_NE(options.storage_credentials_provider, nullptr);
}

TEST_F(TestAzureFileSystem, FromConnectionString) {
  auto options =
      AzureOptions::FromConnectionString(
          "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey="
          "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/"
          "KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
          .ValueOrDie();
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::ConnectionString);
  ASSERT_NE(options.connection_string, "");
}

TEST_F(TestAzureFileSystem, FromServicePrincipleCredential) {
  auto options = AzureOptions::FromServicePrincipleCredential(
                     "dummy_account_name", "dummy_tenant_id", "dummy_client_id",
                     "dummy_client_secret")
                     .ValueOrDie();
  ASSERT_EQ(options.credentials_kind,
            arrow::fs::AzureCredentialsKind::ServicePrincipleCredentials);
  ASSERT_NE(options.service_principle_credentials_provider, nullptr);
}

TEST_F(TestAzureFileSystem, FromSas) {
  auto options =
      AzureOptions::FromSas(
          "https://testcontainer.blob.core.windows.net/?dummy_sas_key=dummy_value")
          .ValueOrDie();
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
  ASSERT_EQ(options.sas_token, "?dummy_sas_key=dummy_value");
}

TEST_F(TestAzureFileSystem, CreateDir) {
  // New container
  AssertFileInfo(fs_.get(), "new-container", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("new-container"));
  AssertFileInfo(fs_.get(), "new-container", FileType::Directory);

  // Existing container
  ASSERT_OK(fs_->CreateDir("container"));
  AssertFileInfo(fs_.get(), "container", FileType::Directory);

  ASSERT_RAISES(IOError, fs_->CreateDir(""));

  // Existing "file", should fail
  ASSERT_RAISES(IOError, fs_->CreateDir("container/somefile"));

  // recursive, false
  ASSERT_RAISES(IOError, fs_->CreateDir("container/newdir/newsub/newsubsub", false));

  // recursive, true
  ASSERT_RAISES(IOError, fs_->CreateDir("container/newdir/newsub/newsubsub", true));
}

TEST_F(TestAzureFileSystem, DeleteDir) {
  // Container
  ASSERT_OK(fs_->DeleteDir("container"));
  AssertFileInfo(fs_.get(), "container", FileType::NotFound);

  // Nonexistent-Container
  ASSERT_OK(fs_->DeleteDir("nonexistent-container"));
  AssertFileInfo(fs_.get(), "nonexistent-container", FileType::NotFound);

  // root
  ASSERT_RAISES(NotImplemented, fs_->DeleteDir(""));

  // Container/File
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somefile"));

  // Container/Nonexistent-File
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/nonexistent-file"));
}

TEST_F(TestAzureFileSystem, DeleteFile) {
  // Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container"));

  // Nonexistent-Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("nonexistent-container"));

  // root
  ASSERT_RAISES(IOError, fs_->DeleteFile(""));

  // Container/File
  ASSERT_OK(fs_->DeleteFile("container/somefile"));

  // Container/Nonexistent-File
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somefile"));

  // Container/Directory/Directory
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somedir/subdir"));
}

TEST_F(TestAzureFileSystem, GetFileInfo) {
  // Containers
  AssertFileInfo(fs_.get(), "container", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-container", FileType::NotFound);

  AssertFileInfo(fs_.get(), "", FileType::Directory);

  // "Files"
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File);
  AssertFileInfo(fs_.get(), "container/nonexistent-file.txt", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoSelector) {
  FileSelector select;

  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(auto infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);

  // Nonexistent-Container
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

  // Container/File
  select.base_dir = "container/somefile";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
}

TEST_F(TestAzureFileSystem, Move) {
  ASSERT_RAISES(IOError, fs_->Move("container", "container/non-existentdir"));
  ASSERT_RAISES(IOError,
                fs_->Move("container/somedir/subdir", "container/newdir/newsub"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/base.txt"));
  ASSERT_RAISES(IOError,
                fs_->Move("container/nonexistent-directory", "container/base.txt"));
  ASSERT_OK_AND_ASSIGN(auto res, fs_->OpenOutputStream("container/somefile"));
  ASSERT_OK(res->Write("Changed the data"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "container/somefile"));
  ASSERT_RAISES(IOError, fs_->Move("container/somefile", "container/base.txt"));
  ASSERT_RAISES(IOError,
                fs_->Move("container/nonexistent-file.txt", "container/non-existentdir"));
}

TEST_F(TestAzureFileSystem, CopyFile) {
  ASSERT_RAISES(IOError, fs_->CopyFile("container", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/somedir/subdir"));

  ASSERT_OK(fs_->CopyFile("container/somefile", "container/base.txt"));
  ASSERT_OK(fs_->CopyFile("container/base.txt", "container/nonexistent-file"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                       "nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", ""));
  ASSERT_RAISES(IOError,
                fs_->CopyFile("container/somedir/subdir/subfile", "container/somedir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                       "container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                       "container/somedir/subdir"));
}

TEST_F(TestAzureFileSystem, OpenInputStream) {
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream(""));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/somedir/subdir"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(auto stream, fs_->OpenInputStream("container/somefile"));
  ASSERT_OK_AND_ASSIGN(auto buf, stream->Read(2));
  AssertBufferEqual(*buf, "so");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "ta");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "");
}

TEST_F(TestAzureFileSystem, OpenInputFile) {
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile(""));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/non-existentdir"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/somedir/subdir"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(auto file, fs_->OpenInputFile("container/somefile"));
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_ASSIGN(auto buf, file->Read(4));
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

  char result[20];
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
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream(""));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/somedir/subdir"));

  // Create new empty file
  ASSERT_OK_AND_ASSIGN(auto stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile1", "");

  // Create new file with 1 small write
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile2"));
  ASSERT_OK(stream->Write("some data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile2", "some data");

  // Create new file with 3 small writes
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile3"));
  ASSERT_OK(stream->Write("some "));
  ASSERT_OK(stream->Write(""));
  ASSERT_OK(stream->Write("new data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile3", "some new data");

  // Overwrite
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Write("overwritten data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile1", "overwritten data");

  // Overwrite and make empty
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile1", "");
}

TEST_F(TestAzureFileSystem, OpenAppendStream) {
  ASSERT_RAISES(IOError, fs_->OpenAppendStream("container"));
  ASSERT_RAISES(IOError, fs_->OpenAppendStream("nonexistent-container"));
  ASSERT_RAISES(IOError, fs_->OpenAppendStream(""));
  ASSERT_RAISES(IOError, fs_->OpenAppendStream("container/somedir/subdir"));

  // Create new empty file
  ASSERT_OK_AND_ASSIGN(auto stream, fs_->OpenAppendStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile1", "");

  // Create new file with 1 small write
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenAppendStream("container/newfile2"));
  ASSERT_OK(stream->Write("some data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile2", "some data");

  // Create new file with 3 small writes
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenAppendStream("container/newfile3"));
  ASSERT_OK(stream->Write("some "));
  ASSERT_OK(stream->Write(""));
  ASSERT_OK(stream->Write("new data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile3", "some new data");

  // Append to empty file
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenAppendStream("container/newfile1"));
  ASSERT_OK(stream->Write("append data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile1", "append data");

  // Append to non-empty file
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenAppendStream("container/newfile1"));
  ASSERT_OK(stream->Write(", more data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(gen2_client_.get(), "container", "newfile1",
                       "append data, more data");
}

TEST_F(TestAzureFileSystem, DeleteDirContents) {
  // Container
  ASSERT_OK(fs_->DeleteDirContents("container"));
  AssertFileInfo(fs_.get(), "container", FileType::Directory);

  // Nonexistent-Container
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("nonexistent-container"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound);

  // root
  ASSERT_RAISES(IOError, fs_->DeleteDirContents(""));

  // Container/File
  ASSERT_OK_AND_ASSIGN(auto res, fs_->OpenOutputStream("container/somefile"));
  ASSERT_OK(res->Write("some data"));
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container/somefile"));
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File);
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

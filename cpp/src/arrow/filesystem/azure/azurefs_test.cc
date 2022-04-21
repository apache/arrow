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

#include "arrow/filesystem/azure/azurefs.h"

#include <chrono>
#include <thread>

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>
#include <azure/storage/files/datalake.hpp>

#include "arrow/util/uri.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/util.h"

namespace arrow {

using internal::Uri;

namespace fs {

class AzureEnvTestMixin{
 public:
  static AzureOptions options_;
  static std::shared_ptr<AzureBlobFileSystem> fs_;
  static std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>
                                                                   gen2Client_;
  static std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> gen1Client_;

  AzureEnvTestMixin() {}

  const std::string& GetAdlsGen2AccountName() {
    static const std::string accountName = [&]() -> std::string {
      return std::getenv("ADLS_GEN2_ACCOUNT_NAME");
    }();
    return accountName;
  }

  const std::string& GetAdlsGen2AccountKey() {
    static const std::string accountKey = [&]() -> std::string {
      return std::getenv("ADLS_GEN2_ACCOUNT_KEY");
    }();
    return accountKey;
  }

  const std::string& GetAdlsGen2ConnectionString() {
    static const std::string connectionString = [&]() -> std::string {
      return std::getenv("ADLS_GEN2_CONNECTION_STRING");
    }();
    return connectionString;
  }

  const std::string& GetAdlsGen2SasUrl() {
    static const std::string sasUrl = [&]() -> std::string {
      return std::getenv("ADLS_GEN2_SASURL");
    }();
    return sasUrl;
  }

  const std::string& GetAadTenantId() {
    static const std::string tenantId = [&]() -> std::string {
      return std::getenv("AAD_TENANT_ID");
    }();
    return tenantId;
  }

  const std::string& GetAadClientId() {
    static const std::string clientId = [&]() -> std::string {
      return std::getenv("AAD_CLIENT_ID");
    }();
    return clientId;
  }

  const std::string& GetAadClientSecret() {
    static const std::string clientSecret = [&]() -> std::string {
      return std::getenv("AAD_CLIENT_SECRET");
    }();
    return clientSecret;
  }

//  private:
//   const std::string& AdlsGen2AccountName = std::getenv("ADLS_GEN2_ACCOUNT_NAME");
//   const std::string& AdlsGen2AccountKey = std::getenv("ADLS_GEN2_ACCOUNT_KEY");
//   const std::string& AdlsGen2ConnectionStringValue = std::getenv(
//                                                    "ADLS_GEN2_CONNECTION_STRING");
//   const std::string& AdlsGen2SasUrl = std::getenv("ADLS_GEN2_SASURL");
//   const std::string& AadTenantIdValue = std::getenv("AAD_TENANT_ID");
//   const std::string& AadClientIdValue = std::getenv("AAD_CLIENT_ID");
//   const std::string& AadClientSecretValue = std::getenv("AAD_CLIENT_SECRET");
};

AzureOptions AzureEnvTestMixin::options_;
std::shared_ptr<AzureBlobFileSystem> AzureEnvTestMixin::fs_;
std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>
                                                         AzureEnvTestMixin::gen2Client_;
std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> AzureEnvTestMixin::gen1Client_;


class SetupEnvironment : public ::testing::Environment, public AzureEnvTestMixin {
 public:
  bool isHeirarchialNamespaceEnabled() {
    return AzureEnvTestMixin::gen1Client_->GetAccountInfo().Value
                                                      .IsHierarchicalNamespaceEnabled;
  }

  void MakeFileSystem() {
    const std::string& account_key = GetAdlsGen2AccountKey();
    const std::string& account_name = GetAdlsGen2AccountName();
    AzureEnvTestMixin::options_.ConfigureAccountKeyCredentials(account_name, account_key);
    auto url = options_.account_dfs_url;
    AzureEnvTestMixin::gen2Client_ = std::make_shared<Azure::Storage::Files::
                                  DataLake::DataLakeServiceClient>(
                                  url, options_.storage_credentials_provider);
    AzureEnvTestMixin::gen1Client_ = std::make_shared<Azure::Storage::Blobs::
                                BlobServiceClient>(options_.account_blob_url,
                                 options_.storage_credentials_provider);
    auto result = AzureBlobFileSystem::Make(options_);
    if (!result.ok()) {
      ARROW_LOG(INFO)
          << "AzureFileSystem::Make failed, err msg is "
          << result.status().ToString();
      return;
    }
    AzureEnvTestMixin::fs_ = *result;
  }

  void SetUp() override {
    {
      auto fileSystemClient = AzureEnvTestMixin::gen2Client_->GetFileSystemClient(
                                                                            "container");
      fileSystemClient.CreateIfNotExists();
      fileSystemClient = AzureEnvTestMixin::gen2Client_->GetFileSystemClient(
                                                                      "empty-container");
      fileSystemClient.CreateIfNotExists();
    }
    {
      if (isHeirarchialNamespaceEnabled()) {
        auto directoryClient = AzureEnvTestMixin::gen2Client_->GetFileSystemClient(
                                              "container").GetDirectoryClient("emptydir");
        directoryClient.CreateIfNotExists();
        directoryClient = AzureEnvTestMixin::gen2Client_->GetFileSystemClient("container")
                                                          .GetDirectoryClient("somedir");
        directoryClient.CreateIfNotExists();
        directoryClient = directoryClient.GetSubdirectoryClient("subdir");
        directoryClient.CreateIfNotExists();
        auto fileClient = directoryClient.GetFileClient("subfile");
        fileClient.CreateIfNotExists();
        std::string s = "sub data";
        fileClient.UploadFrom(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
                                                                      &s[0])), s.size());
        fileClient = gen2Client_->GetFileSystemClient("container")
                                                              .GetFileClient("somefile");
        fileClient.CreateIfNotExists();
        s = "some data";
        fileClient.UploadFrom(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
                                                                      &s[0])), s.size());
      } else {
        auto fc = std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(
                                        options_.account_blob_url + "container/somefile",
                                         options_.storage_credentials_provider);
        std::string s = "some data";
        fc->UploadFrom(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&s[0])),
                                                                             s.size());
      }
    }
  }

  void TearDown() override {
    auto containers = AzureEnvTestMixin::gen2Client_->ListFileSystems();
    for (auto c : containers.FileSystems) {
      auto fileSystemClient = AzureEnvTestMixin::gen2Client_->GetFileSystemClient(c.Name);
      fileSystemClient.DeleteIfExists();
    }
  }
};

class TestAzureFileSystem : public ::testing::Test, public AzureEnvTestMixin {
 public:
  void AssertObjectContents(Azure::Storage::Files::DataLake::DataLakeServiceClient*
                             client, const std::string& container, const std::string&
                              path_to_file, const std::string& expected) {
    auto pathClient_ = std::make_shared<Azure::Storage::Files::DataLake::
      DataLakePathClient>(client->GetUrl() + "/"+ container + "/" + path_to_file,
       options_.storage_credentials_provider);
    auto size = pathClient_->GetProperties().Value.FileSize;
    auto buf = AllocateResizableBuffer(size, fs_->io_context().pool());
    Azure::Storage::Blobs::DownloadBlobToOptions downloadOptions;
    Azure::Core::Http::HttpRange range;
    range.Offset = 0;
    range.Length = size;
    downloadOptions.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    auto fileClient_ = std::make_shared<Azure::Storage::Files::DataLake::
      DataLakeFileClient>(client->GetUrl() + "/"+ container + "/" + path_to_file,
       options_.storage_credentials_provider);
    auto result = fileClient_->DownloadTo(reinterpret_cast<uint8_t*>(
                              buf->get()->mutable_data()), size, downloadOptions).Value;
    buf->get()->Equals(Buffer(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
                                                      &expected[0])), expected.size()));
  }
};

TEST(TestAzureFSOptions, FromUri) {
  AzureOptions options;
  Uri uri;

  //Public container
  ASSERT_OK(uri.Parse("https://testcontainer.dfs.core.windows.net/"));
  ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromUri(uri));
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Anonymous);
  ASSERT_EQ(options.account_dfs_url, "https://testcontainer.dfs.core.windows.net/");

  //Sas Token
  ASSERT_OK(uri.Parse("https://testcontainer.blob.core.windows.net/?dummy_sas_token"));
  ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromUri(uri));
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
  ASSERT_EQ(options.account_dfs_url, "https://testcontainer.dfs.core.windows.net/");
  ASSERT_EQ(options.sas_token, "?dummy_sas_token");
}

TEST_F(TestAzureFileSystem, FromAccountKey) {
  AzureOptions options;
  options = AzureOptions::FromAccountKey(this->GetAdlsGen2AccountKey(),
                                                 this->GetAdlsGen2AccountName());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::
                                                            StorageCredentials);
  ASSERT_NE(options.storage_credentials_provider, nullptr);
}

TEST_F(TestAzureFileSystem, FromConnectionString) {
  AzureOptions options;
  options = AzureOptions::FromConnectionString(this->GetAdlsGen2ConnectionString());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::ConnectionString);
  ASSERT_NE(options.connection_string, "");
}

TEST_F(TestAzureFileSystem, FromServicePrincipleCredential) {
  AzureOptions options;
  options = AzureOptions::FromServicePrincipleCredential(this->GetAdlsGen2AccountName(),
             this->GetAadTenantId(), this->GetAadClientId(), this->GetAadClientSecret());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind
                                            ::ServicePrincipleCredentials);
  ASSERT_NE(options.service_principle_credentials_provider, nullptr);
}

TEST_F(TestAzureFileSystem, FromSas) {
  AzureOptions options;
  options = AzureOptions::FromSas(this->GetAdlsGen2SasUrl());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
  ASSERT_NE(options.sas_token, "");
}

TEST_F(TestAzureFileSystem, CreateDirBlobStorage) {
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

TEST_F(TestAzureFileSystem, DeleteDirBlobStorage) {
  FileSelector select;
  select.base_dir = "container4";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_OK(fs_->DeleteDir("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound);

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

TEST_F(TestAzureFileSystem, DeleteFileBlobStorage) {
  FileSelector select;
  select.base_dir = "container4";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container4"));

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

TEST_F(TestAzureFileSystem, GetFileInfoBlobStorage) {
  //Containers
  AssertFileInfo(fs_.get(), "container", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-container", FileType::NotFound);

  AssertFileInfo(fs_.get(), "", FileType::Directory);

  auto res = fs_->OpenOutputStream("container/base.txt");
  res->get()->Write("Changed the data");

  // "Files"
  AssertFileInfo(fs_.get(), "container/base.txt", FileType::File);
  AssertFileInfo(fs_.get(), "container/base1.txt", FileType::NotFound);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/somedir45/subdir", FileType::NotFound);
  AssertFileInfo(fs_.get(), "containe23r/somedir/subdir/subfile", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoSelectorBlobStorage) {
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
  select.base_dir = "container/base.txt";
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

TEST_F(TestAzureFileSystem, MoveBlobStorage) {
  ASSERT_RAISES(IOError, fs_->Move("container", "container/nshhd"));
  ASSERT_RAISES(IOError, fs_->Move("container/somedir/subdir",
                                                     "container/newdir/newsub"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/base.txt"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/ahsh/gssjd"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "containerqw/ghdj"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir23", "container/base.txt"));
  auto res = fs_->OpenOutputStream("container/somefile");
  res->get()->Write("Changed the data");
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "container/somefile"));
  ASSERT_RAISES(IOError, fs_->Move("container/somefile", "container/base.txt"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "container/ahsh/gssjd"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "containerqw/ghdj"));
  ASSERT_RAISES(IOError, fs_->Move("container/base2.txt", "container/gshh"));
}

TEST_F(TestAzureFileSystem, CopyFileBlobStorage) {
  ASSERT_RAISES(IOError, fs_->CopyFile("container", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir22/subdir",
                                                               "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container23/somedir/subdir",
                                                               "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container3435"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/ahsj/ggws"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container27/hshj"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base2t.txt", "container27/hshj"));

  auto res = fs_->OpenOutputStream("container/somefile");
  res->get()->Write("Changed the data");
  ASSERT_OK(fs_->CopyFile("container/base.txt", "container/somefile"));
  ASSERT_OK(fs_->CopyFile("container/base.txt", "container/somefile3"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                     "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                       "container3435"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                   "container/somedir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                     "container/sjdj"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                           "container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                 "container/ahsj/ggws"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                   "container27/hshj"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                   "container27/hshj"));
}

TEST_F(TestAzureFileSystem, OpenInputStreamBlobStorage) {
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

  auto res = fs_->OpenOutputStream("container/somefile");
  res->get()->Write("some data");

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

TEST_F(TestAzureFileSystem, OpenInputFileBlobStorage) {
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

TEST_F(TestAzureFileSystem, OpenOutputStreamBlobStorage) {
  std::shared_ptr<io::OutputStream> stream;

  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container263"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream(""));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container/shjdj/subdir"));
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("container526/somedir"));

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

TEST_F(TestAzureFileSystem, DeleteDirContentsBlobStorage) {
  FileSelector select;
  select.base_dir = "container4/newdir";
  std::vector<FileInfo> infos;

  // Container
  fs_->CreateDir("container4");
  ASSERT_OK(fs_->DeleteDirContents("container4"));
  AssertFileInfo(fs_.get(), "container4", FileType::Directory);

  // Nonexistent Container
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound);

  // root
  ASSERT_RAISES(IOError, fs_->DeleteDirContents(""));

  // C/F
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container/somefile"));
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File);

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container3/somedir"));
}

TEST_F(TestAzureFileSystem, CreateDirAdlsGen2) {
  // New container
  auto op = fs_->options();
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

  //C/D/D
  AssertFileInfo(fs_.get(), "container/somedir/subdir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("container/somedir/subdir"));
  AssertFileInfo(fs_.get(), "container/somedir/subdir", FileType::Directory);

  auto res = fs_->OpenOutputStream("container/somedir/base.txt");
  res->get()->Write("Changed the data");

  //C/D/F
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

TEST_F(TestAzureFileSystem, DeleteDirAdlsGen2) {
  FileSelector select;
  select.base_dir = "container4";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_OK(fs_->DeleteDir("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound);

  // Nonexistent Container
  ASSERT_OK(fs_->DeleteDir("container3"));
  AssertFileInfo(fs_.get(), "container3", FileType::NotFound); 

  // root
  ASSERT_RAISES(NotImplemented, fs_->DeleteDir(""));

  // C/D
  ASSERT_OK(fs_->DeleteDir("container4/newdir"));
  // ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  // ASSERT_EQ(infos.size(), 0);

  // C/ND
  AssertFileInfo(fs_.get(), "container4/newdir", FileType::NotFound);
  ASSERT_RAISES(IOError, fs_->DeleteDir("container4/newdir"));
  // ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  // ASSERT_EQ(infos.size(), 0);

  // C/F
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somefile"));

  // C/D/D
  ASSERT_OK(fs_->DeleteDir("container/newdir/newsub"));

  // C/D/F
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somedir/base.txt"));

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container3/somedir"));
}

TEST_F(TestAzureFileSystem, DeleteFileAdlsGen2) {
  FileSelector select;
  select.base_dir = "container4";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container4"));

  // Nonexistent Container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container5"));

  // root
  ASSERT_RAISES(IOError, fs_->DeleteFile(""));

  // C/D
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/emptyDir"));

  // C/ND
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/emptyDir1"));

  // C/F
  ASSERT_OK(fs_->DeleteFile("container/somefile"));

  // C/NF
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somefile"));

  // C/D/D
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somedir/subdir"));

  auto res = fs_->OpenOutputStream("container/somedir/base.txt");
  res->get()->Write("Changed the data");

  // C/D/F
  AssertFileInfo(fs_.get(), "container/somedir/base.txt", FileType::File);
  ASSERT_OK(fs_->DeleteFile("container/somedir/base.txt"));

  // C/ND/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container/somedir3/base"));

  // NC/D
  ASSERT_RAISES(IOError, fs_->DeleteDir("container3/somedir"));
}

TEST_F(TestAzureFileSystem, GetFileInfoAdlsGen2) {
  //Containers
  AssertFileInfo(fs_.get(), "container", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-container", FileType::NotFound);

  AssertFileInfo(fs_.get(), "", FileType::Directory);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/emptydir", FileType::Directory);
  AssertFileInfo(fs_.get(), "container/emptydir1", FileType::NotFound);

  auto res = fs_->OpenOutputStream("container/base.txt");
  res->get()->Write("Changed the data");

  // "Files"
  AssertFileInfo(fs_.get(), "container/base.txt", FileType::File);
  AssertFileInfo(fs_.get(), "container/base1.txt", FileType::NotFound);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/somedir/subdir", FileType::Directory);
  AssertFileInfo(fs_.get(), "container/somedir/subdir/subfile", FileType::File);

  // "Directories"
  AssertFileInfo(fs_.get(), "container/somedir45/subdir", FileType::NotFound);
  AssertFileInfo(fs_.get(), "containe23r/somedir/subdir/subfile", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoSelectorAdlsGen2) {
  FileSelector select;
  std::vector<FileInfo> infos;

  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 5);

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
  select.base_dir = "container/base.txt";
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

TEST_F(TestAzureFileSystem, MoveAdlsGen2) {
  ASSERT_RAISES(IOError, fs_->Move("container", "container/nshhd"));
  fs_->CreateDir("container/newdir/newsub/newsubsub", true);
  ASSERT_RAISES(IOError, fs_->Move("container/somedir/subdir",
                                                       "container/newdir/newsub"));
  ASSERT_OK(fs_->Move("container/newdir/newsub", "container/emptydir"));
  ASSERT_OK(fs_->Move("container/emptydir", "container/emptydir1"));
  ASSERT_OK(fs_->Move("container/emptydir1", "container/emptydir"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/base.txt"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir", "container/ahsh/gssjd"));
  ASSERT_OK(fs_->Move("container/emptydir", "containerqw/ghdj"));
  ASSERT_RAISES(IOError, fs_->Move("container/emptydir23", "container/base.txt"));
  auto res = fs_->OpenOutputStream("container/somefile");
  res->get()->Write("Changed the data");
  ASSERT_OK(fs_->Move("container/base.txt", "container/somefile"));
  ASSERT_OK(fs_->Move("container/somefile", "container/base.txt"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "container/ahsh/gssjd"));
  ASSERT_RAISES(IOError, fs_->Move("container/base.txt", "containerqw/ghdj"));
  ASSERT_RAISES(IOError, fs_->Move("container/base2.txt", "container/gshh"));
}

TEST_F(TestAzureFileSystem, CopyFileAdlsGen2) {
  // "File"
  ASSERT_RAISES(IOError, fs_->CopyFile("container", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir", "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir22/subdir",
                                                                   "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container23/somedir/subdir",
                                                                   "container/newfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container3435"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/somedir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container/ahsj/ggws"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base.txt", "container27/hshj"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/base2t.txt", "container27/hshj"));

  auto res = fs_->OpenOutputStream("container/somefile");
  res->get()->Write("Changed the data");
  ASSERT_OK(fs_->CopyFile("container/base.txt", "container/somefile"));
  ASSERT_OK(fs_->CopyFile("container/base.txt", "container/somefile3"));
  ASSERT_OK(fs_->CopyFile("container/base.txt", "container/somedir/subdir/subfile"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", "container"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                   "container3435"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile", ""));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                               "container/somedir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                         "container/somedir/subdir"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                             "container/ahsj/ggws"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                 "container27/hshj"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somedir/subdir/subfile",
                                                                 "container27/hshj"));
  ASSERT_OK(fs_->CopyFile("container/somedir/subdir/subfile", "container/somefile"));
  fs_->DeleteFile("container/somefile3");
  ASSERT_OK(fs_->CopyFile("container/somedir/subdir/subfile", "container/somefile3"));
}

TEST_F(TestAzureFileSystem, OpenInputStreamGen2) {
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

  auto res = fs_->OpenOutputStream("container/somefile");
  res->get()->Write("some data");

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

  res = fs_->OpenOutputStream("container/somedir/subdir/subfile");
  res->get()->Write("sub data");

  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("container/somedir/subdir/subfile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "sub data");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "");
  ASSERT_OK(stream->Close());
}

TEST_F(TestAzureFileSystem, OpenInputFileGen2) {
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

TEST_F(TestAzureFileSystem, OpenOutputStreamGen2) {
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

TEST_F(TestAzureFileSystem, DeleteDirContentsGen2) {
  FileSelector select;
  select.base_dir = "container4/newdir";
  std::vector<FileInfo> infos;

  // Container
  ASSERT_OK(fs_->DeleteDirContents("container4"));
  AssertFileInfo(fs_.get(), "container4", FileType::Directory);

  // Nonexistent Container
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container3"));

  // root
  ASSERT_RAISES(IOError, fs_->DeleteDirContents(""));

  fs_->CreateDir("container4/newdir/subdir", true);

  // C/D
  ASSERT_OK(fs_->DeleteDirContents("container4/newdir"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // C/ND
  AssertFileInfo(fs_.get(), "container4/newdir1", FileType::NotFound);
  ASSERT_RAISES(IOError, fs_->DeleteDirContents("container4/newdir1"));

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

}  // namespace fs
}  // namespace arrow

int main(int argc, char **argv) {
  auto env = new arrow::fs::SetupEnvironment();
  env->MakeFileSystem();
  ::testing::AddGlobalTestEnvironment(env);
  ::testing::InitGoogleTest(&argc, argv);
  if (env->isHeirarchialNamespaceEnabled()) {
    ::testing::GTEST_FLAG(filter) = "*From*:*Gen2";
  } else {
    ::testing::GTEST_FLAG(filter) = "*From*:*BlobStorage";
  }
  return RUN_ALL_TESTS();
}

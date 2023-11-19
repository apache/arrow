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

#include <algorithm>  // Missing include in boost/process

// This boost/asio/io_context.hpp include is needless for no MinGW
// build.
//
// This is for including boost/asio/detail/socket_types.hpp before any
// "#include <windows.h>". boost/asio/detail/socket_types.hpp doesn't
// work if windows.h is already included. boost/process.h ->
// boost/process/args.hpp -> boost/process/detail/basic_cmd.hpp
// includes windows.h. boost/process/args.hpp is included before
// boost/process/async.h that includes
// boost/asio/detail/socket_types.hpp implicitly is included.
#include <boost/asio/io_context.hpp>
// We need BOOST_USE_WINDOWS_H definition with MinGW when we use
// boost/process.hpp. See BOOST_USE_WINDOWS_H=1 in
// cpp/cmake_modules/ThirdpartyToolchain.cmake for details.
#include <boost/process.hpp>

#include "arrow/filesystem/azurefs.h"
#include "arrow/filesystem/azurefs_internal.h"

#include <random>
#include <string>

#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/default_azure_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/files/datalake.hpp>

#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
using internal::TemporaryDir;
namespace fs {
namespace {
namespace bp = boost::process;

using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::NotNull;

auto const* kLoremIpsum = R"""(
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
culpa qui officia deserunt mollit anim id est laborum.
)""";

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
    auto temp_dir_ = *TemporaryDir::Make("azurefs-test-");
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

// Placeholder tests
// TODO: GH-18014 Remove once a proper test is added
TEST(AzureFileSystem, InitializeCredentials) {
  auto default_credential = std::make_shared<Azure::Identity::DefaultAzureCredential>();
  auto managed_identity_credential =
      std::make_shared<Azure::Identity::ManagedIdentityCredential>();
  auto service_principal_credential =
      std::make_shared<Azure::Identity::ClientSecretCredential>("tenant_id", "client_id",
                                                                "client_secret");
}

TEST(AzureFileSystem, OptionsCompare) {
  AzureOptions options;
  EXPECT_TRUE(options.Equals(options));
}

class AzureFileSystemTest : public ::testing::Test {
 public:
  std::shared_ptr<FileSystem> fs_;
  std::unique_ptr<Azure::Storage::Blobs::BlobServiceClient> blob_service_client_;
  std::unique_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>
      datalake_service_client_;
  AzureOptions options_;
  std::mt19937_64 generator_;
  std::string container_name_;
  bool suite_skipped_ = false;

  AzureFileSystemTest() : generator_(std::random_device()()) {}

  virtual Result<AzureOptions> MakeOptions() = 0;

  void SetUp() override {
    auto options = MakeOptions();
    if (options.ok()) {
      options_ = *options;
    } else {
      suite_skipped_ = true;
      GTEST_SKIP() << options.status().message();
    }
    container_name_ = RandomChars(32);
    blob_service_client_ = std::make_unique<Azure::Storage::Blobs::BlobServiceClient>(
        options_.account_blob_url, options_.storage_credentials_provider);
    datalake_service_client_ =
        std::make_unique<Azure::Storage::Files::DataLake::DataLakeServiceClient>(
            options_.account_dfs_url, options_.storage_credentials_provider);
    ASSERT_OK_AND_ASSIGN(fs_, AzureFileSystem::Make(options_));
    auto container_client = blob_service_client_->GetBlobContainerClient(container_name_);
    container_client.CreateIfNotExists();

    auto blob_client = container_client.GetBlockBlobClient(PreexistingObjectName());
    blob_client.UploadFrom(reinterpret_cast<const uint8_t*>(kLoremIpsum),
                           strlen(kLoremIpsum));
  }

  void TearDown() override {
    if (!suite_skipped_) {
      auto containers = blob_service_client_->ListBlobContainers();
      for (auto container : containers.BlobContainers) {
        auto container_client =
            blob_service_client_->GetBlobContainerClient(container.Name);
        container_client.DeleteIfExists();
      }
    }
  }

  std::string PreexistingContainerName() const { return container_name_; }

  std::string PreexistingContainerPath() const {
    return PreexistingContainerName() + '/';
  }

  static std::string PreexistingObjectName() { return "test-object-name"; }

  std::string PreexistingObjectPath() const {
    return PreexistingContainerPath() + PreexistingObjectName();
  }

  std::string NotFoundObjectPath() { return PreexistingContainerPath() + "not-found"; }

  std::string RandomLine(int lineno, std::size_t width) {
    auto line = std::to_string(lineno) + ":    ";
    line += RandomChars(width - line.size() - 1);
    line += '\n';
    return line;
  }

  std::size_t RandomIndex(std::size_t end) {
    return std::uniform_int_distribution<std::size_t>(0, end - 1)(generator_);
  }

  std::string RandomChars(std::size_t count) {
    auto const fillers = std::string("abcdefghijlkmnopqrstuvwxyz0123456789");
    std::uniform_int_distribution<std::size_t> d(0, fillers.size() - 1);
    std::string s;
    std::generate_n(std::back_inserter(s), count, [&] { return fillers[d(generator_)]; });
    return s;
  }

  std::string RandomContainerName() { return RandomChars(32); }

  std::string RandomDirectoryName() { return RandomChars(32); }

  void UploadLines(const std::vector<std::string>& lines, const char* path_to_file,
                   int total_size) {
    const auto path = PreexistingContainerPath() + path_to_file;
    ASSERT_OK_AND_ASSIGN(auto output, fs_->OpenOutputStream(path, {}));
    std::string all_lines = std::accumulate(lines.begin(), lines.end(), std::string(""));
    ASSERT_OK(output->Write(all_lines.data(), all_lines.size()));
    ASSERT_OK(output->Close());
  }

  void RunGetFileInfoObjectWithNestedStructureTest();
  void RunGetFileInfoObjectTest();
};

class AzuriteFileSystemTest : public AzureFileSystemTest {
  Result<AzureOptions> MakeOptions() {
    EXPECT_THAT(GetAzuriteEnv(), NotNull());
    ARROW_EXPECT_OK(GetAzuriteEnv()->status());
    AzureOptions options;
    options.backend = AzureBackend::Azurite;
    ARROW_EXPECT_OK(options.ConfigureAccountKeyCredentials(
        GetAzuriteEnv()->account_name(), GetAzuriteEnv()->account_key()));
    return options;
  }
};

class AzureFlatNamespaceFileSystemTest : public AzureFileSystemTest {
  Result<AzureOptions> MakeOptions() override {
    AzureOptions options;
    const auto account_key = std::getenv("AZURE_FLAT_NAMESPACE_ACCOUNT_KEY");
    const auto account_name = std::getenv("AZURE_FLAT_NAMESPACE_ACCOUNT_NAME");
    if (account_key && account_name) {
      RETURN_NOT_OK(options.ConfigureAccountKeyCredentials(account_name, account_key));
      return options;
    }
    return Status::Cancelled(
        "Connection details not provided for a real flat namespace "
        "account.");
  }
};

// How to enable this test:
//
// You need an Azure account. You should be able to create a free
// account at https://azure.microsoft.com/en-gb/free/ . You should be
// able to create a storage account through the portal Web UI.
//
// See also the official document how to create a storage account:
// https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account
//
// A few suggestions on configuration:
//
// * Use Standard general-purpose v2 not premium
// * Use LRS redundancy
// * Obviously you need to enable hierarchical namespace.
// * Set the default access tier to hot
// * SFTP, NFS and file shares are not required.
class AzureHierarchicalNamespaceFileSystemTest : public AzureFileSystemTest {
  Result<AzureOptions> MakeOptions() override {
    AzureOptions options;
    const auto account_key = std::getenv("AZURE_HIERARCHICAL_NAMESPACE_ACCOUNT_KEY");
    const auto account_name = std::getenv("AZURE_HIERARCHICAL_NAMESPACE_ACCOUNT_NAME");
    if (account_key && account_name) {
      RETURN_NOT_OK(options.ConfigureAccountKeyCredentials(account_name, account_key));
      return options;
    }
    return Status::Cancelled(
        "Connection details not provided for a real hierarchical namespace "
        "account.");
  }
};

TEST_F(AzureFlatNamespaceFileSystemTest, DetectHierarchicalNamespace) {
  auto hierarchical_namespace = internal::HierarchicalNamespaceDetector();
  ASSERT_OK(hierarchical_namespace.Init(datalake_service_client_.get()));
  ASSERT_OK_AND_EQ(false, hierarchical_namespace.Enabled(PreexistingContainerName()));
}

TEST_F(AzureHierarchicalNamespaceFileSystemTest, DetectHierarchicalNamespace) {
  auto hierarchical_namespace = internal::HierarchicalNamespaceDetector();
  ASSERT_OK(hierarchical_namespace.Init(datalake_service_client_.get()));
  ASSERT_OK_AND_EQ(true, hierarchical_namespace.Enabled(PreexistingContainerName()));
}

TEST_F(AzuriteFileSystemTest, DetectHierarchicalNamespace) {
  auto hierarchical_namespace = internal::HierarchicalNamespaceDetector();
  ASSERT_OK(hierarchical_namespace.Init(datalake_service_client_.get()));
  ASSERT_OK_AND_EQ(false, hierarchical_namespace.Enabled(PreexistingContainerName()));
}

TEST_F(AzuriteFileSystemTest, DetectHierarchicalNamespaceFailsWithMissingContainer) {
  auto hierarchical_namespace = internal::HierarchicalNamespaceDetector();
  ASSERT_OK(hierarchical_namespace.Init(datalake_service_client_.get()));
  ASSERT_NOT_OK(hierarchical_namespace.Enabled("non-existent-container"));
}

TEST_F(AzuriteFileSystemTest, GetFileInfoAccount) {
  AssertFileInfo(fs_.get(), "", FileType::Directory);

  // URI
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("abfs://"));
}

TEST_F(AzuriteFileSystemTest, GetFileInfoContainer) {
  AssertFileInfo(fs_.get(), PreexistingContainerName(), FileType::Directory);

  AssertFileInfo(fs_.get(), "non-existent-container", FileType::NotFound);

  // URI
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("abfs://" + PreexistingContainerName()));
}

void AzureFileSystemTest::RunGetFileInfoObjectWithNestedStructureTest() {
  // Adds detailed tests to handle cases of different edge cases
  // with directory naming conventions (e.g. with and without slashes).
  constexpr auto kObjectName = "test-object-dir/some_other_dir/another_dir/foo";
  ASSERT_OK_AND_ASSIGN(
      auto output,
      fs_->OpenOutputStream(PreexistingContainerPath() + kObjectName, /*metadata=*/{}));
  const auto data = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(data.data(), data.size()));
  ASSERT_OK(output->Close());

  // 0 is immediately after "/" lexicographically, ensure that this doesn't
  // cause unexpected issues.
  ASSERT_OK_AND_ASSIGN(output,
                       fs_->OpenOutputStream(
                           PreexistingContainerPath() + "test-object-dir/some_other_dir0",
                           /*metadata=*/{}));
  ASSERT_OK(output->Write(data.data(), data.size()));
  ASSERT_OK(output->Close());
  ASSERT_OK_AND_ASSIGN(
      output, fs_->OpenOutputStream(PreexistingContainerPath() + kObjectName + "0",
                                    /*metadata=*/{}));
  ASSERT_OK(output->Write(data.data(), data.size()));
  ASSERT_OK(output->Close());

  AssertFileInfo(fs_.get(), PreexistingContainerPath() + kObjectName, FileType::File);
  AssertFileInfo(fs_.get(), PreexistingContainerPath() + kObjectName + "/",
                 FileType::NotFound);
  AssertFileInfo(fs_.get(), PreexistingContainerPath() + "test-object-dir",
                 FileType::Directory);
  AssertFileInfo(fs_.get(), PreexistingContainerPath() + "test-object-dir/",
                 FileType::Directory);
  AssertFileInfo(fs_.get(), PreexistingContainerPath() + "test-object-dir/some_other_dir",
                 FileType::Directory);
  AssertFileInfo(fs_.get(),
                 PreexistingContainerPath() + "test-object-dir/some_other_dir/",
                 FileType::Directory);

  AssertFileInfo(fs_.get(), PreexistingContainerPath() + "test-object-di",
                 FileType::NotFound);
  AssertFileInfo(fs_.get(), PreexistingContainerPath() + "test-object-dir/some_other_di",
                 FileType::NotFound);
}

TEST_F(AzuriteFileSystemTest, GetFileInfoObjectWithNestedStructure) {
  RunGetFileInfoObjectWithNestedStructureTest();
}

TEST_F(AzureHierarchicalNamespaceFileSystemTest, GetFileInfoObjectWithNestedStructure) {
  RunGetFileInfoObjectWithNestedStructureTest();
  datalake_service_client_->GetFileSystemClient(PreexistingContainerName())
      .GetDirectoryClient("test-empty-object-dir")
      .Create();

  AssertFileInfo(fs_.get(), PreexistingContainerPath() + "test-empty-object-dir",
                 FileType::Directory);
}

void AzureFileSystemTest::RunGetFileInfoObjectTest() {
  auto object_properties =
      blob_service_client_->GetBlobContainerClient(PreexistingContainerName())
          .GetBlobClient(PreexistingObjectName())
          .GetProperties()
          .Value;

  AssertFileInfo(fs_.get(), PreexistingObjectPath(), FileType::File,
                 std::chrono::system_clock::time_point(object_properties.LastModified),
                 static_cast<int64_t>(object_properties.BlobSize));

  // URI
  ASSERT_RAISES(Invalid, fs_->GetFileInfo("abfs://" + PreexistingObjectName()));
}

TEST_F(AzuriteFileSystemTest, GetFileInfoObject) { RunGetFileInfoObjectTest(); }

TEST_F(AzureHierarchicalNamespaceFileSystemTest, GetFileInfoObject) {
  RunGetFileInfoObjectTest();
}

TEST_F(AzuriteFileSystemTest, CreateDirFailureNoContainer) {
  ASSERT_RAISES(Invalid, fs_->CreateDir("", false));
}

TEST_F(AzuriteFileSystemTest, CreateDirSuccessContainerOnly) {
  auto container_name = RandomContainerName();
  ASSERT_OK(fs_->CreateDir(container_name, false));
  arrow::fs::AssertFileInfo(fs_.get(), container_name, FileType::Directory);
}

TEST_F(AzuriteFileSystemTest, CreateDirSuccessContainerAndDirectory) {
  const auto path = PreexistingContainerPath() + RandomDirectoryName();
  ASSERT_OK(fs_->CreateDir(path, false));
  // There is only virtual directory without hierarchical namespace
  // support. So the CreateDir() does nothing.
  arrow::fs::AssertFileInfo(fs_.get(), path, FileType::NotFound);
}

TEST_F(AzureHierarchicalNamespaceFileSystemTest, CreateDirSuccessContainerAndDirectory) {
  const auto path = PreexistingContainerPath() + RandomDirectoryName();
  ASSERT_OK(fs_->CreateDir(path, false));
  arrow::fs::AssertFileInfo(fs_.get(), path, FileType::Directory);
}

TEST_F(AzuriteFileSystemTest, CreateDirFailureDirectoryWithMissingContainer) {
  const auto path = std::string("not-a-container/new-directory");
  ASSERT_RAISES(IOError, fs_->CreateDir(path, false));
}

TEST_F(AzuriteFileSystemTest, CreateDirRecursiveFailureNoContainer) {
  ASSERT_RAISES(Invalid, fs_->CreateDir("", true));
}

TEST_F(AzureHierarchicalNamespaceFileSystemTest, CreateDirRecursiveSuccessContainerOnly) {
  auto container_name = RandomContainerName();
  ASSERT_OK(fs_->CreateDir(container_name, true));
  arrow::fs::AssertFileInfo(fs_.get(), container_name, FileType::Directory);
}

TEST_F(AzuriteFileSystemTest, CreateDirRecursiveSuccessContainerOnly) {
  auto container_name = RandomContainerName();
  ASSERT_OK(fs_->CreateDir(container_name, true));
  arrow::fs::AssertFileInfo(fs_.get(), container_name, FileType::Directory);
}

TEST_F(AzureHierarchicalNamespaceFileSystemTest, CreateDirRecursiveSuccessDirectoryOnly) {
  const auto parent = PreexistingContainerPath() + RandomDirectoryName();
  const auto path = internal::ConcatAbstractPath(parent, "new-sub");
  ASSERT_OK(fs_->CreateDir(path, true));
  arrow::fs::AssertFileInfo(fs_.get(), path, FileType::Directory);
  arrow::fs::AssertFileInfo(fs_.get(), parent, FileType::Directory);
}

TEST_F(AzuriteFileSystemTest, CreateDirRecursiveSuccessDirectoryOnly) {
  const auto parent = PreexistingContainerPath() + RandomDirectoryName();
  const auto path = internal::ConcatAbstractPath(parent, "new-sub");
  ASSERT_OK(fs_->CreateDir(path, true));
  // There is only virtual directory without hierarchical namespace
  // support. So the CreateDir() does nothing.
  arrow::fs::AssertFileInfo(fs_.get(), path, FileType::NotFound);
  arrow::fs::AssertFileInfo(fs_.get(), parent, FileType::NotFound);
}

TEST_F(AzureHierarchicalNamespaceFileSystemTest,
       CreateDirRecursiveSuccessContainerAndDirectory) {
  auto container_name = RandomContainerName();
  const auto parent = internal::ConcatAbstractPath(container_name, RandomDirectoryName());
  const auto path = internal::ConcatAbstractPath(parent, "new-sub");
  ASSERT_OK(fs_->CreateDir(path, true));
  arrow::fs::AssertFileInfo(fs_.get(), path, FileType::Directory);
  arrow::fs::AssertFileInfo(fs_.get(), parent, FileType::Directory);
  arrow::fs::AssertFileInfo(fs_.get(), container_name, FileType::Directory);
}

TEST_F(AzuriteFileSystemTest, CreateDirRecursiveSuccessContainerAndDirectory) {
  auto container_name = RandomContainerName();
  const auto parent = internal::ConcatAbstractPath(container_name, RandomDirectoryName());
  const auto path = internal::ConcatAbstractPath(parent, "new-sub");
  ASSERT_OK(fs_->CreateDir(path, true));
  // There is only virtual directory without hierarchical namespace
  // support. So the CreateDir() does nothing.
  arrow::fs::AssertFileInfo(fs_.get(), path, FileType::NotFound);
  arrow::fs::AssertFileInfo(fs_.get(), parent, FileType::NotFound);
  arrow::fs::AssertFileInfo(fs_.get(), container_name, FileType::Directory);
}

TEST_F(AzuriteFileSystemTest, CreateDirUri) {
  ASSERT_RAISES(Invalid, fs_->CreateDir("abfs://" + RandomContainerName(), true));
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamString) {
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream(PreexistingObjectPath()));

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Read(1024));
  EXPECT_EQ(buffer->ToString(), kLoremIpsum);
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamStringBuffers) {
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream(PreexistingObjectPath()));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, stream->Read(16));
    contents.append(buffer->ToString());
  } while (buffer && buffer->size() != 0);

  EXPECT_EQ(contents, kLoremIpsum);
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamInfo) {
  ASSERT_OK_AND_ASSIGN(auto info, fs_->GetFileInfo(PreexistingObjectPath()));

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream(info));

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Read(1024));
  EXPECT_EQ(buffer->ToString(), kLoremIpsum);
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamEmpty) {
  const auto path_to_file = "empty-object.txt";
  const auto path = PreexistingContainerPath() + path_to_file;
  blob_service_client_->GetBlobContainerClient(PreexistingContainerName())
      .GetBlockBlobClient(path_to_file)
      .UploadFrom(nullptr, 0);

  ASSERT_OK_AND_ASSIGN(auto stream, fs_->OpenInputStream(path));
  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));
  EXPECT_EQ(size, 0);
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamNotFound) {
  ASSERT_RAISES(IOError, fs_->OpenInputStream(NotFoundObjectPath()));
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamInfoInvalid) {
  ASSERT_OK_AND_ASSIGN(auto info, fs_->GetFileInfo(PreexistingContainerPath()));
  ASSERT_RAISES(IOError, fs_->OpenInputStream(info));

  ASSERT_OK_AND_ASSIGN(auto info2, fs_->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs_->OpenInputStream(info2));
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamUri) {
  ASSERT_RAISES(Invalid, fs_->OpenInputStream("abfs://" + PreexistingObjectPath()));
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamTrailingSlash) {
  ASSERT_RAISES(IOError, fs_->OpenInputStream(PreexistingObjectPath() + '/'));
}

namespace {
std::shared_ptr<const KeyValueMetadata> NormalizerKeyValueMetadata(
    std::shared_ptr<const KeyValueMetadata> metadata) {
  auto normalized = std::make_shared<KeyValueMetadata>();
  for (int64_t i = 0; i < metadata->size(); ++i) {
    auto key = metadata->key(i);
    auto value = metadata->value(i);
    if (key == "Content-Hash") {
      std::vector<uint8_t> output;
      output.reserve(value.size() / 2);
      if (ParseHexValues(value, output.data()).ok()) {
        // Valid value
        value = std::string(value.size(), 'F');
      }
    } else if (key == "Last-Modified" || key == "Created-On" ||
               key == "Access-Tier-Changed-On") {
      auto parser = TimestampParser::MakeISO8601();
      int64_t output;
      if ((*parser)(value.data(), value.size(), TimeUnit::NANO, &output)) {
        // Valid value
        value = "2023-10-31T08:15:20Z";
      }
    } else if (key == "ETag") {
      if (arrow::internal::StartsWith(value, "\"") &&
          arrow::internal::EndsWith(value, "\"")) {
        // Valid value
        value = "\"ETagValue\"";
      }
    }
    normalized->Append(key, value);
  }
  return normalized;
}
};  // namespace

TEST_F(AzuriteFileSystemTest, OpenInputStreamReadMetadata) {
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream(PreexistingObjectPath()));

  std::shared_ptr<const KeyValueMetadata> actual;
  ASSERT_OK_AND_ASSIGN(actual, stream->ReadMetadata());
  ASSERT_EQ(
      "\n"
      "-- metadata --\n"
      "Content-Type: application/octet-stream\n"
      "Content-Encoding: \n"
      "Content-Language: \n"
      "Content-Hash: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\n"
      "Content-Disposition: \n"
      "Cache-Control: \n"
      "Last-Modified: 2023-10-31T08:15:20Z\n"
      "Created-On: 2023-10-31T08:15:20Z\n"
      "Blob-Type: BlockBlob\n"
      "Lease-State: available\n"
      "Lease-Status: unlocked\n"
      "Content-Length: 447\n"
      "ETag: \"ETagValue\"\n"
      "IsServerEncrypted: true\n"
      "Access-Tier: Hot\n"
      "Is-Access-Tier-Inferred: true\n"
      "Access-Tier-Changed-On: 2023-10-31T08:15:20Z\n"
      "Has-Legal-Hold: false",
      NormalizerKeyValueMetadata(actual)->ToString());
}

TEST_F(AzuriteFileSystemTest, OpenInputStreamClosed) {
  ASSERT_OK_AND_ASSIGN(auto stream, fs_->OpenInputStream(PreexistingObjectPath()));
  ASSERT_OK(stream->Close());
  std::array<char, 16> buffer{};
  ASSERT_RAISES(Invalid, stream->Read(buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->Read(buffer.size()));
  ASSERT_RAISES(Invalid, stream->Tell());
}

TEST_F(AzuriteFileSystemTest, TestWriteMetadata) {
  options_.default_metadata = arrow::key_value_metadata({{"foo", "bar"}});

  std::shared_ptr<AzureFileSystem> fs_with_defaults;
  ASSERT_OK_AND_ASSIGN(fs_with_defaults, AzureFileSystem::Make(options_));
  std::string path = "object_with_defaults";
  auto location = PreexistingContainerPath() + path;
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output,
                       fs_with_defaults->OpenOutputStream(location, /*metadata=*/{}));
  const auto expected = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(expected.data(), expected.size()));
  ASSERT_OK(output->Close());

  // Verify the metadata has been set.
  auto blob_metadata =
      blob_service_client_->GetBlobContainerClient(PreexistingContainerName())
          .GetBlockBlobClient(path)
          .GetProperties()
          .Value.Metadata;
  EXPECT_EQ(blob_metadata["foo"], "bar");

  // Check that explicit metadata overrides the defaults.
  ASSERT_OK_AND_ASSIGN(
      output, fs_with_defaults->OpenOutputStream(
                  location, /*metadata=*/arrow::key_value_metadata({{"bar", "foo"}})));
  ASSERT_OK(output->Write(expected.data(), expected.size()));
  ASSERT_OK(output->Close());
  blob_metadata = blob_service_client_->GetBlobContainerClient(PreexistingContainerName())
                      .GetBlockBlobClient(path)
                      .GetProperties()
                      .Value.Metadata;
  EXPECT_EQ(blob_metadata["bar"], "foo");
  // Defaults are overwritten and not merged.
  EXPECT_EQ(blob_metadata.find("foo"), blob_metadata.end());
}

TEST_F(AzuriteFileSystemTest, OpenOutputStreamSmall) {
  const auto path = PreexistingContainerPath() + "test-write-object";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs_->OpenOutputStream(path, {}));
  const auto expected = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(expected.data(), expected.size()));
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs_->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));

  EXPECT_EQ(std::string(inbuf.data(), size), expected);
}

TEST_F(AzuriteFileSystemTest, OpenOutputStreamLarge) {
  const auto path = PreexistingContainerPath() + "test-write-object";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs_->OpenOutputStream(path, {}));
  std::array<std::int64_t, 3> sizes{257 * 1024, 258 * 1024, 259 * 1024};
  std::array<std::string, 3> buffers{
      std::string(sizes[0], 'A'),
      std::string(sizes[1], 'B'),
      std::string(sizes[2], 'C'),
  };
  auto expected = std::int64_t{0};
  for (auto i = 0; i != 3; ++i) {
    ASSERT_OK(output->Write(buffers[i].data(), buffers[i].size()));
    expected += sizes[i];
    ASSERT_EQ(output->Tell(), expected);
  }
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs_->OpenInputStream(path));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, input->Read(128 * 1024));
    ASSERT_TRUE(buffer);
    contents.append(buffer->ToString());
  } while (buffer->size() != 0);

  EXPECT_EQ(contents, buffers[0] + buffers[1] + buffers[2]);
}

TEST_F(AzuriteFileSystemTest, OpenOutputStreamTruncatesExistingFile) {
  const auto path = PreexistingContainerPath() + "test-write-object";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs_->OpenOutputStream(path, {}));
  const std::string expected0 = "Existing blob content";
  ASSERT_OK(output->Write(expected0.data(), expected0.size()));
  ASSERT_OK(output->Close());

  // Check that the initial content has been written - if not this test is not achieving
  // what it's meant to.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs_->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(std::string(inbuf.data(), size), expected0);

  ASSERT_OK_AND_ASSIGN(output, fs_->OpenOutputStream(path, {}));
  const auto expected1 = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(expected1.data(), expected1.size()));
  ASSERT_OK(output->Close());

  // Verify that the initial content has been overwritten.
  ASSERT_OK_AND_ASSIGN(input, fs_->OpenInputStream(path));
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(std::string(inbuf.data(), size), expected1);
}

TEST_F(AzuriteFileSystemTest, OpenAppendStreamDoesNotTruncateExistingFile) {
  const auto path = PreexistingContainerPath() + "test-write-object";
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs_->OpenOutputStream(path, {}));
  const std::string expected0 = "Existing blob content";
  ASSERT_OK(output->Write(expected0.data(), expected0.size()));
  ASSERT_OK(output->Close());

  // Check that the initial content has been written - if not this test is not achieving
  // what it's meant to.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs_->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(std::string(inbuf.data(), size), expected0);

  ASSERT_OK_AND_ASSIGN(output, fs_->OpenAppendStream(path, {}));
  const auto expected1 = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(expected1.data(), expected1.size()));
  ASSERT_OK(output->Close());

  // Verify that the initial content has not been overwritten and that the block from
  // the other client was not committed.
  ASSERT_OK_AND_ASSIGN(input, fs_->OpenInputStream(path));
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(std::string(inbuf.data(), size), expected0 + expected1);
}

TEST_F(AzuriteFileSystemTest, OpenOutputStreamClosed) {
  const auto path = internal::ConcatAbstractPath(PreexistingContainerName(),
                                                 "open-output-stream-closed.txt");
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs_->OpenOutputStream(path, {}));
  ASSERT_OK(output->Close());
  ASSERT_RAISES(Invalid, output->Write(kLoremIpsum, std::strlen(kLoremIpsum)));
  ASSERT_RAISES(Invalid, output->Flush());
  ASSERT_RAISES(Invalid, output->Tell());
}

TEST_F(AzuriteFileSystemTest, OpenOutputStreamUri) {
  const auto path = internal::ConcatAbstractPath(PreexistingContainerName(),
                                                 "open-output-stream-uri.txt");
  ASSERT_RAISES(Invalid, fs_->OpenInputStream("abfs://" + path));
}

TEST_F(AzuriteFileSystemTest, OpenInputFileMixedReadVsReadAt) {
  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(),
                  [&] { return RandomLine(++lineno, kLineWidth); });

  const auto path_to_file = "OpenInputFileMixedReadVsReadAt/object-name";
  const auto path = PreexistingContainerPath() + path_to_file;

  UploadLines(lines, path_to_file, kLineCount * kLineWidth);

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile(path));
  for (int i = 0; i != 32; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Verify sequential reads work as expected.
    std::array<char, kLineWidth> buffer{};
    std::int64_t size;
    {
      ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
      EXPECT_EQ(lines[2 * i], actual->ToString());
    }
    {
      ASSERT_OK_AND_ASSIGN(size, file->Read(buffer.size(), buffer.data()));
      EXPECT_EQ(size, kLineWidth);
      auto actual = std::string{buffer.begin(), buffer.end()};
      EXPECT_EQ(lines[2 * i + 1], actual);
    }

    // Verify random reads interleave too.
    auto const index = RandomIndex(kLineCount);
    auto const position = index * kLineWidth;
    ASSERT_OK_AND_ASSIGN(size, file->ReadAt(position, buffer.size(), buffer.data()));
    EXPECT_EQ(size, kLineWidth);
    auto actual = std::string{buffer.begin(), buffer.end()};
    EXPECT_EQ(lines[index], actual);

    // Verify random reads using buffers work.
    ASSERT_OK_AND_ASSIGN(auto b, file->ReadAt(position, kLineWidth));
    EXPECT_EQ(lines[index], b->ToString());
  }
}

TEST_F(AzuriteFileSystemTest, OpenInputFileRandomSeek) {
  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(),
                  [&] { return RandomLine(++lineno, kLineWidth); });

  const auto path_to_file = "OpenInputFileRandomSeek/object-name";
  const auto path = PreexistingContainerPath() + path_to_file;
  std::shared_ptr<io::OutputStream> output;

  UploadLines(lines, path_to_file, kLineCount * kLineWidth);

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile(path));
  for (int i = 0; i != 32; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Verify sequential reads work as expected.
    auto const index = RandomIndex(kLineCount);
    auto const position = index * kLineWidth;
    ASSERT_OK(file->Seek(position));
    ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
    EXPECT_EQ(lines[index], actual->ToString());
  }
}

TEST_F(AzuriteFileSystemTest, OpenInputFileIoContext) {
  // Create a test file.
  const auto path_to_file = "OpenInputFileIoContext/object-name";
  const auto path = PreexistingContainerPath() + path_to_file;
  const std::string contents = "The quick brown fox jumps over the lazy dog";

  auto blob_client =
      blob_service_client_->GetBlobContainerClient(PreexistingContainerName())
          .GetBlockBlobClient(path_to_file);
  blob_client.UploadFrom(reinterpret_cast<const uint8_t*>(contents.data()),
                         contents.length());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile(path));
  EXPECT_EQ(fs_->io_context().external_id(), file->io_context().external_id());
}

TEST_F(AzuriteFileSystemTest, OpenInputFileInfo) {
  ASSERT_OK_AND_ASSIGN(auto info, fs_->GetFileInfo(PreexistingObjectPath()));

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile(info));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  auto constexpr kStart = 16;
  ASSERT_OK_AND_ASSIGN(size, file->ReadAt(kStart, buffer.size(), buffer.data()));

  auto const expected = std::string(kLoremIpsum).substr(kStart);
  EXPECT_EQ(std::string(buffer.data(), size), expected);
}

TEST_F(AzuriteFileSystemTest, OpenInputFileNotFound) {
  ASSERT_RAISES(IOError, fs_->OpenInputFile(NotFoundObjectPath()));
}

TEST_F(AzuriteFileSystemTest, OpenInputFileInfoInvalid) {
  ASSERT_OK_AND_ASSIGN(auto info, fs_->GetFileInfo(PreexistingContainerPath()));
  ASSERT_RAISES(IOError, fs_->OpenInputFile(info));

  ASSERT_OK_AND_ASSIGN(auto info2, fs_->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs_->OpenInputFile(info2));
}

TEST_F(AzuriteFileSystemTest, OpenInputFileClosed) {
  ASSERT_OK_AND_ASSIGN(auto stream, fs_->OpenInputFile(PreexistingObjectPath()));
  ASSERT_OK(stream->Close());
  std::array<char, 16> buffer{};
  ASSERT_RAISES(Invalid, stream->Tell());
  ASSERT_RAISES(Invalid, stream->Read(buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->Read(buffer.size()));
  ASSERT_RAISES(Invalid, stream->ReadAt(1, buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->ReadAt(1, 1));
  ASSERT_RAISES(Invalid, stream->Seek(2));
}

}  // namespace
}  // namespace fs
}  // namespace arrow

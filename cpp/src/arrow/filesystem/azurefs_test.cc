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

#include <memory>
#include <random>
#include <string>

#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
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
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
using internal::TemporaryDir;
namespace fs {
using internal::ConcatAbstractPath;
namespace bp = boost::process;

using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::NotNull;

namespace Blobs = Azure::Storage::Blobs;
namespace Core = Azure::Core;
namespace DataLake = Azure::Storage::Files::DataLake;

using HNSSupport = internal::HierarchicalNamespaceSupport;

enum class AzureBackend {
  /// \brief Official Azure Remote Backend
  kAzure,
  /// \brief Local Simulated Storage
  kAzurite
};

class BaseAzureEnv : public ::testing::Environment {
 protected:
  std::string account_name_;
  std::string account_key_;

  BaseAzureEnv(std::string account_name, std::string account_key)
      : account_name_(std::move(account_name)), account_key_(std::move(account_key)) {}

 public:
  const std::string& account_name() const { return account_name_; }
  const std::string& account_key() const { return account_key_; }

  virtual AzureBackend backend() const = 0;

  virtual bool WithHierarchicalNamespace() const { return false; }

  virtual Result<int64_t> GetDebugLogSize() { return 0; }
  virtual Status DumpDebugLog(int64_t position) {
    return Status::NotImplemented("BaseAzureEnv::DumpDebugLog");
  }
};

template <class AzureEnvClass>
class AzureEnvImpl : public BaseAzureEnv {
 private:
  /// \brief Factory function that registers the singleton instance as a global test
  /// environment. Must be called only once per implementation (see GetInstance()).
  ///
  /// Every BaseAzureEnv implementation defines a static and parameter-less member
  /// function called Make() that returns a Result<std::unique_ptr<BaseAzureEnv>>.
  /// This templated function performs the following steps:
  ///
  /// 1) Calls AzureEnvClass::Make() to get an instance of AzureEnvClass.
  /// 2) Passes ownership of the AzureEnvClass instance to the testing environment.
  /// 3) Returns a Result<BaseAzureEnv*> wrapping the raw heap-allocated pointer.
  static Result<BaseAzureEnv*> MakeAndAddToGlobalTestEnvironment() {
    ARROW_ASSIGN_OR_RAISE(auto env, AzureEnvClass::Make());
    auto* heap_ptr = env.release();
    ::testing::AddGlobalTestEnvironment(heap_ptr);
    return heap_ptr;
  }

 protected:
  using BaseAzureEnv::BaseAzureEnv;

  /// \brief Create an AzureEnvClass instance from environment variables.
  ///
  /// Reads the account name and key from the environment variables. This can be
  /// used in BaseAzureEnv implementations that don't need to do any additional
  /// setup to create the singleton instance (e.g. AzureFlatNSEnv,
  /// AzureHierarchicalNSEnv).
  static Result<std::unique_ptr<AzureEnvClass>> MakeFromEnvVars(
      const std::string& account_name_var, const std::string& account_key_var) {
    const auto account_name = std::getenv(account_name_var.c_str());
    const auto account_key = std::getenv(account_key_var.c_str());
    if (!account_name && !account_key) {
      return Status::Cancelled(account_name_var + " and " + account_key_var +
                               " are not set. Skipping tests.");
    }
    // If only one of the variables is set. Don't cancel tests,
    // fail with a Status::Invalid.
    if (!account_name) {
      return Status::Invalid(account_name_var + " not set while " + account_key_var +
                             " is set.");
    }
    if (!account_key) {
      return Status::Invalid(account_key_var + " not set while " + account_name_var +
                             " is set.");
    }
    return std::unique_ptr<AzureEnvClass>{new AzureEnvClass(account_name, account_key)};
  }

 public:
  static Result<BaseAzureEnv*> GetInstance() {
    // Ensure MakeAndAddToGlobalTestEnvironment() is called only once by storing the
    // Result<BaseAzureEnv*> in a static variable.
    static auto singleton_env = MakeAndAddToGlobalTestEnvironment();
    return singleton_env;
  }

  AzureBackend backend() const final { return AzureEnvClass::kBackend; }
};

class AzuriteEnv : public AzureEnvImpl<AzuriteEnv> {
 private:
  std::unique_ptr<TemporaryDir> temp_dir_;
  arrow::internal::PlatformFilename debug_log_path_;
  bp::child server_process_;

  using AzureEnvImpl::AzureEnvImpl;

 public:
  static const AzureBackend kBackend = AzureBackend::kAzurite;

  ~AzuriteEnv() override {
    server_process_.terminate();
    server_process_.wait();
  }

  static Result<std::unique_ptr<AzureEnvImpl>> Make() {
    auto self = std::unique_ptr<AzuriteEnv>(
        new AzuriteEnv("devstoreaccount1",
                       "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
                       "K1SZFPTOtr/KBHBeksoGMGw=="));
    auto exe_path = bp::search_path("azurite");
    if (exe_path.empty()) {
      return Status::Invalid("Could not find Azurite emulator.");
    }
    ARROW_ASSIGN_OR_RAISE(self->temp_dir_, TemporaryDir::Make("azurefs-test-"));
    ARROW_ASSIGN_OR_RAISE(self->debug_log_path_,
                          self->temp_dir_->path().Join("debug.log"));
    auto server_process = bp::child(
        boost::this_process::environment(), exe_path, "--silent", "--location",
        self->temp_dir_->path().ToString(), "--debug", self->debug_log_path_.ToString());
    if (!server_process.valid() || !server_process.running()) {
      server_process.terminate();
      server_process.wait();
      return Status::Invalid("Could not start Azurite emulator.");
    }
    self->server_process_ = std::move(server_process);
    return self;
  }

  Result<int64_t> GetDebugLogSize() override {
    ARROW_ASSIGN_OR_RAISE(auto exists, arrow::internal::FileExists(debug_log_path_));
    if (!exists) {
      return 0;
    }
    ARROW_ASSIGN_OR_RAISE(auto file_descriptor,
                          arrow::internal::FileOpenReadable(debug_log_path_));
    ARROW_RETURN_NOT_OK(arrow::internal::FileSeek(file_descriptor.fd(), 0, SEEK_END));
    return arrow::internal::FileTell(file_descriptor.fd());
  }

  Status DumpDebugLog(int64_t position) override {
    ARROW_ASSIGN_OR_RAISE(auto exists, arrow::internal::FileExists(debug_log_path_));
    if (!exists) {
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(auto file_descriptor,
                          arrow::internal::FileOpenReadable(debug_log_path_));
    if (position > 0) {
      ARROW_RETURN_NOT_OK(arrow::internal::FileSeek(file_descriptor.fd(), position));
    }
    std::vector<uint8_t> buffer;
    const int64_t buffer_size = 4096;
    buffer.reserve(buffer_size);
    while (true) {
      ARROW_ASSIGN_OR_RAISE(
          auto n_read_bytes,
          arrow::internal::FileRead(file_descriptor.fd(), buffer.data(), buffer_size));
      if (n_read_bytes <= 0) {
        break;
      }
      std::cerr << std::string_view(reinterpret_cast<const char*>(buffer.data()),
                                    n_read_bytes);
    }
    std::cerr << std::endl;
    return Status::OK();
  }
};

class AzureFlatNSEnv : public AzureEnvImpl<AzureFlatNSEnv> {
 private:
  using AzureEnvImpl::AzureEnvImpl;

 public:
  static const AzureBackend kBackend = AzureBackend::kAzure;

  static Result<std::unique_ptr<AzureFlatNSEnv>> Make() {
    return MakeFromEnvVars("AZURE_FLAT_NAMESPACE_ACCOUNT_NAME",
                           "AZURE_FLAT_NAMESPACE_ACCOUNT_KEY");
  }
};

class AzureHierarchicalNSEnv : public AzureEnvImpl<AzureHierarchicalNSEnv> {
 private:
  using AzureEnvImpl::AzureEnvImpl;

 public:
  static const AzureBackend kBackend = AzureBackend::kAzure;

  static Result<std::unique_ptr<AzureHierarchicalNSEnv>> Make() {
    return MakeFromEnvVars("AZURE_HIERARCHICAL_NAMESPACE_ACCOUNT_NAME",
                           "AZURE_HIERARCHICAL_NAMESPACE_ACCOUNT_KEY");
  }

  bool WithHierarchicalNamespace() const final { return true; }
};

TEST(AzureFileSystem, InitializingFilesystemWithoutAccountNameFails) {
  AzureOptions options;
  ASSERT_RAISES(Invalid, options.ConfigureAccountKeyCredential("account_key"));

  ARROW_EXPECT_OK(
      options.ConfigureClientSecretCredential("tenant_id", "client_id", "client_secret"));
  ASSERT_RAISES(Invalid, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, InitializeWithDefaultCredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(options.ConfigureDefaultCredential());
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, InitializeWithDefaultCredentialImplicitly) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  AzureOptions explictly_default_options;
  explictly_default_options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(explictly_default_options.ConfigureDefaultCredential());
  ASSERT_TRUE(options.Equals(explictly_default_options));
}

TEST(AzureFileSystem, InitializeWithAnonymousCredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(options.ConfigureAnonymousCredential());
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, InitializeWithClientSecretCredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(
      options.ConfigureClientSecretCredential("tenant_id", "client_id", "client_secret"));
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, InitializeWithManagedIdentityCredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(options.ConfigureManagedIdentityCredential());
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));

  ARROW_EXPECT_OK(options.ConfigureManagedIdentityCredential("specific-client-id"));
  EXPECT_OK_AND_ASSIGN(fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, InitializeWithWorkloadIdentityCredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(options.ConfigureWorkloadIdentityCredential());
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, OptionsCompare) {
  AzureOptions options;
  EXPECT_TRUE(options.Equals(options));
}

struct PreexistingData {
 public:
  using RNG = std::mt19937_64;

 public:
  const std::string container_name;
  static constexpr char const* kObjectName = "test-object-name";

  static constexpr char const* kLoremIpsum = R"""(
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
culpa qui officia deserunt mollit anim id est laborum.
)""";

 public:
  explicit PreexistingData(RNG& rng) : container_name{RandomContainerName(rng)} {}

  // Creates a path by concatenating the container name and the stem.
  std::string ContainerPath(std::string_view stem) const {
    return ConcatAbstractPath(container_name, stem);
  }

  std::string ObjectPath() const { return ContainerPath(kObjectName); }
  std::string NotFoundObjectPath() const { return ContainerPath("not-found"); }

  std::string RandomDirectoryPath(RNG& rng) const {
    return ContainerPath(RandomChars(32, rng));
  }

  // Utilities
  static std::string RandomContainerName(RNG& rng) { return RandomChars(32, rng); }

  static std::string RandomChars(int count, RNG& rng) {
    auto const fillers = std::string("abcdefghijlkmnopqrstuvwxyz0123456789");
    std::uniform_int_distribution<int> d(0, static_cast<int>(fillers.size()) - 1);
    std::string s;
    std::generate_n(std::back_inserter(s), count, [&] { return fillers[d(rng)]; });
    return s;
  }

  static int RandomIndex(int end, RNG& rng) {
    return std::uniform_int_distribution<int>(0, end - 1)(rng);
  }

  static std::string RandomLine(int lineno, int width, RNG& rng) {
    auto line = std::to_string(lineno) + ":    ";
    line += RandomChars(width - static_cast<int>(line.size()) - 1, rng);
    line += '\n';
    return line;
  }
};

class TestAzureFileSystem : public ::testing::Test {
 protected:
  // Set in constructor
  std::mt19937_64 rng_;

  // Set in SetUp()
  int64_t debug_log_start_ = 0;
  bool set_up_succeeded_ = false;
  AzureOptions options_;

  std::shared_ptr<AzureFileSystem> fs_dont_use_directly_;  // use fs()
  std::unique_ptr<Blobs::BlobServiceClient> blob_service_client_;
  std::unique_ptr<DataLake::DataLakeServiceClient> datalake_service_client_;

 public:
  TestAzureFileSystem() : rng_(std::random_device()()) {}

  virtual Result<BaseAzureEnv*> GetAzureEnv() const = 0;
  virtual HNSSupport CachedHNSSupport(const BaseAzureEnv& env) const = 0;

  FileSystem* fs(HNSSupport cached_hns_support) const {
    auto* fs_ptr = fs_dont_use_directly_.get();
    fs_ptr->ForceCachedHierarchicalNamespaceSupport(static_cast<int>(cached_hns_support));
    return fs_ptr;
  }

  FileSystem* fs() const {
    EXPECT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    return fs(CachedHNSSupport(*env));
  }

  static Result<AzureOptions> MakeOptions(BaseAzureEnv* env) {
    AzureOptions options;
    options.account_name = env->account_name();
    switch (env->backend()) {
      case AzureBackend::kAzurite:
        options.blob_storage_authority = "127.0.0.1:10000";
        options.dfs_storage_authority = "127.0.0.1:10000";
        options.blob_storage_scheme = "http";
        options.dfs_storage_scheme = "http";
        break;
      case AzureBackend::kAzure:
        // Use the default values
        break;
    }
    ARROW_EXPECT_OK(options.ConfigureAccountKeyCredential(env->account_key()));
    return options;
  }

  void SetUp() override {
    auto make_options = [this]() -> Result<AzureOptions> {
      ARROW_ASSIGN_OR_RAISE(auto env, GetAzureEnv());
      EXPECT_THAT(env, NotNull());
      ARROW_ASSIGN_OR_RAISE(debug_log_start_, env->GetDebugLogSize());
      return MakeOptions(env);
    };
    auto options_res = make_options();
    if (options_res.status().IsCancelled()) {
      GTEST_SKIP() << options_res.status().message();
    } else {
      EXPECT_OK_AND_ASSIGN(options_, options_res);
    }

    ASSERT_OK_AND_ASSIGN(fs_dont_use_directly_, AzureFileSystem::Make(options_));
    EXPECT_OK_AND_ASSIGN(blob_service_client_, options_.MakeBlobServiceClient());
    EXPECT_OK_AND_ASSIGN(datalake_service_client_, options_.MakeDataLakeServiceClient());
    set_up_succeeded_ = true;
  }

  void TearDown() override {
    if (set_up_succeeded_) {
      auto containers = blob_service_client_->ListBlobContainers();
      for (auto container : containers.BlobContainers) {
        auto container_client =
            blob_service_client_->GetBlobContainerClient(container.Name);
        container_client.DeleteIfExists();
      }
    }
    if (HasFailure()) {
      // XXX: This may not include all logs in the target test because
      // Azurite doesn't flush debug logs immediately... You may want
      // to check the log manually...
      EXPECT_OK_AND_ASSIGN(auto env, GetAzureEnv());
      ARROW_IGNORE_EXPR(env->DumpDebugLog(debug_log_start_));
    }
  }

  Blobs::BlobContainerClient CreateContainer(const std::string& name) {
    auto container_client = blob_service_client_->GetBlobContainerClient(name);
    (void)container_client.CreateIfNotExists();
    return container_client;
  }

  Blobs::BlobClient CreateBlob(Blobs::BlobContainerClient& container_client,
                               const std::string& name, const std::string& data = "") {
    auto blob_client = container_client.GetBlockBlobClient(name);
    (void)blob_client.UploadFrom(reinterpret_cast<const uint8_t*>(data.data()),
                                 data.size());
    return blob_client;
  }

  Blobs::Models::BlobProperties GetBlobProperties(const std::string& container_name,
                                                  const std::string& blob_name) {
    return blob_service_client_->GetBlobContainerClient(container_name)
        .GetBlobClient(blob_name)
        .GetProperties()
        .Value;
  }

  void UploadLines(const std::vector<std::string>& lines, const std::string& path,
                   int total_size) {
    ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(path, {}));
    const auto all_lines = std::accumulate(lines.begin(), lines.end(), std::string(""));
    ASSERT_OK(output->Write(all_lines));
    ASSERT_OK(output->Close());
  }

  PreexistingData SetUpPreexistingData() {
    PreexistingData data(rng_);
    auto container_client = CreateContainer(data.container_name);
    CreateBlob(container_client, data.kObjectName, PreexistingData::kLoremIpsum);
    return data;
  }

  struct HierarchicalPaths {
    std::string container;
    std::string directory;
    std::vector<std::string> sub_paths;
  };

  // Need to use "void" as the return type to use ASSERT_* in this method.
  void CreateHierarchicalData(HierarchicalPaths* paths) {
    auto data = SetUpPreexistingData();
    const auto directory_path = data.RandomDirectoryPath(rng_);
    const auto sub_directory_path = ConcatAbstractPath(directory_path, "new-sub");
    const auto sub_blob_path = ConcatAbstractPath(sub_directory_path, "sub.txt");
    const auto top_blob_path = ConcatAbstractPath(directory_path, "top.txt");
    ASSERT_OK(fs()->CreateDir(sub_directory_path, true));
    ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(sub_blob_path));
    ASSERT_OK(output->Write(std::string_view("sub")));
    ASSERT_OK(output->Close());
    ASSERT_OK_AND_ASSIGN(output, fs()->OpenOutputStream(top_blob_path));
    ASSERT_OK(output->Write(std::string_view("top")));
    ASSERT_OK(output->Close());

    AssertFileInfo(fs(), data.container_name, FileType::Directory);
    AssertFileInfo(fs(), directory_path, FileType::Directory);
    AssertFileInfo(fs(), sub_directory_path, FileType::Directory);
    AssertFileInfo(fs(), sub_blob_path, FileType::File);
    AssertFileInfo(fs(), top_blob_path, FileType::File);

    paths->container = data.container_name;
    paths->directory = directory_path;
    paths->sub_paths = {
        sub_directory_path,
        sub_blob_path,
        top_blob_path,
    };
  }

  char const* kSubData = "sub data";
  char const* kSomeData = "some data";
  char const* kOtherData = "other data";

  void SetUpSmallFileSystemTree() {
    // Set up test containers
    CreateContainer("empty-container");
    auto container = CreateContainer("container");

    CreateBlob(container, "emptydir/");
    CreateBlob(container, "somedir/subdir/subfile", kSubData);
    CreateBlob(container, "somefile", kSomeData);
    // Add an explicit marker for a non-empty directory.
    CreateBlob(container, "otherdir/1/2/");
    // otherdir/{1/,2/,3/} are implicitly assumed to exist because of
    // the otherdir/1/2/3/otherfile blob.
    CreateBlob(container, "otherdir/1/2/3/otherfile", kOtherData);
  }

  void AssertInfoAllContainersRecursive(const std::vector<FileInfo>& infos) {
    ASSERT_EQ(infos.size(), 12);
    AssertFileInfo(infos[0], "container", FileType::Directory);
    AssertFileInfo(infos[1], "container/emptydir", FileType::Directory);
    AssertFileInfo(infos[2], "container/otherdir", FileType::Directory);
    AssertFileInfo(infos[3], "container/otherdir/1", FileType::Directory);
    AssertFileInfo(infos[4], "container/otherdir/1/2", FileType::Directory);
    AssertFileInfo(infos[5], "container/otherdir/1/2/3", FileType::Directory);
    AssertFileInfo(infos[6], "container/otherdir/1/2/3/otherfile", FileType::File,
                   strlen(kOtherData));
    AssertFileInfo(infos[7], "container/somedir", FileType::Directory);
    AssertFileInfo(infos[8], "container/somedir/subdir", FileType::Directory);
    AssertFileInfo(infos[9], "container/somedir/subdir/subfile", FileType::File,
                   strlen(kSubData));
    AssertFileInfo(infos[10], "container/somefile", FileType::File, strlen(kSomeData));
    AssertFileInfo(infos[11], "empty-container", FileType::Directory);
  }

  bool WithHierarchicalNamespace() const {
    EXPECT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    return env->WithHierarchicalNamespace();
  }

  constexpr static const char* const kSubmitBatchBugMessage =
      "This test is affected by an Azurite issue: "
      "https://github.com/Azure/Azurite/pull/2302";

  /// Azurite has a bug that causes BlobContainerClient::SubmitBatch to fail on macOS.
  /// SubmitBatch is used by:
  ///  - AzureFileSystem::DeleteDir
  ///  - AzureFileSystem::DeleteDirContents
  bool HasSubmitBatchBug() const {
#ifdef __APPLE__
    EXPECT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    return env->backend() == AzureBackend::kAzurite;
#else
    return false;
#endif
  }

  // Tests that are called from more than one implementation of TestAzureFileSystem

  void TestDetectHierarchicalNamespace(bool trip_up_azurite);
  void TestDetectHierarchicalNamespaceOnMissingContainer();

  void TestGetFileInfoOfRoot() {
    AssertFileInfo(fs(), "", FileType::Directory);

    // URI
    ASSERT_RAISES(Invalid, fs()->GetFileInfo("abfs://"));
  }

  void TestGetFileInfoOnExistingContainer() {
    auto data = SetUpPreexistingData();
    AssertFileInfo(fs(), data.container_name, FileType::Directory);
    AssertFileInfo(fs(), data.container_name + "/", FileType::Directory);
    auto props = GetBlobProperties(data.container_name, data.kObjectName);
    AssertFileInfo(fs(), data.ObjectPath(), FileType::File,
                   std::chrono::system_clock::time_point{props.LastModified},
                   static_cast<int64_t>(props.BlobSize));
    AssertFileInfo(fs(), data.NotFoundObjectPath(), FileType::NotFound);
    AssertFileInfo(fs(), data.ObjectPath() + "/", FileType::NotFound);
    AssertFileInfo(fs(), data.NotFoundObjectPath() + "/", FileType::NotFound);

    // URIs
    ASSERT_RAISES(Invalid, fs()->GetFileInfo("abfs://" + data.container_name));
    ASSERT_RAISES(Invalid, fs()->GetFileInfo("abfs://" + std::string{data.kObjectName}));
    ASSERT_RAISES(Invalid, fs()->GetFileInfo("abfs://" + data.ObjectPath()));
  }

  void TestGetFileInfoOnMissingContainer() {
    auto data = SetUpPreexistingData();
    AssertFileInfo(fs(), "nonexistent", FileType::NotFound);
    AssertFileInfo(fs(), "nonexistent/object", FileType::NotFound);
    AssertFileInfo(fs(), "nonexistent/object/", FileType::NotFound);
  }

  void TestGetFileInfoObjectWithNestedStructure();

  void TestCreateDirOnRoot() {
    auto dir1 = PreexistingData::RandomContainerName(rng_);
    auto dir2 = PreexistingData::RandomContainerName(rng_);

    AssertFileInfo(fs(), dir1, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(dir1, false));
    AssertFileInfo(fs(), dir1, FileType::Directory);

    AssertFileInfo(fs(), dir2, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(dir2, true));
    AssertFileInfo(fs(), dir1, FileType::Directory);

    // Should not fail if the directory already exists.
    ASSERT_OK(fs()->CreateDir(dir1, false));
    ASSERT_OK(fs()->CreateDir(dir1, true));
    AssertFileInfo(fs(), dir1, FileType::Directory);
  }

  void TestCreateDirOnExistingContainer() {
    auto data = SetUpPreexistingData();
    auto dir1 = data.RandomDirectoryPath(rng_);
    auto dir2 = data.RandomDirectoryPath(rng_);

    AssertFileInfo(fs(), dir1, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(dir1, /*recursive=*/false));
    AssertFileInfo(fs(), dir1, FileType::Directory);

    AssertFileInfo(fs(), dir2, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(dir2, /*recursive=*/true));
    AssertFileInfo(fs(), dir2, FileType::Directory);

    auto subdir1 = ConcatAbstractPath(dir1, "subdir");
    auto subdir2 = ConcatAbstractPath(dir2, "subdir");
    AssertFileInfo(fs(), subdir1, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(subdir1, /*recursive=*/false));
    AssertFileInfo(fs(), subdir1, FileType::Directory);
    AssertFileInfo(fs(), subdir2, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(subdir2, /*recursive=*/true));
    AssertFileInfo(fs(), subdir2, FileType::Directory);

    auto dir3 = data.RandomDirectoryPath(rng_);
    AssertFileInfo(fs(), dir3, FileType::NotFound);
    auto subdir3 = ConcatAbstractPath(dir3, "subdir");
    AssertFileInfo(fs(), subdir3, FileType::NotFound);
    // Creating subdir3 with recursive=false should fail.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Path does not exist '" + dir3 + "'"),
        fs()->CreateDir(subdir3, /*recursive=*/false));
    AssertFileInfo(fs(), dir3, FileType::NotFound);
    AssertFileInfo(fs(), subdir3, FileType::NotFound);
    // Creating subdir3 with recursive=true should work.
    ASSERT_OK(fs()->CreateDir(subdir3, /*recursive=*/true));
    AssertFileInfo(fs(), dir3, FileType::Directory);
    AssertFileInfo(fs(), subdir3, FileType::Directory);

    auto dir4 = data.RandomDirectoryPath(rng_);
    auto subdir4 = ConcatAbstractPath(dir4, "subdir4");
    auto subdir5 = ConcatAbstractPath(dir4, "subdir4/subdir5");
    // Creating subdir4 with recursive=false should fail.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Path does not exist '" + dir4 + "'"),
        fs()->CreateDir(subdir4, /*recursive=*/false));
    AssertFileInfo(fs(), dir4, FileType::NotFound);
    AssertFileInfo(fs(), subdir4, FileType::NotFound);
    // Creating subdir5 with recursive=false should fail.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Path does not exist '" + subdir4 + "'"),
        fs()->CreateDir(subdir5, /*recursive=*/false));
    AssertFileInfo(fs(), dir4, FileType::NotFound);
    AssertFileInfo(fs(), subdir4, FileType::NotFound);
    AssertFileInfo(fs(), subdir5, FileType::NotFound);
    // Creating subdir5 with recursive=true should work.
    ASSERT_OK(fs()->CreateDir(subdir5, /*recursive=*/true));
    AssertFileInfo(fs(), dir4, FileType::Directory);
    AssertFileInfo(fs(), subdir4, FileType::Directory);
    AssertFileInfo(fs(), subdir5, FileType::Directory);
  }

  void TestCreateDirOnMissingContainer() {
    auto container1 = PreexistingData::RandomContainerName(rng_);
    auto container2 = PreexistingData::RandomContainerName(rng_);
    AssertFileInfo(fs(), container1, FileType::NotFound);
    AssertFileInfo(fs(), container2, FileType::NotFound);

    auto dir1 = ConcatAbstractPath(container1, "dir");
    AssertFileInfo(fs(), dir1, FileType::NotFound);
    // Creating dir1 with recursive=false should fail.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Path does not exist '" + container1 + "'"),
        fs()->CreateDir(dir1, /*recursive=*/false));
    AssertFileInfo(fs(), container1, FileType::NotFound);
    AssertFileInfo(fs(), dir1, FileType::NotFound);
    // Creating dir1 with recursive=true should work.
    ASSERT_OK(fs()->CreateDir(dir1, /*recursive=*/true));
    AssertFileInfo(fs(), container1, FileType::Directory);
    AssertFileInfo(fs(), dir1, FileType::Directory);

    auto dir2 = ConcatAbstractPath(container2, "dir");
    auto subdir2 = ConcatAbstractPath(dir2, "subdir2");
    auto subdir3 = ConcatAbstractPath(dir2, "subdir2/subdir3");
    // Creating dir2 with recursive=false should fail.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Path does not exist '" + container2 + "'"),
        fs()->CreateDir(dir2, /*recursive=*/false));
    AssertFileInfo(fs(), container2, FileType::NotFound);
    AssertFileInfo(fs(), dir2, FileType::NotFound);
    // Creating subdir2 with recursive=false should fail.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Path does not exist '" + dir2 + "'"),
        fs()->CreateDir(subdir2, /*recursive=*/false));
    AssertFileInfo(fs(), container2, FileType::NotFound);
    AssertFileInfo(fs(), dir2, FileType::NotFound);
    AssertFileInfo(fs(), subdir2, FileType::NotFound);
    // Creating subdir3 with recursive=false should fail.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Path does not exist '" + subdir2 + "'"),
        fs()->CreateDir(subdir3, /*recursive=*/false));
    AssertFileInfo(fs(), container2, FileType::NotFound);
    AssertFileInfo(fs(), dir2, FileType::NotFound);
    AssertFileInfo(fs(), subdir2, FileType::NotFound);
    AssertFileInfo(fs(), subdir3, FileType::NotFound);
    // Creating subdir3 with recursive=true should work.
    ASSERT_OK(fs()->CreateDir(subdir3, /*recursive=*/true));
    AssertFileInfo(fs(), container2, FileType::Directory);
    AssertFileInfo(fs(), dir2, FileType::Directory);
    AssertFileInfo(fs(), subdir2, FileType::Directory);
    AssertFileInfo(fs(), subdir3, FileType::Directory);
  }

  void TestDeleteDirSuccessEmpty() {
    if (HasSubmitBatchBug()) {
      GTEST_SKIP() << kSubmitBatchBugMessage;
    }
    auto data = SetUpPreexistingData();
    const auto directory_path = data.RandomDirectoryPath(rng_);

    AssertFileInfo(fs(), directory_path, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(directory_path, true));
    AssertFileInfo(fs(), directory_path, FileType::Directory);
    ASSERT_OK(fs()->DeleteDir(directory_path));
    AssertFileInfo(fs(), directory_path, FileType::NotFound);
  }

  void TestDeleteDirFailureNonexistent() {
    auto data = SetUpPreexistingData();
    const auto path = data.RandomDirectoryPath(rng_);
    ASSERT_RAISES(IOError, fs()->DeleteDir(path));
  }

  void TestDeleteDirSuccessHaveBlob() {
    if (HasSubmitBatchBug()) {
      GTEST_SKIP() << kSubmitBatchBugMessage;
    }
    auto data = SetUpPreexistingData();
    const auto directory_path = data.RandomDirectoryPath(rng_);
    const auto blob_path = ConcatAbstractPath(directory_path, "hello.txt");
    ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(blob_path));
    ASSERT_OK(output->Write("hello"));
    ASSERT_OK(output->Close());
    AssertFileInfo(fs(), blob_path, FileType::File);
    ASSERT_OK(fs()->DeleteDir(directory_path));
    AssertFileInfo(fs(), blob_path, FileType::NotFound);
  }

  void TestDeleteDirSuccessHaveDirectory() {
    if (HasSubmitBatchBug()) {
      GTEST_SKIP() << kSubmitBatchBugMessage;
    }
    auto data = SetUpPreexistingData();
    const auto parent = data.RandomDirectoryPath(rng_);
    const auto path = ConcatAbstractPath(parent, "new-sub");
    ASSERT_OK(fs()->CreateDir(path, true));
    AssertFileInfo(fs(), path, FileType::Directory);
    AssertFileInfo(fs(), parent, FileType::Directory);
    ASSERT_OK(fs()->DeleteDir(parent));
    AssertFileInfo(fs(), path, FileType::NotFound);
    AssertFileInfo(fs(), parent, FileType::NotFound);
  }

  void TestDeleteDirContentsSuccessExist() {
    if (HasSubmitBatchBug()) {
      GTEST_SKIP() << kSubmitBatchBugMessage;
    }
    auto preexisting_data = SetUpPreexistingData();
    HierarchicalPaths paths;
    CreateHierarchicalData(&paths);
    ASSERT_OK(fs()->DeleteDirContents(paths.directory));
    AssertFileInfo(fs(), paths.directory, FileType::Directory);
    for (const auto& sub_path : paths.sub_paths) {
      AssertFileInfo(fs(), sub_path, FileType::NotFound);
    }
  }

  void TestDeleteDirContentsSuccessNonexistent() {
    if (HasSubmitBatchBug()) {
      GTEST_SKIP() << kSubmitBatchBugMessage;
    }
    auto data = SetUpPreexistingData();
    const auto directory_path = data.RandomDirectoryPath(rng_);
    ASSERT_OK(fs()->DeleteDirContents(directory_path, true));
    AssertFileInfo(fs(), directory_path, FileType::NotFound);
  }

  void TestDeleteDirContentsFailureNonexistent() {
    auto data = SetUpPreexistingData();
    const auto directory_path = data.RandomDirectoryPath(rng_);
    ASSERT_RAISES(IOError, fs()->DeleteDirContents(directory_path, false));
  }
};

void TestAzureFileSystem::TestDetectHierarchicalNamespace(bool trip_up_azurite) {
  EXPECT_OK_AND_ASSIGN(auto env, GetAzureEnv());
  if (trip_up_azurite && env->backend() != AzureBackend::kAzurite) {
    return;
  }

  auto data = SetUpPreexistingData();
  if (trip_up_azurite) {
    // Azurite causes GetDirectoryClient("/") to throw a std::out_of_range
    // exception when a "/" blob exists, so we exercise that code path.
    auto container_client =
        blob_service_client_->GetBlobContainerClient(data.container_name);
    CreateBlob(container_client, "/");
  }

  auto adlfs_client = datalake_service_client_->GetFileSystemClient(data.container_name);
  ASSERT_OK_AND_ASSIGN(auto hns_support, internal::CheckIfHierarchicalNamespaceIsEnabled(
                                             adlfs_client, options_));
  if (env->WithHierarchicalNamespace()) {
    ASSERT_EQ(hns_support, HNSSupport::kEnabled);
  } else {
    ASSERT_EQ(hns_support, HNSSupport::kDisabled);
  }
}

void TestAzureFileSystem::TestDetectHierarchicalNamespaceOnMissingContainer() {
  auto container_name = PreexistingData::RandomContainerName(rng_);
  auto adlfs_client = datalake_service_client_->GetFileSystemClient(container_name);
  ASSERT_OK_AND_ASSIGN(auto hns_support, internal::CheckIfHierarchicalNamespaceIsEnabled(
                                             adlfs_client, options_));
  EXPECT_OK_AND_ASSIGN(auto env, GetAzureEnv());
  switch (env->backend()) {
    case AzureBackend::kAzurite:
      ASSERT_EQ(hns_support, HNSSupport::kDisabled);
      break;
    case AzureBackend::kAzure:
      if (env->WithHierarchicalNamespace()) {
        ASSERT_EQ(hns_support, HNSSupport::kContainerNotFound);
      } else {
        ASSERT_EQ(hns_support, HNSSupport::kDisabled);
      }
      break;
  }
}

void TestAzureFileSystem::TestGetFileInfoObjectWithNestedStructure() {
  auto data = SetUpPreexistingData();
  // Adds detailed tests to handle cases of different edge cases
  // with directory naming conventions (e.g. with and without slashes).
  const std::string kObjectName = "test-object-dir/some_other_dir/another_dir/foo";
  ASSERT_OK_AND_ASSIGN(auto output,
                       fs()->OpenOutputStream(data.ContainerPath(kObjectName),
                                              /*metadata=*/{}));
  const std::string_view lorem_ipsum(PreexistingData::kLoremIpsum);
  ASSERT_OK(output->Write(lorem_ipsum));
  ASSERT_OK(output->Close());

  // 0 is immediately after "/" lexicographically, ensure that this doesn't
  // cause unexpected issues.
  ASSERT_OK_AND_ASSIGN(output, fs()->OpenOutputStream(
                                   data.ContainerPath("test-object-dir/some_other_dir0"),
                                   /*metadata=*/{}));
  ASSERT_OK(output->Write(lorem_ipsum));
  ASSERT_OK(output->Close());
  ASSERT_OK_AND_ASSIGN(output,
                       fs()->OpenOutputStream(data.ContainerPath(kObjectName + "0"),
                                              /*metadata=*/{}));
  ASSERT_OK(output->Write(lorem_ipsum));
  ASSERT_OK(output->Close());

  AssertFileInfo(fs(), data.ContainerPath(kObjectName), FileType::File);
  AssertFileInfo(fs(), data.ContainerPath(kObjectName) + "/", FileType::NotFound);
  AssertFileInfo(fs(), data.ContainerPath("test-object-dir"), FileType::Directory);
  AssertFileInfo(fs(), data.ContainerPath("test-object-dir") + "/", FileType::Directory);
  AssertFileInfo(fs(), data.ContainerPath("test-object-dir/some_other_dir"),
                 FileType::Directory);
  AssertFileInfo(fs(), data.ContainerPath("test-object-dir/some_other_dir") + "/",
                 FileType::Directory);

  AssertFileInfo(fs(), data.ContainerPath("test-object-di"), FileType::NotFound);
  AssertFileInfo(fs(), data.ContainerPath("test-object-dir/some_other_di"),
                 FileType::NotFound);

  if (WithHierarchicalNamespace()) {
    datalake_service_client_->GetFileSystemClient(data.container_name)
        .GetDirectoryClient("test-empty-object-dir")
        .Create();

    AssertFileInfo(fs(), data.ContainerPath("test-empty-object-dir"),
                   FileType::Directory);
  }
}

template <class AzureEnv, bool HNSSupportShouldBeKnown = false>
struct TestingScenario {
  using AzureEnvClass = AzureEnv;
  static constexpr bool kHNSSupportShouldBeKnown = HNSSupportShouldBeKnown;
};

template <class TestingScenario>
class AzureFileSystemTestImpl : public TestAzureFileSystem {
 public:
  using AzureEnvClass = typename TestingScenario::AzureEnvClass;

  using TestAzureFileSystem::TestAzureFileSystem;

  Result<BaseAzureEnv*> GetAzureEnv() const final { return AzureEnvClass::GetInstance(); }

  /// \brief HNSSupport value that should be assumed as the cached
  /// HNSSupport on every fs()->Operation(...) call in tests.
  ///
  /// If TestingScenario::kHNSSupportShouldBeKnown is true, this value
  /// will be HNSSupport::kEnabled or HNSSupport::kDisabled, depending
  /// on the environment. Otherwise, this value will be HNSSupport::kUnknown.
  ///
  /// This ensures all the branches in the AzureFileSystem code operations are tested.
  /// For instance, many operations executed on a missing container, wouldn't
  /// get a HNSSupport::kContainerNotFound error if the cached HNSSupport was
  /// already known due to a previous operation that cached the HNSSupport value.
  HNSSupport CachedHNSSupport(const BaseAzureEnv& env) const final {
    if constexpr (TestingScenario::kHNSSupportShouldBeKnown) {
      return env.WithHierarchicalNamespace() ? HNSSupport::kEnabled
                                             : HNSSupport::kDisabled;
    } else {
      return HNSSupport::kUnknown;
    }
  }
};

// How to enable the non-Azurite tests:
//
// You need an Azure account. You should be able to create a free account [1].
// Through the portal Web UI, you should create a storage account [2].
//
// A few suggestions on configuration:
//
// * Use Standard general-purpose v2 not premium
// * Use LRS redundancy
// * Set the default access tier to hot
// * SFTP, NFS and file shares are not required.
//
// You must not enable Hierarchical Namespace on the storage account used for
// TestAzureFlatNSFileSystem, but you must enable it on the storage account
// used for TestAzureHierarchicalNSFileSystem.
//
// The credentials should be placed in the correct environment variables:
//
// * AZURE_FLAT_NAMESPACE_ACCOUNT_NAME
// * AZURE_FLAT_NAMESPACE_ACCOUNT_KEY
// * AZURE_HIERARCHICAL_NAMESPACE_ACCOUNT_NAME
// * AZURE_HIERARCHICAL_NAMESPACE_ACCOUNT_KEY
//
// [1]: https://azure.microsoft.com/en-gb/free/
// [2]:
// https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account
using TestAzureFlatNSFileSystem =
    AzureFileSystemTestImpl<TestingScenario<AzureFlatNSEnv>>;
using TestAzureHierarchicalNSFileSystem =
    AzureFileSystemTestImpl<TestingScenario<AzureHierarchicalNSEnv>>;
using TestAzuriteFileSystem = AzureFileSystemTestImpl<TestingScenario<AzuriteEnv>>;

// Tests using all the 3 environments (Azurite, Azure w/o HNS (flat), Azure w/ HNS).
template <class TestingScenario>
using TestAzureFileSystemOnAllEnvs = AzureFileSystemTestImpl<TestingScenario>;

using AllEnvironments =
    ::testing::Types<TestingScenario<AzuriteEnv>, TestingScenario<AzureFlatNSEnv>,
                     TestingScenario<AzureHierarchicalNSEnv>>;

TYPED_TEST_SUITE(TestAzureFileSystemOnAllEnvs, AllEnvironments);

TYPED_TEST(TestAzureFileSystemOnAllEnvs, DetectHierarchicalNamespace) {
  this->TestDetectHierarchicalNamespace(true);
  this->TestDetectHierarchicalNamespace(false);
}

TYPED_TEST(TestAzureFileSystemOnAllEnvs, DetectHierarchicalNamespaceOnMissingContainer) {
  this->TestDetectHierarchicalNamespaceOnMissingContainer();
}

TYPED_TEST(TestAzureFileSystemOnAllEnvs, GetFileInfoOfRoot) {
  this->TestGetFileInfoOfRoot();
}

TYPED_TEST(TestAzureFileSystemOnAllEnvs, CreateDirWithEmptyPath) {
  ASSERT_RAISES(Invalid, this->fs()->CreateDir("", false));
}

TYPED_TEST(TestAzureFileSystemOnAllEnvs, CreateDirOnRoot) { this->TestCreateDirOnRoot(); }

// Tests using all the 3 environments (Azurite, Azure w/o HNS (flat), Azure w/ HNS)
// combined with the two scenarios for AzureFileSystem::cached_hns_support_ -- unknown and
// known according to the environment.
template <class TestingScenario>
using TestAzureFileSystemOnAllScenarios = AzureFileSystemTestImpl<TestingScenario>;

using AllScenarios = ::testing::Types<
    TestingScenario<AzuriteEnv, true>, TestingScenario<AzuriteEnv, false>,
    TestingScenario<AzureFlatNSEnv, true>, TestingScenario<AzureFlatNSEnv, false>,
    TestingScenario<AzureHierarchicalNSEnv, true>,
    TestingScenario<AzureHierarchicalNSEnv, false>>;

TYPED_TEST_SUITE(TestAzureFileSystemOnAllScenarios, AllScenarios);

TYPED_TEST(TestAzureFileSystemOnAllScenarios, GetFileInfoOnExistingContainer) {
  this->TestGetFileInfoOnExistingContainer();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, GetFileInfoOnMissingContainer) {
  this->TestGetFileInfoOnMissingContainer();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, GetFileInfoObjectWithNestedStructure) {
  this->TestGetFileInfoObjectWithNestedStructure();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, CreateDirOnExistingContainer) {
  this->TestCreateDirOnExistingContainer();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, CreateDirOnMissingContainer) {
  this->TestCreateDirOnMissingContainer();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirSuccessEmpty) {
  this->TestDeleteDirSuccessEmpty();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirFailureNonexistent) {
  this->TestDeleteDirFailureNonexistent();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirSuccessHaveBlob) {
  this->TestDeleteDirSuccessHaveBlob();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirSuccessHaveDirectory) {
  this->TestDeleteDirSuccessHaveDirectory();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirContentsSuccessExist) {
  this->TestDeleteDirContentsSuccessExist();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirContentsSuccessNonexistent) {
  this->TestDeleteDirContentsSuccessNonexistent();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirContentsFailureNonexistent) {
  this->TestDeleteDirContentsFailureNonexistent();
}

// Tests using Azurite (the local Azure emulator)

TEST_F(TestAzuriteFileSystem, GetFileInfoSelector) {
  SetUpSmallFileSystemTree();

  FileSelector select;
  std::vector<FileInfo> infos;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  ASSERT_EQ(infos, SortedInfos(infos));
  AssertFileInfo(infos[0], "container", FileType::Directory);
  AssertFileInfo(infos[1], "empty-container", FileType::Directory);

  // Empty container
  select.base_dir = "empty-container";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  // Nonexistent container
  select.base_dir = "nonexistent-container";
  ASSERT_RAISES(IOError, fs()->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;
  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos, SortedInfos(infos));
  ASSERT_EQ(infos.size(), 4);
  AssertFileInfo(infos[0], "container/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "container/otherdir", FileType::Directory);
  AssertFileInfo(infos[2], "container/somedir", FileType::Directory);
  AssertFileInfo(infos[3], "container/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "container/emptydir";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  // Non-empty "directories"
  select.base_dir = "container/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "container/somedir/subdir", FileType::Directory);
  select.base_dir = "container/somedir/subdir";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "container/somedir/subdir/subfile", FileType::File, 8);
  // Nonexistent
  select.base_dir = "container/nonexistent";
  ASSERT_RAISES(IOError, fs()->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // Trailing slashes
  select.base_dir = "empty-container/";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.base_dir = "nonexistent-container/";
  ASSERT_RAISES(IOError, fs()->GetFileInfo(select));
  select.base_dir = "container/";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos, SortedInfos(infos));
  ASSERT_EQ(infos.size(), 4);
}

TEST_F(TestAzuriteFileSystem, GetFileInfoSelectorRecursive) {
  SetUpSmallFileSystemTree();

  FileSelector select;
  select.recursive = true;

  std::vector<FileInfo> infos;
  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 12);
  ASSERT_EQ(infos, SortedInfos(infos));
  AssertInfoAllContainersRecursive(infos);

  // Empty container
  select.base_dir = "empty-container";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos, SortedInfos(infos));
  ASSERT_EQ(infos.size(), 10);
  AssertFileInfo(infos[0], "container/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "container/otherdir", FileType::Directory);
  AssertFileInfo(infos[2], "container/otherdir/1", FileType::Directory);
  AssertFileInfo(infos[3], "container/otherdir/1/2", FileType::Directory);
  AssertFileInfo(infos[4], "container/otherdir/1/2/3", FileType::Directory);
  AssertFileInfo(infos[5], "container/otherdir/1/2/3/otherfile", FileType::File, 10);
  AssertFileInfo(infos[6], "container/somedir", FileType::Directory);
  AssertFileInfo(infos[7], "container/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[8], "container/somedir/subdir/subfile", FileType::File, 8);
  AssertFileInfo(infos[9], "container/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "container/emptydir";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // Non-empty "directories"
  select.base_dir = "container/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos, SortedInfos(infos));
  ASSERT_EQ(infos.size(), 2);
  AssertFileInfo(infos[0], "container/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[1], "container/somedir/subdir/subfile", FileType::File, 8);

  select.base_dir = "container/otherdir";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos, SortedInfos(infos));
  ASSERT_EQ(infos.size(), 4);
  AssertFileInfo(infos[0], "container/otherdir/1", FileType::Directory);
  AssertFileInfo(infos[1], "container/otherdir/1/2", FileType::Directory);
  AssertFileInfo(infos[2], "container/otherdir/1/2/3", FileType::Directory);
  AssertFileInfo(infos[3], "container/otherdir/1/2/3/otherfile", FileType::File, 10);
}

TEST_F(TestAzuriteFileSystem, GetFileInfoSelectorExplicitImplicitDirDedup) {
  {
    auto container = CreateContainer("container");
    CreateBlob(container, "mydir/emptydir1/");
    CreateBlob(container, "mydir/emptydir2/");
    CreateBlob(container, "mydir/nonemptydir1/");  // explicit dir marker
    CreateBlob(container, "mydir/nonemptydir1/somefile", kSomeData);
    CreateBlob(container, "mydir/nonemptydir2/somefile", kSomeData);
  }
  std::vector<FileInfo> infos;

  FileSelector select;  // non-recursive
  select.base_dir = "container";

  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  ASSERT_EQ(infos, SortedInfos(infos));
  AssertFileInfo(infos[0], "container/mydir", FileType::Directory);

  select.base_dir = "container/mydir";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 4);
  ASSERT_EQ(infos, SortedInfos(infos));
  AssertFileInfo(infos[0], "container/mydir/emptydir1", FileType::Directory);
  AssertFileInfo(infos[1], "container/mydir/emptydir2", FileType::Directory);
  AssertFileInfo(infos[2], "container/mydir/nonemptydir1", FileType::Directory);
  AssertFileInfo(infos[3], "container/mydir/nonemptydir2", FileType::Directory);

  select.base_dir = "container/mydir/emptydir1";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  select.base_dir = "container/mydir/emptydir2";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  select.base_dir = "container/mydir/nonemptydir1";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "container/mydir/nonemptydir1/somefile", FileType::File);

  select.base_dir = "container/mydir/nonemptydir2";
  ASSERT_OK_AND_ASSIGN(infos, fs()->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "container/mydir/nonemptydir2/somefile", FileType::File);
}

TEST_F(TestAzuriteFileSystem, CreateDirFailureDirectoryWithMissingContainer) {
  const auto path = std::string("not-a-container/new-directory");
  ASSERT_RAISES(IOError, fs()->CreateDir(path, false));
}

TEST_F(TestAzuriteFileSystem, CreateDirRecursiveFailureNoContainer) {
  ASSERT_RAISES(Invalid, fs()->CreateDir("", true));
}

TEST_F(TestAzuriteFileSystem, CreateDirUri) {
  ASSERT_RAISES(
      Invalid,
      fs()->CreateDir("abfs://" + PreexistingData::RandomContainerName(rng_), true));
}

TEST_F(TestAzuriteFileSystem, DeleteDirSuccessContainer) {
  const auto container_name = PreexistingData::RandomContainerName(rng_);
  ASSERT_OK(fs()->CreateDir(container_name));
  AssertFileInfo(fs(), container_name, FileType::Directory);
  ASSERT_OK(fs()->DeleteDir(container_name));
  AssertFileInfo(fs(), container_name, FileType::NotFound);
}

TEST_F(TestAzuriteFileSystem, DeleteDirSuccessNonexistent) {
  if (HasSubmitBatchBug()) {
    GTEST_SKIP() << kSubmitBatchBugMessage;
  }
  auto data = SetUpPreexistingData();
  const auto directory_path = data.RandomDirectoryPath(rng_);
  // DeleteDir() fails if the directory doesn't exist.
  ASSERT_RAISES(IOError, fs()->DeleteDir(directory_path));
  AssertFileInfo(fs(), directory_path, FileType::NotFound);
}

TEST_F(TestAzuriteFileSystem, DeleteDirSuccessHaveBlobs) {
  if (HasSubmitBatchBug()) {
    GTEST_SKIP() << kSubmitBatchBugMessage;
  }
  auto data = SetUpPreexistingData();
  const auto directory_path = data.RandomDirectoryPath(rng_);
  // We must use 257 or more blobs here to test pagination of ListBlobs().
  // Because we can't add 257 or more delete blob requests to one SubmitBatch().
  int64_t n_blobs = 257;
  for (int64_t i = 0; i < n_blobs; ++i) {
    const auto blob_path = ConcatAbstractPath(directory_path, std::to_string(i) + ".txt");
    ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(blob_path));
    ASSERT_OK(output->Write(std::string_view(std::to_string(i))));
    ASSERT_OK(output->Close());
    AssertFileInfo(fs(), blob_path, FileType::File);
  }
  ASSERT_OK(fs()->DeleteDir(directory_path));
  for (int64_t i = 0; i < n_blobs; ++i) {
    const auto blob_path = ConcatAbstractPath(directory_path, std::to_string(i) + ".txt");
    AssertFileInfo(fs(), blob_path, FileType::NotFound);
  }
}

TEST_F(TestAzuriteFileSystem, DeleteDirUri) {
  auto data = SetUpPreexistingData();
  ASSERT_RAISES(Invalid, fs()->DeleteDir("abfs://" + data.container_name + "/"));
}

TEST_F(TestAzuriteFileSystem, DeleteDirContentsSuccessContainer) {
  if (HasSubmitBatchBug()) {
    GTEST_SKIP() << kSubmitBatchBugMessage;
  }
  auto data = SetUpPreexistingData();
  HierarchicalPaths paths;
  CreateHierarchicalData(&paths);
  ASSERT_OK(fs()->DeleteDirContents(paths.container));
  AssertFileInfo(fs(), paths.container, FileType::Directory);
  AssertFileInfo(fs(), paths.directory, FileType::NotFound);
  for (const auto& sub_path : paths.sub_paths) {
    AssertFileInfo(fs(), sub_path, FileType::NotFound);
  }
}

TEST_F(TestAzuriteFileSystem, DeleteDirContentsSuccessDirectory) {
  if (HasSubmitBatchBug()) {
    GTEST_SKIP() << kSubmitBatchBugMessage;
  }
  auto data = SetUpPreexistingData();
  HierarchicalPaths paths;
  CreateHierarchicalData(&paths);
  ASSERT_OK(fs()->DeleteDirContents(paths.directory));
  AssertFileInfo(fs(), paths.directory, FileType::Directory);
  for (const auto& sub_path : paths.sub_paths) {
    AssertFileInfo(fs(), sub_path, FileType::NotFound);
  }
}

TEST_F(TestAzuriteFileSystem, DeleteDirContentsSuccessNonexistent) {
  this->TestDeleteDirContentsSuccessNonexistent();
}

TEST_F(TestAzuriteFileSystem, DeleteDirContentsFailureNonexistent) {
  this->TestDeleteDirContentsFailureNonexistent();
}

TEST_F(TestAzuriteFileSystem, CopyFileSuccessDestinationNonexistent) {
  auto data = SetUpPreexistingData();
  const auto destination_path = data.ContainerPath("copy-destionation");
  ASSERT_OK(fs()->CopyFile(data.ObjectPath(), destination_path));
  ASSERT_OK_AND_ASSIGN(auto info, fs()->GetFileInfo(destination_path));
  ASSERT_OK_AND_ASSIGN(auto stream, fs()->OpenInputStream(info));
  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Read(1024));
  EXPECT_EQ(PreexistingData::kLoremIpsum, buffer->ToString());
}

TEST_F(TestAzuriteFileSystem, CopyFileSuccessDestinationSame) {
  auto data = SetUpPreexistingData();
  ASSERT_OK(fs()->CopyFile(data.ObjectPath(), data.ObjectPath()));
  ASSERT_OK_AND_ASSIGN(auto info, fs()->GetFileInfo(data.ObjectPath()));
  ASSERT_OK_AND_ASSIGN(auto stream, fs()->OpenInputStream(info));
  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Read(1024));
  EXPECT_EQ(PreexistingData::kLoremIpsum, buffer->ToString());
}

TEST_F(TestAzuriteFileSystem, CopyFileFailureDestinationTrailingSlash) {
  auto data = SetUpPreexistingData();
  ASSERT_RAISES(IOError, fs()->CopyFile(data.ObjectPath(), internal::EnsureTrailingSlash(
                                                               data.ObjectPath())));
}

TEST_F(TestAzuriteFileSystem, CopyFileFailureSourceNonexistent) {
  auto data = SetUpPreexistingData();
  const auto destination_path = data.ContainerPath("copy-destionation");
  ASSERT_RAISES(IOError, fs()->CopyFile(data.NotFoundObjectPath(), destination_path));
}

TEST_F(TestAzuriteFileSystem, CopyFileFailureDestinationParentNonexistent) {
  auto data = SetUpPreexistingData();
  const auto destination_path =
      ConcatAbstractPath(PreexistingData::RandomContainerName(rng_), "copy-destionation");
  ASSERT_RAISES(IOError, fs()->CopyFile(data.ObjectPath(), destination_path));
}

TEST_F(TestAzuriteFileSystem, CopyFileUri) {
  auto data = SetUpPreexistingData();
  const auto destination_path = data.ContainerPath("copy-destionation");
  ASSERT_RAISES(Invalid, fs()->CopyFile("abfs://" + data.ObjectPath(), destination_path));
  ASSERT_RAISES(Invalid, fs()->CopyFile(data.ObjectPath(), "abfs://" + destination_path));
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamString) {
  auto data = SetUpPreexistingData();
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs()->OpenInputStream(data.ObjectPath()));

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Read(1024));
  EXPECT_EQ(buffer->ToString(), PreexistingData::kLoremIpsum);
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamStringBuffers) {
  auto data = SetUpPreexistingData();
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs()->OpenInputStream(data.ObjectPath()));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, stream->Read(16));
    contents.append(buffer->ToString());
  } while (buffer && buffer->size() != 0);

  EXPECT_EQ(contents, PreexistingData::kLoremIpsum);
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamInfo) {
  auto data = SetUpPreexistingData();
  ASSERT_OK_AND_ASSIGN(auto info, fs()->GetFileInfo(data.ObjectPath()));

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs()->OpenInputStream(info));

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Read(1024));
  EXPECT_EQ(buffer->ToString(), PreexistingData::kLoremIpsum);
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamEmpty) {
  auto data = SetUpPreexistingData();
  const auto path_to_file = "empty-object.txt";
  const auto path = data.ContainerPath(path_to_file);
  blob_service_client_->GetBlobContainerClient(data.container_name)
      .GetBlockBlobClient(path_to_file)
      .UploadFrom(nullptr, 0);

  ASSERT_OK_AND_ASSIGN(auto stream, fs()->OpenInputStream(path));
  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));
  EXPECT_EQ(size, 0);
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamNotFound) {
  auto data = SetUpPreexistingData();
  ASSERT_RAISES(IOError, fs()->OpenInputStream(data.NotFoundObjectPath()));
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamInfoInvalid) {
  auto data = SetUpPreexistingData();
  ASSERT_OK_AND_ASSIGN(auto info, fs()->GetFileInfo(data.container_name + "/"));
  ASSERT_RAISES(IOError, fs()->OpenInputStream(info));

  ASSERT_OK_AND_ASSIGN(auto info2, fs()->GetFileInfo(data.NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs()->OpenInputStream(info2));
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamUri) {
  auto data = SetUpPreexistingData();
  ASSERT_RAISES(Invalid, fs()->OpenInputStream("abfs://" + data.ObjectPath()));
}

TEST_F(TestAzuriteFileSystem, OpenInputStreamTrailingSlash) {
  auto data = SetUpPreexistingData();
  ASSERT_RAISES(IOError, fs()->OpenInputStream(data.ObjectPath() + '/'));
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

TEST_F(TestAzuriteFileSystem, OpenInputStreamReadMetadata) {
  auto data = SetUpPreexistingData();
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs()->OpenInputStream(data.ObjectPath()));

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

TEST_F(TestAzuriteFileSystem, OpenInputStreamClosed) {
  auto data = SetUpPreexistingData();
  ASSERT_OK_AND_ASSIGN(auto stream, fs()->OpenInputStream(data.ObjectPath()));
  ASSERT_OK(stream->Close());
  std::array<char, 16> buffer{};
  ASSERT_RAISES(Invalid, stream->Read(buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->Read(buffer.size()));
  ASSERT_RAISES(Invalid, stream->Tell());
}

TEST_F(TestAzuriteFileSystem, WriteMetadata) {
  auto data = SetUpPreexistingData();
  options_.default_metadata = arrow::key_value_metadata({{"foo", "bar"}});

  ASSERT_OK_AND_ASSIGN(auto fs_with_defaults, AzureFileSystem::Make(options_));
  std::string blob_path = "object_with_defaults";
  auto full_path = data.ContainerPath(blob_path);
  ASSERT_OK_AND_ASSIGN(auto output,
                       fs_with_defaults->OpenOutputStream(full_path, /*metadata=*/{}));
  const std::string_view expected(PreexistingData::kLoremIpsum);
  ASSERT_OK(output->Write(expected));
  ASSERT_OK(output->Close());

  // Verify the metadata has been set.
  auto blob_metadata = blob_service_client_->GetBlobContainerClient(data.container_name)
                           .GetBlockBlobClient(blob_path)
                           .GetProperties()
                           .Value.Metadata;
  EXPECT_EQ(Core::CaseInsensitiveMap{std::make_pair("foo", "bar")}, blob_metadata);

  // Check that explicit metadata overrides the defaults.
  ASSERT_OK_AND_ASSIGN(
      output, fs_with_defaults->OpenOutputStream(
                  full_path, /*metadata=*/arrow::key_value_metadata({{"bar", "foo"}})));
  ASSERT_OK(output->Write(expected));
  ASSERT_OK(output->Close());
  blob_metadata = blob_service_client_->GetBlobContainerClient(data.container_name)
                      .GetBlockBlobClient(blob_path)
                      .GetProperties()
                      .Value.Metadata;
  // Defaults are overwritten and not merged.
  EXPECT_EQ(Core::CaseInsensitiveMap{std::make_pair("bar", "foo")}, blob_metadata);
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamSmall) {
  auto data = SetUpPreexistingData();
  const auto path = data.ContainerPath("test-write-object");
  ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(path, {}));
  const std::string_view expected(PreexistingData::kLoremIpsum);
  ASSERT_OK(output->Write(expected));
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  ASSERT_OK_AND_ASSIGN(auto input, fs()->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  ASSERT_OK_AND_ASSIGN(auto size, input->Read(inbuf.size(), inbuf.data()));

  EXPECT_EQ(expected, std::string_view(inbuf.data(), size));
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamLarge) {
  auto data = SetUpPreexistingData();
  const auto path = data.ContainerPath("test-write-object");
  ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(path, {}));
  std::array<std::int64_t, 3> sizes{257 * 1024, 258 * 1024, 259 * 1024};
  std::array<std::string, 3> buffers{
      std::string(sizes[0], 'A'),
      std::string(sizes[1], 'B'),
      std::string(sizes[2], 'C'),
  };
  auto expected = std::int64_t{0};
  for (auto i = 0; i != 3; ++i) {
    ASSERT_OK(output->Write(buffers[i]));
    expected += sizes[i];
    ASSERT_EQ(expected, output->Tell());
  }
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  ASSERT_OK_AND_ASSIGN(auto input, fs()->OpenInputStream(path));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, input->Read(128 * 1024));
    ASSERT_TRUE(buffer);
    contents.append(buffer->ToString());
  } while (buffer->size() != 0);

  EXPECT_EQ(contents, buffers[0] + buffers[1] + buffers[2]);
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamTruncatesExistingFile) {
  auto data = SetUpPreexistingData();
  const auto path = data.ContainerPath("test-write-object");
  ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(path, {}));
  const std::string_view expected0("Existing blob content");
  ASSERT_OK(output->Write(expected0));
  ASSERT_OK(output->Close());

  // Check that the initial content has been written - if not this test is not achieving
  // what it's meant to.
  ASSERT_OK_AND_ASSIGN(auto input, fs()->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  ASSERT_OK_AND_ASSIGN(auto size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(expected0, std::string_view(inbuf.data(), size));

  ASSERT_OK_AND_ASSIGN(output, fs()->OpenOutputStream(path, {}));
  const std::string_view expected1(PreexistingData::kLoremIpsum);
  ASSERT_OK(output->Write(expected1));
  ASSERT_OK(output->Close());

  // Verify that the initial content has been overwritten.
  ASSERT_OK_AND_ASSIGN(input, fs()->OpenInputStream(path));
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(expected1, std::string_view(inbuf.data(), size));
}

TEST_F(TestAzuriteFileSystem, OpenAppendStreamDoesNotTruncateExistingFile) {
  auto data = SetUpPreexistingData();
  const auto path = data.ContainerPath("test-write-object");
  ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(path, {}));
  const std::string_view expected0("Existing blob content");
  ASSERT_OK(output->Write(expected0));
  ASSERT_OK(output->Close());

  // Check that the initial content has been written - if not this test is not achieving
  // what it's meant to.
  ASSERT_OK_AND_ASSIGN(auto input, fs()->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  ASSERT_OK_AND_ASSIGN(auto size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(expected0, std::string_view(inbuf.data()));

  ASSERT_OK_AND_ASSIGN(output, fs()->OpenAppendStream(path, {}));
  const std::string_view expected1(PreexistingData::kLoremIpsum);
  ASSERT_OK(output->Write(expected1));
  ASSERT_OK(output->Close());

  // Verify that the initial content has not been overwritten and that the block from
  // the other client was not committed.
  ASSERT_OK_AND_ASSIGN(input, fs()->OpenInputStream(path));
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));
  EXPECT_EQ(std::string(inbuf.data(), size),
            std::string(expected0) + std::string(expected1));
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamClosed) {
  auto data = SetUpPreexistingData();
  const auto path = data.ContainerPath("open-output-stream-closed.txt");
  ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(path, {}));
  ASSERT_OK(output->Close());
  ASSERT_RAISES(Invalid, output->Write(PreexistingData::kLoremIpsum,
                                       std::strlen(PreexistingData::kLoremIpsum)));
  ASSERT_RAISES(Invalid, output->Flush());
  ASSERT_RAISES(Invalid, output->Tell());
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamUri) {
  auto data = SetUpPreexistingData();
  const auto path = data.ContainerPath("open-output-stream-uri.txt");
  ASSERT_RAISES(Invalid, fs()->OpenInputStream("abfs://" + path));
}

TEST_F(TestAzuriteFileSystem, OpenInputFileMixedReadVsReadAt) {
  auto data = SetUpPreexistingData();
  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(), [&] {
    return PreexistingData::RandomLine(++lineno, kLineWidth, rng_);
  });

  const auto path = data.ContainerPath("OpenInputFileMixedReadVsReadAt/object-name");

  UploadLines(lines, path, kLineCount * kLineWidth);

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs()->OpenInputFile(path));
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
    auto const index = PreexistingData::RandomIndex(kLineCount, rng_);
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

TEST_F(TestAzuriteFileSystem, OpenInputFileRandomSeek) {
  auto data = SetUpPreexistingData();
  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(), [&] {
    return PreexistingData::RandomLine(++lineno, kLineWidth, rng_);
  });

  const auto path = data.ContainerPath("OpenInputFileRandomSeek/object-name");
  std::shared_ptr<io::OutputStream> output;

  UploadLines(lines, path, kLineCount * kLineWidth);

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs()->OpenInputFile(path));
  for (int i = 0; i != 32; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Verify sequential reads work as expected.
    auto const index = PreexistingData::RandomIndex(kLineCount, rng_);
    auto const position = index * kLineWidth;
    ASSERT_OK(file->Seek(position));
    ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
    EXPECT_EQ(lines[index], actual->ToString());
  }
}

TEST_F(TestAzuriteFileSystem, OpenInputFileIoContext) {
  auto data = SetUpPreexistingData();
  // Create a test file.
  const auto blob_path = "OpenInputFileIoContext/object-name";
  const auto path = data.ContainerPath(blob_path);
  const std::string contents = "The quick brown fox jumps over the lazy dog";

  auto blob_client = blob_service_client_->GetBlobContainerClient(data.container_name)
                         .GetBlockBlobClient(blob_path);
  blob_client.UploadFrom(reinterpret_cast<const uint8_t*>(contents.data()),
                         contents.length());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs()->OpenInputFile(path));
  EXPECT_EQ(fs()->io_context().external_id(), file->io_context().external_id());
}

TEST_F(TestAzuriteFileSystem, OpenInputFileInfo) {
  auto data = SetUpPreexistingData();
  ASSERT_OK_AND_ASSIGN(auto info, fs()->GetFileInfo(data.ObjectPath()));

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs()->OpenInputFile(info));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  auto constexpr kStart = 16;
  ASSERT_OK_AND_ASSIGN(size, file->ReadAt(kStart, buffer.size(), buffer.data()));

  auto const expected = std::string(PreexistingData::kLoremIpsum).substr(kStart);
  EXPECT_EQ(std::string(buffer.data(), size), expected);
}

TEST_F(TestAzuriteFileSystem, OpenInputFileNotFound) {
  auto data = SetUpPreexistingData();
  ASSERT_RAISES(IOError, fs()->OpenInputFile(data.NotFoundObjectPath()));
}

TEST_F(TestAzuriteFileSystem, OpenInputFileInfoInvalid) {
  auto data = SetUpPreexistingData();
  ASSERT_OK_AND_ASSIGN(auto info, fs()->GetFileInfo(data.container_name));
  ASSERT_RAISES(IOError, fs()->OpenInputFile(info));

  ASSERT_OK_AND_ASSIGN(auto info2, fs()->GetFileInfo(data.NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs()->OpenInputFile(info2));
}

TEST_F(TestAzuriteFileSystem, OpenInputFileClosed) {
  auto data = SetUpPreexistingData();
  ASSERT_OK_AND_ASSIGN(auto stream, fs()->OpenInputFile(data.ObjectPath()));
  ASSERT_OK(stream->Close());
  std::array<char, 16> buffer{};
  ASSERT_RAISES(Invalid, stream->Tell());
  ASSERT_RAISES(Invalid, stream->Read(buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->Read(buffer.size()));
  ASSERT_RAISES(Invalid, stream->ReadAt(1, buffer.size(), buffer.data()));
  ASSERT_RAISES(Invalid, stream->ReadAt(1, 1));
  ASSERT_RAISES(Invalid, stream->Seek(2));
}

}  // namespace fs
}  // namespace arrow

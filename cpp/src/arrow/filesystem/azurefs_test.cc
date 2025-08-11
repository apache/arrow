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
#include "arrow/filesystem/azurefs_internal.h"

#include <memory>
#include <random>
#include <string>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
#include <azure/storage/blobs.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/files/datalake.hpp>

#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/process.h"
#include "arrow/testing/util.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/pcg_random.h"
#include "arrow/util/string.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
using internal::TemporaryDir;
namespace fs {
using internal::ConcatAbstractPath;

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

  virtual bool HasSubmitBatchBug() const { return false; }
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
  std::unique_ptr<util::Process> server_process_;

  using AzureEnvImpl::AzureEnvImpl;

 public:
  static const AzureBackend kBackend = AzureBackend::kAzurite;

  ~AzuriteEnv() = default;

  static Result<std::unique_ptr<AzureEnvImpl>> Make() {
    auto self = std::unique_ptr<AzuriteEnv>(
        new AzuriteEnv("devstoreaccount1",
                       "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
                       "K1SZFPTOtr/KBHBeksoGMGw=="));
    self->server_process_ = std::make_unique<util::Process>();
    ARROW_RETURN_NOT_OK(self->server_process_->SetExecutable("azurite"));
    ARROW_ASSIGN_OR_RAISE(self->temp_dir_, TemporaryDir::Make("azurefs-test-"));
    ARROW_ASSIGN_OR_RAISE(self->debug_log_path_,
                          self->temp_dir_->path().Join("debug.log"));
    self->server_process_->SetArgs({"--silent", "--location",
                                    self->temp_dir_->path().ToString(), "--debug",
                                    self->debug_log_path_.ToString(),
                                    // For old Azurite. We can't install the latest
                                    // Azurite with old Node.js on old Ubuntu.
                                    "--skipApiVersionCheck"});
    ARROW_RETURN_NOT_OK(self->server_process_->Execute());
    return self;
  }

  /// Azurite has a bug that causes BlobContainerClient::SubmitBatch to fail on macOS.
  /// SubmitBatch is used by:
  ///  - AzureFileSystem::DeleteDir
  ///  - AzureFileSystem::DeleteDirContents
  bool HasSubmitBatchBug() const override {
#ifdef __APPLE__
    return true;
#else
    return false;
#endif
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

namespace {
Result<AzureOptions> MakeOptions(BaseAzureEnv* env) {
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
}  // namespace

struct PreexistingData {
 public:
  using RNG = random::pcg32_fast;

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
  std::string ContainerPath(std::string_view stem) const { return Path(stem); }

  // Short alias to ContainerPath()
  std::string Path(std::string_view stem) const {
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

class TestGeneric : public ::testing::Test, public GenericFileSystemTest {
 public:
  void TearDown() override {
    if (azure_fs_) {
      ASSERT_OK(azure_fs_->DeleteDir(container_name_));
    }
  }

 protected:
  void SetUpInternal(BaseAzureEnv* env) {
    env_ = env;
    random::pcg32_fast rng((std::random_device()()));
    container_name_ = PreexistingData::RandomContainerName(rng);
    ASSERT_OK_AND_ASSIGN(auto options, MakeOptions(env_));
    ASSERT_OK_AND_ASSIGN(azure_fs_, AzureFileSystem::Make(options));
    ASSERT_OK(azure_fs_->CreateDir(container_name_, true));
    fs_ = std::make_shared<SubTreeFileSystem>(container_name_, azure_fs_);
  }

  std::shared_ptr<FileSystem> GetEmptyFileSystem() override { return fs_; }

  bool have_implicit_directories() const override { return true; }
  bool allow_write_file_over_dir() const override { return false; }
  bool allow_read_dir_as_file() const override { return false; }
  bool allow_move_dir() const override { return true; }
  bool allow_move_file() const override { return true; }
  bool allow_append_to_file() const override { return true; }
  bool have_directory_mtimes() const override { return true; }
  bool have_flaky_directory_tree_deletion() const override { return false; }
  bool have_file_metadata() const override { return true; }
  // calloc() used in libxml2's xmlNewGlobalState() is detected as a
  // memory leak like the following. But it's a false positive. It's
  // used in ListBlobsByHierarchy() for GetFileInfo() and it's freed
  // in the call. This is detected as a memory leak only with
  // generator API (GetFileInfoGenerator()) and not detected with
  // non-generator API (GetFileInfo()). So this is a false positive.
  //
  // ==2875409==ERROR: LeakSanitizer: detected memory leaks
  //
  // Direct leak of 968 byte(s) in 1 object(s) allocated from:
  //     #0 0x55d02c967bdc in calloc (build/debug/arrow-azurefs-test+0x17bbdc) (BuildId:
  //     520690d1b20e860cc1feef665dce8196e64f955e) #1 0x7fa914b1cd1e in xmlNewGlobalState
  //     builddir/main/../../threads.c:580:10 #2 0x7fa914b1cd1e in xmlGetGlobalState
  //     builddir/main/../../threads.c:666:31
  bool have_false_positive_memory_leak_with_generator() const override { return true; }
  // This false positive leak is similar to the one pinpointed in the
  // have_false_positive_memory_leak_with_generator() comments above,
  // though the stack trace is different. It happens when a block list
  // is committed from a background thread.
  //
  // clang-format off
  // Direct leak of 968 byte(s) in 1 object(s) allocated from:
  //   #0 calloc
  //   #1 (/lib/x86_64-linux-gnu/libxml2.so.2+0xe25a4)
  //   #2 __xmlDefaultBufferSize
  //   #3 xmlBufferCreate
  //   #4 Azure::Storage::_internal::XmlWriter::XmlWriter()
  //   #5 Azure::Storage::Blobs::_detail::BlockBlobClient::CommitBlockList
  //   #6 Azure::Storage::Blobs::BlockBlobClient::CommitBlockList
  //   #7 arrow::fs::(anonymous namespace)::CommitBlockList
  //   #8 arrow::fs::(anonymous namespace)::ObjectAppendStream::FlushAsync()::'lambda'
  // clang-format on
  //
  // TODO perhaps remove this skip once we can rely on
  // https://github.com/Azure/azure-sdk-for-cpp/pull/5767
  //
  // Also note that ClickHouse has a workaround for a similar issue:
  // https://github.com/ClickHouse/ClickHouse/pull/45796
  bool have_false_positive_memory_leak_with_async_close() const override { return true; }

  BaseAzureEnv* env_;
  std::shared_ptr<AzureFileSystem> azure_fs_;
  std::shared_ptr<FileSystem> fs_;

 private:
  std::string container_name_;
};

class TestAzuriteGeneric : public TestGeneric {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(auto env, AzuriteEnv::GetInstance());
    SetUpInternal(env);
  }

 protected:
  // Azurite doesn't block writing files over directories.
  bool allow_write_file_over_dir() const override { return true; }
  // Azurite doesn't support moving directories.
  bool allow_move_dir() const override { return false; }
  // Azurite doesn't support moving files.
  bool allow_move_file() const override { return false; }
  // Azurite doesn't support directory mtime.
  bool have_directory_mtimes() const override { return false; }
  // DeleteDir() doesn't work with Azurite on macOS
  bool have_flaky_directory_tree_deletion() const override {
    return env_->HasSubmitBatchBug();
  }
};

class TestAzureFlatNSGeneric : public TestGeneric {
 public:
  void SetUp() override {
    auto env_result = AzureFlatNSEnv::GetInstance();
    if (env_result.status().IsCancelled()) {
      GTEST_SKIP() << env_result.status().message();
    }
    ASSERT_OK_AND_ASSIGN(auto env, env_result);
    SetUpInternal(env);
  }

 protected:
  // Flat namespace account doesn't block writing files over directories.
  bool allow_write_file_over_dir() const override { return true; }
  // Flat namespace account doesn't support moving directories.
  bool allow_move_dir() const override { return false; }
  // Flat namespace account doesn't support moving files.
  bool allow_move_file() const override { return false; }
  // Flat namespace account doesn't support directory mtime.
  bool have_directory_mtimes() const override { return false; }
};

class TestAzureHierarchicalNSGeneric : public TestGeneric {
 public:
  void SetUp() override {
    auto env_result = AzureHierarchicalNSEnv::GetInstance();
    if (env_result.status().IsCancelled()) {
      GTEST_SKIP() << env_result.status().message();
    }
    ASSERT_OK_AND_ASSIGN(auto env, env_result);
    SetUpInternal(env);
  }
};

GENERIC_FS_TEST_FUNCTIONS(TestAzuriteGeneric);
GENERIC_FS_TEST_FUNCTIONS(TestAzureFlatNSGeneric);
GENERIC_FS_TEST_FUNCTIONS(TestAzureHierarchicalNSGeneric);

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
  AzureOptions explicitly_default_options;
  explicitly_default_options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(explicitly_default_options.ConfigureDefaultCredential());
  ASSERT_TRUE(options.Equals(explicitly_default_options));
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

TEST(AzureFileSystem, InitializeWithCLICredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(options.ConfigureCLICredential());
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, InitializeWithWorkloadIdentityCredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(options.ConfigureWorkloadIdentityCredential());
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, InitializeWithEnvironmentCredential) {
  AzureOptions options;
  options.account_name = "dummy-account-name";
  ARROW_EXPECT_OK(options.ConfigureEnvironmentCredential());
  EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));
}

TEST(AzureFileSystem, OptionsCompare) {
  AzureOptions options;
  EXPECT_TRUE(options.Equals(options));
}

class TestAzureOptions : public ::testing::Test {
 public:
  void TestFromUriBlobStorage() {
    AzureOptions default_options;
    std::string path;
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob",
                              &path));
    ASSERT_EQ(options.account_name, "account");
    ASSERT_EQ(options.blob_storage_authority, default_options.blob_storage_authority);
    ASSERT_EQ(options.dfs_storage_authority, default_options.dfs_storage_authority);
    ASSERT_EQ(options.blob_storage_scheme, default_options.blob_storage_scheme);
    ASSERT_EQ(options.dfs_storage_scheme, default_options.dfs_storage_scheme);
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kDefault);
    ASSERT_EQ(path, "container/dir/blob");
    ASSERT_EQ(options.background_writes, true);
  }

  void TestFromUriDfsStorage() {
    AzureOptions default_options;
    std::string path;
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://file_system@account.dfs.core.windows.net/dir/file",
                              &path));
    ASSERT_EQ(options.account_name, "account");
    ASSERT_EQ(options.blob_storage_authority, default_options.blob_storage_authority);
    ASSERT_EQ(options.dfs_storage_authority, default_options.dfs_storage_authority);
    ASSERT_EQ(options.blob_storage_scheme, default_options.blob_storage_scheme);
    ASSERT_EQ(options.dfs_storage_scheme, default_options.dfs_storage_scheme);
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kDefault);
    ASSERT_EQ(path, "file_system/dir/file");
    ASSERT_EQ(options.background_writes, true);
  }

  void TestFromUriAbfs() {
    std::string path;
    ASSERT_OK_AND_ASSIGN(auto options,
                         AzureOptions::FromUri(
                             "abfs://account@127.0.0.1:10000/container/dir/blob", &path));
    ASSERT_EQ(options.account_name, "account");
    ASSERT_EQ(options.blob_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.dfs_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.blob_storage_scheme, "https");
    ASSERT_EQ(options.dfs_storage_scheme, "https");
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kDefault);
    ASSERT_EQ(path, "container/dir/blob");
    ASSERT_EQ(options.background_writes, true);
  }

  void TestFromUriAbfss() {
    std::string path;
    ASSERT_OK_AND_ASSIGN(
        auto options, AzureOptions::FromUri(
                          "abfss://account@127.0.0.1:10000/container/dir/blob", &path));
    ASSERT_EQ(options.account_name, "account");
    ASSERT_EQ(options.blob_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.dfs_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.blob_storage_scheme, "https");
    ASSERT_EQ(options.dfs_storage_scheme, "https");
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kDefault);
    ASSERT_EQ(path, "container/dir/blob");
    ASSERT_EQ(options.background_writes, true);
  }

  void TestFromUriEnableTls() {
    std::string path;
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account@127.0.0.1:10000/container/dir/blob?"
                              "enable_tls=false",
                              &path));
    ASSERT_EQ(options.account_name, "account");
    ASSERT_EQ(options.blob_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.dfs_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.blob_storage_scheme, "http");
    ASSERT_EQ(options.dfs_storage_scheme, "http");
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kDefault);
    ASSERT_EQ(path, "container/dir/blob");
    ASSERT_EQ(options.background_writes, true);
  }

  void TestFromUriDisableBackgroundWrites() {
    std::string path;
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account@127.0.0.1:10000/container/dir/blob?"
                              "background_writes=false",
                              &path));
    ASSERT_EQ(options.background_writes, false);
  }

  void TestFromUriCredentialDefault() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "credential_kind=default",
                              nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kDefault);
  }

  void TestFromUriCredentialAnonymous() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "credential_kind=anonymous",
                              nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kAnonymous);
  }

  void TestFromUriCredentialClientSecret() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "tenant_id=tenant-id&"
                              "client_id=client-id&"
                              "client_secret=client-secret",
                              nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kClientSecret);
  }

  void TestFromUriCredentialManagedIdentity() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "client_id=client-id",
                              nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kManagedIdentity);
  }

  void TestFromUriCredentialCLI() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "credential_kind=cli",
                              nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kCLI);
  }

  void TestFromUriCredentialWorkloadIdentity() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "credential_kind=workload_identity",
                              nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kWorkloadIdentity);
  }

  void TestFromUriCredentialEnvironment() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "credential_kind=environment",
                              nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kEnvironment);
  }

  void TestFromUriCredentialSASToken() {
    const std::string sas_token =
        "?se=2024-12-12T18:57:47Z&sig=pAs7qEBdI6sjUhqX1nrhNAKsTY%2B1SqLxPK%"
        "2BbAxLiopw%3D&sp=racwdxylti&spr=https,http&sr=c&sv=2024-08-04";
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri(
            "abfs://file_system@account.dfs.core.windows.net/" + sas_token, nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kSASToken);
    ASSERT_EQ(options.sas_token_, sas_token);
  }

  void TestFromUriCredentialSASTokenWithOtherParameters() {
    const std::string uri_query_string =
        "?enable_tls=false&se=2024-12-12T18:57:47Z&sig=pAs7qEBdI6sjUhqX1nrhNAKsTY%"
        "2B1SqLxPK%"
        "2BbAxLiopw%3D&sp=racwdxylti&spr=https,http&sr=c&sv=2024-08-04";
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri(
            "abfs://account@127.0.0.1:10000/container/dir/blob" + uri_query_string,
            nullptr));
    ASSERT_EQ(options.credential_kind_, AzureOptions::CredentialKind::kSASToken);
    ASSERT_EQ(options.sas_token_, uri_query_string);
    ASSERT_EQ(options.blob_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.dfs_storage_authority, "127.0.0.1:10000");
    ASSERT_EQ(options.blob_storage_scheme, "http");
    ASSERT_EQ(options.dfs_storage_scheme, "http");
  }

  void TestFromUriCredentialInvalid() {
    ASSERT_RAISES(Invalid, AzureOptions::FromUri(
                               "abfs://file_system@account.dfs.core.windows.net/dir/file?"
                               "credential_kind=invalid",
                               nullptr));
  }
  void TestFromUriBlobStorageAuthority() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://account.blob.core.windows.net/container/dir/blob?"
                              "blob_storage_authority=.blob.local",
                              nullptr));
    ASSERT_EQ(options.blob_storage_authority, ".blob.local");
  }

  void TestFromUriDfsStorageAuthority() {
    ASSERT_OK_AND_ASSIGN(
        auto options,
        AzureOptions::FromUri("abfs://file_system@account.dfs.core.windows.net/dir/file?"
                              "dfs_storage_authority=.dfs.local",
                              nullptr));
    ASSERT_EQ(options.dfs_storage_authority, ".dfs.local");
  }

  void TestFromUriInvalidQueryParameter() {
    ASSERT_RAISES(Invalid, AzureOptions::FromUri(
                               "abfs://file_system@account.dfs.core.windows.net/dir/file?"
                               "unknown=invalid",
                               nullptr));
  }

  void TestMakeBlobServiceClientInvalidAccountName() {
    AzureOptions options;
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: AzureOptions doesn't contain a valid account name",
        options.MakeBlobServiceClient());
  }

  void TestMakeBlobServiceClientInvalidBlobStorageScheme() {
    AzureOptions options;
    options.account_name = "user";
    options.blob_storage_scheme = "abfs";
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: AzureOptions::blob_storage_scheme must be http or https: abfs",
        options.MakeBlobServiceClient());
  }

  void TestMakeDataLakeServiceClientInvalidAccountName() {
    AzureOptions options;
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: AzureOptions doesn't contain a valid account name",
        options.MakeDataLakeServiceClient());
  }

  void TestMakeDataLakeServiceClientInvalidDfsStorageScheme() {
    AzureOptions options;
    options.account_name = "user";
    options.dfs_storage_scheme = "abfs";
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid, "Invalid: AzureOptions::dfs_storage_scheme must be http or https: abfs",
        options.MakeDataLakeServiceClient());
  }
};

TEST_F(TestAzureOptions, FromUriBlobStorage) { TestFromUriBlobStorage(); }
TEST_F(TestAzureOptions, FromUriDfsStorage) { TestFromUriDfsStorage(); }
TEST_F(TestAzureOptions, FromUriAbfs) { TestFromUriAbfs(); }
TEST_F(TestAzureOptions, FromUriAbfss) { TestFromUriAbfss(); }
TEST_F(TestAzureOptions, FromUriEnableTls) { TestFromUriEnableTls(); }
TEST_F(TestAzureOptions, FromUriDisableBackgroundWrites) {
  TestFromUriDisableBackgroundWrites();
}
TEST_F(TestAzureOptions, FromUriCredentialDefault) { TestFromUriCredentialDefault(); }
TEST_F(TestAzureOptions, FromUriCredentialAnonymous) { TestFromUriCredentialAnonymous(); }
TEST_F(TestAzureOptions, FromUriCredentialClientSecret) {
  TestFromUriCredentialClientSecret();
}
TEST_F(TestAzureOptions, FromUriCredentialManagedIdentity) {
  TestFromUriCredentialManagedIdentity();
}
TEST_F(TestAzureOptions, FromUriCredentialCLI) { TestFromUriCredentialCLI(); }
TEST_F(TestAzureOptions, FromUriCredentialWorkloadIdentity) {
  TestFromUriCredentialWorkloadIdentity();
}
TEST_F(TestAzureOptions, FromUriCredentialEnvironment) {
  TestFromUriCredentialEnvironment();
}
TEST_F(TestAzureOptions, FromUriCredentialSASToken) { TestFromUriCredentialSASToken(); }
TEST_F(TestAzureOptions, FromUriCredentialSASTokenWithOtherParameters) {
  TestFromUriCredentialSASTokenWithOtherParameters();
}
TEST_F(TestAzureOptions, FromUriCredentialInvalid) { TestFromUriCredentialInvalid(); }
TEST_F(TestAzureOptions, FromUriBlobStorageAuthority) {
  TestFromUriBlobStorageAuthority();
}
TEST_F(TestAzureOptions, FromUriDfsStorageAuthority) { TestFromUriDfsStorageAuthority(); }
TEST_F(TestAzureOptions, FromUriInvalidQueryParameter) {
  TestFromUriInvalidQueryParameter();
}
TEST_F(TestAzureOptions, MakeBlobServiceClientInvalidAccountName) {
  TestMakeBlobServiceClientInvalidAccountName();
}
TEST_F(TestAzureOptions, MakeBlobServiceClientInvalidBlobStorageScheme) {
  TestMakeBlobServiceClientInvalidBlobStorageScheme();
}
TEST_F(TestAzureOptions, MakeDataLakeServiceClientInvalidAccountName) {
  TestMakeDataLakeServiceClientInvalidAccountName();
}
TEST_F(TestAzureOptions, MakeDataLakeServiceClientInvalidDfsStorageScheme) {
  TestMakeDataLakeServiceClientInvalidDfsStorageScheme();
}

class TestAzureFileSystem : public ::testing::Test {
 protected:
  // Set in constructor
  random::pcg32_fast rng_;

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
    ARROW_UNUSED(container_client.CreateIfNotExists());
    return container_client;
  }

  DataLake::DataLakeFileSystemClient CreateFilesystem(const std::string& name) {
    auto adlfs_client = datalake_service_client_->GetFileSystemClient(name);
    ARROW_UNUSED(adlfs_client.CreateIfNotExists());
    return adlfs_client;
  }

  Blobs::BlobClient CreateBlob(Blobs::BlobContainerClient& container_client,
                               const std::string& name, const std::string& data = "") {
    auto blob_client = container_client.GetBlockBlobClient(name);
    ARROW_UNUSED(blob_client.UploadFrom(reinterpret_cast<const uint8_t*>(data.data()),
                                        data.size()));
    return blob_client;
  }

  DataLake::DataLakeFileClient CreateFile(
      DataLake::DataLakeFileSystemClient& filesystem_client, const std::string& name,
      const std::string& data = "") {
    auto file_client = filesystem_client.GetFileClient(name);
    ARROW_UNUSED(file_client.UploadFrom(reinterpret_cast<const uint8_t*>(data.data()),
                                        data.size()));
    return file_client;
  }

  DataLake::DataLakeDirectoryClient CreateDirectory(
      DataLake::DataLakeFileSystemClient& adlfs_client, const std::string& name) {
    EXPECT_TRUE(WithHierarchicalNamespace());
    auto dir_client = adlfs_client.GetDirectoryClient(name);
    dir_client.Create();
    return dir_client;
  }

  Blobs::Models::BlobProperties GetBlobProperties(const std::string& container_name,
                                                  const std::string& blob_name) {
    return blob_service_client_->GetBlobContainerClient(container_name)
        .GetBlobClient(blob_name)
        .GetProperties()
        .Value;
  }

  Result<std::string> GetContainerSASToken(
      const std::string& container_name,
      Azure::Storage::StorageSharedKeyCredential storage_shared_key_credential) {
    std::string sas_token;
    Azure::Storage::Sas::BlobSasBuilder builder;
    std::chrono::seconds available_period(60);
    builder.ExpiresOn = std::chrono::system_clock::now() + available_period;
    builder.BlobContainerName = container_name;
    builder.Resource = Azure::Storage::Sas::BlobSasResource::BlobContainer;
    builder.SetPermissions(Azure::Storage::Sas::BlobContainerSasPermissions::All);
    builder.Protocol = Azure::Storage::Sas::SasProtocol::HttpsAndHttp;
    return builder.GenerateSasToken(storage_shared_key_credential);
  }

  void UploadLines(const std::vector<std::string>& lines, const std::string& path,
                   int total_size) {
    ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(path, {}));
    for (auto const& line : lines) {
      ASSERT_OK(output->Write(line.data(), line.size()));
    }
    ASSERT_OK(output->Close());
  }

  PreexistingData SetUpPreexistingData() {
    PreexistingData data(rng_);
    if (WithHierarchicalNamespace()) {
      auto filesystem_client = CreateFilesystem(data.container_name);
      CreateFile(filesystem_client, data.kObjectName, PreexistingData::kLoremIpsum);
    } else {
      auto container_client = CreateContainer(data.container_name);
      CreateBlob(container_client, data.kObjectName, PreexistingData::kLoremIpsum);
    }
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

  static bool WithErrno(const Status& status, int expected_errno) {
    auto* detail = status.detail().get();
    return detail &&
           arrow::internal::ErrnoFromStatusDetail(*detail).value_or(-1) == expected_errno;
  }

#define ASSERT_RAISES_ERRNO(expr, expected_errno)                                     \
  for (::arrow::Status _st = ::arrow::ToStatus((expr));                               \
       !WithErrno(_st, (expected_errno));)                                            \
  FAIL() << "'" ARROW_STRINGIFY(expr) "' did not fail with errno=" << #expected_errno \
         << ": " << _st.ToString()

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

  void TestCreateDirOnRootWithTrailingSlash() {
    auto dir1 = PreexistingData::RandomContainerName(rng_) + "/";

    AssertFileInfo(fs(), dir1, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(dir1, false));
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

  void TestCreateDirOnExistingContainerWithTrailingSlash() {
    auto data = SetUpPreexistingData();
    auto dir1 = data.RandomDirectoryPath(rng_) + "/";

    AssertFileInfo(fs(), dir1, FileType::NotFound);
    ASSERT_OK(fs()->CreateDir(dir1, /*recursive=*/false));
    AssertFileInfo(fs(), dir1, FileType::Directory);
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

  void TestDisallowReadingOrWritingDirectoryMarkers() {
    auto data = SetUpPreexistingData();
    auto directory_path = data.Path("directory");

    ASSERT_OK(fs()->CreateDir(directory_path));
    ASSERT_RAISES(IOError, fs()->OpenInputFile(directory_path));
    ASSERT_RAISES(IOError, fs()->OpenOutputStream(directory_path));
    ASSERT_RAISES(IOError, fs()->OpenAppendStream(directory_path));

    auto directory_path_with_slash = directory_path + "/";
    ASSERT_RAISES(IOError, fs()->OpenInputFile(directory_path_with_slash));
    ASSERT_RAISES(IOError, fs()->OpenOutputStream(directory_path_with_slash));
    ASSERT_RAISES(IOError, fs()->OpenAppendStream(directory_path_with_slash));
  }

  void TestDisallowCreatingFileAndDirectoryWithTheSameName() {
    auto data = SetUpPreexistingData();
    auto path1 = data.Path("directory1");
    ASSERT_OK(fs()->CreateDir(path1));
    ASSERT_RAISES(IOError, fs()->OpenOutputStream(path1));
    ASSERT_RAISES(IOError, fs()->OpenAppendStream(path1));
    AssertFileInfo(fs(), path1, FileType::Directory);

    auto path2 = data.Path("directory2");
    ASSERT_OK(fs()->OpenOutputStream(path2));
    ASSERT_RAISES(IOError, fs()->CreateDir(path2));
    AssertFileInfo(fs(), path2, FileType::File);
  }

  void TestOpenOutputStreamWithMissingContainer() {
    ASSERT_RAISES(IOError, fs()->OpenOutputStream("not-a-container/file", {}));
  }

  void TestDeleteDirSuccessEmpty() {
    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    if (env->HasSubmitBatchBug()) {
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
    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    if (env->HasSubmitBatchBug()) {
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

  void TestNonEmptyDirWithTrailingSlash() {
    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    if (env->HasSubmitBatchBug()) {
      GTEST_SKIP() << kSubmitBatchBugMessage;
    }
    auto data = SetUpPreexistingData();
    const auto directory_path = data.RandomDirectoryPath(rng_);
    const auto blob_path = ConcatAbstractPath(directory_path, "hello.txt");
    ASSERT_OK_AND_ASSIGN(auto output, fs()->OpenOutputStream(blob_path));
    ASSERT_OK(output->Write("hello"));
    ASSERT_OK(output->Close());
    AssertFileInfo(fs(), blob_path, FileType::File);
    ASSERT_OK(fs()->DeleteDir(directory_path + "/"));
    AssertFileInfo(fs(), blob_path, FileType::NotFound);
  }

  void TestDeleteDirSuccessHaveDirectory() {
    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    if (env->HasSubmitBatchBug()) {
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
    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    if (env->HasSubmitBatchBug()) {
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

  void TestDeleteDirContentsSuccessExistWithTrailingSlash() {
    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    if (env->HasSubmitBatchBug()) {
      GTEST_SKIP() << kSubmitBatchBugMessage;
    }
    auto preexisting_data = SetUpPreexistingData();
    HierarchicalPaths paths;
    CreateHierarchicalData(&paths);
    ASSERT_OK(fs()->DeleteDirContents(paths.directory + "/"));
    AssertFileInfo(fs(), paths.directory, FileType::Directory);
    for (const auto& sub_path : paths.sub_paths) {
      AssertFileInfo(fs(), sub_path, FileType::NotFound);
    }
  }

  void TestDeleteDirContentsSuccessNonexistent() {
    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    if (env->HasSubmitBatchBug()) {
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

  void TestDeleteFileAtRoot() {
    ASSERT_RAISES_ERRNO(fs()->DeleteFile("file0"), ENOENT);
    ASSERT_RAISES_ERRNO(fs()->DeleteFile("file1/"), ENOENT);
    const auto container_name = PreexistingData::RandomContainerName(rng_);
    if (WithHierarchicalNamespace()) {
      ARROW_UNUSED(CreateFilesystem(container_name));
    } else {
      ARROW_UNUSED(CreateContainer(container_name));
    }
    arrow::fs::AssertFileInfo(fs(), container_name, FileType::Directory);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Not a regular file: '" + container_name + "'"),
        fs()->DeleteFile(container_name));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Not a regular file: '" + container_name + "/'"),
        fs()->DeleteFile(container_name + "/"));
  }

  void TestDeleteFileAtContainerRoot() {
    auto data = SetUpPreexistingData();

    ASSERT_RAISES_ERRNO(fs()->DeleteFile(data.Path("nonexistent-path")), ENOENT);
    ASSERT_RAISES_ERRNO(fs()->DeleteFile(data.Path("nonexistent-path/")), ENOENT);

    arrow::fs::AssertFileInfo(fs(), data.ObjectPath(), FileType::File);
    ASSERT_OK(fs()->DeleteFile(data.ObjectPath()));
    arrow::fs::AssertFileInfo(fs(), data.ObjectPath(), FileType::NotFound);

    if (WithHierarchicalNamespace()) {
      auto adlfs_client =
          datalake_service_client_->GetFileSystemClient(data.container_name);
      CreateFile(adlfs_client, data.kObjectName, PreexistingData::kLoremIpsum);
    } else {
      auto container_client = CreateContainer(data.container_name);
      CreateBlob(container_client, data.kObjectName, PreexistingData::kLoremIpsum);
    }
    arrow::fs::AssertFileInfo(fs(), data.ObjectPath(), FileType::File);

    ASSERT_RAISES_ERRNO(fs()->DeleteFile(data.ObjectPath() + "/"), ENOTDIR);
    ASSERT_OK(fs()->DeleteFile(data.ObjectPath()));
    arrow::fs::AssertFileInfo(fs(), data.ObjectPath(), FileType::NotFound);
  }

  void TestDeleteFileAtSubdirectory(bool create_empty_dir_marker_first) {
    auto data = SetUpPreexistingData();

    auto setup_dir_file0 = [this, create_empty_dir_marker_first, &data]() {
      if (WithHierarchicalNamespace()) {
        ASSERT_FALSE(create_empty_dir_marker_first);
        auto adlfs_client =
            datalake_service_client_->GetFileSystemClient(data.container_name);
        CreateFile(adlfs_client, "dir/file0", PreexistingData::kLoremIpsum);
      } else {
        auto container_client = CreateContainer(data.container_name);
        if (create_empty_dir_marker_first) {
          CreateBlob(container_client, "dir/", "");
        }
        CreateBlob(container_client, "dir/file0", PreexistingData::kLoremIpsum);
      }
    };
    setup_dir_file0();

    // Trying to delete a non-existing file in an existing directory should fail
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError,
        ::testing::HasSubstr("Path does not exist '" + data.Path("dir/nonexistent-path") +
                             "'"),
        fs()->DeleteFile(data.Path("dir/nonexistent-path")));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError,
        ::testing::HasSubstr("Path does not exist '" +
                             data.Path("dir/nonexistent-path/") + "'"),
        fs()->DeleteFile(data.Path("dir/nonexistent-path/")));

    // Trying to delete the directory with DeleteFile should fail
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Not a regular file: '" + data.Path("dir") + "'"),
        fs()->DeleteFile(data.Path("dir")));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, ::testing::HasSubstr("Not a regular file: '" + data.Path("dir/") + "'"),
        fs()->DeleteFile(data.Path("dir/")));

    arrow::fs::AssertFileInfo(fs(), data.Path("dir"), FileType::Directory);
    arrow::fs::AssertFileInfo(fs(), data.Path("dir/"), FileType::Directory);
    arrow::fs::AssertFileInfo(fs(), data.Path("dir/file0"), FileType::File);
    ASSERT_OK(fs()->DeleteFile(data.Path("dir/file0")));
    arrow::fs::AssertFileInfo(fs(), data.Path("dir"), FileType::Directory);
    arrow::fs::AssertFileInfo(fs(), data.Path("dir/"), FileType::Directory);
    arrow::fs::AssertFileInfo(fs(), data.Path("dir/file0"), FileType::NotFound);

    // Recreating the file on the same path gurantees leases were properly released/broken
    setup_dir_file0();

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError,
        ::testing::HasSubstr("Not a directory: '" + data.Path("dir/file0/") + "'"),
        fs()->DeleteFile(data.Path("dir/file0/")));
    arrow::fs::AssertFileInfo(fs(), data.Path("dir/file0"), FileType::File);
  }

  void AssertObjectContents(AzureFileSystem* fs, std::string_view path,
                            std::string_view expected) {
    ASSERT_OK_AND_ASSIGN(auto input, fs->OpenInputStream(std::string{path}));
    std::string contents;
    std::shared_ptr<Buffer> buffer;
    do {
      ASSERT_OK_AND_ASSIGN(buffer, input->Read(128 * 1024));
      contents.append(buffer->ToString());
    } while (buffer->size() != 0);

    EXPECT_EQ(expected, contents);
  }

  void TestOpenOutputStreamSmall() {
    ASSERT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options_));

    auto data = SetUpPreexistingData();
    const auto path = data.ContainerPath("test-write-object");
    ASSERT_OK_AND_ASSIGN(auto output, fs->OpenOutputStream(path, {}));
    const std::string_view expected(PreexistingData::kLoremIpsum);
    ASSERT_OK(output->Write(expected));
    ASSERT_OK(output->Close());

    // Verify we can read the object back.
    AssertObjectContents(fs.get(), path, expected);
  }

  void TestOpenOutputStreamLarge() {
    ASSERT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options_));

    auto data = SetUpPreexistingData();
    const auto path = data.ContainerPath("test-write-object");
    ASSERT_OK_AND_ASSIGN(auto output, fs->OpenOutputStream(path, {}));

    // Upload 5 MB, 4 MB und 2 MB and a very small write to test varying sizes
    std::vector<std::int64_t> sizes{5 * 1024 * 1024, 4 * 1024 * 1024, 2 * 1024 * 1024,
                                    2000};

    std::vector<std::string> buffers{};
    char current_char = 'A';
    for (const auto size : sizes) {
      buffers.emplace_back(size, current_char++);
    }

    auto expected_size = std::int64_t{0};
    for (size_t i = 0; i < buffers.size(); ++i) {
      ASSERT_OK(output->Write(buffers[i]));
      expected_size += sizes[i];
      ASSERT_EQ(expected_size, output->Tell());
    }
    ASSERT_OK(output->Close());

    AssertObjectContents(fs.get(), path,
                         buffers[0] + buffers[1] + buffers[2] + buffers[3]);
  }

  void TestOpenOutputStreamLargeSingleWrite() {
    ASSERT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options_));

    auto data = SetUpPreexistingData();
    const auto path = data.ContainerPath("test-write-object");
    ASSERT_OK_AND_ASSIGN(auto output, fs->OpenOutputStream(path, {}));

    constexpr std::int64_t size{12 * 1024 * 1024};
    const std::string large_string(size, 'X');

    ASSERT_OK(output->Write(large_string));
    ASSERT_EQ(size, output->Tell());
    ASSERT_OK(output->Close());

    AssertObjectContents(fs.get(), path, large_string);
  }

  void TestOpenOutputStreamCloseAsync() {
#if defined(ADDRESS_SANITIZER) || defined(ARROW_VALGRIND)
    // See comment about have_false_positive_memory_leak_with_generator above.
    if (options_.background_writes) {
      GTEST_SKIP() << "False positive memory leak in libxml2 with CloseAsync";
    }
#endif
    ASSERT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options_));
    auto data = SetUpPreexistingData();
    const std::string path = data.ContainerPath("test-write-object");
    constexpr auto payload = PreexistingData::kLoremIpsum;

    ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenOutputStream(path));
    ASSERT_OK(stream->Write(payload));
    auto close_fut = stream->CloseAsync();

    ASSERT_OK(close_fut.MoveResult());

    AssertObjectContents(fs.get(), path, payload);
  }

  void TestOpenOutputStreamCloseAsyncDestructor() {
#if defined(ADDRESS_SANITIZER) || defined(ARROW_VALGRIND)
    // See above.
    if (options_.background_writes) {
      GTEST_SKIP() << "False positive memory leak in libxml2 with CloseAsync";
    }
#endif
    ASSERT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options_));
    auto data = SetUpPreexistingData();
    const std::string path = data.ContainerPath("test-write-object");
    constexpr auto payload = PreexistingData::kLoremIpsum;

    ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenOutputStream(path));
    ASSERT_OK(stream->Write(payload));
    // Destructor implicitly closes stream and completes the upload.
    // Testing it doesn't matter whether flush is triggered asynchronously
    // after CloseAsync or synchronously after stream.reset() since we're just
    // checking that the future keeps the stream alive until completion
    // rather than segfaulting on a dangling stream.
    auto close_fut = stream->CloseAsync();
    stream.reset();
    ASSERT_OK(close_fut.MoveResult());

    AssertObjectContents(fs.get(), path, payload);
  }

  void TestOpenOutputStreamDestructor() {
    ASSERT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options_));
    constexpr auto* payload = "new data";
    auto data = SetUpPreexistingData();
    const std::string path = data.ContainerPath("test-write-object");

    ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenOutputStream(path));
    ASSERT_OK(stream->Write(payload));
    // Destructor implicitly closes stream and completes the multipart upload.
    stream.reset();

    AssertObjectContents(fs.get(), path, payload);
  }

  void TestSASCredential() {
    auto data = SetUpPreexistingData();

    ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    ASSERT_OK_AND_ASSIGN(auto options, MakeOptions(env));
    ASSERT_OK_AND_ASSIGN(
        auto sas_token,
        GetContainerSASToken(data.container_name,
                             Azure::Storage::StorageSharedKeyCredential(
                                 env->account_name(), env->account_key())));
    // AzureOptions::FromUri will not cut off extra query parameters that it consumes, so
    // make sure these don't cause problems.
    ARROW_EXPECT_OK(options.ConfigureSASCredential(
        "?blob_storage_authority=dummy_value0&" + sas_token.substr(1) +
        "&credential_kind=dummy-value1"));
    EXPECT_OK_AND_ASSIGN(auto fs, AzureFileSystem::Make(options));

    AssertFileInfo(fs.get(), data.ObjectPath(), FileType::File);

    // Test CopyFile because the most obvious implementation requires generating a SAS
    // token at runtime which doesn't work when the original auth is SAS token.
    ASSERT_OK(fs->CopyFile(data.ObjectPath(), data.ObjectPath() + "_copy"));
    AssertFileInfo(fs.get(), data.ObjectPath() + "_copy", FileType::File);
  }

 private:
  using StringMatcher =
      ::testing::PolymorphicMatcher<::testing::internal::HasSubstrMatcher<std::string>>;

  StringMatcher HasDirMoveToSubdirMessage(const std::string& src,
                                          const std::string& dest) {
    return ::testing::HasSubstr("Cannot Move to '" + dest + "' and make '" + src +
                                "' a sub-directory of itself.");
  }

  StringMatcher HasCrossContainerNotImplementedMessage(const std::string& container_name,
                                                       const std::string& dest) {
    return ::testing::HasSubstr("Move of '" + container_name + "' to '" + dest +
                                "' requires moving data between "
                                "containers, which is not implemented.");
  }

  StringMatcher HasMissingParentDirMessage(const std::string& dest) {
    return ::testing::HasSubstr("The parent directory of the destination path '" + dest +
                                "' does not exist.");
  }

  /// \brief Expected POSIX semantics for the rename operation on multiple
  /// scenarios.
  ///
  /// If the src doesn't exist, the error is always ENOENT, otherwise we are
  /// left with the following combinations:
  ///
  /// 1. src's type
  ///    a. File
  ///    b. Directory
  /// 2. dest's existence
  ///    a. NotFound
  ///    b. File
  ///    c. Directory
  ///       - empty
  ///       - non-empty
  /// 3. src path has a trailing slash (or not)
  /// 4. dest path has a trailing slash (or not)
  ///
  /// Limitations: this function doesn't consider paths so it assumes that the
  /// paths don't lead requests for moves that would make the source a subdir of
  /// the destination.
  ///
  /// \param paths_are_equal src and dest paths without trailing slashes are equal
  /// \return std::nullopt if success is expected in the scenario or the errno
  /// if failure is expected.
  static std::optional<int> RenameSemantics(FileType src_type, bool src_trailing_slash,
                                            FileType dest_type, bool dest_trailing_slash,
                                            bool dest_is_empty_dir = false,
                                            bool paths_are_equal = false) {
    DCHECK(src_type != FileType::Unknown && dest_type != FileType::Unknown);
    DCHECK(!dest_is_empty_dir || dest_type == FileType::Directory)
        << "dest_is_empty_dir must imply dest_type == FileType::Directory";
    switch (src_type) {
      case FileType::Unknown:
        break;
      case FileType::NotFound:
        return {ENOENT};
      case FileType::File:
        switch (dest_type) {
          case FileType::Unknown:
            break;
          case FileType::NotFound:
            if (src_trailing_slash) {
              return {ENOTDIR};
            }
            if (dest_trailing_slash) {
              // A slash on the destination path requires that it exists,
              // so a confirmation that it's a directory can be performed.
              return {ENOENT};
            }
            return {};
          case FileType::File:
            if (src_trailing_slash || dest_trailing_slash) {
              return {ENOTDIR};
            }
            // The existing file is replaced successfuly.
            return {};
          case FileType::Directory:
            if (src_trailing_slash) {
              return {ENOTDIR};
            }
            return EISDIR;
        }
        break;
      case FileType::Directory:
        switch (dest_type) {
          case FileType::Unknown:
            break;
          case FileType::NotFound:
            // We don't have to care about the slashes when the source is a directory.
            return {};
          case FileType::File:
            return {ENOTDIR};
          case FileType::Directory:
            if (!paths_are_equal && !dest_is_empty_dir) {
              return {ENOTEMPTY};
            }
            return {};
        }
        break;
    }
    Unreachable("Invalid parameters passed to RenameSemantics");
  }

  Status CheckExpectedErrno(const std::string& src, const std::string& dest,
                            std::optional<int> expected_errno,
                            const char* expected_errno_name, FileInfo* out_src_info) {
    auto the_fs = fs();
    const bool src_trailing_slash = internal::HasTrailingSlash(src);
    const bool dest_trailing_slash = internal::HasTrailingSlash(dest);
    const auto src_path = std::string{internal::RemoveTrailingSlash(src)};
    const auto dest_path = std::string{internal::RemoveTrailingSlash(dest)};
    ARROW_ASSIGN_OR_RAISE(*out_src_info, the_fs->GetFileInfo(src_path));
    ARROW_ASSIGN_OR_RAISE(auto dest_info, the_fs->GetFileInfo(dest_path));
    bool dest_is_empty_dir = false;
    if (dest_info.type() == FileType::Directory) {
      FileSelector select;
      select.base_dir = dest_path;
      select.recursive = false;
      // TODO(ARROW-40014): investigate why this can't be false here
      select.allow_not_found = true;
      ARROW_ASSIGN_OR_RAISE(auto dest_contents, the_fs->GetFileInfo(select));
      if (dest_contents.empty()) {
        dest_is_empty_dir = true;
      }
    }
    auto paths_are_equal = src_path == dest_path;
    auto truly_expected_errno =
        RenameSemantics(out_src_info->type(), src_trailing_slash, dest_info.type(),
                        dest_trailing_slash, dest_is_empty_dir, paths_are_equal);
    if (truly_expected_errno != expected_errno) {
      if (expected_errno.has_value()) {
        return Status::Invalid("expected_errno=", expected_errno_name, "=",
                               *expected_errno,
                               " used in ASSERT_MOVE is incorrect. "
                               "POSIX semantics for this scenario require errno=",
                               strerror(truly_expected_errno.value_or(0)));
      } else {
        DCHECK(truly_expected_errno.has_value());
        return Status::Invalid(
            "ASSERT_MOVE used to assert success in a scenario for which "
            "POSIX semantics requires errno=",
            strerror(*truly_expected_errno));
      }
    }
    return Status::OK();
  }

  void AssertAfterMove(const std::string& src, const std::string& dest, FileType type) {
    if (internal::RemoveTrailingSlash(src) != internal::RemoveTrailingSlash(dest)) {
      AssertFileInfo(fs(), src, FileType::NotFound);
    }
    AssertFileInfo(fs(), dest, type);
  }

  std::optional<StringMatcher> MoveErrorMessageMatcher(const FileInfo& src_info,
                                                       const std::string& src,
                                                       const std::string& dest,
                                                       int for_errno) {
    switch (for_errno) {
      case ENOENT: {
        auto& path = src_info.type() == FileType::NotFound ? src : dest;
        return ::testing::HasSubstr("Path does not exist '" + path + "'");
      }
      case ENOTEMPTY:
        return ::testing::HasSubstr("Directory not empty: '" + dest + "'");
    }
    return std::nullopt;
  }

#define ASSERT_MOVE(src, dest, expected_errno)                                          \
  do {                                                                                  \
    auto _src = (src);                                                                  \
    auto _dest = (dest);                                                                \
    std::optional<int> _expected_errno = (expected_errno);                              \
    FileInfo _src_info;                                                                 \
    ASSERT_OK(                                                                          \
        CheckExpectedErrno(_src, _dest, _expected_errno, #expected_errno, &_src_info)); \
    auto _move_st = ::arrow::ToStatus(fs()->Move(_src, _dest));                         \
    if (_expected_errno.has_value()) {                                                  \
      if (WithErrno(_move_st, *_expected_errno)) {                                      \
        /* If the Move failed, the source should remain unchanged. */                   \
        AssertFileInfo(fs(), std::string{internal::RemoveTrailingSlash(_src)},          \
                       _src_info.type());                                               \
        auto _message_matcher =                                                         \
            MoveErrorMessageMatcher(_src_info, _src, _dest, *_expected_errno);          \
        if (_message_matcher.has_value()) {                                             \
          EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, *_message_matcher, _move_st);        \
        } else {                                                                        \
          SUCCEED();                                                                    \
        }                                                                               \
      } else {                                                                          \
        FAIL() << "Move '" ARROW_STRINGIFY(src) "' to '" ARROW_STRINGIFY(dest)          \
               << "' did not fail with errno=" << #expected_errno;                      \
      }                                                                                 \
    } else {                                                                            \
      if (!_move_st.ok()) {                                                             \
        FAIL() << "Move '" ARROW_STRINGIFY(src) "' to '" ARROW_STRINGIFY(dest)          \
               << "' failed with " << _move_st.ToString();                              \
      } else {                                                                          \
        AssertAfterMove(_src, _dest, _src_info.type());                                 \
      }                                                                                 \
    }                                                                                   \
  } while (false)

#define ASSERT_MOVE_OK(src, dest) ASSERT_MOVE((src), (dest), std::nullopt)

  // Tests for Move()

 public:
  void TestRenameContainer() {
    EXPECT_OK_AND_ASSIGN(auto env, GetAzureEnv());
    auto data = SetUpPreexistingData();
    // Container exists, so renaming to the same name succeeds because it's a no-op.
    ASSERT_MOVE_OK(data.container_name, data.container_name);
    // Renaming a container that doesn't exist fails.
    ASSERT_MOVE("missing-container", "missing-container", ENOENT);
    ASSERT_MOVE("missing-container", data.container_name, ENOENT);
    // Renaming a container to an existing non-empty container fails.
    auto non_empty_container = PreexistingData::RandomContainerName(rng_);
    auto non_empty_container_client = CreateContainer(non_empty_container);
    CreateBlob(non_empty_container_client, "object1", PreexistingData::kLoremIpsum);
    ASSERT_MOVE(data.container_name, non_empty_container, ENOTEMPTY);
    // Renaming to an empty container fails to replace it
    auto empty_container = PreexistingData::RandomContainerName(rng_);
    auto empty_container_client = CreateContainer(empty_container);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError,
        ::testing::HasSubstr("Unable to replace empty container: '" + empty_container +
                             "'"),
        fs()->Move(data.container_name, empty_container));
    // Renaming to a non-existing container creates it
    auto missing_container = PreexistingData::RandomContainerName(rng_);
    AssertFileInfo(fs(), missing_container, FileType::NotFound);
    if (env->backend() == AzureBackend::kAzurite) {
      // Azurite returns a 201 Created for RenameBlobContainer, but the created
      // container doesn't contain the blobs from the source container and
      // the source container remains undeleted after the "rename".
    } else {
      // See Azure SDK issue/question:
      // https://github.com/Azure/azure-sdk-for-cpp/issues/5262
      EXPECT_RAISES_WITH_MESSAGE_THAT(
          IOError,
          ::testing::HasSubstr("The 'rename' operation is not supported on containers."),
          fs()->Move(data.container_name, missing_container));
      // ASSERT_MOVE_OK(data.container_name, missing_container);
      // AssertFileInfo(fs(),
      //                ConcatAbstractPath(missing_container,
      //                PreexistingData::kObjectName), FileType::File);
    }
    // Renaming to an empty container can work if the source is also empty
    auto new_empty_container = PreexistingData::RandomContainerName(rng_);
    auto new_empty_container_client = CreateContainer(new_empty_container);
    ASSERT_MOVE_OK(empty_container, new_empty_container);
  }

  void TestMoveContainerToPath() {
    auto data = SetUpPreexistingData();
    ASSERT_MOVE("missing-container", data.ContainerPath("new-subdir"), ENOENT);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        HasDirMoveToSubdirMessage(data.container_name, data.ContainerPath("new-subdir")),
        fs()->Move(data.container_name, data.ContainerPath("new-subdir")));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented,
        HasCrossContainerNotImplementedMessage(data.container_name,
                                               "missing-container/new-subdir"),
        fs()->Move(data.container_name, "missing-container/new-subdir"));
  }

  void TestCreateContainerFromPath() {
    auto data = SetUpPreexistingData();
    auto missing_path = data.RandomDirectoryPath(rng_);
    ASSERT_MOVE(missing_path, "new-container", ENOENT);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("Creating files at '/' is not possible, only directories."),
        fs()->Move(data.ObjectPath(), "new-file"));
    auto src_dir_path = data.RandomDirectoryPath(rng_);
    ASSERT_OK(fs()->CreateDir(src_dir_path, false));
    EXPECT_OK_AND_ASSIGN(auto src_dir_info, fs()->GetFileInfo(src_dir_path));
    EXPECT_EQ(src_dir_info.type(), FileType::Directory);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented,
        HasCrossContainerNotImplementedMessage(src_dir_path, "new-container"),
        fs()->Move(src_dir_path, "new-container"));
  }

  void TestMovePath() {
    Status st;
    auto data = SetUpPreexistingData();
    auto another_container = PreexistingData::RandomContainerName(rng_);
    CreateContainer(another_container);
    // When source doesn't exist.
    ASSERT_MOVE("missing-container/src-path", data.ContainerPath("dest-path"), ENOENT);
    auto missing_path1 = data.RandomDirectoryPath(rng_);
    ASSERT_MOVE(missing_path1, "missing-container/path", ENOENT);

    // But when source exists...
    // ...and containers are different, we get an error message telling cross-container
    // moves are not implemented.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented,
        HasCrossContainerNotImplementedMessage(data.ObjectPath(),
                                               "missing-container/path"),
        fs()->Move(data.ObjectPath(), "missing-container/path"));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented,
        HasCrossContainerNotImplementedMessage(
            data.ObjectPath(), ConcatAbstractPath(another_container, "path")),
        fs()->Move(data.ObjectPath(), ConcatAbstractPath(another_container, "path")));
    AssertFileInfo(fs(), data.ObjectPath(), FileType::File);

    if (!WithHierarchicalNamespace()) {
      GTEST_SKIP() << "The rest of TestMovePath is not implemented for non-HNS scenarios";
    }

    EXPECT_RAISES_WITH_MESSAGE_THAT(
        IOError, HasMissingParentDirMessage(data.Path("missing-subdir/file")),
        fs()->Move(data.ObjectPath(), data.Path("missing-subdir/file")));
    AssertFileInfo(fs(), data.ObjectPath(), FileType::File);

    // src is a file and dest does not exists
    ASSERT_MOVE_OK(data.ObjectPath(), data.Path("file0"));
    ASSERT_MOVE(data.Path("file0/"), data.Path("file1"), ENOTDIR);
    ASSERT_MOVE(data.Path("file0"), data.Path("file1/"), ENOENT);
    ASSERT_MOVE(data.Path("file0/"), data.Path("file1/"), ENOTDIR);
    // "file0" exists

    // src is a file and dest exists (as a file)
    auto adlfs_client =
        datalake_service_client_->GetFileSystemClient(data.container_name);
    CreateFile(adlfs_client, PreexistingData::kObjectName, PreexistingData::kLoremIpsum);
    CreateFile(adlfs_client, "file1", PreexistingData::kLoremIpsum);
    ASSERT_MOVE_OK(data.ObjectPath(), data.Path("file0"));
    ASSERT_MOVE(data.Path("file1/"), data.Path("file0"), ENOTDIR);
    ASSERT_MOVE(data.Path("file1"), data.Path("file0/"), ENOTDIR);
    ASSERT_MOVE(data.Path("file1/"), data.Path("file0/"), ENOTDIR);
    // "file0" and "file1" exist

    // src is a file and dest exists (as an empty dir)
    CreateDirectory(adlfs_client, "subdir0");
    ASSERT_MOVE(data.Path("file0"), data.Path("subdir0"), EISDIR);
    ASSERT_MOVE(data.Path("file0/"), data.Path("subdir0"), ENOTDIR);
    ASSERT_MOVE(data.Path("file0"), data.Path("subdir0/"), EISDIR);
    ASSERT_MOVE(data.Path("file0/"), data.Path("subdir0/"), ENOTDIR);

    // src is a file and dest exists (as a non-empty dir)
    CreateFile(adlfs_client, "subdir0/file-at-subdir");
    ASSERT_MOVE(data.Path("file0"), data.Path("subdir0"), EISDIR);
    ASSERT_MOVE(data.Path("file0/"), data.Path("subdir0"), ENOTDIR);
    ASSERT_MOVE(data.Path("file0"), data.Path("subdir0/"), EISDIR);
    ASSERT_MOVE(data.Path("file0/"), data.Path("subdir0/"), ENOTDIR);
    // "subdir0/file-at-subdir" exists

    // src is a directory and dest does not exists
    ASSERT_MOVE_OK(data.Path("subdir0"), data.Path("subdir1"));
    ASSERT_MOVE_OK(data.Path("subdir1/"), data.Path("subdir2"));
    ASSERT_MOVE_OK(data.Path("subdir2"), data.Path("subdir3/"));
    ASSERT_MOVE_OK(data.Path("subdir3/"), data.Path("subdir4/"));
    AssertFileInfo(fs(), data.Path("subdir4/file-at-subdir"), FileType::File);
    // "subdir4/file-at-subdir" exists

    // src is a directory and dest exists as an empty directory
    CreateDirectory(adlfs_client, "subdir0");
    CreateDirectory(adlfs_client, "subdir1");
    CreateDirectory(adlfs_client, "subdir2");
    CreateDirectory(adlfs_client, "subdir3");
    ASSERT_MOVE_OK(data.Path("subdir4"), data.Path("subdir0"));
    ASSERT_MOVE_OK(data.Path("subdir0/"), data.Path("subdir1"));
    ASSERT_MOVE_OK(data.Path("subdir1"), data.Path("subdir2/"));
    ASSERT_MOVE_OK(data.Path("subdir2/"), data.Path("subdir3/"));
    AssertFileInfo(fs(), data.Path("subdir3/file-at-subdir"), FileType::File);
    // "subdir3/file-at-subdir" exists

    // src is directory and dest exists as a non-empty directory
    CreateDirectory(adlfs_client, "subdir0");
    ASSERT_MOVE(data.Path("subdir0"), data.Path("subdir3"), ENOTEMPTY);
    ASSERT_MOVE(data.Path("subdir0/"), data.Path("subdir3"), ENOTEMPTY);
    ASSERT_MOVE(data.Path("subdir0"), data.Path("subdir3/"), ENOTEMPTY);
    ASSERT_MOVE(data.Path("subdir0/"), data.Path("subdir3/"), ENOTEMPTY);
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

  // . is immediately before "/" lexicographically, ensure that this doesn't
  // cause unexpected issues. NOTE: Its seems real Azure blob storage doesn't
  // allow blob names to end in `.`
  ASSERT_OK_AND_ASSIGN(output, fs()->OpenOutputStream(
                                   data.ContainerPath("test-object-dir/some_other_dir.a"),
                                   /*metadata=*/{}));
  ASSERT_OK(output->Write(lorem_ipsum));
  ASSERT_OK(output->Close());
  ASSERT_OK_AND_ASSIGN(output,
                       fs()->OpenOutputStream(data.ContainerPath(kObjectName + ".a"),
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
    auto adlfs_client =
        datalake_service_client_->GetFileSystemClient(data.container_name);
    CreateDirectory(adlfs_client, "test-empty-object-dir");
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

TYPED_TEST(TestAzureFileSystemOnAllEnvs, CreateDirOnRootWithTrailingSlash) {
  this->TestCreateDirOnRootWithTrailingSlash();
}

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

TYPED_TEST(TestAzureFileSystemOnAllScenarios,
           CreateDirOnExistingContainerWithTrailingSlash) {
  this->TestCreateDirOnExistingContainerWithTrailingSlash();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, CreateDirOnMissingContainer) {
  this->TestCreateDirOnMissingContainer();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DisallowReadingOrWritingDirectoryMarkers) {
  this->TestDisallowReadingOrWritingDirectoryMarkers();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios,
           DisallowCreatingFileAndDirectoryWithTheSameName) {
  this->TestDisallowCreatingFileAndDirectoryWithTheSameName();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, OpenOutputStreamWithMissingContainer) {
  this->TestOpenOutputStreamWithMissingContainer();
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

TYPED_TEST(TestAzureFileSystemOnAllScenarios, NonEmptyDirWithTrailingSlash) {
  this->TestNonEmptyDirWithTrailingSlash();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirSuccessHaveDirectory) {
  this->TestDeleteDirSuccessHaveDirectory();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirContentsSuccessExist) {
  this->TestDeleteDirContentsSuccessExist();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios,
           DeleteDirContentsSuccessExistWithTrailingSlash) {
  this->TestDeleteDirContentsSuccessExistWithTrailingSlash();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirContentsSuccessNonexistent) {
  this->TestDeleteDirContentsSuccessNonexistent();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteDirContentsFailureNonexistent) {
  this->TestDeleteDirContentsFailureNonexistent();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteFileAtRoot) {
  this->TestDeleteFileAtRoot();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteFileAtContainerRoot) {
  this->TestDeleteFileAtContainerRoot();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, DeleteFileAtSubdirectory) {
  this->TestDeleteFileAtSubdirectory(/*create_empty_dir_marker_first=*/false);
  if (!this->WithHierarchicalNamespace()) {
    this->TestDeleteFileAtSubdirectory(/*create_empty_dir_marker_first=*/true);
  }
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, RenameContainer) {
  this->TestRenameContainer();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, MoveContainerToPath) {
  this->TestMoveContainerToPath();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, CreateContainerFromPath) {
  this->TestCreateContainerFromPath();
}

TYPED_TEST(TestAzureFileSystemOnAllScenarios, MovePath) { this->TestMovePath(); }

TYPED_TEST(TestAzureFileSystemOnAllScenarios, SASCredential) {
  this->TestSASCredential();
}

// Tests using Azurite (the local Azure emulator)

TEST_F(TestAzuriteFileSystem, CheckIfHierarchicalNamespaceIsEnabledRuntimeError) {
  ASSERT_OK(options_.ConfigureAccountKeyCredential("not-base64"));
  ASSERT_OK_AND_ASSIGN(auto datalake_service_client,
                       options_.MakeDataLakeServiceClient());
  auto adlfs_client = datalake_service_client->GetFileSystemClient("nonexistent");
  ASSERT_RAISES(UnknownError,
                internal::CheckIfHierarchicalNamespaceIsEnabled(adlfs_client, options_));
}

TEST_F(TestAzuriteFileSystem, CheckIfHierarchicalNamespaceIsEnabledTransportError) {
  options_.dfs_storage_authority = "127.0.0.1:20000";  // Wrong port
  ASSERT_OK_AND_ASSIGN(auto datalake_service_client,
                       options_.MakeDataLakeServiceClient());
  auto adlfs_client = datalake_service_client->GetFileSystemClient("nonexistent");
  ASSERT_RAISES(IOError,
                internal::CheckIfHierarchicalNamespaceIsEnabled(adlfs_client, options_));
}

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
  ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
  if (env->HasSubmitBatchBug()) {
    GTEST_SKIP() << kSubmitBatchBugMessage;
  }
  auto data = SetUpPreexistingData();
  const auto directory_path = data.RandomDirectoryPath(rng_);
  // DeleteDir() fails if the directory doesn't exist.
  ASSERT_RAISES(IOError, fs()->DeleteDir(directory_path));
  AssertFileInfo(fs(), directory_path, FileType::NotFound);
}

TEST_F(TestAzuriteFileSystem, DeleteDirSuccessHaveBlobs) {
  ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
  if (env->HasSubmitBatchBug()) {
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
  ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
  if (env->HasSubmitBatchBug()) {
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
  ASSERT_OK_AND_ASSIGN(auto env, GetAzureEnv());
  if (env->HasSubmitBatchBug()) {
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

TEST_F(TestAzuriteFileSystem, CopyFileSuccessDestinationDifferentContainer) {
  auto data = SetUpPreexistingData();
  auto data2 = SetUpPreexistingData();
  const auto destination_path = data2.ContainerPath("copy-destionation");
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
      output.resize(value.size() / 2);
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
  ASSERT_OK_AND_ASSIGN(auto input, fs()->OpenInputStream(full_path));
  ASSERT_OK_AND_ASSIGN(auto metadata, input->ReadMetadata());
  ASSERT_OK_AND_ASSIGN(auto foo_value, metadata->Get("foo"));
  ASSERT_EQ("bar", foo_value);

  // Check that explicit metadata overrides the defaults.
  ASSERT_OK_AND_ASSIGN(output,
                       fs_with_defaults->OpenOutputStream(
                           full_path, arrow::key_value_metadata({{"bar", "foo"}})));
  ASSERT_OK(output->Write(expected));
  ASSERT_OK(output->Close());
  ASSERT_OK_AND_ASSIGN(input, fs()->OpenInputStream(full_path));
  ASSERT_OK_AND_ASSIGN(metadata, input->ReadMetadata());
  // Defaults are overwritten and not merged.
  ASSERT_NOT_OK(metadata->Get("foo"));
  ASSERT_OK_AND_ASSIGN(auto bar_value, metadata->Get("bar"));
  ASSERT_EQ("foo", bar_value);

  // Metadata can be written without writing any data.
  ASSERT_OK_AND_ASSIGN(output,
                       fs_with_defaults->OpenAppendStream(
                           full_path, arrow::key_value_metadata({{"bar", "baz"}})));
  ASSERT_OK(output->Close());
  ASSERT_OK_AND_ASSIGN(input, fs()->OpenInputStream(full_path));
  ASSERT_OK_AND_ASSIGN(metadata, input->ReadMetadata());
  // Defaults are overwritten and not merged.
  ASSERT_NOT_OK(metadata->Get("foo"));
  ASSERT_OK_AND_ASSIGN(bar_value, metadata->Get("bar"));
  ASSERT_EQ("baz", bar_value);
}

TEST_F(TestAzuriteFileSystem, WriteMetadataHttpHeaders) {
  auto data = SetUpPreexistingData();
  ASSERT_OK_AND_ASSIGN(auto output,
                       fs()->OpenOutputStream(
                           data.ObjectPath(),
                           arrow::key_value_metadata({{"Content-Type", "text/plain"}})));
  ASSERT_OK(output->Write(PreexistingData::kLoremIpsum));
  ASSERT_OK(output->Close());

  ASSERT_OK_AND_ASSIGN(auto input, fs()->OpenInputStream(data.ObjectPath()));
  ASSERT_OK_AND_ASSIGN(auto metadata, input->ReadMetadata());
  ASSERT_OK_AND_ASSIGN(auto content_type, metadata->Get("Content-Type"));
  ASSERT_EQ("text/plain", content_type);
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamSmallNoBackgroundWrites) {
  options_.background_writes = false;
  TestOpenOutputStreamSmall();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamSmall) { TestOpenOutputStreamSmall(); }

TEST_F(TestAzuriteFileSystem, OpenOutputStreamLargeNoBackgroundWrites) {
  options_.background_writes = false;
  TestOpenOutputStreamLarge();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamLarge) { TestOpenOutputStreamLarge(); }

TEST_F(TestAzuriteFileSystem, OpenOutputStreamLargeSingleWriteNoBackgroundWrites) {
  options_.background_writes = false;
  TestOpenOutputStreamLargeSingleWrite();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamLargeSingleWrite) {
  TestOpenOutputStreamLargeSingleWrite();
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

TEST_F(TestAzuriteFileSystem, OpenOutputStreamCloseAsync) {
  TestOpenOutputStreamCloseAsync();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamCloseAsyncNoBackgroundWrites) {
  options_.background_writes = false;
  TestOpenOutputStreamCloseAsync();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamAsyncDestructor) {
  TestOpenOutputStreamCloseAsyncDestructor();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamAsyncDestructorNoBackgroundWrites) {
  options_.background_writes = false;
  TestOpenOutputStreamCloseAsyncDestructor();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamDestructor) {
  TestOpenOutputStreamDestructor();
}

TEST_F(TestAzuriteFileSystem, OpenOutputStreamDestructorNoBackgroundWrites) {
  options_.background_writes = false;
  TestOpenOutputStreamDestructor();
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

TEST_F(TestAzuriteFileSystem, PathFromUri) {
  ASSERT_EQ(
      "container/some/path",
      fs()->PathFromUri("abfss://storageacc.blob.core.windows.net/container/some/path"));
  ASSERT_EQ("container/some/path",
            fs()->PathFromUri("abfss://acc:pw@container/some/path"));
  ASSERT_RAISES(Invalid, fs()->PathFromUri("http://acc:pw@container/some/path"));
}
}  // namespace fs
}  // namespace arrow

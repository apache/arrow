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

#ifndef _WIN32
#  include <sys/wait.h>
#endif

#include "arrow/filesystem/s3_test_cert.h"
#include "arrow/filesystem/s3_test_util.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/testing/process.h"
#include "arrow/testing/util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace fs {

using ::arrow::internal::FileClose;
using ::arrow::internal::FileDescriptor;
using ::arrow::internal::FileOpenWritable;
using ::arrow::internal::FileWrite;
using ::arrow::internal::PlatformFilename;
using ::arrow::internal::TemporaryDir;

namespace {

const char* kMinioExecutableName = "minio";
const char* kMinioAccessKey = "minio";
const char* kMinioSecretKey = "miniopass";

// Environment variables to configure another S3-compatible service
const char* kEnvConnectString = "ARROW_TEST_S3_CONNECT_STRING";
const char* kEnvAccessKey = "ARROW_TEST_S3_ACCESS_KEY";
const char* kEnvSecretKey = "ARROW_TEST_S3_SECRET_KEY";

std::string GenerateConnectString() { return GetListenAddress(); }

}  // namespace

struct MinioTestServer::Impl {
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::unique_ptr<TemporaryDir> temp_dir_ca_;
  std::string connect_string_;
  std::string access_key_ = kMinioAccessKey;
  std::string secret_key_ = kMinioSecretKey;
  std::unique_ptr<util::Process> server_process_;
};

MinioTestServer::MinioTestServer() : impl_(new Impl) {}

MinioTestServer::~MinioTestServer() {
  auto st = Stop();
  ARROW_UNUSED(st);
}

std::string MinioTestServer::connect_string() const { return impl_->connect_string_; }

std::string MinioTestServer::access_key() const { return impl_->access_key_; }

std::string MinioTestServer::secret_key() const { return impl_->secret_key_; }

void MinioTestServer::GenerateCertificateFile() {
  PlatformFilename public_crt_file, private_key_file;
  ASSERT_OK_AND_ASSIGN(public_crt_file,
                       PlatformFilename::FromString(
                           impl_->temp_dir_ca_->path().ToString() + "/public.crt"));
  ASSERT_OK_AND_ASSIGN(FileDescriptor public_cert_fd, FileOpenWritable(public_crt_file));
  ASSERT_OK(FileWrite(public_cert_fd.fd(),
                      reinterpret_cast<const uint8_t*>(kMinioPublicCert),
                      strlen(kMinioPublicCert)));
  ASSERT_OK(public_cert_fd.Close());

  ASSERT_OK_AND_ASSIGN(private_key_file,
                       PlatformFilename::FromString(
                           impl_->temp_dir_ca_->path().ToString() + "/private.key"));
  ASSERT_OK_AND_ASSIGN(FileDescriptor private_key_fd, FileOpenWritable(private_key_file));
  ASSERT_OK(FileWrite(private_key_fd.fd(),
                      reinterpret_cast<const uint8_t*>(kMinioPrivateKey),
                      strlen(kMinioPrivateKey)));
  ASSERT_OK(private_key_fd.Close());
}

Status MinioTestServer::Start(bool enable_tls) {
  const char* connect_str = std::getenv(kEnvConnectString);
  const char* access_key = std::getenv(kEnvAccessKey);
  const char* secret_key = std::getenv(kEnvSecretKey);
  if (connect_str && access_key && secret_key) {
    // Use external instance
    impl_->connect_string_ = connect_str;
    impl_->access_key_ = access_key;
    impl_->secret_key_ = secret_key;
    return Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(impl_->temp_dir_, TemporaryDir::Make("s3fs-test-"));

  impl_->server_process_ = std::make_unique<util::Process>();
  impl_->server_process_->SetEnv("MINIO_ACCESS_KEY", kMinioAccessKey);
  impl_->server_process_->SetEnv("MINIO_SECRET_KEY", kMinioSecretKey);
  // Disable the embedded console (one less listening address to care about)
  impl_->server_process_->SetEnv("MINIO_BROWSER", "off");
  impl_->connect_string_ = GenerateConnectString();
  ARROW_RETURN_NOT_OK(impl_->server_process_->SetExecutable(kMinioExecutableName));
  // NOTE: --quiet makes startup faster by suppressing remote version check
  std::vector<std::string> start_args = {"server", "--quiet", "--compat", "--address",
                                         impl_->connect_string_};
  if (enable_tls) {
    // create the dedicated folder for certificate file, rather than reuse the data
    // folder, since there is case to check whether the folder is empty.
    ARROW_ASSIGN_OR_RAISE(impl_->temp_dir_ca_, TemporaryDir::Make("s3fs-test-ca"));
    GenerateCertificateFile();
    start_args.push_back("--certs-dir");
    start_args.push_back(impl_->temp_dir_ca_->path().ToString());
    arrow::fs::FileSystemGlobalOptions global_options;
    global_options.tls_ca_dir_path = impl_->temp_dir_ca_->path().ToString();
    ARROW_RETURN_NOT_OK(arrow::fs::Initialize(global_options));
  }
  // apply the data path at the end since some minio version only support it at the end
  start_args.push_back(impl_->temp_dir_->path().ToString());
  impl_->server_process_->SetArgs(start_args);
  ARROW_RETURN_NOT_OK(impl_->server_process_->Execute());
  return Status::OK();
}

Status MinioTestServer::Stop() {
  impl_->server_process_ = nullptr;
  return Status::OK();
}

struct MinioTestEnvironment::Impl {
  std::function<Future<std::shared_ptr<MinioTestServer>>()> server_generator_;

  Result<std::shared_ptr<MinioTestServer>> LaunchOneServer() {
    auto server = std::make_shared<MinioTestServer>();
    RETURN_NOT_OK(server->Start());
    return server;
  }
};

MinioTestEnvironment::MinioTestEnvironment() : impl_(new Impl) {}

MinioTestEnvironment::~MinioTestEnvironment() = default;

void MinioTestEnvironment::SetUp() {
  auto pool = ::arrow::internal::GetCpuThreadPool();

  auto launch_one_server = []() -> Result<std::shared_ptr<MinioTestServer>> {
    auto server = std::make_shared<MinioTestServer>();
    RETURN_NOT_OK(server->Start());
    return server;
  };
  impl_->server_generator_ = [pool, launch_one_server]() {
    return DeferNotOk(pool->Submit(launch_one_server));
  };
  impl_->server_generator_ =
      MakeReadaheadGenerator(std::move(impl_->server_generator_), pool->GetCapacity());
}

Result<std::shared_ptr<MinioTestServer>> MinioTestEnvironment::GetOneServer() {
  return impl_->server_generator_().result();
}

}  // namespace fs
}  // namespace arrow

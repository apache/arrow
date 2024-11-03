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

#include "arrow/filesystem/s3_test_cert_internal.h"
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

}  // namespace

struct MinioTestServer::Impl {
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::unique_ptr<TemporaryDir> temp_dir_ca_;
  std::string connect_string_;
  std::string access_key_ = kMinioAccessKey;
  std::string secret_key_ = kMinioSecretKey;
  std::unique_ptr<util::Process> server_process_;
  std::string scheme_ = "http";
};

MinioTestServer::MinioTestServer() : impl_(new Impl) {}

MinioTestServer::~MinioTestServer() {
  auto st = Stop();
  ARROW_UNUSED(st);
}

std::string MinioTestServer::connect_string() const { return impl_->connect_string_; }

std::string MinioTestServer::access_key() const { return impl_->access_key_; }

std::string MinioTestServer::secret_key() const { return impl_->secret_key_; }

std::string MinioTestServer::ca_dir_path() const {
  return impl_->temp_dir_ca_->path().ToString();
}

std::string MinioTestServer::ca_file_path() const {
  return impl_->temp_dir_ca_->path().ToString() + "/public.crt";
}

std::string MinioTestServer::scheme() const { return impl_->scheme_; }

Status MinioTestServer::GenerateCertificateFile() {
  // create the dedicated folder for certificate file, rather than reuse the data
  // folder, since there is test case to check whether the folder is empty.
  ARROW_ASSIGN_OR_RAISE(impl_->temp_dir_ca_, TemporaryDir::Make("s3fs-test-ca-"));

  ARROW_ASSIGN_OR_RAISE(auto public_crt_file,
                        PlatformFilename::FromString(ca_dir_path() + "/public.crt"));
  ARROW_ASSIGN_OR_RAISE(auto public_cert_fd, FileOpenWritable(public_crt_file));
  ARROW_RETURN_NOT_OK(FileWrite(public_cert_fd.fd(),
                                reinterpret_cast<const uint8_t*>(kMinioCert),
                                strlen(kMinioCert)));
  ARROW_RETURN_NOT_OK(public_cert_fd.Close());

  ARROW_ASSIGN_OR_RAISE(auto private_key_file,
                        PlatformFilename::FromString(ca_dir_path() + "/private.key"));
  ARROW_ASSIGN_OR_RAISE(auto private_key_fd, FileOpenWritable(private_key_file));
  ARROW_RETURN_NOT_OK(FileWrite(private_key_fd.fd(),
                                reinterpret_cast<const uint8_t*>(kMinioPrivateKey),
                                strlen(kMinioPrivateKey)));
  ARROW_RETURN_NOT_OK(private_key_fd.Close());

  return Status::OK();
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
  // NOTE: --quiet makes startup faster by suppressing remote version check
  std::vector<std::string> minio_args({"server", "--quiet", "--compat"});
  if (enable_tls) {
    ARROW_RETURN_NOT_OK(GenerateCertificateFile());
    minio_args.emplace_back("--certs-dir");
    minio_args.emplace_back(ca_dir_path());
    impl_->scheme_ = "https";
    // With TLS enabled, we need the connection hostname to match the certificate's
    // subject name. This also constrains the actual listening IP address.
    impl_->connect_string_ = GetListenAddress("localhost");
  } else {
    // Without TLS enabled, we want to minimize the likelihood of address collisions
    // by varying the listening IP address (note that most tests don't enable TLS).
    impl_->connect_string_ = GetListenAddress();
  }
  minio_args.emplace_back("--address");
  minio_args.emplace_back(impl_->connect_string_);
  minio_args.emplace_back(impl_->temp_dir_->path().ToString());

  ARROW_RETURN_NOT_OK(impl_->server_process_->SetExecutable(kMinioExecutableName));
  impl_->server_process_->SetArgs(minio_args);
  ARROW_RETURN_NOT_OK(impl_->server_process_->Execute());
  return Status::OK();
}

Status MinioTestServer::Stop() {
  impl_->server_process_ = nullptr;
  return Status::OK();
}

struct MinioTestEnvironment::Impl {
  std::function<Future<std::shared_ptr<MinioTestServer>>()> server_generator_;
  bool enable_tls_;

  explicit Impl(bool enable_tls) : enable_tls_(enable_tls) {}

  Result<std::shared_ptr<MinioTestServer>> LaunchOneServer() {
    auto server = std::make_shared<MinioTestServer>();
    RETURN_NOT_OK(server->Start(enable_tls_));
    return server;
  }
};

MinioTestEnvironment::MinioTestEnvironment(bool enable_tls)
    : impl_(new Impl(enable_tls)) {}

MinioTestEnvironment::~MinioTestEnvironment() = default;

void MinioTestEnvironment::SetUp() {
  auto pool = ::arrow::internal::GetCpuThreadPool();

  auto launch_one_server =
      [enable_tls = impl_->enable_tls_]() -> Result<std::shared_ptr<MinioTestServer>> {
    auto server = std::make_shared<MinioTestServer>();
    RETURN_NOT_OK(server->Start(enable_tls));
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

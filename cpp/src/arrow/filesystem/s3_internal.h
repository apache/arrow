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

#pragma once

#include <sstream>
#include <string>
#include <tuple>
#include <utility>

#include <aws/core/Aws.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/StringUtils.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/print.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace fs {
namespace internal {

// XXX Should we expose this at some point?
enum class S3Backend { Amazon, Minio, Other };

// Detect the S3 backend type from the S3 server's response headers
S3Backend DetectS3Backend(const Aws::Http::HeaderValueCollection& headers) {
  const auto it = headers.find("server");
  if (it != headers.end()) {
    const auto& value = util::string_view(it->second);
    if (value.find("AmazonS3") != std::string::npos) {
      return S3Backend::Amazon;
    }
    if (value.find("MinIO") != std::string::npos) {
      return S3Backend::Minio;
    }
  }
  return S3Backend::Other;
}

template <typename Error>
S3Backend DetectS3Backend(const Aws::Client::AWSError<Error>& error) {
  return DetectS3Backend(error.GetResponseHeaders());
}

template <typename Error>
inline bool IsConnectError(const Aws::Client::AWSError<Error>& error) {
  if (error.ShouldRetry()) {
    return true;
  }
  // Sometimes Minio may fail with a 503 error
  // (exception name: XMinioServerNotInitialized,
  //  message: "Server not initialized, please try again")
  if (error.GetExceptionName() == "XMinioServerNotInitialized") {
    return true;
  }
  return false;
}

inline bool IsNotFound(const Aws::Client::AWSError<Aws::S3::S3Errors>& error) {
  const auto error_type = error.GetErrorType();
  return (error_type == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
          error_type == Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
}

inline bool IsAlreadyExists(const Aws::Client::AWSError<Aws::S3::S3Errors>& error) {
  const auto error_type = error.GetErrorType();
  return (error_type == Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS ||
          error_type == Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU);
}

// TODO qualify error messages with a prefix indicating context
// (e.g. "When completing multipart upload to bucket 'xxx', key 'xxx': ...")
template <typename ErrorType>
Status ErrorToStatus(const std::string& prefix, const std::string& operation,
                     const Aws::Client::AWSError<ErrorType>& error) {
  // XXX Handle fine-grained error types
  // See
  // https://sdk.amazonaws.com/cpp/api/LATEST/namespace_aws_1_1_s3.html#ae3f82f8132b619b6e91c88a9f1bde371
  return Status::IOError(prefix, "AWS Error [code ",
                         static_cast<int>(error.GetErrorType()), "] during ", operation,
                         " operation: ", error.GetMessage());
}

template <typename ErrorType, typename... Args>
Status ErrorToStatus(const std::tuple<Args&...>& prefix, const std::string& operation,
                     const Aws::Client::AWSError<ErrorType>& error) {
  std::stringstream ss;
  ::arrow::internal::PrintTuple(&ss, prefix);
  return ErrorToStatus(ss.str(), operation, error);
}

template <typename ErrorType>
Status ErrorToStatus(const std::string& operation,
                     const Aws::Client::AWSError<ErrorType>& error) {
  return ErrorToStatus(std::string(), operation, error);
}

template <typename AwsResult, typename Error>
Status OutcomeToStatus(const std::string& prefix, const std::string& operation,
                       const Aws::Utils::Outcome<AwsResult, Error>& outcome) {
  if (outcome.IsSuccess()) {
    return Status::OK();
  } else {
    return ErrorToStatus(prefix, operation, outcome.GetError());
  }
}

template <typename AwsResult, typename Error, typename... Args>
Status OutcomeToStatus(const std::tuple<Args&...>& prefix, const std::string& operation,
                       const Aws::Utils::Outcome<AwsResult, Error>& outcome) {
  if (outcome.IsSuccess()) {
    return Status::OK();
  } else {
    return ErrorToStatus(prefix, operation, outcome.GetError());
  }
}

template <typename AwsResult, typename Error>
Status OutcomeToStatus(const std::string& operation,
                       const Aws::Utils::Outcome<AwsResult, Error>& outcome) {
  return OutcomeToStatus(std::string(), operation, outcome);
}

template <typename AwsResult, typename Error>
Result<AwsResult> OutcomeToResult(const std::string& operation,
                                  Aws::Utils::Outcome<AwsResult, Error> outcome) {
  if (outcome.IsSuccess()) {
    return std::move(outcome).GetResultWithOwnership();
  } else {
    return ErrorToStatus(operation, outcome.GetError());
  }
}

inline Aws::String ToAwsString(const std::string& s) {
  // Direct construction of Aws::String from std::string doesn't work because
  // it uses a specific Allocator class.
  return Aws::String(s.begin(), s.end());
}

inline util::string_view FromAwsString(const Aws::String& s) {
  return {s.data(), s.length()};
}

inline Aws::String ToURLEncodedAwsString(const std::string& s) {
  return Aws::Utils::StringUtils::URLEncode(s.data());
}

inline TimePoint FromAwsDatetime(const Aws::Utils::DateTime& dt) {
  return std::chrono::time_point_cast<std::chrono::nanoseconds>(dt.UnderlyingTimestamp());
}

// A connect retry strategy with a controlled max duration.

class ConnectRetryStrategy : public Aws::Client::RetryStrategy {
 public:
  static const int32_t kDefaultRetryInterval = 200;     /* milliseconds */
  static const int32_t kDefaultMaxRetryDuration = 6000; /* milliseconds */

  explicit ConnectRetryStrategy(int32_t retry_interval = kDefaultRetryInterval,
                                int32_t max_retry_duration = kDefaultMaxRetryDuration)
      : retry_interval_(retry_interval), max_retry_duration_(max_retry_duration) {}

  bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                   long attempted_retries) const override {  // NOLINT runtime/int
    if (!IsConnectError(error)) {
      // Not a connect error, don't retry
      return false;
    }
    return attempted_retries * retry_interval_ < max_retry_duration_;
  }

  long CalculateDelayBeforeNextRetry(  // NOLINT runtime/int
      const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
      long attempted_retries) const override {  // NOLINT runtime/int
    return retry_interval_;
  }

 protected:
  int32_t retry_interval_;
  int32_t max_retry_duration_;
};

}  // namespace internal
}  // namespace fs
}  // namespace arrow

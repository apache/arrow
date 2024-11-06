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

#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include <aws/core/Aws.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/StringUtils.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"
#include "arrow/util/print.h"
#include "arrow/util/string.h"

#ifndef ARROW_AWS_SDK_VERSION_CHECK
// AWS_SDK_VERSION_{MAJOR,MINOR,PATCH} are available since 1.9.7.
#  if defined(AWS_SDK_VERSION_MAJOR) && defined(AWS_SDK_VERSION_MINOR) && \
      defined(AWS_SDK_VERSION_PATCH)
// Redundant "(...)" are for suppressing "Weird number of spaces at
// line-start. Are you using a 2-space indent? [whitespace/indent]
// [3]" errors...
#    define ARROW_AWS_SDK_VERSION_CHECK(major, minor, patch)                      \
      ((AWS_SDK_VERSION_MAJOR > (major) ||                                        \
        (AWS_SDK_VERSION_MAJOR == (major) && AWS_SDK_VERSION_MINOR > (minor)) ||  \
        ((AWS_SDK_VERSION_MAJOR == (major) && AWS_SDK_VERSION_MINOR == (minor) && \
          AWS_SDK_VERSION_PATCH >= (patch)))))
#  else
#    define ARROW_AWS_SDK_VERSION_CHECK(major, minor, patch) 0
#  endif
#endif  // !ARROW_AWS_SDK_VERSION_CHECK

#if ARROW_AWS_SDK_VERSION_CHECK(1, 9, 201)
#  define ARROW_S3_HAS_SSE_CUSTOMER_KEY
#endif

namespace arrow {
namespace fs {
namespace internal {

// XXX Should we expose this at some point?
enum class S3Backend { Amazon, Minio, Other };

// Detect the S3 backend type from the S3 server's response headers
inline S3Backend DetectS3Backend(const Aws::Http::HeaderValueCollection& headers) {
  const auto it = headers.find("server");
  if (it != headers.end()) {
    const auto& value = std::string_view(it->second);
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
inline S3Backend DetectS3Backend(const Aws::Client::AWSError<Error>& error) {
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

template <typename ErrorType>
inline std::optional<std::string> BucketRegionFromError(
    const Aws::Client::AWSError<ErrorType>& error) {
  if constexpr (std::is_same_v<ErrorType, Aws::S3::S3Errors>) {
    const auto& headers = error.GetResponseHeaders();
    const auto it = headers.find("x-amz-bucket-region");
    if (it != headers.end()) {
      const std::string region(it->second.begin(), it->second.end());
      return region;
    }
  }
  return std::nullopt;
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

inline std::string S3ErrorToString(Aws::S3::S3Errors error_type) {
  switch (error_type) {
#define S3_ERROR_CASE(NAME)     \
  case Aws::S3::S3Errors::NAME: \
    return #NAME;

    S3_ERROR_CASE(INCOMPLETE_SIGNATURE)
    S3_ERROR_CASE(INTERNAL_FAILURE)
    S3_ERROR_CASE(INVALID_ACTION)
    S3_ERROR_CASE(INVALID_CLIENT_TOKEN_ID)
    S3_ERROR_CASE(INVALID_PARAMETER_COMBINATION)
    S3_ERROR_CASE(INVALID_QUERY_PARAMETER)
    S3_ERROR_CASE(INVALID_PARAMETER_VALUE)
    S3_ERROR_CASE(MISSING_ACTION)
    S3_ERROR_CASE(MISSING_AUTHENTICATION_TOKEN)
    S3_ERROR_CASE(MISSING_PARAMETER)
    S3_ERROR_CASE(OPT_IN_REQUIRED)
    S3_ERROR_CASE(REQUEST_EXPIRED)
    S3_ERROR_CASE(SERVICE_UNAVAILABLE)
    S3_ERROR_CASE(THROTTLING)
    S3_ERROR_CASE(VALIDATION)
    S3_ERROR_CASE(ACCESS_DENIED)
    S3_ERROR_CASE(RESOURCE_NOT_FOUND)
    S3_ERROR_CASE(UNRECOGNIZED_CLIENT)
    S3_ERROR_CASE(MALFORMED_QUERY_STRING)
    S3_ERROR_CASE(SLOW_DOWN)
    S3_ERROR_CASE(REQUEST_TIME_TOO_SKEWED)
    S3_ERROR_CASE(INVALID_SIGNATURE)
    S3_ERROR_CASE(SIGNATURE_DOES_NOT_MATCH)
    S3_ERROR_CASE(INVALID_ACCESS_KEY_ID)
    S3_ERROR_CASE(REQUEST_TIMEOUT)
    S3_ERROR_CASE(NETWORK_CONNECTION)
    S3_ERROR_CASE(UNKNOWN)
    S3_ERROR_CASE(BUCKET_ALREADY_EXISTS)
    S3_ERROR_CASE(BUCKET_ALREADY_OWNED_BY_YOU)
    // The following is the most recent addition to S3Errors
    // and is not supported yet for some versions of the SDK
    // that Apache Arrow is using. This is not a big deal
    // since this error will happen only in very specialized
    // settings and we will print the correct numerical error
    // code as per the "default" case down below. We should
    // put it back once the SDK has been upgraded in all
    // Apache Arrow build configurations.
    // S3_ERROR_CASE(INVALID_OBJECT_STATE)
    S3_ERROR_CASE(NO_SUCH_BUCKET)
    S3_ERROR_CASE(NO_SUCH_KEY)
    S3_ERROR_CASE(NO_SUCH_UPLOAD)
    S3_ERROR_CASE(OBJECT_ALREADY_IN_ACTIVE_TIER)
    S3_ERROR_CASE(OBJECT_NOT_IN_ACTIVE_TIER)

#undef S3_ERROR_CASE
    default:
      return "[code " + ::arrow::internal::ToChars(static_cast<int>(error_type)) + "]";
  }
}

// TODO qualify error messages with a prefix indicating context
// (e.g. "When completing multipart upload to bucket 'xxx', key 'xxx': ...")
template <typename ErrorType>
Status ErrorToStatus(const std::string& prefix, const std::string& operation,
                     const Aws::Client::AWSError<ErrorType>& error,
                     const std::optional<std::string>& region = std::nullopt) {
  // XXX Handle fine-grained error types
  // See
  // https://sdk.amazonaws.com/cpp/api/LATEST/namespace_aws_1_1_s3.html#ae3f82f8132b619b6e91c88a9f1bde371
  auto error_type = static_cast<Aws::S3::S3Errors>(error.GetErrorType());
  std::stringstream ss;
  ss << S3ErrorToString(error_type);
  if (error_type == Aws::S3::S3Errors::UNKNOWN) {
    ss << " (HTTP status " << static_cast<int>(error.GetResponseCode()) << ")";
  }

  // Possibly an error due to wrong region configuration from client and bucket.
  std::optional<std::string> wrong_region_msg = std::nullopt;
  if (region.has_value()) {
    const auto maybe_region = BucketRegionFromError(error);
    if (maybe_region.has_value() && maybe_region.value() != region.value()) {
      wrong_region_msg = " Looks like the configured region is '" + region.value() +
                         "' while the bucket is located in '" + maybe_region.value() +
                         "'.";
    }
  }
  return Status::IOError(prefix, "AWS Error ", ss.str(), " during ", operation,
                         " operation: ", error.GetMessage(),
                         wrong_region_msg.value_or(""));
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

inline std::string_view FromAwsString(const Aws::String& s) {
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

/// \brief calculate the MD5 of the input SSE-C key (raw key, not base64 encoded)
/// \param sse_customer_key is the input SSE-C key
/// \return the base64 encoded MD5 for the input key
inline Result<std::string> CalculateSSECustomerKeyMD5(
    const std::string& sse_customer_key) {
  // The key needs to be 256 bits (32 bytes) according to
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html#specifying-s3-c-encryption
  if (sse_customer_key.length() != 32) {
    return Status::Invalid("32 bytes SSE-C key is expected");
  }

  // Convert the raw binary key to an Aws::String
  Aws::String sse_customer_key_aws_string(sse_customer_key.data(),
                                          sse_customer_key.length());

  // Compute the MD5 hash of the raw binary key
  Aws::Utils::ByteBuffer sse_customer_key_md5 =
      Aws::Utils::HashingUtils::CalculateMD5(sse_customer_key_aws_string);

  // Base64-encode the MD5 hash
  return arrow::util::base64_encode(std::string_view(
      reinterpret_cast<const char*>(sse_customer_key_md5.GetUnderlyingData()),
      sse_customer_key_md5.GetLength()));
}

struct SSECustomerKeyHeaders {
  std::string sse_customer_key;
  std::string sse_customer_key_md5;
  std::string sse_customer_algorithm;
};

inline Result<std::optional<SSECustomerKeyHeaders>> GetSSECustomerKeyHeaders(
    const std::string& sse_customer_key) {
  if (sse_customer_key.empty()) {
    return std::nullopt;
  }
#ifdef ARROW_S3_HAS_SSE_CUSTOMER_KEY
  ARROW_ASSIGN_OR_RAISE(auto md5, internal::CalculateSSECustomerKeyMD5(sse_customer_key));
  return SSECustomerKeyHeaders{arrow::util::base64_encode(sse_customer_key), md5,
                               "AES256"};
#else
  return Status::NotImplemented(
      "SSE customer key not supported by this version of the AWS SDK");
#endif
}

template <typename S3RequestType>
Status SetSSECustomerKey(S3RequestType* request, const std::string& sse_customer_key) {
  ARROW_ASSIGN_OR_RAISE(auto maybe_headers, GetSSECustomerKeyHeaders(sse_customer_key));
  if (!maybe_headers.has_value()) {
    return Status::OK();
  }
#ifdef ARROW_S3_HAS_SSE_CUSTOMER_KEY
  auto headers = std::move(maybe_headers).value();
  request->SetSSECustomerKey(headers.sse_customer_key);
  request->SetSSECustomerKeyMD5(headers.sse_customer_key_md5);
  request->SetSSECustomerAlgorithm(headers.sse_customer_algorithm);
  return Status::OK();
#else
  return Status::NotImplemented(
      "SSE customer key not supported by this version of the AWS SDK");
#endif
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow

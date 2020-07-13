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

#include "arrow/filesystem/s3fs.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <utility>

#ifdef _WIN32
// Undefine preprocessor macros that interfere with AWS function / method names
#ifdef GetMessage
#undef GetMessage
#endif
#ifdef GetObject
#undef GetObject
#endif
#endif

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListBucketsResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "arrow/buffer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/s3_internal.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/windows_fixup.h"

namespace arrow {

using internal::Uri;

namespace fs {

using ::Aws::Client::AWSError;
using ::Aws::S3::S3Errors;
namespace S3Model = Aws::S3::Model;

using internal::ConnectRetryStrategy;
using internal::ErrorToStatus;
using internal::FromAwsDatetime;
using internal::FromAwsString;
using internal::IsAlreadyExists;
using internal::IsNotFound;
using internal::OutcomeToResult;
using internal::OutcomeToStatus;
using internal::ToAwsString;
using internal::ToURLEncodedAwsString;

const char* kS3DefaultRegion = "us-east-1";

static const char kSep = '/';

namespace {

std::mutex aws_init_lock;
Aws::SDKOptions aws_options;
std::atomic<bool> aws_initialized(false);

Status DoInitializeS3(const S3GlobalOptions& options) {
  Aws::Utils::Logging::LogLevel aws_log_level;

#define LOG_LEVEL_CASE(level_name)                             \
  case S3LogLevel::level_name:                                 \
    aws_log_level = Aws::Utils::Logging::LogLevel::level_name; \
    break;

  switch (options.log_level) {
    LOG_LEVEL_CASE(Fatal)
    LOG_LEVEL_CASE(Error)
    LOG_LEVEL_CASE(Warn)
    LOG_LEVEL_CASE(Info)
    LOG_LEVEL_CASE(Debug)
    LOG_LEVEL_CASE(Trace)
    default:
      aws_log_level = Aws::Utils::Logging::LogLevel::Off;
  }

#undef LOG_LEVEL_CASE

  aws_options.loggingOptions.logLevel = aws_log_level;
  // By default the AWS SDK logs to files, log to console instead
  aws_options.loggingOptions.logger_create_fn = [] {
    return std::make_shared<Aws::Utils::Logging::ConsoleLogSystem>(
        aws_options.loggingOptions.logLevel);
  };
  Aws::InitAPI(aws_options);
  aws_initialized.store(true);
  return Status::OK();
}

}  // namespace

Status InitializeS3(const S3GlobalOptions& options) {
  std::lock_guard<std::mutex> lock(aws_init_lock);
  return DoInitializeS3(options);
}

Status FinalizeS3() {
  std::lock_guard<std::mutex> lock(aws_init_lock);
  Aws::ShutdownAPI(aws_options);
  aws_initialized.store(false);
  return Status::OK();
}

Status EnsureS3Initialized() {
  std::lock_guard<std::mutex> lock(aws_init_lock);
  if (!aws_initialized.load()) {
    S3GlobalOptions options{S3LogLevel::Fatal};
    return DoInitializeS3(options);
  }
  return Status::OK();
}

void S3Options::ConfigureDefaultCredentials() {
  credentials_provider =
      std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

void S3Options::ConfigureAnonymousCredentials() {
  credentials_provider = std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>();
}

void S3Options::ConfigureAccessKey(const std::string& access_key,
                                   const std::string& secret_key) {
  credentials_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      ToAwsString(access_key), ToAwsString(secret_key));
}

std::string S3Options::GetAccessKey() const {
  auto credentials = credentials_provider->GetAWSCredentials();
  return std::string(FromAwsString(credentials.GetAWSAccessKeyId()));
}

std::string S3Options::GetSecretKey() const {
  auto credentials = credentials_provider->GetAWSCredentials();
  return std::string(FromAwsString(credentials.GetAWSSecretKey()));
}

S3Options S3Options::Defaults() {
  S3Options options;
  options.ConfigureDefaultCredentials();
  return options;
}

S3Options S3Options::Anonymous() {
  S3Options options;
  options.ConfigureAnonymousCredentials();
  return options;
}

S3Options S3Options::FromAccessKey(const std::string& access_key,
                                   const std::string& secret_key) {
  S3Options options;
  options.ConfigureAccessKey(access_key, secret_key);
  return options;
}

Result<S3Options> S3Options::FromUri(const Uri& uri, std::string* out_path) {
  S3Options options;

  const auto bucket = uri.host();
  auto path = uri.path();
  if (bucket.empty()) {
    if (!path.empty()) {
      return Status::Invalid("Missing bucket name in S3 URI");
    }
  } else {
    if (path.empty()) {
      path = bucket;
    } else {
      if (path[0] != '/') {
        return Status::Invalid("S3 URI should absolute, not relative");
      }
      path = bucket + path;
    }
  }
  if (out_path != nullptr) {
    *out_path = std::string(internal::RemoveTrailingSlash(path));
  }

  std::unordered_map<std::string, std::string> options_map;
  ARROW_ASSIGN_OR_RAISE(const auto options_items, uri.query_items());
  for (const auto& kv : options_items) {
    options_map.emplace(kv.first, kv.second);
  }

  const auto username = uri.username();
  if (!username.empty()) {
    options.ConfigureAccessKey(username, uri.password());
  } else {
    options.ConfigureDefaultCredentials();
  }

  auto it = options_map.find("region");
  if (it != options_map.end()) {
    options.region = it->second;
  }
  it = options_map.find("scheme");
  if (it != options_map.end()) {
    options.scheme = it->second;
  }
  it = options_map.find("endpoint_override");
  if (it != options_map.end()) {
    options.endpoint_override = it->second;
  }

  return options;
}

Result<S3Options> S3Options::FromUri(const std::string& uri_string,
                                     std::string* out_path) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, out_path);
}

bool S3Options::Equals(const S3Options& other) const {
  return (region == other.region && endpoint_override == other.endpoint_override &&
          scheme == other.scheme && background_writes == other.background_writes &&
          GetAccessKey() == other.GetAccessKey() &&
          GetSecretKey() == other.GetSecretKey());
}

namespace {

Status CheckS3Initialized() {
  if (!aws_initialized.load()) {
    return Status::Invalid(
        "S3 subsystem not initialized; please call InitializeS3() "
        "before carrying out any S3-related operation");
  }
  return Status::OK();
}

// XXX Sanitize paths by removing leading slash?

struct S3Path {
  std::string full_path;
  std::string bucket;
  std::string key;
  std::vector<std::string> key_parts;

  static Result<S3Path> FromString(const std::string& s) {
    const auto src = internal::RemoveTrailingSlash(s);
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      return S3Path{std::string(src), std::string(src), "", {}};
    }
    S3Path path;
    path.full_path = std::string(src);
    path.bucket = std::string(src.substr(0, first_sep));
    path.key = std::string(src.substr(first_sep + 1));
    path.key_parts = internal::SplitAbstractPath(path.key);
    RETURN_NOT_OK(Validate(&path));
    return path;
  }

  static Status Validate(const S3Path* path) {
    auto result = internal::ValidateAbstractPathParts(path->key_parts);
    if (!result.ok()) {
      return Status::Invalid(result.message(), " in path ", path->full_path);
    } else {
      return result;
    }
  }

  Aws::String ToURLEncodedAwsString() const {
    // URL-encode individual parts, not the '/' separator
    Aws::String res;
    res += internal::ToURLEncodedAwsString(bucket);
    for (const auto& part : key_parts) {
      res += kSep;
      res += internal::ToURLEncodedAwsString(part);
    }
    return res;
  }

  S3Path parent() const {
    DCHECK(!key_parts.empty());
    auto parent = S3Path{"", bucket, "", key_parts};
    parent.key_parts.pop_back();
    parent.key = internal::JoinAbstractPath(parent.key_parts);
    parent.full_path = parent.bucket + kSep + parent.key;
    return parent;
  }

  bool has_parent() const { return !key.empty(); }

  bool empty() const { return bucket.empty() && key.empty(); }

  bool operator==(const S3Path& other) const {
    return bucket == other.bucket && key == other.key;
  }
};

// XXX return in OutcomeToStatus instead?
Status PathNotFound(const S3Path& path) {
  return ::arrow::fs::internal::PathNotFound(path.full_path);
}

Status PathNotFound(const std::string& bucket, const std::string& key) {
  return ::arrow::fs::internal::PathNotFound(bucket + kSep + key);
}

Status NotAFile(const S3Path& path) {
  return ::arrow::fs::internal::NotAFile(path.full_path);
}

Status ValidateFilePath(const S3Path& path) {
  if (path.bucket.empty() || path.key.empty()) {
    return NotAFile(path);
  }
  return Status::OK();
}

std::string FormatRange(int64_t start, int64_t length) {
  // Format a HTTP range header value
  std::stringstream ss;
  ss << "bytes=" << start << "-" << start + length - 1;
  return ss.str();
}

// A non-copying iostream.
// See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
 public:
  StringViewStream(const void* data, int64_t nbytes)
      : Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
            static_cast<size_t>(nbytes)),
        std::iostream(this) {}
};

// By default, the AWS SDK reads object data into an auto-growing StringStream.
// To avoid copies, read directly into our preallocated buffer instead.
// See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
// functionally similar recipe.
Aws::IOStreamFactory AwsWriteableStreamFactory(void* data, int64_t nbytes) {
  return [=]() { return new StringViewStream(data, nbytes); };
}

Result<S3Model::GetObjectResult> GetObjectRange(Aws::S3::S3Client* client,
                                                const S3Path& path, int64_t start,
                                                int64_t length, void* out) {
  S3Model::GetObjectRequest req;
  req.SetBucket(ToAwsString(path.bucket));
  req.SetKey(ToAwsString(path.key));
  req.SetRange(ToAwsString(FormatRange(start, length)));
  req.SetResponseStreamFactory(AwsWriteableStreamFactory(out, length));
  return OutcomeToResult(client->GetObject(req));
}

// A RandomAccessFile that reads from a S3 object
class ObjectInputFile : public io::RandomAccessFile {
 public:
  ObjectInputFile(Aws::S3::S3Client* client, const S3Path& path)
      : client_(client), path_(path) {}

  ObjectInputFile(Aws::S3::S3Client* client, const S3Path& path, int64_t size)
      : client_(client), path_(path), content_length_(size) {}

  Status Init() {
    // Issue a HEAD Object to get the content-length and ensure any
    // errors (e.g. file not found) don't wait until the first Read() call.
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }

    S3Model::HeadObjectRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));

    auto outcome = client_->HeadObject(req);
    if (!outcome.IsSuccess()) {
      if (IsNotFound(outcome.GetError())) {
        return PathNotFound(path_);
      } else {
        return ErrorToStatus(
            std::forward_as_tuple("When reading information for key '", path_.key,
                                  "' in bucket '", path_.bucket, "': "),
            outcome.GetError());
      }
    }
    content_length_ = outcome.GetResult().GetContentLength();
    DCHECK_GE(content_length_, 0);
    return Status::OK();
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return Status::OK();
  }

  Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return Status::IOError("Cannot ", action, " past end of file");
    }
    return Status::OK();
  }

  // RandomAccessFile APIs

  Status Close() override {
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, content_length_ - position);
    if (nbytes == 0) {
      return 0;
    }

    // Read the desired range of bytes
    ARROW_ASSIGN_OR_RAISE(S3Model::GetObjectResult result,
                          GetObjectRange(client_, path_, position, nbytes, out));

    auto& stream = result.GetBody();
    stream.ignore(nbytes);
    // NOTE: the stream is a stringstream by default, there is no actual error
    // to check for.  However, stream.fail() may return true if EOF is reached.
    return stream.gcount();
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(nbytes));
    if (nbytes > 0) {
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            ReadAt(position, nbytes, buf->mutable_data()));
      DCHECK_LE(bytes_read, nbytes);
      RETURN_NOT_OK(buf->Resize(bytes_read));
    }
    return std::move(buf);
  }

  Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

 protected:
  Aws::S3::S3Client* client_;
  S3Path path_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
};

// Minimum size for each part of a multipart upload, except for the last part.
// AWS doc says "5 MB" but it's not clear whether those are MB or MiB,
// so I chose the safer value.
// (see https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html)
static constexpr int64_t kMinimumPartUpload = 5 * 1024 * 1024;

// An OutputStream that writes to a S3 object
class ObjectOutputStream : public io::OutputStream {
 protected:
  struct UploadState;

 public:
  ObjectOutputStream(Aws::S3::S3Client* client, const S3Path& path,
                     const S3Options& options)
      : client_(client), path_(path), options_(options) {}

  ~ObjectOutputStream() override {
    // For compliance with the rest of the IO stack, Close rather than Abort,
    // even though it may be more expensive.
    io::internal::CloseFromDestructor(this);
  }

  Status Init() {
    // Initiate the multi-part upload
    S3Model::CreateMultipartUploadRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));

    auto outcome = client_->CreateMultipartUpload(req);
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(
          std::forward_as_tuple("When initiating multiple part upload for key '",
                                path_.key, "' in bucket '", path_.bucket, "': "),
          outcome.GetError());
    }
    upload_id_ = outcome.GetResult().GetUploadId();
    upload_state_ = std::make_shared<UploadState>();
    closed_ = false;
    return Status::OK();
  }

  Status Abort() override {
    if (closed_) {
      return Status::OK();
    }

    S3Model::AbortMultipartUploadRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));
    req.SetUploadId(upload_id_);

    auto outcome = client_->AbortMultipartUpload(req);
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(
          std::forward_as_tuple("When aborting multiple part upload for key '", path_.key,
                                "' in bucket '", path_.bucket, "': "),
          outcome.GetError());
    }
    current_part_.reset();
    closed_ = true;
    return Status::OK();
  }

  // OutputStream interface

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }

    if (current_part_) {
      // Upload last part
      RETURN_NOT_OK(CommitCurrentPart());
    }

    // S3 mandates at least one part, upload an empty one if necessary
    if (part_number_ == 1) {
      RETURN_NOT_OK(UploadPart("", 0));
    }

    // Wait for in-progress uploads to finish (if async writes are enabled)
    RETURN_NOT_OK(Flush());

    // At this point, all part uploads have finished successfully
    DCHECK_GT(part_number_, 1);
    DCHECK_EQ(upload_state_->completed_parts.size(),
              static_cast<size_t>(part_number_ - 1));

    S3Model::CompletedMultipartUpload completed_upload;
    completed_upload.SetParts(upload_state_->completed_parts);
    S3Model::CompleteMultipartUploadRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));
    req.SetUploadId(upload_id_);
    req.SetMultipartUpload(std::move(completed_upload));

    auto outcome = client_->CompleteMultipartUpload(req);
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(
          std::forward_as_tuple("When completing multiple part upload for key '",
                                path_.key, "' in bucket '", path_.bucket, "': "),
          outcome.GetError());
    }

    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return pos_;
  }

  Status Write(const std::shared_ptr<Buffer>& buffer) override {
    return DoWrite(buffer->data(), buffer->size(), buffer);
  }

  Status Write(const void* data, int64_t nbytes) override {
    return DoWrite(data, nbytes);
  }

  Status DoWrite(const void* data, int64_t nbytes,
                 std::shared_ptr<Buffer> owned_buffer = nullptr) {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }

    if (!current_part_ && nbytes >= part_upload_threshold_) {
      // No current part and data large enough, upload it directly
      // (without copying if the buffer is owned)
      RETURN_NOT_OK(UploadPart(data, nbytes, owned_buffer));
      pos_ += nbytes;
      return Status::OK();
    }
    // Can't upload data on its own, need to buffer it
    if (!current_part_) {
      ARROW_ASSIGN_OR_RAISE(current_part_,
                            io::BufferOutputStream::Create(part_upload_threshold_));
      current_part_size_ = 0;
    }
    RETURN_NOT_OK(current_part_->Write(data, nbytes));
    pos_ += nbytes;
    current_part_size_ += nbytes;

    if (current_part_size_ >= part_upload_threshold_) {
      // Current part large enough, upload it
      RETURN_NOT_OK(CommitCurrentPart());
    }

    return Status::OK();
  }

  Status Flush() override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    // Wait for background writes to finish
    std::unique_lock<std::mutex> lock(upload_state_->mutex);
    upload_state_->cv.wait(lock,
                           [this]() { return upload_state_->parts_in_progress == 0; });
    return upload_state_->status;
  }

  // Upload-related helpers

  Status CommitCurrentPart() {
    ARROW_ASSIGN_OR_RAISE(auto buf, current_part_->Finish());
    current_part_.reset();
    current_part_size_ = 0;
    return UploadPart(buf);
  }

  Status UploadPart(std::shared_ptr<Buffer> buffer) {
    return UploadPart(buffer->data(), buffer->size(), buffer);
  }

  Status UploadPart(const void* data, int64_t nbytes,
                    std::shared_ptr<Buffer> owned_buffer = nullptr) {
    S3Model::UploadPartRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));
    req.SetUploadId(upload_id_);
    req.SetPartNumber(part_number_);
    req.SetContentLength(nbytes);

    if (!options_.background_writes) {
      req.SetBody(std::make_shared<StringViewStream>(data, nbytes));
      auto outcome = client_->UploadPart(req);
      if (!outcome.IsSuccess()) {
        return UploadPartError(req, outcome);
      } else {
        AddCompletedPart(upload_state_, part_number_, outcome.GetResult());
      }
    } else {
      std::unique_lock<std::mutex> lock(upload_state_->mutex);
      auto state = upload_state_;  // Keep upload state alive in closure
      auto part_number = part_number_;

      // If the data isn't owned, make an immutable copy for the lifetime of the closure
      if (owned_buffer == nullptr) {
        ARROW_ASSIGN_OR_RAISE(owned_buffer, AllocateBuffer(nbytes));
        memcpy(owned_buffer->mutable_data(), data, nbytes);
      } else {
        DCHECK_EQ(data, owned_buffer->data());
        DCHECK_EQ(nbytes, owned_buffer->size());
      }
      req.SetBody(
          std::make_shared<StringViewStream>(owned_buffer->data(), owned_buffer->size()));

      auto handler =
          [state, owned_buffer, part_number](
              const Aws::S3::S3Client*, const S3Model::UploadPartRequest& req,
              const S3Model::UploadPartOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) -> void {
        std::unique_lock<std::mutex> lock(state->mutex);
        if (!outcome.IsSuccess()) {
          state->status &= UploadPartError(req, outcome);
        } else {
          AddCompletedPart(state, part_number, outcome.GetResult());
        }
        // Notify completion, regardless of success / error status
        if (--state->parts_in_progress == 0) {
          state->cv.notify_all();
        }
      };
      ++upload_state_->parts_in_progress;
      client_->UploadPartAsync(req, handler);
    }

    ++part_number_;
    // With up to 10000 parts in an upload (S3 limit), a stream writing chunks
    // of exactly 5MB would be limited to 50GB total.  To avoid that, we bump
    // the upload threshold every 100 parts.  So the pattern is:
    // - part 1 to 99: 5MB threshold
    // - part 100 to 199: 10MB threshold
    // - part 200 to 299: 15MB threshold
    // ...
    // - part 9900 to 9999: 500MB threshold
    // So the total size limit is 2475000MB or ~2.4TB, while keeping manageable
    // chunk sizes and avoiding too much buffering in the common case of a small-ish
    // stream.  If the limit's not enough, we can revisit.
    if (part_number_ % 100 == 0) {
      part_upload_threshold_ += kMinimumPartUpload;
    }

    return Status::OK();
  }

  static void AddCompletedPart(const std::shared_ptr<UploadState>& state, int part_number,
                               const S3Model::UploadPartResult& result) {
    S3Model::CompletedPart part;
    // Append ETag and part number for this uploaded part
    // (will be needed for upload completion in Close())
    part.SetPartNumber(part_number);
    part.SetETag(result.GetETag());
    int slot = part_number - 1;
    if (state->completed_parts.size() <= static_cast<size_t>(slot)) {
      state->completed_parts.resize(slot + 1);
    }
    DCHECK(!state->completed_parts[slot].PartNumberHasBeenSet());
    state->completed_parts[slot] = std::move(part);
  }

  static Status UploadPartError(const S3Model::UploadPartRequest& req,
                                const S3Model::UploadPartOutcome& outcome) {
    return ErrorToStatus(
        std::forward_as_tuple("When uploading part for key '", req.GetKey(),
                              "' in bucket '", req.GetBucket(), "': "),
        outcome.GetError());
  }

 protected:
  Aws::S3::S3Client* client_;
  S3Path path_;
  const S3Options& options_;
  Aws::String upload_id_;
  bool closed_ = true;
  int64_t pos_ = 0;
  int32_t part_number_ = 1;
  std::shared_ptr<io::BufferOutputStream> current_part_;
  int64_t current_part_size_ = 0;
  int64_t part_upload_threshold_ = kMinimumPartUpload;

  // This struct is kept alive through background writes to avoid problems
  // in the completion handler.
  struct UploadState {
    std::mutex mutex;
    std::condition_variable cv;
    Aws::Vector<S3Model::CompletedPart> completed_parts;
    int64_t parts_in_progress = 0;
    Status status;

    UploadState() : status(Status::OK()) {}
  };
  std::shared_ptr<UploadState> upload_state_;
};

// This function assumes info->path() is already set
void FileObjectToInfo(const S3Model::HeadObjectResult& obj, FileInfo* info) {
  info->set_type(FileType::File);
  info->set_size(static_cast<int64_t>(obj.GetContentLength()));
  info->set_mtime(FromAwsDatetime(obj.GetLastModified()));
}

void FileObjectToInfo(const S3Model::Object& obj, FileInfo* info) {
  info->set_type(FileType::File);
  info->set_size(static_cast<int64_t>(obj.GetSize()));
  info->set_mtime(FromAwsDatetime(obj.GetLastModified()));
}

}  // namespace

class S3FileSystem::Impl {
 public:
  S3Options options_;
  Aws::Client::ClientConfiguration client_config_;
  Aws::Auth::AWSCredentials credentials_;
  std::unique_ptr<Aws::S3::S3Client> client_;

  const int32_t kListObjectsMaxKeys = 1000;
  // At most 1000 keys per multiple-delete request
  const int32_t kMultipleDeleteMaxKeys = 1000;
  // Limit recursing depth, since a recursion bomb can be created
  const int32_t kMaxNestingDepth = 100;

  explicit Impl(S3Options options) : options_(std::move(options)) {}

  Status Init() {
    credentials_ = options_.credentials_provider->GetAWSCredentials();
    client_config_.region = ToAwsString(options_.region);
    client_config_.endpointOverride = ToAwsString(options_.endpoint_override);
    if (options_.scheme == "http") {
      client_config_.scheme = Aws::Http::Scheme::HTTP;
    } else if (options_.scheme == "https") {
      client_config_.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      return Status::Invalid("Invalid S3 connection scheme '", options_.scheme, "'");
    }
    client_config_.retryStrategy = std::make_shared<ConnectRetryStrategy>();
    if (!internal::global_options.tls_ca_file_path.empty()) {
      client_config_.caFile = ToAwsString(internal::global_options.tls_ca_file_path);
    }
    if (!internal::global_options.tls_ca_dir_path.empty()) {
      client_config_.caPath = ToAwsString(internal::global_options.tls_ca_dir_path);
    }

    bool use_virtual_addressing = options_.endpoint_override.empty();
    client_.reset(
        new Aws::S3::S3Client(credentials_, client_config_,
                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                              use_virtual_addressing));
    return Status::OK();
  }

  S3Options options() const { return options_; }

  // Create a bucket.  Successful if bucket already exists.
  Status CreateBucket(const std::string& bucket) {
    S3Model::CreateBucketConfiguration config;
    S3Model::CreateBucketRequest req;
    config.SetLocationConstraint(
        S3Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(
            ToAwsString(options_.region)));
    req.SetBucket(ToAwsString(bucket));
    req.SetCreateBucketConfiguration(config);

    auto outcome = client_->CreateBucket(req);
    if (!outcome.IsSuccess() && !IsAlreadyExists(outcome.GetError())) {
      return ErrorToStatus(std::forward_as_tuple("When creating bucket '", bucket, "': "),
                           outcome.GetError());
    }
    return Status::OK();
  }

  // Create an object with empty contents.  Successful if object already exists.
  Status CreateEmptyObject(const std::string& bucket, const std::string& key) {
    S3Model::PutObjectRequest req;
    req.SetBucket(ToAwsString(bucket));
    req.SetKey(ToAwsString(key));
    return OutcomeToStatus(
        std::forward_as_tuple("When creating key '", key, "' in bucket '", bucket, "': "),
        client_->PutObject(req));
  }

  Status CreateEmptyDir(const std::string& bucket, const std::string& key) {
    DCHECK(!key.empty());
    return CreateEmptyObject(bucket, key + kSep);
  }

  Status DeleteObject(const std::string& bucket, const std::string& key) {
    S3Model::DeleteObjectRequest req;
    req.SetBucket(ToAwsString(bucket));
    req.SetKey(ToAwsString(key));
    return OutcomeToStatus(
        std::forward_as_tuple("When delete key '", key, "' in bucket '", bucket, "': "),
        client_->DeleteObject(req));
  }

  Status CopyObject(const S3Path& src_path, const S3Path& dest_path) {
    S3Model::CopyObjectRequest req;
    req.SetBucket(ToAwsString(dest_path.bucket));
    req.SetKey(ToAwsString(dest_path.key));
    // Copy source "Must be URL-encoded" according to AWS SDK docs.
    req.SetCopySource(src_path.ToURLEncodedAwsString());
    return OutcomeToStatus(
        std::forward_as_tuple("When copying key '", src_path.key, "' in bucket '",
                              src_path.bucket, "' to key '", dest_path.key,
                              "' in bucket '", dest_path.bucket, "': "),
        client_->CopyObject(req));
  }

  // On Minio, an empty "directory" doesn't satisfy the same API requests as
  // a non-empty "directory".  This is a Minio-specific quirk, but we need
  // to handle it for unit testing.

  Status IsEmptyDirectory(const std::string& bucket, const std::string& key, bool* out) {
    S3Model::HeadObjectRequest req;
    req.SetBucket(ToAwsString(bucket));
    req.SetKey(ToAwsString(key) + kSep);

    auto outcome = client_->HeadObject(req);
    if (outcome.IsSuccess()) {
      *out = true;
      return Status::OK();
    }
    if (IsNotFound(outcome.GetError())) {
      *out = false;
      return Status::OK();
    }
    return ErrorToStatus(std::forward_as_tuple("When reading information for key '", key,
                                               "' in bucket '", bucket, "': "),
                         outcome.GetError());
  }

  Status IsEmptyDirectory(const S3Path& path, bool* out) {
    return IsEmptyDirectory(path.bucket, path.key, out);
  }

  Status IsNonEmptyDirectory(const S3Path& path, bool* out) {
    S3Model::ListObjectsV2Request req;
    req.SetBucket(ToAwsString(path.bucket));
    req.SetPrefix(ToAwsString(path.key) + kSep);
    req.SetDelimiter(Aws::String() + kSep);
    req.SetMaxKeys(1);
    auto outcome = client_->ListObjectsV2(req);
    if (outcome.IsSuccess()) {
      *out = outcome.GetResult().GetKeyCount() > 0;
      return Status::OK();
    }
    if (IsNotFound(outcome.GetError())) {
      *out = false;
      return Status::OK();
    }
    return ErrorToStatus(
        std::forward_as_tuple("When listing objects under key '", path.key,
                              "' in bucket '", path.bucket, "': "),
        outcome.GetError());
  }

  // List objects under a given prefix, issuing continuation requests if necessary
  template <typename ResultCallable, typename ErrorCallable>
  Status ListObjectsV2(const std::string& bucket, const std::string& prefix,
                       ResultCallable&& result_callable, ErrorCallable&& error_callable) {
    S3Model::ListObjectsV2Request req;
    req.SetBucket(ToAwsString(bucket));
    if (!prefix.empty()) {
      req.SetPrefix(ToAwsString(prefix) + kSep);
    }
    req.SetDelimiter(Aws::String() + kSep);
    req.SetMaxKeys(kListObjectsMaxKeys);

    while (true) {
      auto outcome = client_->ListObjectsV2(req);
      if (!outcome.IsSuccess()) {
        return error_callable(outcome.GetError());
      }
      const auto& result = outcome.GetResult();
      RETURN_NOT_OK(result_callable(result));
      // Was the result limited by max-keys? If so, use the continuation token
      // to fetch further results.
      if (!result.GetIsTruncated()) {
        break;
      }
      DCHECK(!result.GetNextContinuationToken().empty());
      req.SetContinuationToken(result.GetNextContinuationToken());
    }
    return Status::OK();
  }

  // Recursive workhorse for GetTargetStats(FileSelector...)
  Status Walk(const FileSelector& select, const std::string& bucket,
              const std::string& key, std::vector<FileInfo>* out) {
    int32_t nesting_depth = 0;
    return Walk(select, bucket, key, nesting_depth, out);
  }

  Status Walk(const FileSelector& select, const std::string& bucket,
              const std::string& key, int32_t nesting_depth, std::vector<FileInfo>* out) {
    if (nesting_depth >= kMaxNestingDepth) {
      return Status::IOError("S3 filesystem tree exceeds maximum nesting depth (",
                             kMaxNestingDepth, ")");
    }

    bool is_empty = true;
    std::vector<std::string> child_keys;

    auto handle_results = [&](const S3Model::ListObjectsV2Result& result) -> Status {
      // Walk "files"
      for (const auto& obj : result.GetContents()) {
        is_empty = false;
        FileInfo info;
        const auto child_key = internal::RemoveTrailingSlash(FromAwsString(obj.GetKey()));
        if (child_key == util::string_view(key)) {
          // Amazon can return the "directory" key itself as part of the results, skip
          continue;
        }
        std::stringstream child_path;
        child_path << bucket << kSep << child_key;
        info.set_path(child_path.str());
        FileObjectToInfo(obj, &info);
        out->push_back(std::move(info));
      }
      // Walk "directories"
      for (const auto& prefix : result.GetCommonPrefixes()) {
        is_empty = false;
        const auto child_key =
            internal::RemoveTrailingSlash(FromAwsString(prefix.GetPrefix()));
        std::stringstream ss;
        ss << bucket << kSep << child_key;
        FileInfo info;
        info.set_path(ss.str());
        info.set_type(FileType::Directory);
        out->push_back(std::move(info));
        if (select.recursive) {
          child_keys.emplace_back(child_key);
        }
      }
      return Status::OK();
    };

    auto handle_error = [&](const AWSError<S3Errors>& error) -> Status {
      if (select.allow_not_found && IsNotFound(error)) {
        return Status::OK();
      }
      return ErrorToStatus(std::forward_as_tuple("When listing objects under key '", key,
                                                 "' in bucket '", bucket, "': "),
                           error);
    };

    RETURN_NOT_OK(
        ListObjectsV2(bucket, key, std::move(handle_results), std::move(handle_error)));

    // Recurse
    if (select.recursive && nesting_depth < select.max_recursion) {
      for (const auto& child_key : child_keys) {
        RETURN_NOT_OK(Walk(select, bucket, child_key, nesting_depth + 1, out));
      }
    }

    // If no contents were found, perhaps it's an empty "directory",
    // or perhaps it's a nonexistent entry.  Check.
    if (is_empty && !select.allow_not_found) {
      RETURN_NOT_OK(IsEmptyDirectory(bucket, key, &is_empty));
      if (!is_empty) {
        return PathNotFound(bucket, key);
      }
    }
    return Status::OK();
  }

  Status WalkForDeleteDir(const std::string& bucket, const std::string& key,
                          std::vector<std::string>* file_keys,
                          std::vector<std::string>* dir_keys) {
    int32_t nesting_depth = 0;
    return WalkForDeleteDir(bucket, key, nesting_depth, file_keys, dir_keys);
  }

  Status WalkForDeleteDir(const std::string& bucket, const std::string& key,
                          int32_t nesting_depth, std::vector<std::string>* file_keys,
                          std::vector<std::string>* dir_keys) {
    if (nesting_depth >= kMaxNestingDepth) {
      return Status::IOError("S3 filesystem tree exceeds maximum nesting depth (",
                             kMaxNestingDepth, ")");
    }

    std::vector<std::string> child_keys;

    auto handle_results = [&](const S3Model::ListObjectsV2Result& result) -> Status {
      // Walk "files"
      for (const auto& obj : result.GetContents()) {
        file_keys->emplace_back(FromAwsString(obj.GetKey()));
      }
      // Walk "directories"
      for (const auto& prefix : result.GetCommonPrefixes()) {
        auto child_key = FromAwsString(prefix.GetPrefix());
        dir_keys->emplace_back(child_key);
        child_keys.emplace_back(internal::RemoveTrailingSlash(child_key));
      }
      return Status::OK();
    };

    auto handle_error = [&](const AWSError<S3Errors>& error) -> Status {
      return ErrorToStatus(std::forward_as_tuple("When listing objects under key '", key,
                                                 "' in bucket '", bucket, "': "),
                           error);
    };

    RETURN_NOT_OK(
        ListObjectsV2(bucket, key, std::move(handle_results), std::move(handle_error)));

    // Recurse
    for (const auto& child_key : child_keys) {
      RETURN_NOT_OK(
          WalkForDeleteDir(bucket, child_key, nesting_depth + 1, file_keys, dir_keys));
    }
    return Status::OK();
  }

  // Delete multiple objects at once
  Status DeleteObjects(const std::string& bucket, const std::vector<std::string>& keys) {
    const auto chunk_size = static_cast<size_t>(kMultipleDeleteMaxKeys);
    for (size_t start = 0; start < keys.size(); start += chunk_size) {
      S3Model::DeleteObjectsRequest req;
      S3Model::Delete del;
      for (size_t i = start; i < std::min(keys.size(), chunk_size); ++i) {
        del.AddObjects(S3Model::ObjectIdentifier().WithKey(ToAwsString(keys[i])));
      }
      req.SetBucket(ToAwsString(bucket));
      req.SetDelete(std::move(del));
      auto outcome = client_->DeleteObjects(req);
      if (!outcome.IsSuccess()) {
        return ErrorToStatus(outcome.GetError());
      }
      // Also need to check per-key errors, even on successful outcome
      // See
      // https://docs.aws.amazon.com/fr_fr/AmazonS3/latest/API/multiobjectdeleteapi.html
      const auto& errors = outcome.GetResult().GetErrors();
      if (!errors.empty()) {
        std::stringstream ss;
        ss << "Got the following " << errors.size()
           << " errors when deleting objects in S3 bucket '" << bucket << "':\n";
        for (const auto& error : errors) {
          ss << "- key '" << error.GetKey() << "': " << error.GetMessage() << "\n";
        }
        return Status::IOError(ss.str());
      }
    }
    return Status::OK();
  }

  Status DeleteDirContents(const std::string& bucket, const std::string& key) {
    std::vector<std::string> file_keys;
    std::vector<std::string> dir_keys;
    RETURN_NOT_OK(WalkForDeleteDir(bucket, key, &file_keys, &dir_keys));
    if (file_keys.empty() && dir_keys.empty() && !key.empty()) {
      // No contents found, is it an empty directory?
      bool exists = false;
      RETURN_NOT_OK(IsEmptyDirectory(bucket, key, &exists));
      if (!exists) {
        return PathNotFound(bucket, key);
      }
    }
    // First delete all "files", then delete all child "directories"
    RETURN_NOT_OK(DeleteObjects(bucket, file_keys));
    // Delete directories in reverse lexicographic order, to ensure children
    // are deleted before their parents (Minio).
    std::sort(dir_keys.rbegin(), dir_keys.rend());
    return DeleteObjects(bucket, dir_keys);
  }

  Status EnsureDirectoryExists(const S3Path& path) {
    if (!path.key.empty()) {
      return CreateEmptyDir(path.bucket, path.key);
    }
    return Status::OK();
  }

  Status EnsureParentExists(const S3Path& path) {
    if (path.has_parent()) {
      return EnsureDirectoryExists(path.parent());
    }
    return Status::OK();
  }

  Status ListBuckets(std::vector<std::string>* out) {
    out->clear();
    auto outcome = client_->ListBuckets();
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(std::forward_as_tuple("When listing buckets: "),
                           outcome.GetError());
    }
    for (const auto& bucket : outcome.GetResult().GetBuckets()) {
      out->emplace_back(FromAwsString(bucket.GetName()));
    }
    return Status::OK();
  }
};

S3FileSystem::S3FileSystem(const S3Options& options) : impl_(new Impl{options}) {}

S3FileSystem::~S3FileSystem() {}

Result<std::shared_ptr<S3FileSystem>> S3FileSystem::Make(const S3Options& options) {
  RETURN_NOT_OK(CheckS3Initialized());

  std::shared_ptr<S3FileSystem> ptr(new S3FileSystem(options));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

bool S3FileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& s3fs = ::arrow::internal::checked_cast<const S3FileSystem&>(other);
  return options().Equals(s3fs.options());
}

S3Options S3FileSystem::options() const { return impl_->options(); }

Result<FileInfo> S3FileSystem::GetFileInfo(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));
  FileInfo info;
  info.set_path(s);

  if (path.empty()) {
    // It's the root path ""
    info.set_type(FileType::Directory);
    return info;
  } else if (path.key.empty()) {
    // It's a bucket
    S3Model::HeadBucketRequest req;
    req.SetBucket(ToAwsString(path.bucket));

    auto outcome = impl_->client_->HeadBucket(req);
    if (!outcome.IsSuccess()) {
      if (!IsNotFound(outcome.GetError())) {
        return ErrorToStatus(
            std::forward_as_tuple("When getting information for bucket '", path.bucket,
                                  "': "),
            outcome.GetError());
      }
      info.set_type(FileType::NotFound);
      return info;
    }
    // NOTE: S3 doesn't have a bucket modification time.  Only a creation
    // time is available, and you have to list all buckets to get it.
    info.set_type(FileType::Directory);
    return info;
  } else {
    // It's an object
    S3Model::HeadObjectRequest req;
    req.SetBucket(ToAwsString(path.bucket));
    req.SetKey(ToAwsString(path.key));

    auto outcome = impl_->client_->HeadObject(req);
    if (outcome.IsSuccess()) {
      // "File" object found
      FileObjectToInfo(outcome.GetResult(), &info);
      return info;
    }
    if (!IsNotFound(outcome.GetError())) {
      return ErrorToStatus(
          std::forward_as_tuple("When getting information for key '", path.key,
                                "' in bucket '", path.bucket, "': "),
          outcome.GetError());
    }
    // Not found => perhaps it's an empty "directory"
    bool is_dir = false;
    RETURN_NOT_OK(impl_->IsEmptyDirectory(path, &is_dir));
    if (is_dir) {
      info.set_type(FileType::Directory);
      return info;
    }
    // Not found => perhaps it's a non-empty "directory"
    RETURN_NOT_OK(impl_->IsNonEmptyDirectory(path, &is_dir));
    if (is_dir) {
      info.set_type(FileType::Directory);
    } else {
      info.set_type(FileType::NotFound);
    }
    return info;
  }
}

Result<std::vector<FileInfo>> S3FileSystem::GetFileInfo(const FileSelector& select) {
  ARROW_ASSIGN_OR_RAISE(auto base_path, S3Path::FromString(select.base_dir));

  std::vector<FileInfo> results;

  if (base_path.empty()) {
    // List all buckets
    std::vector<std::string> buckets;
    RETURN_NOT_OK(impl_->ListBuckets(&buckets));
    for (const auto& bucket : buckets) {
      FileInfo info;
      info.set_path(bucket);
      info.set_type(FileType::Directory);
      results.push_back(std::move(info));
      if (select.recursive) {
        RETURN_NOT_OK(impl_->Walk(select, bucket, "", &results));
      }
    }
    return results;
  }

  // Nominal case -> walk a single bucket
  RETURN_NOT_OK(impl_->Walk(select, base_path.bucket, base_path.key, &results));
  return results;
}

Status S3FileSystem::CreateDir(const std::string& s, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));

  if (path.key.empty()) {
    // Create bucket
    return impl_->CreateBucket(path.bucket);
  }

  // Create object
  if (recursive) {
    // Ensure bucket exists
    RETURN_NOT_OK(impl_->CreateBucket(path.bucket));
    // Ensure that all parents exist, then the directory itself
    std::string parent_key;
    for (const auto& part : path.key_parts) {
      parent_key += part;
      parent_key += kSep;
      RETURN_NOT_OK(impl_->CreateEmptyObject(path.bucket, parent_key));
    }
    return Status::OK();
  } else {
    // Check parent dir exists
    if (path.has_parent()) {
      S3Path parent_path = path.parent();
      bool exists;
      RETURN_NOT_OK(impl_->IsNonEmptyDirectory(parent_path, &exists));
      if (!exists) {
        RETURN_NOT_OK(impl_->IsEmptyDirectory(parent_path, &exists));
      }
      if (!exists) {
        return Status::IOError("Cannot create directory '", path.full_path,
                               "': parent directory does not exist");
      }
    }

    // XXX Should we check that no non-directory entry exists?
    // Minio does it for us, not sure about other S3 implementations.
    return impl_->CreateEmptyDir(path.bucket, path.key);
  }
}

Status S3FileSystem::DeleteDir(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));

  if (path.empty()) {
    return Status::NotImplemented("Cannot delete all S3 buckets");
  }
  RETURN_NOT_OK(impl_->DeleteDirContents(path.bucket, path.key));
  if (path.key.empty()) {
    // Delete bucket
    S3Model::DeleteBucketRequest req;
    req.SetBucket(ToAwsString(path.bucket));
    return OutcomeToStatus(
        std::forward_as_tuple("When deleting bucket '", path.bucket, "': "),
        impl_->client_->DeleteBucket(req));
  } else {
    // Delete "directory"
    RETURN_NOT_OK(impl_->DeleteObject(path.bucket, path.key + kSep));
    // Parent may be implicitly deleted if it became empty, recreate it
    return impl_->EnsureParentExists(path);
  }
}

Status S3FileSystem::DeleteDirContents(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));

  if (path.empty()) {
    return Status::NotImplemented("Cannot delete all S3 buckets");
  }
  RETURN_NOT_OK(impl_->DeleteDirContents(path.bucket, path.key));
  // Directory may be implicitly deleted, recreate it
  return impl_->EnsureDirectoryExists(path);
}

Status S3FileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("Cannot delete all S3 buckets");
}

Status S3FileSystem::DeleteFile(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));
  RETURN_NOT_OK(ValidateFilePath(path));

  // Check the object exists
  S3Model::HeadObjectRequest req;
  req.SetBucket(ToAwsString(path.bucket));
  req.SetKey(ToAwsString(path.key));

  auto outcome = impl_->client_->HeadObject(req);
  if (!outcome.IsSuccess()) {
    if (IsNotFound(outcome.GetError())) {
      return PathNotFound(path);
    } else {
      return ErrorToStatus(
          std::forward_as_tuple("When getting information for key '", path.key,
                                "' in bucket '", path.bucket, "': "),
          outcome.GetError());
    }
  }
  // Object found, delete it
  RETURN_NOT_OK(impl_->DeleteObject(path.bucket, path.key));
  // Parent may be implicitly deleted if it became empty, recreate it
  return impl_->EnsureParentExists(path);
}

Status S3FileSystem::Move(const std::string& src, const std::string& dest) {
  // XXX We don't implement moving directories as it would be too expensive:
  // one must copy all directory contents one by one (including object data),
  // then delete the original contents.

  ARROW_ASSIGN_OR_RAISE(auto src_path, S3Path::FromString(src));
  RETURN_NOT_OK(ValidateFilePath(src_path));
  ARROW_ASSIGN_OR_RAISE(auto dest_path, S3Path::FromString(dest));
  RETURN_NOT_OK(ValidateFilePath(dest_path));

  if (src_path == dest_path) {
    return Status::OK();
  }
  RETURN_NOT_OK(impl_->CopyObject(src_path, dest_path));
  RETURN_NOT_OK(impl_->DeleteObject(src_path.bucket, src_path.key));
  // Source parent may be implicitly deleted if it became empty, recreate it
  return impl_->EnsureParentExists(src_path);
}

Status S3FileSystem::CopyFile(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto src_path, S3Path::FromString(src));
  RETURN_NOT_OK(ValidateFilePath(src_path));
  ARROW_ASSIGN_OR_RAISE(auto dest_path, S3Path::FromString(dest));
  RETURN_NOT_OK(ValidateFilePath(dest_path));

  if (src_path == dest_path) {
    return Status::OK();
  }
  return impl_->CopyObject(src_path, dest_path);
}

Result<std::shared_ptr<io::InputStream>> S3FileSystem::OpenInputStream(
    const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<io::InputStream>> S3FileSystem::OpenInputStream(
    const FileInfo& info) {
  if (info.type() == FileType::NotFound) {
    return ::arrow::fs::internal::PathNotFound(info.path());
  }
  if (info.type() != FileType::File && info.type() != FileType::Unknown) {
    return ::arrow::fs::internal::NotAFile(info.path());
  }

  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(info.path()));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path, info.size());
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<io::RandomAccessFile>> S3FileSystem::OpenInputFile(
    const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<io::RandomAccessFile>> S3FileSystem::OpenInputFile(
    const FileInfo& info) {
  if (info.type() == FileType::NotFound) {
    return ::arrow::fs::internal::PathNotFound(info.path());
  }
  if (info.type() != FileType::File && info.type() != FileType::Unknown) {
    return ::arrow::fs::internal::NotAFile(info.path());
  }

  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(info.path()));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path, info.size());
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<io::OutputStream>> S3FileSystem::OpenOutputStream(
    const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, S3Path::FromString(s));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr =
      std::make_shared<ObjectOutputStream>(impl_->client_.get(), path, impl_->options_);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<io::OutputStream>> S3FileSystem::OpenAppendStream(
    const std::string& path) {
  // XXX Investigate UploadPartCopy? Does it work with source == destination?
  // https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
  // (but would need to fall back to GET if the current data is < 5 MB)
  return Status::NotImplemented("It is not possible to append efficiently to S3 objects");
}

}  // namespace fs
}  // namespace arrow
